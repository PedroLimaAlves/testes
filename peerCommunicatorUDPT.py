from socket import *
from constMPT import * # Supondo que 'N' pode estar aqui, mas será sobrescrito
import threading
import random
import time
import pickle
import sys
from requests import get

# --- Variáveis Globais ---
logical_clock = 0
incoming_message_queues = {}
expected_sequence_numbers = {}

handShakeCount = 0

PEERS = [] # Lista de IPs dos peers, populada por getListOfPeers()
myself = -1 # ID deste peer
nMsgs = 0   # Número de mensagens que este peer enviará

# N é inicializado aqui e será atualizado por getListOfPeers()
# Isso garante que N tenha um valor antes de ser usado, mesmo que constMPT.N não exista ou seja 0
N = 0

# --- Sockets ---
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

# --- Funções Auxiliares ---
def get_public_ip():
    ipAddr = get('https://api.ipify.org').content.decode('utf8')
    print('My public IP address is: {}'.format(ipAddr))
    return ipAddr

def registerWithGroupManager():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    ipAddr = get_public_ip()
    req = {"op":"register", "ipaddr":ipAddr, "port":PEER_UDP_PORT}
    msg = pickle.dumps(req)
    print ('Registering with group manager: ', req)
    clientSock.send(msg)
    clientSock.close()

def getListOfPeers():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    req = {"op":"list"}
    msg = pickle.dumps(req)
    print ('Getting list of peers from group manager: ', req)
    clientSock.send(msg)
    msg = clientSock.recv(2048)
    
    global PEERS, N # Declarando N como global para poder modificá-lo
    PEERS = pickle.loads(msg)
    N = len(PEERS) # *** CRUCIAL: N é definido aqui com o número real de peers ***
    
    print ('Got list of peers: ', PEERS)
    print ('Total number of peers (N): ', N) # Ajuda para depuração
    clientSock.close()
    return PEERS

def update_logical_clock(received_timestamp=None):
    global logical_clock
    if received_timestamp is not None:
        logical_clock = max(logical_clock, received_timestamp) + 1
    else:
        logical_clock += 1

def send_ack(target_ip, original_sender_id, original_msg_number):
    update_logical_clock()
    ack_msg = ('ACK', myself, original_sender_id, original_msg_number, logical_clock)
    ack_msg_pack = pickle.dumps(ack_msg)
    sendSocket.sendto(ack_msg_pack, (target_ip, PEER_UDP_PORT))

def try_deliver_messages(sender_id, logList):
    global expected_sequence_numbers, incoming_message_queues # Garantir que são globais aqui também

    if sender_id not in expected_sequence_numbers:
        expected_sequence_numbers[sender_id] = 0
    if sender_id not in incoming_message_queues:
        incoming_message_queues[sender_id] = []

    incoming_message_queues[sender_id].sort(key=lambda x: x[2])

    while incoming_message_queues[sender_id] and \
          incoming_message_queues[sender_id][0][2] == expected_sequence_numbers[sender_id]:
        
        msg_to_deliver = incoming_message_queues[sender_id].pop(0)
        
        print('Message ' + str(msg_to_deliver[2]) + ' from process ' + str(msg_to_deliver[1]) + \
              ' at logical time ' + str(msg_to_deliver[3]))
        logList.append(msg_to_deliver)
        expected_sequence_numbers[sender_id] += 1
    
# --- Thread para Tratar Mensagens ---
class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock
        self.logList = []

    def run(self):
        print('Handler is ready. Waiting for the handshakes...')
        
        global handShakeCount, N # N já é global e deve ter sido definido no main thread
        
        while handShakeCount < N: # Aguarda handshakes de N peers
            try:
                msgPack, sender_addr_port = self.sock.recvfrom(1024)
                msg = pickle.loads(msgPack)
                # sender_ip = sender_addr_port[0] # Não usado diretamente aqui, mas útil para depuração

                if msg[0] == 'READY':
                    handShakeCount += 1
                    print('--- Handshake received from: ', msg[1])
                    # Inicializa a fila e o número esperado para este novo peer
                    if msg[1] not in expected_sequence_numbers:
                        expected_sequence_numbers[msg[1]] = 0
                    if msg[1] not in incoming_message_queues:
                        incoming_message_queues[msg[1]] = []
            except Exception as e:
                print(f"Error receiving handshake: {e}")

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')
        
        stopCount = 0 
        while True:            
            try:
                msgPack, sender_addr_port = self.sock.recvfrom(1024)
                msg = pickle.loads(msgPack)
                sender_ip = sender_addr_port[0]

                if msg[0] in ['DATA', 'ACK']:
                    update_logical_clock(msg[3])
                else:
                    update_logical_clock()

                if msg[0] == -1: # Mensagem de parada
                    stopCount += 1
                    if stopCount == N - 1: # Todos os OUTROS processos enviaram parada (N-1)
                        # Se você quer que o próprio processo também envie, então seria 'N'
                        # Mas a lógica comum é esperar N-1 mensagens de parada dos outros
                        break
                elif msg[0] == 'DATA':
                    sender_id = msg[1]
                    
                    if sender_id not in incoming_message_queues:
                        incoming_message_queues[sender_id] = []
                    incoming_message_queues[sender_id].append(msg)
                    
                    send_ack(sender_ip, sender_id, msg[2])

                    try_deliver_messages(sender_id, self.logList)
                elif msg[0] == 'ACK':
                    pass

            except Exception as e:
                print(f"Error in MsgHandler receiving loop: {e}")

        # --- Encerramento da Rodada ---
        logFile = open('logfile'+str(myself)+'.log', 'w')
        logFile.writelines(str(self.logList))
        logFile.close()
        
        print('Sending the list of messages to the server for comparison...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        msgPack = pickle.dumps(self.logList)
        clientSock.send(msgPack)
        clientSock.close()
        
        handShakeCount = 0 # Reset para uma potencial nova rodada (embora sys.exit(0) termine o processo)
        sys.exit(0) # Termina o processo do peer após uma rodada

# --- Loop Principal do Peer ---
registerWithGroupManager()
while 1: # Loop para múltiplas rodadas (se o servidor assim desejar)
    print('Waiting for signal to start...')
    (myself, nMsgs) = waitToStart() # Recebe ID e número de mensagens do servidor de comparação
    print('I am up, and my ID is: ', str(myself))
    
    if nMsgs == 0: # Sinal para terminar o peer
        print('Terminating.')
        sendSocket.close()
        recvSocket.close()
        serverSock.close()
        sys.exit(0)

    # *** MUDANÇA CRUCIAL AQUI: Obtenha a lista de peers E DEFINA N antes de inicializar as filas ***
    PEERS = getListOfPeers() # Esta chamada AGORA define o N global corretamente (len(PEERS))
    
    # Inicializa as filas e sequências esperadas para a nova rodada
    # N já tem o valor correto aqui, obtido de getListOfPeers()
    expected_sequence_numbers = {i: 0 for i in range(N) if i != myself} 
    incoming_message_queues = {i: [] for i in range(N) if i != myself}

    # Cria e inicia a thread para lidar com as mensagens recebidas
    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()
    print('Handler started')
    
    # --- Fase de Handshake ---
    # Envia handshakes para todos os outros peers
    for addrToSend in PEERS:
        if addrToSend != get_public_ip(): # Não envia handshake para si mesmo
            print('Sending handshake to ', addrToSend)
            msg = ('READY', myself)
            msgPack = pickle.dumps(msg)
            sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))

    print('Main Thread: Sent all handshakes. handShakeCount=', str(handShakeCount))
    
    # Aguarda que a thread MsgHandler receba todos os handshakes dos outros peers
    # N já está definido corretamente aqui
    while (handShakeCount < N):
        pass # Loop de espera ocupada (spin-wait). Em sistemas de produção, threading.Event é melhor.
    
    # --- Fase de Envio de Mensagens de Dados ---
    print(f'Main Thread: All handshakes received. Starting to send {nMsgs} data messages.')
    for msgNumber in range(0, nMsgs):
        update_logical_clock()
        msg = ('DATA', myself, msgNumber, logical_clock)
        msgPack = pickle.dumps(msg)
        for addrToSend in PEERS:
            if addrToSend != get_public_ip():
                sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
                print('Sent message ' + str(msgNumber) + ' with clock ' + str(logical_clock) + ' to ' + addrToSend)
    
    # --- Fase de Sinal de Parada ---
    # Avisa os outros processos que não tem mais mensagens para enviar
    print('Main Thread: Finished sending data messages. Sending stop signals.')
    for addrToSend in PEERS:
        if addrToSend != get_public_ip():
            msg = (-1,-1) # Mensagem de parada
            msgPack = pickle.dumps(msg)
            sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))

    # O loop principal agora apenas 'passa' porque a thread MsgHandler
    # é quem decide quando o processo deve terminar (sys.exit(0)).
    pass
