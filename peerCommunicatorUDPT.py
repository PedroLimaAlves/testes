from socket import *
from constMPT import *
import threading
import random
import pickle
from requests import get

# Variáveis globais para relógio lógico e controle de mensagens
logical_clock = 0
message_queue = []
ack_tracker = {}
handShakeCount = 0
PEERS = []

# UDP sockets para envio e recebimento
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# TCP socket para receber start signal do comparison server
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

def get_public_ip():
    ipAddr = get('https://api.ipify.org').content.decode('utf8')
    print('My public IP address is: {}'.format(ipAddr))
    return ipAddr

def registerWithGroupManager():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    ipAddr = get_public_ip()
    req = {"op":"register", "ipaddr":ipAddr, "port":PEER_UDP_PORT}
    msg = pickle.dumps(req)
    print('Registering with group manager: ', req)
    clientSock.send(msg)
    clientSock.close()

def getListOfPeers():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    req = {"op":"list"}
    msg = pickle.dumps(req)
    print('Getting list of peers from group manager: ', req)
    clientSock.send(msg)
    msg = clientSock.recv(2048)
    peers = pickle.loads(msg)
    print('Got list of peers: ', peers)
    clientSock.close()
    return peers

lock = threading.Lock()

class MsgHandler(threading.Thread):
    def __init__(self, sock, myself, nMsgs):
        threading.Thread.__init__(self)
        self.sock = sock
        self.myself = myself
        self.nMsgs = nMsgs
        self.local_log = []

    def run(self):
        global handShakeCount, logical_clock, message_queue, ack_tracker

        print('Handler is ready. Waiting for the handshakes...')

        stopCount = 0

        while True:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)

            # Passo 1: Atualiza relógio lógico ao receber mensagem
            with lock:
                if len(msg) >= 3:
                    # Mensagem com timestamp: (sender_id, msg_number, timestamp, type)
                    # Se não tiver type, considere como dados
                    # Vamos assumir msg do tipo (sender, msgNum, timestamp, tipo) ou (sender,msgNum,timestamp)
                    sender = msg[0]
                    ts = msg[2]
                    logical_clock = max(logical_clock, ts) + 1
                else:
                    logical_clock += 1

            # Passo 2: Coloca mensagem na fila
            # Diferencia tipo mensagem - dados ou ACK ou stop
            # Para manter compatibilidade, vamos definir que msg de ACK tem 4º elemento == 'ACK'

            if msg[0] == -1:
                # Mensagem de stop
                stopCount += 1
                if stopCount == N:
                    break
                continue

            # Exemplo tipos de mensagem:
            # Dados: (sender_id, msg_num, timestamp)
            # ACK: (sender_id, msg_num, timestamp, 'ACK')

            # Se for ACK
            if len(msg) == 4 and msg[3] == 'ACK':
                # Passo 3: só atualiza relógio e fila (já feito)
                # Marca que o ACK para essa mensagem foi recebido do sender
                with lock:
                    key = (msg[0], msg[1], msg[2])
                    if key not in ack_tracker:
                        ack_tracker[key] = set()
                    ack_tracker[key].add(sender)
                # Não envia ACK para ACK
                continue
            else:
                # Mensagem de dados
                with lock:
                    key = (msg[0], msg[1], msg[2])
                    if key not in [m[0] for m in message_queue]:
                        message_queue.append( (key, msg) )
                # Passo 3: Enviar ACK com timestamp
                ack_msg = (self.myself, msg[1], logical_clock, 'ACK')
                ackPack = pickle.dumps(ack_msg)
                # Envia ACK para remetente via UDP (IP do peer é msg[0], mas isso não é o endereço, só id)
                # Então, como no seu código original você tem lista de PEERS IPs, usamos para enviar:
                # Aqui vamos procurar o IP na lista PEERS:
                with lock:
                    for addr in PEERS:
                        # Se addr == IP do sender (msg[0]), envia ACK
                        # msg[0] é IP na sua implementação
                        if addr == sender:
                            sendSocket.sendto(ackPack, (addr, PEER_UDP_PORT))
                            break

            # Passo 4: Verifica se "destrava" primeira mensagem da fila e entrega na aplicação
            self.try_deliver_messages()

        # Escreve log
        with open('logfile'+str(self.myself)+'.log','w') as logFile:
            logFile.writelines(str(self.local_log))

        # Envia log para o servidor de comparação
        print('Sending the list of messages to the server for comparison...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        msgPack = pickle.dumps(self.local_log)
        clientSock.send(msgPack)
        clientSock.close()

        handShakeCount = 0
        exit(0)

    def try_deliver_messages(self):
        global message_queue, ack_tracker, logical_clock

        delivered_any = True
        while delivered_any:
            delivered_any = False
            with lock:
                if not message_queue:
                    return
                # Ordena por timestamp e sender para garantir ordem
                message_queue.sort(key=lambda x: (x[0][2], x[0][0]))
                key, msg = message_queue[0]
                # Verifica se recebeu ACK de todos os peers para essa mensagem
                if key in ack_tracker and len(ack_tracker[key]) >= N:
                    # Entrega mensagem
                    print('Message ' + str(msg[1]) + ' from process ' + str(msg[0]))
                    self.local_log.append(msg)
                    message_queue.pop(0)
                    del ack_tracker[key]
                    delivered_any = True
                else:
                    # Ainda não pode entregar
                    return


def waitToStart():
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    myself = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps('Peer process '+str(myself)+' started.'))
    conn.close()
    return (myself,nMsgs)


registerWithGroupManager()

while True:
    print('Waiting for signal to start...')
    (myself, nMsgs) = waitToStart()
    print('I am up, and my ID is: ', str(myself))

    if nMsgs == 0:
        print('Terminating.')
        exit(0)

    # Espera para outros estarem prontos (pode ser melhorado)
    import time
    time.sleep(5)

    msgHandler = MsgHandler(recvSocket, myself, nMsgs)
    msgHandler.start()
    print('Handler started')

    PEERS = getListOfPeers()

    # Envia handshakes
    for addrToSend in PEERS:
        print('Sending handshake to ', addrToSend)
        msg = ('READY', myself)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    print('Main Thread: Sent all handshakes. handShakeCount=', str(handShakeCount))

    while handShakeCount < N:
        pass

    # Envia sequência de mensagens com timestamp e incremento do relógio lógico
    for msgNumber in range(nMsgs):
        with lock:
            logical_clock += 1
            ts = logical_clock
        msg = (myself, msgNumber, ts)
        msgPack = pickle.dumps(msg)
        for addrToSend in PEERS:
            sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
            print('Sent message ' + str(msgNumber))

    # Envia mensagens de parada para todos
    for addrToSend in PEERS:
        msg = (-1,-1)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
