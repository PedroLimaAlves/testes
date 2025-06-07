from socket import *
import threading
import random
import time
import pickle
from requests import get
from constMPT import * # Importar constMPT

# Variáveis globais para sincronização e controle
handShakeCount = 0
PEERS = []
myself = -1 # ID deste peer
nMsgs = 0 # Número de mensagens a serem enviadas

# Buffer para armazenar mensagens recebidas fora de ordem
received_messages_buffer = {}
expected_sequence_number = {} # Dicionário para armazenar o próximo número de sequência esperado por cada peer

# UDP sockets
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# TCP socket para o servidor de comparação
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

def get_public_ip():
    ipAddr = get('https://api.ipify.org').content.decode('utf8')
    print('My public IP address is: {}'.format(ipAddr))
    return ipAddr

# Função para registrar este peer com o gerenciador de grupo
def registerWithGroupManager():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    ipAddr = get_public_ip()
    req = {
        "op": "register",
        "ipaddr": ipAddr,
        "port": PEER_UDP_PORT
    }
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
    peers_list = pickle.loads(msg) # Usar um nome diferente para evitar conflito com a global PEERS
    print ('Got list of peers: ', peers_list)
    clientSock.close()
    return peers_list

class MsgHandler(threading.Thread):
    def __init__(self, sock, myself_id):
        threading.Thread.__init__(self)
        self.sock = sock
        self.myself_id = myself_id
        self.logList = [] # Lista para armazenar as mensagens ordenadas

    def run(self):
        print('Handler is ready. Waiting for the handshakes...')
        global handShakeCount
        
        # Inicializa os números de sequência esperados para cada peer
        # Pense em como você vai inicializar `N` aqui. Se `N` vem de `constMPT.py`, tudo bem.
        # Caso contrário, você pode precisar obtê-lo do `PEERS` depois de populado.
        for i in range(N):
            if i != self.myself_id:
                expected_sequence_number[i] = 0 # Espera a primeira mensagem (seq 0) de cada peer

        # Espera até que os handshakes sejam recebidos de todos os outros processos
        while handShakeCount < N: # N-1 porque não contamos a si mesmo
            try:
                msgPack, addr = self.sock.recvfrom(1024)
                msg = pickle.loads(msgPack)
                if msg[0] == 'READY':
                    peer_id = msg[1]
                    if peer_id not in expected_sequence_number:
                        expected_sequence_number[peer_id] = 0
                    handShakeCount += 1
                    print(f'--- Handshake received from peer: {peer_id}')
                    # Enviar ACK para o handshake
                    ack_msg = ('ACK_HANDSHAKE', self.myself_id)
                    sendSocket.sendto(pickle.dumps(ack_msg), (addr[0], PEER_UDP_PORT))
            except Exception as e:
                print(f"Error receiving handshake: {e}")
                time.sleep(0.1) # Pequeno atraso para evitar CPU ocupada

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        stop_counts = 0
        while stop_counts < N - 1: # N-1 porque não contamos a si mesmo
            try:
                msgPack, addr = self.sock.recvfrom(32768)
                msg_type, sender_id, seq_num, content = pickle.loads(msgPack)

                if msg_type == 'DATA':
                    print(f'Received DATA message {seq_num} from process {sender_id} (Expected: {expected_sequence_number.get(sender_id, 0)})')

                    # Envia ACK para a mensagem de dados
                    ack_msg = ('ACK_DATA', self.myself_id, sender_id, seq_num)
                    sendSocket.sendto(pickle.dumps(ack_msg), (addr[0], PEER_UDP_PORT))

                    # Processamento de mensagens fora de ordem
                    if sender_id not in received_messages_buffer:
                        received_messages_buffer[sender_id] = {}

                    received_messages_buffer[sender_id][seq_num] = (sender_id, content)

                    # Tenta entregar mensagens em ordem
                    while (sender_id in expected_sequence_number and
                           expected_sequence_number[sender_id] in received_messages_buffer[sender_id]):
                        
                        message_to_deliver = received_messages_buffer[sender_id].pop(expected_sequence_number[sender_id])
                        self.logList.append(message_to_deliver)
                        print(f'Delivered message {expected_sequence_number[sender_id]} from process {sender_id}')
                        expected_sequence_number[sender_id] += 1

                elif msg_type == 'STOP':
                    print(f'Received STOP signal from process {sender_id}')
                    stop_counts += 1

                elif msg_type == 'ACK_HANDSHAKE':
                    # Este ACK é para handshakes, não precisa de processamento aqui (é tratado na thread principal ou no laço de envio)
                    pass
                elif msg_type == 'ACK_DATA':
                    # Este ACK é para mensagens de dados, tratado pela thread principal ou no laço de envio
                    pass

            except Exception as e:
                print(f"Error receiving message: {e}")
                # Não é bom usar exit(0) em uma thread secundária, pode interromper o programa
                break # Sai do loop se houver um erro grave

        print('Secondary Thread: All peers finished sending messages.')
        
        # Envia a lista de mensagens ordenadas para o servidor de comparação
        print('Sending the list of messages to the server for comparison...')
        try:
            clientSock = socket(AF_INET, SOCK_STREAM)
            clientSock.connect((SERVER_ADDR, SERVER_PORT))
            msgPack = pickle.dumps(self.logList)
            clientSock.send(msgPack)
            clientSock.close()
            print('Log sent to server.')
        except Exception as e:
            print(f"Error sending log to comparison server: {e}")
        
        # O reset do handshakeCount deve ser feito pela thread principal
        # handShakeCount = 0 # Isso pode causar problemas se a thread principal ainda estiver usando-o.
        # exit(0) # Não use exit(0) em uma thread! A thread terminará quando a função run() terminar.

# Função para esperar o sinal de início do servidor de comparação
def waitToStart():
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    peer_id = msg[0]
    num_msgs = msg[1]
    conn.send(pickle.dumps('Peer process '+str(peer_id)+' started.'))
    conn.close()
    return (peer_id, num_msgs)

# Função para enviar mensagens com retransmissão e ACK
def send_reliable_message(target_addr, target_port, message, timeout=0.5, retries=5):
    msg_pack = pickle.dumps(message)
    for attempt in range(retries):
        try:
            sendSocket.sendto(msg_pack, (target_addr, target_port))
            # print(f"Sent {message[0]} {message[2] if len(message) > 2 else ''} to {target_addr}:{target_port} (Attempt {attempt+1})")
            
            # Se for uma mensagem de dados, esperamos um ACK_DATA específico
            if message[0] == 'DATA':
                sendSocket.settimeout(timeout)
                while True:
                    try:
                        ack_pack, ack_addr = recvSocket.recvfrom(1024)
                        ack_msg_type, ack_sender_id, original_sender_id, ack_seq_num = pickle.loads(ack_pack)
                        if ack_msg_type == 'ACK_DATA' and original_sender_id == myself and ack_seq_num == message[2]:
                            # print(f"Received ACK for DATA {ack_seq_num} from {ack_sender_id}")
                            return True
                    except timeout:
                        # print(f"Timeout waiting for ACK for DATA {message[2]} from {target_addr}:{target_port}. Retrying...")
                        break # Sai do loop interno para tentar retransmitir
                    except Exception as e:
                        print(f"Error receiving ACK for DATA: {e}")
                        break
            elif message[0] == 'READY':
                sendSocket.settimeout(timeout)
                try:
                    ack_pack, ack_addr = recvSocket.recvfrom(1024)
                    ack_msg_type, ack_sender_id = pickle.loads(ack_pack)
                    if ack_msg_type == 'ACK_HANDSHAKE' and ack_sender_id == message[1]:
                        # print(f"Received ACK for HANDSHAKE from {ack_sender_id}")
                        return True
                except timeout:
                    # print(f"Timeout waiting for ACK for HANDSHAKE from {target_addr}:{target_port}. Retrying...")
                    pass
                except Exception as e:
                    print(f"Error receiving ACK for HANDSHAKE: {e}")
                    pass
            else: # Para mensagens STOP, não esperamos ACK para simplificar, mas em sistemas robustos, seria necessário
                return True # Assume que STOP foi enviado
        except Exception as e:
            print(f"Error sending message to {target_addr}:{target_port}: {e}")
        time.sleep(0.1) # Pequeno atraso antes de retransmitir
    print(f"Failed to send message after {retries} attempts to {target_addr}:{target_port}")
    return False

# Código principal
registerWithGroupManager()

while True:
    print('Waiting for signal to start...')
    myself, nMsgs = waitToStart() # Atualiza as variáveis globais myself e nMsgs
    print(f'I am up, and my ID is: {myself}')

    if nMsgs == 0:
        print('Terminating.')
        exit(0)

    # Cria e inicia a thread de tratamento de mensagens
    msgHandler = MsgHandler(recvSocket, myself)
    msgHandler.start()
    print('Handler started')

    # Atualiza a lista de PEERS (global)
    PEERS = getListOfPeers()
    # Remove o próprio IP da lista de peers para enviar mensagens
    PEERS = [peer_ip for peer_ip in PEERS if peer_ip != get_public_ip()]

    # Sincronização de handshakes (garante que todos os peers estejam prontos)
    print('Main Thread: Sending handshakes...')
    handshake_acks_received = 0
    # Inicializa expected_sequence_number para os peers remotos que esperamos ACK de handshake
    # Isso é redundante se já feito no MsgHandler, mas garante que a thread principal tenha a visão completa
    for i in range(N):
        if i != myself:
            expected_sequence_number[i] = 0

    for addrToSend in PEERS:
        msg = ('READY', myself)
        if send_reliable_message(addrToSend, PEER_UDP_PORT, msg):
            handshake_acks_received += 1
            print(f"Handshake sent successfully to {addrToSend}")
        else:
            print(f"Failed to send handshake to {addrToSend}. This peer might not participate correctly.")

    print(f'Main Thread: Sent all handshakes. Waiting for all HANDSHAKE ACKs. handShakeCount (from handler)={handShakeCount}')
    
    # Espera que a thread de mensagens confirme que todos os handshakes foram recebidos
    while handShakeCount < N:
        time.sleep(0.1) # Espera ativa, pode ser melhorada com semáforos ou eventos

    print('Main Thread: All handshakes acknowledged. Starting to send data messages.')

    # Envia uma sequência de mensagens de dados para todos os outros processos
    for msgNumber in range(nMsgs):
        time.sleep(random.randrange(10, 100) / 1000) # Pequeno atraso para simular tráfego
        msg = ('DATA', myself, msgNumber, f"Message content {msgNumber}") # Adiciona tipo, ID do remetente, número de sequência, e conteúdo
        for addrToSend in PEERS:
            send_reliable_message(addrToSend, PEER_UDP_PORT, msg)
            print(f'Sent DATA message {msgNumber} to {addrToSend}')

    # Informa a todos os processos que não há mais mensagens a enviar
    for addrToSend in PEERS:
        msg = ('STOP', myself, -1, -1) # Tipo, ID do remetente, seq_num (-1 para STOP), content (-1 para STOP)
        send_reliable_message(addrToSend, PEER_UDP_PORT, msg)
        print(f'Sent STOP signal to {addrToSend}')

    # A thread principal espera a thread do handler terminar de processar
    msgHandler.join()
    print("MsgHandler thread finished.")

    # Reseta o contador de handshake para a próxima rodada
    handShakeCount = 0
    received_messages_buffer.clear()
    expected_sequence_number.clear()
