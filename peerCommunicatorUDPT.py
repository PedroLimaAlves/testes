from socket import *
from constMPT import *
import threading
import random
import time
import pickle
from requests import get

# Relógio lógico de Lamport
logical_clock = 0
# ID do próprio peer
myself = -1
# Número de mensagens a serem enviadas/recebidas
nMsgs = 0
# Lista de peers registrados
PEERS = []
# Contador de handshakes recebidos
handShakeCount = 0

# Fila de mensagens para ordenação (tuplas: (timestamp, remetente, numero_mensagem))
message_queue = []
# Próximo timestamp esperado para cada peer. Inicializado com 1.
# Dicionário: {ID_peer: proximo_timestamp_esperado}
expected_timestamps = {}
# Para cada peer, o último ACK recebido
last_ack_from_peer = {}

# Sockets
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

# Lock para proteger acesso ao relógio lógico e fila de mensagens
clock_lock = threading.Lock()
queue_lock = threading.Lock()
peers_lock = threading.Lock()

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
    peers_list = pickle.loads(msg)
    print ('Got list of peers: ', peers_list)
    clientSock.close()
    return peers_list

# Atualiza o relógio lógico
def update_logical_clock(received_timestamp=0):
    global logical_clock
    with clock_lock:
        logical_clock = max(logical_clock, received_timestamp) + 1
        return logical_clock

# Envia uma mensagem UDP
def send_udp_message(destination_addr, message_type, sender_id, message_data):
    current_time = update_logical_clock()
    message = (message_type, sender_id, message_data, current_time) # (tipo, remetente, dados, timestamp)
    msgPack = pickle.dumps(message)
    sendSocket.sendto(msgPack, (destination_addr, PEER_UDP_PORT))

# Classe para lidar com mensagens recebidas
class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock
        self.logList = [] # Lista de mensagens de dados recebidas e entregues

    def run(self):
        global handShakeCount
        global N

        print('Handler is ready. Waiting for the handshakes...')

        # Aguardar handshakes de todos os outros processos
        while handShakeCount < N - 1: # N-1 pois não esperamos handshake de nós mesmos
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == 'READY': # Tipo de mensagem 'READY' para handshake
                # To do: enviar reply de handshake e esperar confirmação
                with peers_lock:
                    handShakeCount += 1
                print('--- Handshake received from peer: ', msg[1], ' (Current handshakes:', handShakeCount, ')')
                # Inicializa o timestamp esperado para este peer
                expected_timestamps[msg[1]] = 1 # O primeiro timestamp de Lamport é 1

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        stopCount = 0
        while True:
            msgPack, addr = self.sock.recvfrom(32768) # Recebe dados com endereço
            msg = pickle.loads(msgPack)
            msg_type, sender_id, message_data, received_timestamp = msg

            # 1º passo: atualiza o relógio lógico
            update_logical_clock(received_timestamp)

            # 2º passo: coloca a mensagem na fila
            with queue_lock:
                message_queue.append(msg)
                message_queue.sort(key=lambda x: x[3]) # Ordena pela timestamp de Lamport

            # 3º passo: Se é mensagem de dados, enviar ACK
            if msg_type == 'DATA':
                print(f'Message {message_data} from process {sender_id} received with timestamp {received_timestamp}. My clock: {logical_clock}')
                # Envia ACK da aplicação
                send_udp_message(addr[0], 'ACK', myself, f'ACK for {message_data}')
            elif msg_type == 'ACK':
                print(f'ACK for "{message_data}" from process {sender_id} received with timestamp {received_timestamp}. My clock: {logical_clock}')
                # Armazena o último ACK para este peer. Isso pode ser usado para depuração ou lógica mais complexa.
                last_ack_from_peer[sender_id] = received_timestamp
                # Não é necessário destravamento ou adição à logList para ACKs, eles são apenas para confirmação.
                continue # Pula para a próxima iteração do loop

            elif msg_type == 'STOP': # Mensagem de parada
                stopCount += 1
                print(f'STOP message from peer {sender_id}. Current stop count: {stopCount}/{N}')
                if stopCount == N: # N é o número total de peers (incluindo o próprio)
                    break # Para o loop quando todos os outros processos finalizaram

            # 4º passo: verifica se "destrava" a primeira mensagem da fila e entrega a mensagem para a aplicação
            # Implementação de entrega ordenada (Lamport)
            self.deliver_ordered_messages()


        # Finaliza o handler e envia logs
        print('Secondary Thread: All STOP messages received. Exiting message reception loop.')
        self.send_logs_to_server()
        handShakeCount = 0 # Reseta o contador de handshakes para um novo ciclo
        exit(0)

    def deliver_ordered_messages(self):
        global myself
        global nMsgs

        with queue_lock:
            # Continuamente tenta entregar mensagens enquanto a fila não estiver vazia
            # e a primeira mensagem na fila tem o timestamp esperado
            while message_queue and \
                  message_queue[0][0] == 'DATA' and \
                  message_queue[0][3] == expected_timestamps.get(message_queue[0][1], 0): # Verifica tipo DATA e timestamp esperada

                msg_to_deliver = message_queue.pop(0)
                msg_type, sender_id, message_data, timestamp = msg_to_deliver

                print(f'Delivering message (ID: {sender_id}, Data: {message_data}) with timestamp {timestamp}.')
                self.logList.append((sender_id, message_data)) # Adiciona à logList após entrega

                # Atualiza o próximo timestamp esperado para este peer
                expected_timestamps[sender_id] += 1
            # Se a próxima mensagem na fila não for a esperada, ou não for DATA, para de entregar
            # e espera por mais mensagens (ou ACKs que podem "destravar" dependendo da lógica do ACK)

    def send_logs_to_server(self):
        # Escreve o arquivo de log
        logFile = open('logfile'+str(myself)+'.log', 'w')
        logFile.writelines(str(self.logList))
        logFile.close()

        # Envia a lista de mensagens para o servidor (usando um socket TCP) para comparação
        print('Sending the list of messages to the server for comparison...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        msgPack = pickle.dumps(self.logList)
        clientSock.send(msgPack)
        clientSock.close()

# Função para aguardar o sinal de início do servidor de comparação
def waitToStart():
    global myself
    global nMsgs
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    myself = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps('Peer process '+str(myself)+' started.'))
    conn.close()
    return (myself,nMsgs)

# Código principal executado quando o programa inicia:
registerWithGroupManager()
while 1:
    print('Waiting for signal to start...')
    (myself, nMsgs) = waitToStart()
    print('I am up, and my ID is: ', str(myself))

    if nMsgs == 0:
        print('Terminating.')
        exit(0)

    # Inicializa o relógio lógico para o próprio peer
    update_logical_clock()
    # Inicializa o timestamp esperado para cada peer, incluindo ele mesmo
    # (Embora não receba mensagens de si mesmo para ordenação, é bom ter consistência)
    for i in range(N):
        expected_timestamps[i] = 1

    # Cria e inicia o manipulador de mensagens de recebimento
    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()
    print('Handler started')

    # Obtém a lista de peers
    with peers_lock:
        PEERS = getListOfPeers()
        # Remove a própria IP da lista de peers para não enviar mensagens para si mesmo
        my_ip = get_public_ip()
        PEERS = [peer_ip for peer_ip in PEERS if peer_ip != my_ip]

    # Envia handshakes
    print('Sending handshakes...')
    # N-1 é o número de outros peers
    # loop para garantir que os handshakes sejam enviados para todos
    while handShakeCount < N - 1: # Espera até que todos os handshakes sejam recebidos pelo handler
        for addrToSend in PEERS:
            send_udp_message(addrToSend, 'READY', myself, 'Handshake')
            print(f'Sent handshake to {addrToSend}. My clock: {logical_clock}')
        time.sleep(0.1) # Pequeno atraso para evitar sobrecarga de rede

    print('Main Thread: Sent all handshakes. handShakeCount=', str(handShakeCount))

    # Envia uma sequência de mensagens de dados para todos os outros processos
    for msgNumber in range(0, nMsgs):
        # Espera um tempo aleatório entre mensagens sucessivas
        time.sleep(random.randrange(10,100)/1000)
        for addrToSend in PEERS:
            send_udp_message(addrToSend, 'DATA', myself, msgNumber)
            print(f'Sent DATA message {msgNumber} to {addrToSend}. My clock: {logical_clock}')

    # Informa a todos os processos que não tenho mais mensagens para enviar
    print('Sending STOP messages...')
    for addrToSend in PEERS:
        send_udp_message(addrToSend, 'STOP', myself, 'No more messages')
        print(f'Sent STOP message to {addrToSend}. My clock: {logical_clock}')

    # Aguarda o handler terminar antes de recomeçar o loop principal
    msgHandler.join()
    print('MsgHandler finished. Ready for next cycle.')
