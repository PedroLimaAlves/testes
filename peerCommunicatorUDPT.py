from socket import *
from constMPT import *
import threading
import random
import time
import pickle
from requests import get
import heapq # Para fila de prioridade (mensagens fora de ordem)

# --- Variáveis Globais ---
handShakeCount = 0
PEERS = []
myself = -1
logical_clock = 0
# Dicionário para armazenar mensagens pendentes de ACK:
# chave: (dest_ip, dest_port, msg_id), valor: {"message": msg, "timestamp": last_sent_time, "retries": num_retries}
pending_acks = {}
# Fila de mensagens recebidas fora de ordem, esperando suas dependências
# Armazenará tuplas: (logical_clock_from_message, (sender_id, message_number))
unordered_received_messages = []
received_messages_log = [] # O log final de mensagens recebidas em ordem
expected_logical_clock_from_peer = {} # Para cada peer, o relógio lógico esperado para a próxima mensagem


# --- Sockets ---
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

# --- Locks ---
clock_lock = threading.Lock()
pending_acks_lock = threading.Lock()
unordered_messages_lock = threading.Lock()
log_list_lock = threading.Lock() # Para proteger o logList na classe MsgHandler


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
    clientSock.close()
    return pickle.loads(msg)

def generate_message_id():
    # Um ID de mensagem simples, pode ser incrementado ou usar UUID
    return time.time_ns() % 1000000 # exemplo simples, use algo mais robusto em produção

def increment_logical_clock(received_clock=None):
    global logical_clock
    with clock_lock:
        if received_clock is not None:
            logical_clock = max(logical_clock, received_clock) + 1
        else:
            logical_clock += 1
        return logical_clock

# --- Threads ---

class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        print('Handler is ready. Waiting for the handshakes...')

        global handShakeCount
        global logical_clock
        global PEERS # Necessário para inicializar expected_logical_clock_from_peer
        global unordered_received_messages
        global received_messages_log

        # Inicializa expected_logical_clock_from_peer para todos os peers
        for peer_ip in PEERS:
            # PEERS é uma lista de IPs, precisamos do ID do peer
            # Isso é um ponto fraco do design original, pois o IP não é um ID de peer direto
            # Assumindo que o ID do peer é o índice na lista PEERS para simplificação neste exemplo
            # Em um sistema real, você teria um mapeamento de IP para ID ou um ID único no registro
            # Por enquanto, usaremos o IP como uma chave para o dicionário
            if peer_ip != get_public_ip(): # Não esperamos mensagens de nós mesmos
                expected_logical_clock_from_peer[peer_ip] = 0

        # Wait until handshakes are received from all other processes
        while handShakeCount < N - 1: # N-1 porque não esperamos handshake de nós mesmos
            try:
                msgPack, addr = self.sock.recvfrom(1024)
                msg = pickle.loads(msgPack)

                if msg["type"] == 'READY':
                    # Send ACK for handshake
                    ack_msg = {"type": "ACK", "msg_id": msg["msg_id"], "clock": increment_logical_clock(), "original_type": "READY"}
                    sendSocket.sendto(pickle.dumps(ack_msg), addr)
                    handShakeCount += 1
                    print(f'--- Handshake received from {addr[0]}. Total handshakes: {handShakeCount}')
                elif msg["type"] == 'ACK' and msg["original_type"] == 'READY':
                    # This peer sent a handshake and received an ACK. Don't increment handShakeCount.
                    pass
                else:
                    # Se receber uma mensagem de dados antes do handshake completo, guarda na fila de desordenados
                    # e tenta processar mais tarde.
                    print(f"Received non-handshake message from {addr[0]} during handshake phase. Buffering.")
                    with unordered_messages_lock:
                        heapq.heappush(unordered_received_messages, (msg["clock"], msg))

            except timeout:
                continue # Continua esperando handshakes

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        stopCount = 0
        while True:
            try:
                msgPack, addr = self.sock.recvfrom(32768)
                received_msg = pickle.loads(msgPack)

                # Processar ACKs
                if received_msg["type"] == "ACK":
                    # print(f"Received ACK for msg_id: {received_msg['msg_id']} from {addr[0]}")
                    with pending_acks_lock:
                        key = (addr[0], PEER_UDP_PORT, received_msg["msg_id"])
                        if key in pending_acks:
                            del pending_acks[key]
                        else:
                            print(f"Warning: Received ACK for unknown message {received_msg['msg_id']} from {addr[0]}")
                    # Atualiza o relógio lógico com base no ACK
                    increment_logical_clock(received_msg["clock"])
                    continue # Não processa ACKs como mensagens de dados

                # Processar mensagens de dados
                if received_msg["type"] == "DATA":
                    # Send ACK immediately
                    ack_msg = {"type": "ACK", "msg_id": received_msg["msg_id"], "clock": increment_logical_clock(), "original_type": "DATA"}
                    sendSocket.sendto(pickle.dumps(ack_msg), addr)

                    # Atualiza o relógio lógico com base na mensagem recebida
                    current_peer_clock = increment_logical_clock(received_msg["clock"])

                    print(f'Message {received_msg["message_number"]} from process {received_msg["sender_id"]} (Clock: {received_msg["clock"]})')

                    # Verifica se a mensagem está em ordem
                    # Para simplificar, assumimos que o sender_id é o próprio IP do remetente
                    sender_ip = received_msg["sender_ip"] # Adicionamos sender_ip ao dicionário de mensagem
                    if sender_ip not in expected_logical_clock_from_peer:
                        expected_logical_clock_from_peer[sender_ip] = received_msg["clock"] # Inicializa
                    
                    if received_msg["clock"] >= expected_logical_clock_from_peer[sender_ip]:
                        # A mensagem está em ordem ou é posterior ao esperado.
                        # Processa a mensagem e tenta processar as mensagens da fila de desordenados.
                        with log_list_lock:
                            received_messages_log.append((received_msg["sender_id"], received_msg["message_number"], received_msg["clock"]))
                        expected_logical_clock_from_peer[sender_ip] = received_msg["clock"] + 1 # Atualiza o relógio esperado para o remetente

                        # Tenta processar mensagens da fila de desordenados
                        with unordered_messages_lock:
                            while unordered_received_messages and \
                                  unordered_received_messages[0][0] <= expected_logical_clock_from_peer[unordered_received_messages[0][1]["sender_ip"]]:
                                # A próxima mensagem na fila (menor relógio) está em ordem ou pode ser processada
                                ordered_msg_tuple = heapq.heappop(unordered_received_messages)
                                ordered_msg = ordered_msg_tuple[1]
                                with log_list_lock:
                                    received_messages_log.append((ordered_msg["sender_id"], ordered_msg["message_number"], ordered_msg["clock"]))
                                expected_logical_clock_from_peer[ordered_msg["sender_ip"]] = ordered_msg["clock"] + 1
                                print(f'Processed buffered message {ordered_msg["message_number"]} from process {ordered_msg["sender_id"]}')
                    else:
                        # Mensagem fora de ordem, armazena na fila de desordenados
                        with unordered_messages_lock:
                            heapq.heappush(unordered_received_messages, (received_msg["clock"], received_msg))
                        print(f"Buffering out-of-order message {received_msg['message_number']} from {received_msg['sender_id']} (Clock: {received_msg['clock']}) Expected: {expected_logical_clock_from_peer[sender_ip]}")

                elif received_msg["type"] == "STOP":
                    stopCount += 1
                    print(f"Received STOP from {received_msg['sender_id']}. Total STOPs: {stopCount}")
                    if stopCount == N - 1: # Todos os outros peers pararam
                        break
            except timeout:
                continue # Continua esperando mensagens
            except Exception as e:
                print(f"Error in MsgHandler: {e}")
                continue

        print('Secondary Thread: All peers finished sending messages. Sending logs to comparison server.')

        # Write log file
        logFile = open('logfile'+str(myself)+'.log', 'w')
        with log_list_lock:
            logFile.writelines(str(received_messages_log))
        logFile.close()

        # Send the list of messages to the server (using a TCP socket) for comparison
        print('Sending the list of messages to the server for comparison...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        with log_list_lock:
            msgPack = pickle.dumps(received_messages_log)
        clientSock.send(msgPack)
        clientSock.close()

        # Reset the handshake counter
        global handShakeCount # Redefine para a próxima rodada, se houver
        handShakeCount = 0
        global logical_clock
        logical_clock = 0 # Redefine o relógio lógico
        global pending_acks
        pending_acks = {} # Limpa ACKs pendentes
        global unordered_received_messages
        unordered_received_messages = [] # Limpa mensagens fora de ordem
        global received_messages_log
        received_messages_log = [] # Limpa o log
        global expected_logical_clock_from_peer
        expected_logical_clock_from_peer = {} # Limpa o relógio esperado

        exit(0) # Termina a thread

class AcknowledgeMonitor(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            time.sleep(ACK_TIMEOUT) # Verifica a cada ACK_TIMEOUT
            with pending_acks_lock:
                current_time = time.time()
                keys_to_remove = []
                for key, data in pending_acks.items():
                    if current_time - data["timestamp"] > ACK_TIMEOUT:
                        if data["retries"] < MAX_RETRIES:
                            print(f"Retransmitting message {key[2]} to {key[0]} (retry {data['retries'] + 1})")
                            sendSocket.sendto(pickle.dumps(data["message"]), (key[0], key[1]))
                            data["timestamp"] = current_time # Atualiza o timestamp
                            data["retries"] += 1
                        else:
                            print(f"Failed to send message {key[2]} to {key[0]} after {MAX_RETRIES} retries. Giving up.")
                            keys_to_remove.append(key)
                for key in keys_to_remove:
                    del pending_acks[key]


# --- Funções Principais ---

# Function to wait for start signal from comparison server:
def waitToStart():
    global myself
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    myself = msg[0] # Meu ID
    nMsgs = msg[1] # Número de mensagens a serem enviadas
    conn.send(pickle.dumps('Peer process '+str(myself)+' started.'))
    conn.close()
    return (myself,nMsgs)

# From here, code is executed when program starts:
registerWithGroupManager()

# Inicia o monitor de ACKs
ack_monitor = AcknowledgeMonitor()
ack_monitor.daemon = True # Permite que a thread termine com o programa principal
ack_monitor.start()

while True:
    print('Waiting for signal to start...')
    (myself, nMsgs) = waitToStart()
    print('I am up, and my ID is: ', str(myself))

    if nMsgs == 0:
        print('Terminating.')
        exit(0)

    # Reset handshakes and clocks for a new round
    handShakeCount = 0
    logical_clock = 0
    pending_acks.clear()
    unordered_received_messages.clear()
    received_messages_log.clear()
    expected_logical_clock_from_peer.clear()

    # Create receiving message handler
    msgHandler = MsgHandler(recvSocket)
    msgHandler.daemon = True # Permite que a thread termine com o programa principal
    msgHandler.start()
    print('Handler started')

    PEERS = getListOfPeers()
    # Remove o próprio IP da lista de peers para comunicação
    my_ip = get_public_ip()
    PEERS = [peer_ip for peer_ip in PEERS if peer_ip != my_ip]

    # Send handshakes
    for addrToSend in PEERS:
        print(f'Sending handshake to {addrToSend}')
        msg_id = generate_message_id()
        current_clock = increment_logical_clock()
        msg_to_send = {"type": "READY", "msg_id": msg_id, "clock": current_clock, "sender_id": myself, "sender_ip": my_ip}
        msgPack = pickle.dumps(msg_to_send)

        with pending_acks_lock:
            pending_acks[(addrToSend, PEER_UDP_PORT, msg_id)] = {"message": msg_to_send, "timestamp": time.time(), "retries": 0}
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    # Wait for all handshakes to be acknowledged (or timeout)
    # A MsgHandler já está contando os handshakes recebidos.
    # Esta thread principal espera que a MsgHandler finalize o "handshake phase".
    # Uma maneira mais robusta seria usar um Condition Variable ou Event
    # para sinalizar quando todos os handshakes foram recebidos.
    # Por enquanto, usaremos a contagem direta e um pequeno sleep para evitar busy-waiting.
    while handShakeCount < N - 1:
        time.sleep(0.01) # Pequena pausa para evitar busy-waiting excessivo

    print('Main Thread: Sent all handshakes and confirmed. handShakeCount=', str(handShakeCount))

    # Send a sequence of data messages to all other processes
    for msgNumber in range(0, nMsgs):
        time.sleep(random.randrange(10,100)/1000) # Small random delay

        current_clock = increment_logical_clock()
        msg_id = generate_message_id()
        msg_to_send = {"type": "DATA", "sender_id": myself, "message_number": msgNumber, "clock": current_clock, "msg_id": msg_id, "sender_ip": my_ip}
        msgPack = pickle.dumps(msg_to_send)

        for addrToSend in PEERS:
            with pending_acks_lock:
                pending_acks[(addrToSend, PEER_UDP_PORT, msg_id)] = {"message": msg_to_send, "timestamp": time.time(), "retries": 0}
            sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
            # print(f'Sent message {msgNumber} with clock {current_clock} to {addrToSend}')

    # Wait for all pending ACKs for data messages
    # Isso garante que todas as mensagens foram enviadas com sucesso (ou esgotaram as retries)
    while True:
        with pending_acks_lock:
            if not pending_acks:
                break
        time.sleep(0.01) # Wait a bit for ACKs

    # Tell all processes that I have no more messages to send
    print("Sending STOP messages to all peers...")
    for addrToSend in PEERS:
        msg_id = generate_message_id()
        current_clock = increment_logical_clock()
        msg_to_send = {"type": "STOP", "sender_id": myself, "msg_id": msg_id, "clock": current_clock}
        msgPack = pickle.dumps(msg_to_send)
        # Não precisamos de ACK para STOP, pois é um sinal final
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    # A thread MsgHandler agora deve estar esperando por STOPs ou ter terminado
    # Espera a MsgHandler terminar de processar e enviar o log.
    msgHandler.join() # Bloqueia até a MsgHandler thread terminar
    print("Main thread: MsgHandler finished. Ready for next round.")
