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
    # Esta função faz uma requisição HTTP para obter o IP público.
    # Certifique-se de que sua instância tem acesso à internet.
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
    # Gera um ID de mensagem único usando o tempo em nanossegundos.
    # Garante que cada mensagem enviada por este peer tem um identificador único para ACKs.
    return time.time_ns() % 1000000000 # Aumentado para maior unicidade

def increment_logical_clock(received_clock=None):
    # Implementa o relógio lógico de Lamport.
    # O relógio é incrementado em 1, ou para (max(relógio_atual, relógio_recebido) + 1).
    global logical_clock
    with clock_lock: # Protege o acesso ao relógio lógico em ambientes multi-thread
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

        # Declarar variáveis globais que serão modificadas dentro desta thread
        global handShakeCount
        global logical_clock
        global PEERS
        global unordered_received_messages
        global received_messages_log
        global expected_logical_clock_from_peer

        # Inicializa expected_logical_clock_from_peer para todos os peers conhecidos
        current_peers_ips = getListOfPeers()
        my_current_ip = get_public_ip()
        for peer_ip in current_peers_ips:
            if peer_ip != my_current_ip:
                expected_logical_clock_from_peer[peer_ip] = 0

        # Espera até que todos os handshakes sejam recebidos
        while handShakeCount < N - 1: # N-1 porque não esperamos handshake de nós mesmos
            try:
                msgPack, addr = self.sock.recvfrom(1024)
                msg = pickle.loads(msgPack)

                if msg["type"] == 'READY':
                    # Envia ACK para o handshake recebido
                    ack_msg = {"type": "ACK", "msg_id": msg["msg_id"], "clock": increment_logical_clock(), "original_type": "READY"}
                    sendSocket.sendto(pickle.dumps(ack_msg), addr)
                    handShakeCount += 1
                    print(f'--- Handshake received from {addr[0]}. Total handshakes: {handShakeCount}')
                elif msg["type"] == 'ACK' and msg["original_type"] == 'READY':
                    # Este peer enviou um handshake e recebeu um ACK. Não incrementa handShakeCount aqui.
                    pass
                else:
                    # Se uma mensagem de dados for recebida antes do handshake completo, a bufferiza.
                    print(f"Received non-handshake message from {addr[0]} during handshake phase. Buffering.")
                    with unordered_messages_lock:
                        heapq.heappush(unordered_received_messages, (msg["clock"], msg))

            except timeout:
                continue
            except Exception as e:
                print(f"Error during handshake in MsgHandler: {e}")
                continue

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        stopCount = 0
        while True:
            try:
                msgPack, addr = self.sock.recvfrom(32768)
                received_msg = pickle.loads(msgPack)

                # Processar ACKs
                if received_msg["type"] == "ACK":
                    with pending_acks_lock:
                        key = (addr[0], PEER_UDP_PORT, received_msg["msg_id"])
                        if key in pending_acks:
                            del pending_acks[key] # Remove da lista de pendentes
                        else:
                            print(f"Warning: Received ACK for unknown message {received_msg['msg_id']} from {addr[0]}")
                    increment_logical_clock(received_msg["clock"]) # Atualiza relógio com base no ACK
                    continue

                # Processar mensagens de dados
                if received_msg["type"] == "DATA":
                    # Envia ACK imediatamente para o remetente
                    ack_msg = {"type": "ACK", "msg_id": received_msg["msg_id"], "clock": increment_logical_clock(), "original_type": "DATA"}
                    sendSocket.sendto(pickle.dumps(ack_msg), addr)

                    increment_logical_clock(received_msg["clock"]) # Atualiza relógio com base na mensagem de dados

                    print(f'Message {received_msg["message_number"]} from process {received_msg["sender_id"]} (Clock: {received_msg["clock"]})')

                    sender_ip = received_msg["sender_ip"]
                    if sender_ip not in expected_logical_clock_from_peer:
                        expected_logical_clock_from_peer[sender_ip] = 0
                    
                    # Verifica se a mensagem está em ordem
                    if received_msg["clock"] >= expected_logical_clock_from_peer[sender_ip]:
                        # A mensagem está em ordem ou é posterior ao esperado.
                        with log_list_lock:
                            received_messages_log.append((received_msg["sender_id"], received_msg["message_number"], received_msg["clock"]))
                        expected_logical_clock_from_peer[sender_ip] = received_msg["clock"] + 1

                        # Tenta processar mensagens da fila de desordenados que agora estão em ordem
                        with unordered_messages_lock:
                            processable_messages = []
                            remaining_messages = []
                            for clock_val, buffered_msg in unordered_received_messages:
                                buffered_sender_ip = buffered_msg["sender_ip"]
                                if buffered_sender_ip in expected_logical_clock_from_peer and \
                                   buffered_msg["clock"] >= expected_logical_clock_from_peer[buffered_sender_ip]:
                                    processable_messages.append((clock_val, buffered_msg))
                                else:
                                    remaining_messages.append((clock_val, buffered_msg))
                            
                            unordered_received_messages.clear()
                            for msg_tuple in remaining_messages:
                                heapq.heappush(unordered_received_messages, msg_tuple)

                            for clock_val, ordered_msg in sorted(processable_messages): # Processa em ordem de relógio lógico
                                with log_list_lock:
                                    received_messages_log.append((ordered_msg["sender_id"], ordered_msg["message_number"], ordered_msg["clock"]))
                                expected_logical_clock_from_peer[ordered_msg["sender_ip"]] = ordered_msg["clock"] + 1
                                print(f'Processed buffered message {ordered_msg["message_number"]} from process {ordered_msg["sender_id"]} (Clock: {ordered_msg["clock"]})')
                    else:
                        # Mensagem fora de ordem, armazena na fila de desordenados para processamento futuro
                        with unordered_messages_lock:
                            heapq.heappush(unordered_received_messages, (received_msg["clock"], received_msg))
                        print(f"Buffering out-of-order message {received_msg['message_number']} from {received_msg['sender_id']} (Clock: {received_msg['clock']}) Expected: {expected_logical_clock_from_peer.get(sender_ip, 0)}")

                elif received_msg["type"] == "STOP":
                    stopCount += 1
                    print(f"Received STOP from {received_msg['sender_id']}. Total STOPs: {stopCount}")
                    if stopCount == N - 1: # Se todos os outros peers enviaram STOP
                        break
            except timeout:
                continue
            except Exception as e:
                print(f"Error in MsgHandler main loop: {e}")
                continue

        print('Secondary Thread: All peers finished sending messages. Sending logs to comparison server.')

        # Escreve o log de mensagens recebidas em um arquivo
        logFile = open('logfile'+str(myself)+'.log', 'w')
        with log_list_lock:
            logFile.writelines(str(received_messages_log))
        logFile.close()

        # Envia o log para o servidor de comparação via TCP
        print('Sending the list of messages to the server for comparison...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        with log_list_lock:
            msgPack = pickle.dumps(received_messages_log)
        clientSock.send(msgPack)
        clientSock.close()
        
        # Reseta as variáveis globais para a próxima rodada
        # (Isso é importante caso o peer seja reutilizado sem reiniciar o processo)
        handShakeCount = 0
        logical_clock = 0
        pending_acks.clear()
        unordered_received_messages.clear()
        received_messages_log.clear()
        expected_logical_clock_from_peer.clear()

        exit(0) # Termina a thread de tratamento de mensagens

class AcknowledgeMonitor(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        # Monitora mensagens pendentes de ACK e retransmite se necessário
        while True:
            time.sleep(ACK_TIMEOUT)
            with pending_acks_lock: # Protege o dicionário de ACKs pendentes
                current_time = time.time()
                keys_to_remove = []
                for key, data in pending_acks.items():
                    if current_time - data["timestamp"] > ACK_TIMEOUT:
                        if data["retries"] < MAX_RETRIES:
                            # print(f"Retransmitting message {key[2]} to {key[0]} (retry {data['retries'] + 1})")
                            sendSocket.sendto(pickle.dumps(data["message"]), (key[0], key[1]))
                            data["timestamp"] = current_time
                            data["retries"] += 1
                        else:
                            print(f"Failed to send message {key[2]} to {key[0]} after {MAX_RETRIES} retries. Giving up.")
                            keys_to_remove.append(key)
                for key in keys_to_remove:
                    del pending_acks[key]


# --- Funções Principais ---

# Função para aguardar o sinal de início do servidor de comparação
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

# Código principal que é executado quando o programa inicia
registerWithGroupManager()

# Inicia o monitor de ACKs em uma thread separada
ack_monitor = AcknowledgeMonitor()
ack_monitor.daemon = True # Permite que a thread termine com o programa principal
ack_monitor.start()

while True:
    # **CORREÇÃO CRÍTICA AQUI:**
    # Declarar variáveis globais no início do loop para que o Python saiba que
    # estamos nos referindo às variáveis globais e não a novas variáveis locais
    # ao longo das iterações do loop.
    global handShakeCount
    global logical_clock
    global pending_acks
    global unordered_received_messages
    global received_messages_log
    global expected_logical_clock_from_peer

    print('Waiting for signal to start...')
    (myself, nMsgs) = waitToStart()
    print('I am up, and my ID is: ', str(myself))

    if nMsgs == 0:
        print('Terminating.')
        exit(0)

    # Reset das variáveis globais para uma nova rodada de comunicação
    handShakeCount = 0
    logical_clock = 0
    pending_acks.clear()
    unordered_received_messages.clear()
    received_messages_log.clear()
    expected_logical_clock_from_peer.clear()


    # Cria e inicia a thread para tratamento de mensagens recebidas
    msgHandler = MsgHandler(recvSocket)
    msgHandler.daemon = True
    msgHandler.start()
    print('Handler started')

    # Obtém a lista mais recente de peers do Group Manager
    PEERS = getListOfPeers()
    my_ip = get_public_ip()
    # Remove o próprio IP da lista para evitar enviar mensagens para si mesmo
    PEERS = [peer_ip for peer_ip in PEERS if peer_ip != my_ip]

    # Envia handshakes para todos os outros peers
    for addrToSend in PEERS:
        print(f'Sending handshake to {addrToSend}')
        msg_id = generate_message_id()
        current_clock = increment_logical_clock()
        msg_to_send = {"type": "READY", "msg_id": msg_id, "clock": current_clock, "sender_id": myself, "sender_ip": my_ip}
        msgPack = pickle.dumps(msg_to_send)

        with pending_acks_lock:
            pending_acks[(addrToSend, PEER_UDP_PORT, msg_id)] = {"message": msg_to_send, "timestamp": time.time(), "retries": 0}
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    # Espera até que todos os handshakes sejam confirmados
    while handShakeCount < N - 1:
        time.sleep(0.01) # Pequena pausa para evitar "busy-waiting"

    print('Main Thread: Sent all handshakes and confirmed. handShakeCount=', str(handShakeCount))

    # Envia uma sequência de mensagens de dados para todos os outros peers
    for msgNumber in range(0, nMsgs):
        time.sleep(random.randrange(10,100)/1000) # Pequeno atraso aleatório

        current_clock = increment_logical_clock()
        msg_id = generate_message_id()
        msg_to_send = {"type": "DATA", "sender_id": myself, "message_number": msgNumber, "clock": current_clock, "msg_id": msg_id, "sender_ip": my_ip}
        msgPack = pickle.dumps(msg_to_send)

        for addrToSend in PEERS:
            with pending_acks_lock:
                pending_acks[(addrToSend, PEER_UDP_PORT, msg_id)] = {"message": msg_to_send, "timestamp": time.time(), "retries": 0}
            sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    # Espera até que todos os ACKs para as mensagens de dados sejam recebidos (ou as retransmissões falhem)
    while True:
        with pending_acks_lock:
            if not pending_acks:
                break
        time.sleep(0.01)

    # Sinaliza a todos os peers que não há mais mensagens a serem enviadas
    print("Sending STOP messages to all peers...")
    for addrToSend in PEERS:
        msg_id = generate_message_id()
        current_clock = increment_logical_clock()
        msg_to_send = {"type": "STOP", "sender_id": myself, "msg_id": msg_id, "clock": current_clock}
        msgPack = pickle.dumps(msg_to_send)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    # Espera a thread de tratamento de mensagens finalizar sua operação
    msgHandler.join()
    print("Main thread: MsgHandler finished. Ready for next round.")
