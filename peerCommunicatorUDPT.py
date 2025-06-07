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
PEERS = [] # Lista de IPs de outros peers
myself = -1 # ID deste peer (0, 1, 2, ...)
logical_clock = 0 # Relógio lógico de Lamport
# Dicionário para armazenar mensagens pendentes de ACK:
# chave: (dest_ip, dest_port, msg_id), valor: {"message": msg, "timestamp": last_sent_time, "retries": num_retries}
pending_acks = {}
# Fila de mensagens recebidas fora de ordem, esperando suas dependências.
# Armazenará tuplas: (logical_clock_from_message, sender_id, full_message_dict)
unordered_received_messages = []
# O log final de mensagens recebidas em ordem. Cada entrada será:
# (clock_da_mensagem, sender_id_da_mensagem, message_number_da_mensagem) para garantir ordenação total
received_messages_log = []
# Para cada peer, o relógio lógico esperado para a próxima mensagem daquele peer.
# Ajuda a identificar mensagens fora de ordem.
expected_logical_clock_from_peer = {}
# Conjunto para rastrear IDs de mensagens já processadas para evitar duplicatas (Idempotência)
# Armazena tuplas: (sender_id, message_number, msg_id)
processed_msg_ids = set()


# --- Sockets ---
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

# --- Locks ---
clock_lock = threading.Lock() # Protege o acesso ao relógio lógico
pending_acks_lock = threading.Lock() # Protege o dicionário de ACKs pendentes
unordered_messages_lock = threading.Lock() # Protege a fila de mensagens fora de ordem
log_list_lock = threading.Lock() # Protege o log de mensagens recebidas
processed_msg_ids_lock = threading.Lock() # Protege o conjunto de IDs de mensagens processadas


# --- Funções Auxiliares ---
def get_public_ip():
    """Obtém o endereço IP público da máquina."""
    ipAddr = get('https://api.ipify.org').content.decode('utf8')
    # print('My public IP address is: {}'.format(ipAddr)) # Descomente para debug se necessário
    return ipAddr

def registerWithGroupManager():
    """Registra este peer com o Group Manager."""
    clientSock = socket(AF_INET, SOCK_STREAM)
    print (f'[{time.time()}] Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    ipAddr = get_public_ip()
    req = {"op":"register", "ipaddr":ipAddr, "port":PEER_UDP_PORT}
    msg = pickle.dumps(req)
    print (f'[{time.time()}] Registering with group manager: ', req)
    clientSock.send(msg)
    clientSock.close()

def getListOfPeers():
    """Obtém a lista de IPs de todos os peers registrados do Group Manager."""
    clientSock = socket(AF_INET, SOCK_STREAM)
    print (f'[{time.time()}] Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    req = {"op":"list"}
    msg = pickle.dumps(req)
    print (f'[{time.time()}] Getting list of peers from group manager: ', req)
    clientSock.send(msg)
    msg = clientSock.recv(2048)
    clientSock.close()
    return pickle.loads(msg)

def generate_message_id():
    """Gera um ID de mensagem único usando o tempo em nanossegundos."""
    return time.time_ns()

def increment_logical_clock(received_clock=None):
    """
    Incrementa o relógio lógico de Lamport.
    Se um relógio recebido for fornecido, o relógio local é ajustado
    para o máximo entre o relógio local e o recebido, mais um.
    Caso contrário, é apenas incrementado em um.
    """
    global logical_clock
    with clock_lock:
        if received_clock is not None:
            logical_clock = max(logical_clock, received_clock) + 1
        else:
            logical_clock += 1
        return logical_clock

def reset_peer_state():
    """
    Reinicia o estado global do peer para uma nova rodada de comunicação.
    Isso inclui contadores, relógios, filas e logs.
    """
    global handShakeCount
    global logical_clock
    global pending_acks
    global unordered_received_messages
    global received_messages_log
    global expected_logical_clock_from_peer
    global processed_msg_ids # Também resetamos os IDs de mensagens processadas

    handShakeCount = 0
    logical_clock = 0
    pending_acks.clear()
    unordered_received_messages.clear()
    received_messages_log.clear()
    expected_logical_clock_from_peer.clear()
    processed_msg_ids.clear()
    print(f"[{time.time()}] Peer state reset for a new round.")


# --- Threads ---

class MsgHandler(threading.Thread):
    """
    Thread responsável por receber e processar mensagens UDP de outros peers.
    Lida com handshakes, mensagens de dados (incluindo ordenação e bufferização)
    e mensagens de término (STOP).
    """
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        print(f'[{time.time()}] Handler is ready. Waiting for the handshakes...')

        # Declarar variáveis globais que serão modificadas dentro desta thread.
        global handShakeCount
        global logical_clock
        global PEERS
        global unordered_received_messages
        global received_messages_log
        global expected_logical_clock_from_peer
        global processed_msg_ids

        # Inicializa expected_logical_clock_from_peer para todos os peers conhecidos.
        current_peers_ips = getListOfPeers()
        my_current_ip = get_public_ip()
        for peer_ip in current_peers_ips:
            if peer_ip != my_current_ip:
                expected_logical_clock_from_peer[peer_ip] = 0

        # Espera até que todos os handshakes sejam recebidos.
        while handShakeCount < N - 1: # N-1 porque não esperamos handshake de nós mesmos
            try:
                msgPack, addr = self.sock.recvfrom(1024)
                msg = pickle.loads(msgPack)

                if msg["type"] == 'READY':
                    # Envia ACK para o handshake recebido
                    ack_msg = {"type": "ACK", "msg_id": msg["msg_id"], "clock": increment_logical_clock(), "original_type": "READY"}
                    sendSocket.sendto(pickle.dumps(ack_msg), addr)
                    handShakeCount += 1
                    print(f'[{time.time()}] --- Handshake received from {addr[0]}. Total handshakes: {handShakeCount}')
                elif msg["type"] == 'ACK' and msg["original_type"] == 'READY':
                    # ESTE É O AJUSTE CRÍTICO:
                    # Este peer enviou um handshake e recebeu um ACK. Precisamos removê-lo de pending_acks.
                    with pending_acks_lock:
                        key = (addr[0], PEER_UDP_PORT, msg["msg_id"]) # O IP no ACK recebido é o IP do REMETENTE original (ou seja, quem enviou o READY original)
                        if key in pending_acks:
                            del pending_acks[key]
                            print(f"[{time.time()}] Peer {myself} RECEIVED and REMOVED ACK for MY READY msg ID {msg['msg_id']} from {addr[0]}. Pending ACKs count: {len(pending_acks)}")
                        else:
                            print(f"[{time.time()}] Peer {myself} WARNING: Received ACK for UNKNOWN READY msg ID {msg['msg_id']} from {addr[0]}. Already processed or never sent?")
                    increment_logical_clock(msg["clock"]) # Atualiza o relógio com base no ACK do READY
                    # Não precisamos de 'continue' aqui, apenas saímos do 'elif'
                else:
                    # Se uma mensagem de dados for recebida antes do handshake completo, a bufferiza.
                    print(f"[{time.time()}] Received non-handshake message from {addr[0]} during handshake phase. Buffering.")
                    # A tupla aqui inclui clock, sender_id e a mensagem completa para futura ordenação total
                    with unordered_messages_lock:
                        # Armazena (clock, sender_id) para ordenação no heapq, e a mensagem completa
                        heapq.heappush(unordered_received_messages, (msg["clock"], msg["sender_id"], msg))

            except timeout:
                continue
            except Exception as e:
                print(f"[{time.time()}] Error during handshake in MsgHandler: {e}")
                continue

        print(f'[{time.time()}] Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

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
                            print(f"[{time.time()}] Peer {myself} RECEIVED and REMOVED ACK for msg ID {received_msg['msg_id']} from {addr[0]} (Original: {received_msg['original_type']}). Pending ACKs count: {len(pending_acks)}") # Modificado
                        else:
                            print(f"[{time.time()}] Peer {myself} WARNING: Received ACK for UNKNOWN msg ID {received_msg['msg_id']} from {addr[0]} (Original: {received_msg['original_type']}). Already processed or never sent?") # Modificado
                    increment_logical_clock(received_msg["clock"]) # Atualiza relógio com base no ACK
                    continue

                # Processar mensagens de dados
                if received_msg["type"] == "DATA":
                    # --- Lógica de Idempotência: Verifica se a mensagem já foi processada ---
                    with processed_msg_ids_lock:
                        msg_unique_id = (received_msg["sender_id"], received_msg["message_number"], received_msg["msg_id"])
                        if msg_unique_id in processed_msg_ids:
                            print(f"[{time.time()}] Peer {myself} WARNING: Received duplicate message {received_msg['message_number']} from {received_msg['sender_id']} (ID: {received_msg['msg_id']}). Skipping processing, but sending ACK.")
                            # Ainda envia o ACK, pois a retransmissão indica que o ACK anterior pode ter sido perdido
                            ack_msg = {"type": "ACK", "msg_id": received_msg["msg_id"], "clock": increment_logical_clock(), "original_type": "DATA"}
                            sendSocket.sendto(pickle.dumps(ack_msg), addr)
                            continue # Pula o resto do processamento para esta mensagem duplicada
                    # --- Fim da Lógica de Idempotência ---

                    # Envia ACK imediatamente para o remetente
                    ack_msg = {"type": "ACK", "msg_id": received_msg["msg_id"], "clock": increment_logical_clock(), "original_type": "DATA"}
                    sendSocket.sendto(pickle.dumps(ack_msg), addr)
                    print(f"[{time.time()}] Peer {myself} SENT ACK for DATA msg {received_msg['message_number']} (ID: {received_msg['msg_id']}) to {addr[0]}.") # Adicionado

                    # Atualiza relógio lógico com base na mensagem de dados recebida
                    increment_logical_clock(received_msg["clock"])

                    print(f'[{time.time()}] Peer {myself} received DATA msg {received_msg["message_number"]} from {received_msg["sender_id"]} (IP: {received_msg["sender_ip"]}) with Clock: {received_msg["clock"]}. My current clock: {logical_clock}. Expected from {received_msg["sender_ip"]}: {expected_logical_clock_from_peer.get(received_msg["sender_ip"], "N/A")}')

                    sender_ip = received_msg["sender_ip"]
                    # Garante que o relógio esperado para este remetente esteja inicializado
                    if sender_ip not in expected_logical_clock_from_peer:
                        expected_logical_clock_from_peer[sender_ip] = 0
                    
                    # Verifica se a mensagem está em ordem causal (Lamport)
                    if received_msg["clock"] >= expected_logical_clock_from_peer[sender_ip]:
                        # A mensagem está em ordem ou é posterior ao esperado.
                        with log_list_lock:
                            # Adiciona ao log principal usando (clock, sender_id, message_number) para ordenação total
                            received_messages_log.append((received_msg["clock"], received_msg["sender_id"], received_msg["message_number"]))
                        with processed_msg_ids_lock:
                            processed_msg_ids.add(msg_unique_id) # Adiciona ao conjunto de IDs processados

                        expected_logical_clock_from_peer[sender_ip] = received_msg["clock"] + 1
                        print(f'[{time.time()}] Peer {myself} LOGGED: ({received_msg["sender_id"]}, {received_msg["message_number"]}, Clock: {received_msg["clock"]})')

                        # Tenta processar mensagens da fila de desordenados que agora estão em ordem
                        with unordered_messages_lock:
                            processable_messages = []
                            remaining_messages = []
                            # Filtra as mensagens que podem ser processadas AGORA
                            for buffered_msg_tuple in unordered_received_messages:
                                # Extrai os dados da tupla armazenada no heapq
                                buffered_clock = buffered_msg_tuple[0]
                                buffered_sender_id = buffered_msg_tuple[1]
                                buffered_msg = buffered_msg_tuple[2] # O dicionário da mensagem original
                                
                                buffered_sender_ip = buffered_msg["sender_ip"]

                                if buffered_sender_ip in expected_logical_clock_from_peer and \
                                   buffered_msg["clock"] >= expected_logical_clock_from_peer[buffered_sender_ip]:
                                    processable_messages.append((buffered_clock, buffered_sender_id, buffered_msg))
                                else:
                                    remaining_messages.append(buffered_msg_tuple) # Mantém a tupla original para o heap
                            
                            # Limpa e reconstrói a fila com as mensagens que ainda não podem ser processadas
                            unordered_received_messages.clear()
                            for msg_tuple in remaining_messages:
                                heapq.heappush(unordered_received_messages, msg_tuple)

                            # Processa as mensagens que agora estão em ordem
                            # sorted() irá ordenar primeiro pelo clock, e depois pelo sender_id (segundo elemento da tupla)
                            for clock_val, sender_id_val, ordered_msg in sorted(processable_messages):
                                with processed_msg_ids_lock:
                                    ordered_msg_unique_id = (ordered_msg["sender_id"], ordered_msg["message_number"], ordered_msg["msg_id"])
                                    if ordered_msg_unique_id in processed_msg_ids:
                                        print(f"[{time.time()}] Peer {myself} WARNING: Skipping buffered duplicate {ordered_msg['message_number']} from {ordered_msg['sender_id']} (ID: {ordered_msg['msg_id']}).")
                                        continue # Pula duplicatas da fila de buffer também

                                with log_list_lock:
                                    received_messages_log.append((ordered_msg["clock"], ordered_msg["sender_id"], ordered_msg["message_number"]))
                                with processed_msg_ids_lock:
                                    processed_msg_ids.add(ordered_msg_unique_id)

                                expected_logical_clock_from_peer[ordered_msg["sender_ip"]] = ordered_msg["clock"] + 1
                                print(f'[{time.time()}] Peer {myself} PROCESSED BUFFERED: ({ordered_msg["sender_id"]}, {ordered_msg["message_number"]}, Clock: {ordered_msg["clock"]})')
                    else:
                        # Mensagem fora de ordem, armazena na fila de desordenados para processamento futuro
                        with unordered_messages_lock:
                            # Armazena (clock, sender_id) para ordenação no heapq, e a mensagem completa
                            heapq.heappush(unordered_received_messages, (received_msg["clock"], received_msg["sender_id"], received_msg))
                        print(f"[{time.time()}] Peer {myself} BUFFERING OUT-OF-ORDER: ({received_msg['sender_id']}, {received_msg['message_number']}, Clock: {received_msg['clock']}) Expected: {expected_logical_clock_from_peer.get(sender_ip, 0)})")

                elif received_msg["type"] == "STOP":
                    stopCount += 1
                    print(f"[{time.time()}] Received STOP from {received_msg['sender_id']}. Total STOPs: {stopCount}")
                    if stopCount == N - 1: # Se todos os outros peers enviaram STOP
                        break # Sai do loop de recebimento de mensagens

            except timeout:
                continue
            except Exception as e:
                print(f"[{time.time()}] Error in MsgHandler main loop: {e}")
                continue

        print(f'[{time.time()}] Secondary Thread: All peers finished sending messages. Sending logs to comparison server.')

        # Escreve o log de mensagens recebidas em um arquivo local
        logFile = open('logfile'+str(myself)+'.log', 'w')
        with log_list_lock:
            # Ordena o log final antes de escrever para garantir que a comparação seja justa.
            # Isso é crucial se houver mensagens bufferizadas que foram processadas tardiamente.
            sorted_log = sorted(received_messages_log)
            logFile.writelines(str(sorted_log))
        logFile.close()
        print(f"[{time.time()}] Logfile logfile{myself}.log written.")

        # Envia o log para o servidor de comparação via TCP
        print(f'[{time.time()}] Sending the list of messages to the server for comparison...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        with log_list_lock:
            msgPack = pickle.dumps(sorted_log) # Envia o log já ordenado
        clientSock.send(msgPack)
        clientSock.close()
        print(f"[{time.time()}] Log sent to server.")
        
        exit(0) # Termina a thread de tratamento de mensagens

class AcknowledgeMonitor(threading.Thread):
    """
    Thread responsável por monitorar mensagens enviadas que aguardam ACK.
    Retransmite mensagens se o ACK não for recebido dentro de um tempo limite,
    até um número máximo de tentativas.
    """
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            # Não use ACK_TIMEOUT diretamente aqui para o sleep,
            # ou ele pode ser muito longo. Use um valor menor para o monitor ser ágil.
            time.sleep(0.05) # Checa a cada 50ms

            with pending_acks_lock: # Protege o dicionário de ACKs pendentes
                current_time = time.time()
                keys_to_remove = []
                # print(f"[{time.time()}] ACK Monitor (Peer {myself}): Checking {len(pending_acks)} pending ACKs.") # Descomente para ver mais logs do monitor

                for key, data in pending_acks.items():
                    if current_time - data["timestamp"] > ACK_TIMEOUT: # Usa o ACK_TIMEOUT do constMPT
                        if data["retries"] < MAX_RETRIES:
                            print(f"[{time.time()}] Peer {myself} Retransmitting msg ID {key[2]} of type {data['message']['type']} to {key[0]} (retry {data['retries'] + 1}).")
                            sendSocket.sendto(pickle.dumps(data["message"]), (key[0], key[1]))
                            data["timestamp"] = current_time
                            data["retries"] += 1
                        else:
                            print(f"[{time.time()}] Peer {myself} FAILED to send msg ID {key[2]} of type {data['message']['type']} to {key[0]} after {MAX_RETRIES} retries. Giving up. This msg will likely cause discrepancy.") # Modificado
                            keys_to_remove.append(key)
                for key in keys_to_remove:
                    del pending_acks[key]


# --- Funções Principais ---

def waitToStart():
    """
    Aguarda um sinal de início do servidor de comparação via TCP.
    Recebe o ID deste peer e o número de mensagens a serem enviadas.
    """
    global myself # 'myself' é atribuída aqui, então precisa ser global
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    myself = msg[0] # Meu ID
    nMsgs = msg[1] # Número de mensagens a serem enviadas
    conn.send(pickle.dumps('Peer process '+str(myself)+' started.'))
    conn.close()
    return (myself,nMsgs)

# --- Código Principal que é executado quando o programa inicia ---
registerWithGroupManager()

# Inicia o monitor de ACKs em uma thread separada
ack_monitor = AcknowledgeMonitor()
ack_monitor.daemon = True # Torna a thread um daemon para que ela termine quando o programa principal terminar
ack_monitor.start()

while True:
    print(f'[{time.time()}] Waiting for signal to start a new round...')
    (myself, nMsgs) = waitToStart()
    print(f'[{time.time()}] I am up, and my ID is: {myself}. Number of messages to send: {nMsgs}')

    if nMsgs == 0:
        print(f'[{time.time()}] Terminating program as nMsgs is 0.')
        exit(0)

    # Reset das variáveis globais para uma nova rodada de comunicação
    reset_peer_state()

    # Cria e inicia a thread para tratamento de mensagens recebidas
    msgHandler = MsgHandler(recvSocket)
    msgHandler.daemon = True
    msgHandler.start()
    print(f'[{time.time()}] MsgHandler thread started.')

    # Obtém a lista mais recente de peers do Group Manager
    PEERS = getListOfPeers()
    my_ip = get_public_ip()
    # Remove o próprio IP da lista para evitar enviar mensagens para si mesmo
    PEERS = [peer_ip for peer_ip in PEERS if peer_ip != my_ip]
    print(f'[{time.time()}] Current peers to communicate with: {PEERS}')

    # Envia handshakes (mensagens "READY") para todos os outros peers
    for addrToSend in PEERS:
        print(f'[{time.time()}] Sending handshake to {addrToSend}')
        msg_id = generate_message_id()
        current_clock = increment_logical_clock()
        msg_to_send = {"type": "READY", "msg_id": msg_id, "clock": current_clock, "sender_id": myself, "sender_ip": my_ip}
        msgPack = pickle.dumps(msg_to_send)

        with pending_acks_lock:
            pending_acks[(addrToSend, PEER_UDP_PORT, msg_id)] = {"message": msg_to_send, "timestamp": time.time(), "retries": 0}
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    # Espera até que todos os handshakes sejam confirmados (ACKs recebidos)
    print(f'[{time.time()}] Waiting for all handshakes to be confirmed (expected {N-1})...')
    while handShakeCount < N - 1:
        time.sleep(0.01) # Pequena pausa para evitar "busy-waiting"

    print(f'[{time.time()}] Main Thread: Sent all handshakes and confirmed. handShakeCount={handShakeCount}')

    # Envia uma sequência de mensagens de dados para todos os outros peers
    print(f'[{time.time()}] Sending {nMsgs} data messages to peers...')
    for msgNumber in range(0, nMsgs):
        # Aumentamos o atraso aleatório para simular melhor o tráfego de rede e reduzir a "pressão"
        time.sleep(random.uniform(0.1, 0.5)) # Atraso entre 100ms e 500ms

        current_clock = increment_logical_clock()
        msg_id = generate_message_id()
        msg_to_send = {"type": "DATA", "sender_id": myself, "message_number": msgNumber, "clock": current_clock, "msg_id": msg_id, "sender_ip": my_ip}
        msgPack = pickle.dumps(msg_to_send)

        for addrToSend in PEERS:
            with pending_acks_lock:
                pending_acks[(addrToSend, PEER_UDP_PORT, msg_id)] = {"message": msg_to_send, "timestamp": time.time(), "retries": 0}
            sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
        # print(f"[{time.time()}] Sent DATA msg {msgNumber} with clock {current_clock} (ID: {msg_id})") # Descomente para debug se necessário

    # Espera até que todos os ACKs para as mensagens de dados sejam recebidos (ou as retransmissões falhem)
    print(f'[{time.time()}] Waiting for all data message ACKs to be confirmed...')
    while True:
        with pending_acks_lock:
            if not pending_acks: # Se o dicionário de ACKs pendentes estiver vazio
                break
        time.sleep(0.01)

    # Sinaliza a todos os peers que este peer não tem mais mensagens a serem enviadas
    print(f"[{time.time()}] Sending STOP messages to all peers...")
    for addrToSend in PEERS:
        msg_id = generate_message_id()
        current_clock = increment_logical_clock()
        msg_to_send = {"type": "STOP", "sender_id": myself, "msg_id": msg_id, "clock": current_clock}
        msgPack = pickle.dumps(msg_to_send)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    # Espera a thread de tratamento de mensagens finalizar sua operação
    # (Ela finalizará quando receber N-1 mensagens STOP)
    print(f'[{time.time()}] Waiting for MsgHandler to finish...')
    msgHandler.join()
    print(f"[{time.time()}] Main thread: MsgHandler finished. Ready for next round.")
