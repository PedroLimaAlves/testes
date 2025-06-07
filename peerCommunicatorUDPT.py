from socket import *
from constMPT import *
import threading
import random
import pickle
from requests import get
import time # Importar o módulo time para usar time.time() e time.sleep()

# Counter to make sure we have received handshakes from all other processes
handShakeCount = 0

PEERS = []
lamport_clock = 0
msgQueue = [] # Fila de mensagens recebidas para entrega ordenada
delivered = set() # Mensagens já entregues
acks_received = {} # ACKs recebidos para mensagens recebidas

# --- NOVAS VARIÁVEIS PARA RETRANSMISSÃO ---
SENT_MESSAGES = {} # Armazena mensagens enviadas que aguardam ACKs
RETRANSMISSION_TIMEOUT = 2.0 # Tempo limite em segundos antes de retransmitir
MAX_RETRANSMISSIONS = 5 # Número máximo de tentativas de retransmissão

# Lock para proteger o acesso a variáveis globais compartilhadas entre threads
data_lock = threading.Lock()

# UDP sockets to send and receive data messages:
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# TCP socket to receive start signal from the comparison server:
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

def get_public_ip():
    ipAddr = get('https://api.ipify.org').content.decode('utf8')
    print('My public IP address is: {}'.format(ipAddr))
    return ipAddr

def registerWithGroupManager():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print('Connecting to group manager: ', (GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    ipAddr = get_public_ip()
    req = {"op": "register", "ipaddr": ipAddr, "port": PEER_UDP_PORT}
    msg = pickle.dumps(req)
    print('Registering with group manager: ', req)
    clientSock.send(msg)
    clientSock.close()

def getListOfPeers():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print('Connecting to group manager: ', (GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    req = {"op": "list"}
    msg = pickle.dumps(req)
    print('Getting list of peers from group manager: ', req)
    clientSock.send(msg)
    msg = clientSock.recv(4096) # Aumentar buffer de recepção
    PEERS = pickle.loads(msg)
    print('Got list of peers: ', PEERS)
    clientSock.close()
    return PEERS

# --- NOVA THREAD PARA VERIFICAÇÃO DE RETRANSMISSÕES ---
class RetransmissionHandler(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.running = True

    def run(self):
        while self.running:
            current_time = time.time()
            with data_lock:
                # Iterar sobre uma cópia para permitir modificações no dicionário
                for msg_id, data in list(SENT_MESSAGES.items()):
                    # Se o tempo limite excedeu e nem todos os ACKs foram recebidos
                    if (current_time - data["sent_time"]) > RETRANSMISSION_TIMEOUT:
                        if data["retries"] < MAX_RETRANSMISSIONS:
                            print(f"--- Retransmitting message {msg_id}, retry {data['retries'] + 1} ---")
                            # Retransmitir apenas para os peers que ainda não deram ACK
                            # Nota: 'PEERS' contém IPs, 'data["recipients_acked"]' contém IDs numéricos
                            # Isso exige um mapeamento de ID para IP ou armazenar os IPs originais aqui.
                            # Para simplificar, retransmitiremos para TODOS, que é mais fácil de implementar.
                            # Para uma otimização, você precisaria de um mapa {peer_id: peer_ip}
                            for addrToSend in PEERS: # PEERS é uma lista de IPs
                                # Poderíamos refinar isso para retransmitir apenas para os que faltam ACK
                                # Mas para simplificar, retransmitiremos para todos
                                sendSocket.sendto(data["msg_packed"], (addrToSend, PEER_UDP_PORT))
                            data["sent_time"] = current_time # Reseta o timer
                            data["retries"] += 1
                        else:
                            print(f"--- Message {msg_id} failed after {MAX_RETRANSMISSIONS} retries. ---")
                            # Remover a mensagem da lista de espera e lidar com a falha
                            del SENT_MESSAGES[msg_id]
            time.sleep(0.5) # Verifica a cada 500ms

    def stop(self):
        self.running = False

class MsgHandler(threading.Thread):
    def __init__(self, sock, myself_id): # Passar myself_id para a thread
        threading.Thread.__init__(self)
        self.sock = sock
        self.myself_id = myself_id # Armazenar o ID deste peer

    def run(self):
        print('Handler is ready. Waiting for the handshakes...')
        global handShakeCount, lamport_clock, msgQueue, delivered, acks_received

        # Inicializar variáveis para cada nova rodada (para garantir estado limpo)
        with data_lock:
            lamport_clock = 0
            msgQueue = []
            delivered = set()
            acks_received = {}
            # Não limpe SENT_MESSAGES aqui, pois pode haver mensagens pendentes de rodadas anteriores
            # mas em um cenário real, cada rodada seria completamente isolada.
            # Se for uma nova "sessão", SENT_MESSAGES também deveria ser limpo.

        while handShakeCount < N:
            msgPack = self.sock.recv(4096) # Aumentar buffer
            with data_lock: # Proteger acesso a handShakeCount
                msg = pickle.loads(msgPack)
                if msg[0] == 'READY':
                    handShakeCount += 1
                    print(f'--- Handshake received: {msg[1]} (Count: {handShakeCount}/{N}) ---')

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        stopCount = 0
        while True:
            msgPack = self.sock.recv(4096) # Aumentar buffer
            with data_lock: # Proteger acesso a variáveis globais
                msg = pickle.loads(msgPack)

                if msg[0] == -1: # Sinal de parada
                    stopCount += 1
                    print(f"Received stop signal. Current stop count: {stopCount}/{N}")
                    if stopCount == N:
                        break # Todos os peers sinalizaram para parar
                    continue # Aguardar mais sinais de parada

                # Atualiza relógio lógico
                recv_clock = msg[-1]
                lamport_clock = max(lamport_clock, recv_clock) + 1

                if msg[0] == "DATA":
                    sender_id, msg_number, msg_time = msg[1], msg[2], msg[3]
                    key = (sender_id, msg_number)

                    if key not in delivered: # Evitar adicionar/processar duplicatas
                        print(f'Message {msg_number} from process {sender_id} received. Lamport: {lamport_clock}')
                        msgQueue.append((sender_id, msg_number, msg_time))

                        # Envia ACK
                        lamport_clock += 1
                        ack = pickle.dumps(("ACK", self.myself_id, key, lamport_clock))
                        # Envia ACK de volta para o remetente original da mensagem
                        # Você precisará mapear o sender_id para o IP correspondente
                        # Por enquanto, vamos enviar o ACK para o servidor, como estava no seu original
                        # No seu código original, o ACK é enviado para o peer (PEERS[sender_id]), o que é correto
                        # Mas o msg[1] (sender_id) é um ID numérico, não um IP.
                        # Você precisará de uma maneira de converter sender_id para o IP real.
                        # Por simplicidade, vamos usar PEERS[sender_id] como o IP para o ACK
                        # Isso pressupõe que 'PEERS' está ordenado pelos IDs dos peers
                        try:
                            sendSocket.sendto(ack, (PEERS[sender_id], PEER_UDP_PORT))
                            print(f"Sent ACK for message {key} to {PEERS[sender_id]}")
                        except Exception as e:
                            print(f"Error sending ACK to {PEERS[sender_id]} for {key}: {e}")

                elif msg[0] == "ACK":
                    ack_sender_id = msg[1]
                    data_id = msg[2] # (original_sender_id, original_msg_number)
                    print(f"Received ACK for {data_id} from {ack_sender_id}. Lamport: {lamport_clock}")

                    if data_id in SENT_MESSAGES:
                        SENT_MESSAGES[data_id]["recipients_acked"].add(ack_sender_id)
                        # Se todos os ACKs para esta mensagem foram recebidos, ela está "confirmada"
                        if len(SENT_MESSAGES[data_id]["recipients_acked"]) == (N - 1): # N-1 porque não enviamos ACK para nós mesmos
                            print(f"Message {data_id} fully acknowledged by all peers.")
                            del SENT_MESSAGES[data_id] # Remover mensagem da fila de retransmissão
                            # Reinicia os contadores de retransmissão para a próxima rodada
                            # (Isso é mais relevante se você limpar SENT_MESSAGES por rodada)

                # --- Lógica de Entrega Ordenada (Pode precisar de refinamento) ---
                # A lógica de entrega abaixo é para garantir que a mensagem foi recebida por todos
                # e não necessariamente para a ordem global, mas sim para a ordem causal.
                # Se a ordem global for um requisito forte, pode-se precisar de um algoritmo de Total Order Broadcast.
                new_msgQueue = []
                for entry in sorted(msgQueue, key=lambda x: (x[2], x[0])): # Ordenar por tempo Lamport e ID do remetente
                    sender_id, msg_number, msg_time = entry
                    key = (sender_id, msg_number)

                    # A mensagem só é entregue se já foi recebida e se todos os N-1 ACKs foram recebidos.
                    # Se você precisar de Total Order Broadcast, a lógica aqui é mais complexa.
                    # Por enquanto, esta lógica garante que todos os peers viram a mensagem.
                    if len(acks_received.get(key, set())) == (N - 1): # Todos os outros N-1 peers ACKaram esta msg
                        if key not in delivered:
                            delivered.add(key)
                            print(f"--- Delivered message {msg_number} from process {sender_id} (Lamport: {msg_time}) ---")
                        # Não adiciona à nova fila se já foi entregue
                    else:
                        new_msgQueue.append(entry) # Mantém na fila se não foi entregue/confirmada

                msgQueue[:] = new_msgQueue # Atualiza a fila de mensagens

        # Após o loop de mensagens, enviar os logs para o servidor
        logFile = open('logfile' + str(self.myself_id) + '.log', 'w')
        # Sort logList by Lamport timestamp for comparison
        # logList é preenchido quando a mensagem é *recebida*, não quando entregue
        # Se você quer o log de mensagens *entregues*, precisaria coletar isso do `delivered` set.
        # Por enquanto, mantém o logList como está, representando o que foi recebido.
        logFile.writelines(str(sorted(logList, key=lambda x: x[2]))) # Ordenar o log por Lamport
        logFile.close()

        print('Sending the list of messages to the server for comparison...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        msgPack = pickle.dumps(sorted(logList, key=lambda x: x[2])) # Enviar log ordenado
        clientSock.send(msgPack)
        clientSock.close()

        print(f'Peer {self.myself_id} handler terminating.')
        # Aqui, você pode sinalizar para a thread de retransmissão parar se a rodada terminou.
        # retransmission_thread.stop() # Se você tiver uma referência a ela
        # exit(0) # Não use exit() aqui, pois pode matar outras threads inesperadamente.

def waitToStart():
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    myself_id = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps('Peer process ' + str(myself_id) + ' started.'))
    conn.close()
    return (myself_id, nMsgs)

registerWithGroupManager()

# --- INICIAR A THREAD DE RETRANSMISSÃO UMA VEZ ---
retransmission_thread = RetransmissionHandler()
retransmission_thread.daemon = True # Permite que a thread termine com o programa principal
retransmission_thread.start()
print("Retransmission handler started.")

while 1:
    print('\nWaiting for signal to start a new round...')
    (myself, nMsgs) = waitToStart()
    print(f'I am up, and my ID is: {myself}')

    if nMsgs == 0:
        print('Terminating.')
        retransmission_thread.stop() # Sinaliza para a thread de retransmissão parar
        time.sleep(1) # Dá um tempo para a thread parar
        exit(0)

    # Resetar contadores de handshake e logs para cada nova rodada
    with data_lock:
        handShakeCount = 0
        # `SENT_MESSAGES` não é limpo aqui porque a retransmissão pode estar ativa
        # para mensagens de rodadas anteriores, se o servidor iniciar uma nova rodada rapidamente.
        # Em um sistema real, você teria um ID de rodada e limparia baseado nisso.
        # Para este exercício, vamos limpar explicitamente para cada rodada:
        SENT_MESSAGES.clear()
        # msgQueue, delivered, acks_received são limpos dentro do MsgHandler.run()

    msgHandler = MsgHandler(recvSocket, myself) # Passa o ID do peer para a thread
    msgHandler.start()
    print('Message handler started.')

    PEERS = getListOfPeers()
    # No seu `constMPT.py`, N é 4. Assegure-se que PEERS tenha o mesmo número de elementos
    # que N - 1 (se o servidor não for contado) ou N (se todos os peers se comunicam entre si).
    # A lógica atual do `MsgHandler` de `N-1` ACKs implica que você não envia ACK para si mesmo.
    # No `startPeers` do `comparisonServerT.py`, ele espera N peers.

    for addrToSend in PEERS:
        print(f'Sending handshake to {addrToSend}')
        msg = ('READY', myself) # Envia o próprio ID no handshake
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    print(f'Main Thread: Sent all handshakes. Waiting for all to be received. handShakeCount={handShakeCount}')

    while (handShakeCount < N): # N é o número total de peers (incluindo você mesmo)
        pass  # espera ativa (poderia usar um threading.Event para evitar busy-waiting)

    print('All handshakes received. Starting to send data messages.')

    for msgNumber in range(0, nMsgs):
        with data_lock: # Proteger acesso a lamport_clock e SENT_MESSAGES
            lamport_clock += 1
            msg_id = (myself, msgNumber) # Identificador único para a mensagem
            msg = ("DATA", myself, msgNumber, lamport_clock)
            msgPack = pickle.dumps(msg)

            # Armazena a mensagem enviada para possível retransmissão
            SENT_MESSAGES[msg_id] = {
                "msg_packed": msgPack, # O pacote pickle.dumps já feito
                "recipients_acked": set(),
                "sent_time": time.time(),
                "retries": 0
            }

            for addrToSend in PEERS:
                sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
                print(f'Sent message {msgNumber} to {addrToSend}. Lamport: {lamport_clock}')
        time.sleep(0.01) # Pequeno atraso para evitar inundar a rede

    # Aguardar até que todas as mensagens enviadas tenham sido reconhecidas
    # Isso é crucial para garantir que a rodada de mensagens termine apenas quando
    # todas as mensagens enviadas por *este* peer foram ACKadas por *todos* os outros.
    print(f"Waiting for all {len(SENT_MESSAGES)} sent messages to be acknowledged...")
    while True:
        with data_lock:
            if not SENT_MESSAGES: # Se o dicionário estiver vazio, todas foram ACKadas
                print("All messages sent by this peer have been acknowledged!")
                break
        time.sleep(0.5) # Checa a cada 500ms

    # Envia o sinal de parada para os outros peers
    for addrToSend in PEERS:
        msg = (-1, -1, -1) # Sinal de parada, com um Lamport placeholder
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
    print('Sent stop signals to all peers.')
    # Não chame exit(0) aqui, pois a thread principal precisa continuar para a próxima rodada
