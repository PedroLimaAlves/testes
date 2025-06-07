from socket import *
from constMPT import *
import threading
import random
import pickle
from requests import get
import time #time.time() é usado, time.sleep() é removido

handShakeCount = 0

PEERS = []
lamport_clock = 0
msgQueue = []
delivered = set()

SENT_MESSAGES = {}
RETRANSMISSION_TIMEOUT = 2.0
MAX_RETRANSMISSIONS = 5

data_lock = threading.Lock() # Lock geral para dados compartilhados
retransmission_condition = threading.Condition(data_lock) # Condição para o RetransmissionHandler

# Eventos para sincronização entre threads (ainda relevantes para o fluxo principal)
handshake_complete_event = threading.Event()
all_sent_acked_event = threading.Event()

sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

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
    msg = clientSock.recv(4096)
    PEERS = pickle.loads(msg)
    print('Got list of peers: ', PEERS)
    clientSock.close()
    return PEERS

class RetransmissionHandler(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.running = True

    def run(self):
        with retransmission_condition: # Adquire o lock antes de começar o loop
            while self.running:
                current_time = time.time()
                next_timeout = float('inf') # O próximo timeout mais cedo

                # Verifica se há mensagens para retransmitir e calcula o próximo timeout
                messages_to_retransmit = []
                for msg_id, data in list(SENT_MESSAGES.items()): # Copia a lista para iterar com segurança
                    if (current_time - data["sent_time"]) > RETRANSMISSION_TIMEOUT:
                        messages_to_retransmit.append((msg_id, data))
                    else:
                        # Calcula quanto tempo falta para o próximo timeout
                        time_left = RETRANSMISSION_TIMEOUT - (current_time - data["sent_time"])
                        if time_left < next_timeout:
                            next_timeout = time_left
                
                # Processa as mensagens que precisam ser retransmitidas
                for msg_id, data in messages_to_retransmit:
                    if data["retries"] < MAX_RETRANSMISSIONS:
                        print(f"--- Retransmitting message {msg_id}, retry {data['retries'] + 1} ---")
                        # O reenvio precisa ser feito fora do data_lock se sendSocket for bloqueante
                        # Mas como é UDP e sendto não bloqueia muito, manter o lock por agora
                        for addrToSend in PEERS:
                            sendSocket.sendto(data["msg_packed"], (addrToSend, PEER_UDP_PORT))
                        data["sent_time"] = time.time() # Atualiza tempo de envio
                        data["retries"] += 1
                        # Atualiza next_timeout para esta mensagem após retransmissão
                        if RETRANSMISSION_TIMEOUT < next_timeout:
                            next_timeout = RETRANSMISSION_TIMEOUT # A próxima retransmissão desta msg

                    else:
                        print(f"--- Message {msg_id} failed after {MAX_RETRANSMISSIONS} retries. ---")
                        del SENT_MESSAGES[msg_id]
                        if not SENT_MESSAGES:
                            all_sent_acked_event.set() # Sinaliza que todas as mensagens falharam ou foram ACKadas

                # Se não há mensagens para monitorar, ou todas falharam/ACKadas, esperar indefinidamente
                if not SENT_MESSAGES:
                    next_timeout = None # Espera indefinidamente até ser notificado

                # Espera pelo próximo timeout ou por uma notificação
                # Se next_timeout for None, wait() bloqueia até notify()
                # Se next_timeout for um valor, wait() bloqueia por esse tempo ou até notify()
                retransmission_condition.wait(next_timeout)
                # O lock é liberado durante o wait() e readquirido ao retornar

    def stop(self):
        with retransmission_condition:
            self.running = False
            retransmission_condition.notify_all() # Acorda a thread para que ela possa terminar

class MsgHandler(threading.Thread):
    def __init__(self, sock, myself_id):
        threading.Thread.__init__(self)
        self.sock = sock
        self.myself_id = myself_id
        self.logList = []
        # Eventos compartilhados
        self.handshake_event = handshake_complete_event
        self.acked_event = all_sent_acked_event

    def run(self):
        print('Handler is ready. Waiting for the handshakes...')
        global handShakeCount, lamport_clock, msgQueue, delivered

        with data_lock: # Usa o data_lock para resetar estados compartilhados
            lamport_clock = 0
            msgQueue = []
            delivered = set()
            self.logList = []

        # Loop de Handshake
        # A thread bloqueia em recv() até receber um handshake
        while handShakeCount < N:
            msgPack = self.sock.recv(4096)
            with data_lock:
                msg = pickle.loads(msgPack)
                if msg[0] == 'READY':
                    handShakeCount += 1
                    print(f'--- Handshake received: {msg[1]} (Count: {handShakeCount}/{N}) ---')
                    if handShakeCount == N:
                        self.handshake_event.set() # Sinaliza que todos os handshakes foram recebidos

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        stopCount = 0
        while True:
            msgPack = self.sock.recv(4096) # Bloqueia aqui esperando por mensagens
            with data_lock: # Adquire o lock para processar a mensagem recebida
                msg = pickle.loads(msgPack)

                if msg[0] == -1:
                    stopCount += 1
                    print(f"Received stop signal. Current stop count: {stopCount}/{N}")
                    if stopCount == N:
                        break
                    continue

                recv_clock = msg[-1]
                lamport_clock = max(lamport_clock, recv_clock) + 1

                if msg[0] == "DATA":
                    sender_id, msg_number, msg_time = msg[1], msg[2], msg[3]
                    key = (sender_id, msg_number)

                    if key not in delivered:
                        print(f'Message {msg_number} from process {sender_id} received. Lamport: {lamport_clock}')
                        self.logList.append((sender_id, msg_number, msg_time))
                        msgQueue.append((sender_id, msg_number, msg_time))

                        lamport_clock += 1
                        ack = pickle.dumps(("ACK", self.myself_id, key, lamport_clock))
                        try:
                            # Send ACK. Note: sendto should be fine, but if it blocks,
                            # consider moving it outside the lock briefly.
                            sendSocket.sendto(ack, (PEERS[sender_id], PEER_UDP_PORT))
                            print(f"Sent ACK for message {key} to {PEERS[sender_id]}")
                        except Exception as e:
                            print(f"Error sending ACK to {PEERS[sender_id]} for {key}: {e}")

                elif msg[0] == "ACK":
                    ack_sender_id = msg[1]
                    data_id = msg[2]
                    print(f"Received ACK for {data_id} from {ack_sender_id}. Lamport: {lamport_clock}")

                    if data_id in SENT_MESSAGES:
                        SENT_MESSAGES[data_id]["recipients_acked"].add(ack_sender_id)
                        if len(SENT_MESSAGES[data_id]["recipients_acked"]) == (N - 1):
                            print(f"Message {data_id} fully acknowledged by all peers.")
                            del SENT_MESSAGES[data_id]
                            # Notifica a thread de retransmissão sobre uma mudança no SENT_MESSAGES
                            # para que ela possa reavaliar seus timers ou terminar se tudo foi ackado
                            retransmission_condition.notify() 
                            if not SENT_MESSAGES:
                                self.acked_event.set()

                # Lógica de entrega (correção anterior, sem sleeps)
                while True:
                    if not msgQueue:
                        break

                    msgQueue.sort(key=lambda x: (x[2], x[0])) # Garante ordenação
                    entry_to_deliver = msgQueue[0]
                    sender_id, msg_number, msg_time = entry_to_deliver
                    key = (sender_id, msg_number)

                    if key not in delivered:
                        delivered.add(key)
                        print(f"--- Delivered message {msg_number} from process {sender_id} (Lamport: {msg_time}) ---")
                        self.logList.append(entry_to_deliver) # Certifica-se de que a mensagem entregue é adicionada ao log
                        msgQueue.pop(0)
                    else:
                        print(f"--- Warning: Message {key} already delivered. Removing from queue. ---")
                        msgQueue.pop(0)
                        break

        # --- Trecho alterado para imprimir o log no console ---
        print(f"\n--- FINAL LOG FOR PEER {self.myself_id} ---")
        final_sorted_log = sorted(self.logList, key=lambda x: (x[2], x[0]))
        print(str(final_sorted_log))
        print(f"--- END FINAL LOG FOR PEER {self.myself_id} ---\n")
        # --- Fim do trecho alterado ---

        print('Sending the list of messages to the server for comparison...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        msgPack = pickle.dumps(final_sorted_log)
        clientSock.send(msgPack)
        clientSock.close()

        print(f'Peer {self.myself_id} handler terminating.')

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

retransmission_thread = RetransmissionHandler()
retransmission_thread.daemon = True
retransmission_thread.start()
print("Retransmission handler started.")

while 1:
    print('\nWaiting for signal to start a new round...')
    (myself, nMsgs) = waitToStart()
    print(f'I am up, and my ID is: {myself}')

    if nMsgs == 0:
        print('Terminating.')
        retransmission_thread.stop()
        exit(0)

    with data_lock:
        handShakeCount = 0
        SENT_MESSAGES.clear()
        handshake_complete_event.clear()
        all_sent_acked_event.clear()
        # Notifica a thread de retransmissão para que ela reavalie (agora com SENT_MESSAGES vazio)
        retransmission_condition.notify()

    msgHandler = MsgHandler(recvSocket, myself)
    msgHandler.start()
    print('Message handler started.')

    PEERS = getListOfPeers()

    for addrToSend in PEERS:
        print(f'Sending handshake to {addrToSend}')
        msg = ('READY', myself)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    print(f'Main Thread: Sent all handshakes. Waiting for all to be received. handShakeCount={handShakeCount}')

    handshake_complete_event.wait()
    print('All handshakes received. Starting to send data messages.')

    for msgNumber in range(0, nMsgs):
        with data_lock:
            lamport_clock += 1
            msg_id = (myself, msgNumber)
            msg = ("DATA", myself, msgNumber, lamport_clock)
            msgPack = pickle.dumps(msg)

            SENT_MESSAGES[msg_id] = {
                "msg_packed": msgPack,
                "recipients_acked": set(),
                "sent_time": time.time(),
                "retries": 0
            }
            # Notifica a thread de retransmissão que há uma nova mensagem para monitorar
            retransmission_condition.notify()

            for addrToSend in PEERS:
                sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
                print(f'Sent message {msgNumber} to {addrToSend}. Lamport: {lamport_clock}')

    print(f"Waiting for all sent messages to be acknowledged...")
    with data_lock:
        if not SENT_MESSAGES:
            print("No messages to wait for or already acknowledged.")
        else:
            all_sent_acked_event.wait()

    print("All messages sent by this peer have been acknowledged!")

    for addrToSend in PEERS:
        msg = (-1, -1, -1)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
    print('Sent stop signals to all peers.')
