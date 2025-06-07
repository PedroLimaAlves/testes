from socket import *
from constMPT import *
import threading
import random
import pickle
from requests import get
import time

handShakeCount = 0

PEERS = []
lamport_clock = 0
msgQueue = []
delivered = set()
acks_received = {} # Esta variável agora é usada principalmente para o SENT_MESSAGES.

SENT_MESSAGES = {}
RETRANSMISSION_TIMEOUT = 2.0
MAX_RETRANSMISSIONS = 5

data_lock = threading.Lock()

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
        while self.running:
            current_time = time.time()
            with data_lock:
                for msg_id, data in list(SENT_MESSAGES.items()):
                    if (current_time - data["sent_time"]) > RETRANSMISSION_TIMEOUT:
                        if data["retries"] < MAX_RETRANSMISSIONS:
                            print(f"--- Retransmitting message {msg_id}, retry {data['retries'] + 1} ---")
                            for addrToSend in PEERS:
                                sendSocket.sendto(data["msg_packed"], (addrToSend, PEER_UDP_PORT))
                            data["sent_time"] = current_time
                            data["retries"] += 1
                        else:
                            print(f"--- Message {msg_id} failed after {MAX_RETRANSMISSIONS} retries. ---")
                            del SENT_MESSAGES[msg_id]
            time.sleep(0.5)

    def stop(self):
        self.running = False

class MsgHandler(threading.Thread):
    def __init__(self, sock, myself_id):
        threading.Thread.__init__(self)
        self.sock = sock
        self.myself_id = myself_id
        self.logList = []

    def run(self):
        print('Handler is ready. Waiting for the handshakes...')
        global handShakeCount, lamport_clock, msgQueue, delivered, acks_received

        with data_lock:
            lamport_clock = 0
            msgQueue = []
            delivered = set()
            acks_received = {}
            self.logList = []

        while handShakeCount < N:
            msgPack = self.sock.recv(4096)
            with data_lock:
                msg = pickle.loads(msgPack)
                if msg[0] == 'READY':
                    handShakeCount += 1
                    print(f'--- Handshake received: {msg[1]} (Count: {handShakeCount}/{N}) ---')

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        stopCount = 0
        while True:
            msgPack = self.sock.recv(4096)
            with data_lock:
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

                # --- INÍCIO DA LÓGICA DE ENTREGA CORRIGIDA ---
                while True:
                    if not msgQueue:
                        break

                    # Garante que a fila está sempre ordenada pelo relógio de Lamport e ID do remetente
                    msgQueue.sort(key=lambda x: (x[2], x[0]))

                    entry_to_deliver = msgQueue[0]
                    sender_id, msg_number, msg_time = entry_to_deliver
                    key = (sender_id, msg_number)

                    if key not in delivered:
                        # Se a mensagem com menor Lamport não foi entregue, entregue-a.
                        # Não há mais a condição de esperar por ACKs de outros peers aqui.
                        delivered.add(key)
                        print(f"--- Delivered message {msg_number} from process {sender_id} (Lamport: {msg_time}) ---")
                        msgQueue.pop(0) # Remove a mensagem entregue da fila
                    else:
                        # Se a mensagem no topo já foi entregue (duplicata ou erro), remove e tenta a próxima.
                        print(f"--- Warning: Message {key} already delivered. Removing from queue. ---")
                        msgQueue.pop(0)
                        # Se a mensagem que está no topo já foi entregue,
                        # é um caso atípico, mas podemos continuar para o próximo item
                        # ou, de forma mais cautelosa, quebrar o loop e reavaliar
                        # na próxima iteração de recebimento de mensagens.
                        # Optamos por quebrar para evitar loops infinitos se houver um erro de lógica mais profundo.
                        break # Sai do loop de entrega para reavaliar na próxima mensagem recebida.
                # --- FIM DA LÓGICA DE ENTREGA CORRIGIDA ---

        logFile = open('logfile' + str(self.myself_id) + '.log', 'w')
        logFile.writelines(str(sorted(self.logList, key=lambda x: x[2])))
        logFile.close()

        print('Sending the list of messages to the server for comparison...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        msgPack = pickle.dumps(sorted(self.logList, key=lambda x: x[2]))
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
        time.sleep(1)
        exit(0)

    with data_lock:
        handShakeCount = 0
        SENT_MESSAGES.clear()

    msgHandler = MsgHandler(recvSocket, myself)
    msgHandler.start()
    print('Message handler started.')

    PEERS = getListOfPeers()
    time.sleep(1)

    for addrToSend in PEERS:
        print(f'Sending handshake to {addrToSend}')
        msg = ('READY', myself)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    print(f'Main Thread: Sent all handshakes. Waiting for all to be received. handShakeCount={handShakeCount}')

    while (handShakeCount < N):
        time.sleep(0.1)

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

            for addrToSend in PEERS:
                sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
                print(f'Sent message {msgNumber} to {addrToSend}. Lamport: {lamport_clock}')
        time.sleep(0.01)

    print(f"Waiting for all {len(SENT_MESSAGES)} sent messages to be acknowledged...")
    while True:
        with data_lock:
            if not SENT_MESSAGES:
                print("All messages sent by this peer have been acknowledged!")
                break
        time.sleep(0.5)

    for addrToSend in PEERS:
        msg = (-1, -1, -1)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
    print('Sent stop signals to all peers.')
