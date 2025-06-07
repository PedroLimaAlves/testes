from socket import *
import pickle
import sys
import threading
import time
from constMPT import *

myId = int(sys.argv[1])
nMsgs = int(sys.argv[2])
peerList = sys.argv[3:]
myPort = PEER_BASE_UDP_PORT + myId

msgCounter = 0
timestamp = 0
lock = threading.Lock()
delivered = []
msgQueue = []

sock = socket(AF_INET, SOCK_DGRAM)
sock.bind(('', myPort))

acks = {}  # acks[(msgId)] = set of peer ids that acked
buffer = {}  # buffer[(msgId)] = message
receivedTimestamps = {}  # receivedTimestamps[peerId] = last timestamp seen

# Função auxiliar para incrementar relógio lógico
def increment_timestamp():
    global timestamp
    with lock:
        timestamp += 1
        return timestamp

# Envia mensagem para todos os peers
def sendMessages():
    global msgCounter, timestamp

    for i in range(nMsgs):
        time.sleep(1)
        ts = increment_timestamp()
        msgId = (myId, msgCounter)
        msg = {
            'type': 'data',
            'from': myId,
            'seq': msgCounter,
            'timestamp': ts,
            'msgId': msgId,
            'text': f'message {msgCounter} from peer {myId}'
        }
        data = pickle.dumps(msg)
        for peer in peerList:
            sock.sendto(data, (peer, PEER_BASE_UDP_PORT + int(peerList.index(peer))))
        with lock:
            acks[msgId] = set([myId])  # já sabemos que enviamos
            buffer[msgId] = msg
        msgCounter += 1

# Escuta mensagens e ACKs
def receiveMessages():
    while True:
        pkt, addr = sock.recvfrom(4096)
        msg = pickle.loads(pkt)

        if msg['type'] == 'data':
            handleDataMessage(msg)

        elif msg['type'] == 'ack':
            handleAck(msg)

# Lida com mensagem do tipo "data"
def handleDataMessage(msg):
    global timestamp

    sender = msg['from']
    ts = msg['timestamp']
    msgId = msg['msgId']

    with lock:
        timestamp = max(timestamp, ts) + 1
        if msgId not in buffer:
            buffer[msgId] = msg
            acks[msgId] = set()
        # Armazena ACK de quem enviou
        acks[msgId].add(sender)

    # Responde com ACK
    ack = {
        'type': 'ack',
        'from': myId,
        'msgId': msgId
    }
    data = pickle.dumps(ack)
    sock.sendto(data, addr)

    tryDeliver()

# Lida com ACKs recebidos
def handleAck(msg):
    msgId = msg['msgId']
    sender = msg['from']

    with lock:
        if msgId in acks:
            acks[msgId].add(sender)
        else:
            acks[msgId] = set([sender])

    tryDeliver()

# Tenta entregar mensagens em ordem com base nos timestamps
def tryDeliver():
    global delivered
    changed = True
    while changed:
        changed = False
        with lock:
            # Ordena pela tupla (timestamp, peer_id, seq) para desempatar
            sortedMsgs = sorted([
                (m['timestamp'], m['from'], m['seq'], m['msgId'])
                for m in buffer.values()
                if m['msgId'] not in delivered and len(acks[m['msgId']]) == N
            ])

            for _, _, _, msgId in sortedMsgs:
                msg = buffer[msgId]
                delivered.append(msgId)
                print(f'Delivered: {msg["text"]} (ts={msg["timestamp"]})')
                changed = True
                break  # Entrega uma por vez para manter ordem

# Envia log de mensagens ao servidor após terminar
def sendLogToServer():
    time.sleep(5 + nMsgs)
    logMsgs = [buffer[msgId] for msgId in delivered if msgId in buffer]
    sockTCP = socket(AF_INET, SOCK_STREAM)
    sockTCP.connect((SERVER_ADDR, SERVER_PORT))
    sockTCP.send(pickle.dumps(logMsgs))
    sockTCP.close()

# Inicia as threads
threading.Thread(target=receiveMessages, daemon=True).start()
threading.Thread(target=sendMessages).start()
threading.Thread(target=sendLogToServer).start()
