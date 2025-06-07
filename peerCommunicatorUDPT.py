from socket import *
from constMPT import *
import threading
import random
import time
import pickle
from requests import get

handShakeCount = 0
PEERS = []
lamportClock = 0
myself = None
recvBuffer = []
ackTable = {}
delivered = set()

sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

def get_public_ip():
    ipAddr = get('https://api.ipify.org').content.decode('utf8')
    return ipAddr

def registerWithGroupManager():
    clientSock = socket(AF_INET, SOCK_STREAM)
    ipAddr = get_public_ip()
    req = {"op":"register", "ipaddr":ipAddr, "port":PEER_UDP_PORT}
    clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    clientSock.send(pickle.dumps(req))
    clientSock.close()

def getListOfPeers():
    clientSock = socket(AF_INET, SOCK_STREAM)
    req = {"op":"list"}
    clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    clientSock.send(pickle.dumps(req))
    msg = clientSock.recv(2048)
    clientSock.close()
    return pickle.loads(msg)

def incrementClock(receivedClock=None):
    global lamportClock
    if receivedClock is None:
        lamportClock += 1
    else:
        lamportClock = max(lamportClock, receivedClock) + 1

def waitToStart():
    global myself
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    myself = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps(f'Peer process {myself} started.'))
    conn.close()
    return (myself, nMsgs)

def broadcast(msgType, data):
    for addr in PEERS:
        msgPack = pickle.dumps((msgType, data))
        sendSocket.sendto(msgPack, (addr, PEER_UDP_PORT))

class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock
        self.logList = []

    def run(self):
        global handShakeCount
        while handShakeCount < N:
            msgPack, _ = self.sock.recvfrom(1024)
            msgType, content = pickle.loads(msgPack)
            if msgType == 'READY':
                handShakeCount += 1

        stopCount = 0
        while True:
            msgPack, addr = self.sock.recvfrom(2048)
            msgType, content = pickle.loads(msgPack)

            if msgType == 'DATA':
                sender, msgId, clock = content
                incrementClock(clock)
                key = (sender, msgId)

                # Armazena mensagem
                recvBuffer.append((clock, key, content))
                if key not in ackTable:
                    ackTable[key] = set()
                ackTable[key].add(sender)

                # Envia ACK
                ack = pickle.dumps(('ACK', (myself, key)))
                sendSocket.sendto(ack, (addr[0], PEER_UDP_PORT))

                tryDeliver()

            elif msgType == 'ACK':
                senderAck, key = content
                if key not in ackTable:
                    ackTable[key] = set()
                ackTable[key].add(senderAck)
                tryDeliver()

            elif msgType == 'STOP':
                stopCount += 1
                if stopCount == N:
                    break

        self.writeAndSendLogs()

    def writeAndSendLogs(self):
        recvBuffer.sort()  # Ordena pelo clock de Lamport
        for _, key, content in recvBuffer:
            if key not in delivered:
                self.logList.append(content)
                delivered.add(key)

        with open(f'logfile{myself}.log', 'w') as f:
            f.writelines(str(self.logList))

        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        clientSock.send(pickle.dumps(self.logList))
        clientSock.close()

def tryDeliver():
    for (clock, key, content) in sorted(recvBuffer):
        if key in delivered:
            continue
        if len(ackTable.get(key, set())) >= N:
            delivered.add(key)

def main():
    registerWithGroupManager()
    while True:
        print('Waiting for signal to start...')
        myself, nMsgs = waitToStart()
        if nMsgs == 0:
            exit(0)

        time.sleep(5)
        msgHandler = MsgHandler(recvSocket)
        msgHandler.start()
        global PEERS
        PEERS = getListOfPeers()

        for addr in PEERS:
            msg = ('READY', myself)
            sendSocket.sendto(pickle.dumps(msg), (addr, PEER_UDP_PORT))

        while handShakeCount < N:
            time.sleep(0.1)

        for msgId in range(nMsgs):
            time.sleep(random.uniform(0.01, 0.05))
            incrementClock()
            msg = (myself, msgId, lamportClock)
            broadcast('DATA', msg)

        broadcast('STOP', None)

main()
