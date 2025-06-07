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
logList = []
myself = 0

sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

def get_public_ip():
    ipAddr = get('https://api.ipify.org').content.decode('utf8')
    print('My public IP address is:', ipAddr)
    return ipAddr

def registerWithGroupManager():
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    ipAddr = get_public_ip()
    req = {"op": "register", "ipaddr": ipAddr, "port": PEER_UDP_PORT}
    clientSock.send(pickle.dumps(req))
    clientSock.close()

def getListOfPeers():
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    req = {"op": "list"}
    clientSock.send(pickle.dumps(req))
    peerList = pickle.loads(clientSock.recv(2048))
    clientSock.close()
    return peerList

class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        global handShakeCount, lamportClock, logList

        print('Handler is ready. Waiting for the handshakes...')
        while handShakeCount < N:
            msgPack, addr = self.sock.recvfrom(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == 'READY':
                handShakeCount += 1
                print('Handshake received from', msg[1])

        print('Received all handshakes. Entering message loop.')

        stopCount = 0
        while True:
            msgPack, addr = self.sock.recvfrom(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == 'ACK':
                continue

            if msg[0] == -1:
                stopCount += 1
                if stopCount == N:
                    break
            else:
                peerId, timestamp, msgNumber = msg
                lamportClock = max(lamportClock, timestamp) + 1
                print(f'Message {msgNumber} from {peerId} with timestamp {timestamp}, updated clock to {lamportClock}')
                logList.append((peerId, timestamp, msgNumber))
                ack = pickle.dumps(('ACK',))
                sendSocket.sendto(ack, addr)

        logFile = open(f'logfile{myself}.log', 'w')
        logFile.writelines(str(logList))
        logFile.close()

        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        clientSock.send(pickle.dumps(logList))
        clientSock.close()
        handShakeCount = 0
        exit(0)

def waitToStart():
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    myself = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps(f'Peer process {myself} started.'))
    conn.close()
    return myself, nMsgs

registerWithGroupManager()

while True:
    print('Waiting for start signal...')
    myself, nMsgs = waitToStart()
    if nMsgs == 0:
        print('Terminating.')
        exit(0)

    time.sleep(5)
    lamportClock = 0
    logList = []

    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()
    print('Handler started.')

    PEERS = getListOfPeers()

    for addrToSend in PEERS:
        msg = ('READY', myself)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    while handShakeCount < N:
        pass

    for msgNumber in range(nMsgs):
        time.sleep(random.uniform(0.01, 0.1))
        lamportClock += 1
        msg = (myself, lamportClock, msgNumber)
        msgPack = pickle.dumps(msg)
        for addrToSend in PEERS:
            sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
            while True:
                recvData, _ = recvSocket.recvfrom(1024)
                ack = pickle.loads(recvData)
                if ack[0] == 'ACK':
                    break
        print(f'Sent message {msgNumber} at timestamp {lamportClock}')

    for addrToSend in PEERS:
        msg = (-1, -1)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
