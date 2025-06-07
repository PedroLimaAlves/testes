from socket  import *
from constMPT import *
import threading
import random
import time
import pickle
from requests import get

handShakeCount = 0
PEERS = []
recvQueue = []
ackTracker = {}  # (senderID, msgID): set(peerIDs)
deliveredSet = set()

sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

myself = None
clock = 0


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
    msg = clientSock.recv(2048)
    peers = pickle.loads(msg)
    print('Got list of peers: ', peers)
    clientSock.close()
    return peers


def tryDeliver():
    global recvQueue, deliveredSet
    recvQueue.sort(key=lambda m: (m[2], m[0], m[1]))
    delivered = []

    for m in recvQueue:
        msgKey = (m[0], m[1])
        if msgKey in deliveredSet:
            continue
        if msgKey not in ackTracker or len(ackTracker[msgKey]) < N:
            break
        print('Message', m[1], 'from process', m[0])
        delivered.append(m)
        deliveredSet.add(msgKey)

    for m in delivered:
        recvQueue.remove(m)


class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        global handShakeCount, clock
        print('Handler is ready. Waiting for the handshakes...')
        logList = []
        stopCount = 0

        while handShakeCount < N:
            msgPack, addr = self.sock.recvfrom(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == 'READY':
                handShakeCount += 1
                print('--- Handshake received: ', msg[1])

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        while True:
            msgPack, addr = self.sock.recvfrom(1024)
            msg = pickle.loads(msgPack)

            if msg[0] == -1:
                stopCount += 1
                if stopCount == N:
                    break
                continue

            if msg[0] == 'ACK':
                ackKey = msg[1]
                sender = msg[2]
                if ackKey not in ackTracker:
                    ackTracker[ackKey] = set()
                ackTracker[ackKey].add(sender)
            else:
                sender, msgID, ts = msg
                clock = max(clock, ts) + 1
                recvQueue.append((sender, msgID, ts))
                ackMsg = ('ACK', (sender, msgID), myself)
                ackPack = pickle.dumps(ackMsg)
                for peer in PEERS:
                    sendSocket.sendto(ackPack, (peer, PEER_UDP_PORT))

            tryDeliver()

        logList = sorted(deliveredSet, key=lambda x: x[1])
        logFile = open('logfile' + str(myself) + '.log', 'w')
        logFile.writelines(str(logList))
        logFile.close()

        print('Sending the list of messages to the server for comparison...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        msgPack = pickle.dumps(logList)
        clientSock.send(msgPack)
        clientSock.close()

        handShakeCount = 0
        exit(0)


def waitToStart():
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    global myself
    myself = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps('Peer process ' + str(myself) + ' started.'))
    conn.close()
    return myself, nMsgs


registerWithGroupManager()
while True:
    print('Waiting for signal to start...')
    myself, nMsgs = waitToStart()
    print('I am up, and my ID is: ', str(myself))

    if nMsgs == 0:
        print('Terminating.')
        exit(0)

    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()
    print('Handler started')

    PEERS = getListOfPeers()

    for addrToSend in PEERS:
        print('Sending handshake to ', addrToSend)
        msg = ('READY', myself)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    while handShakeCount < N:
        pass

    global clock
    for msgNumber in range(0, nMsgs):
        clock += 1
        msg = (myself, msgNumber, clock)
        msgPack = pickle.dumps(msg)
        for addrToSend in PEERS:
            sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
            print('Sent message', msgNumber)

    for addrToSend in PEERS:
        msg = (-1, -1)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
