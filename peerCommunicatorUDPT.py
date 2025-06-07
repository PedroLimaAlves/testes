from socket import *
from constMPT import *
import threading
import random
import pickle
from requests import get

handShakeCount = 0
PEERS = []
lamport_clock = 0
msgQueue = []
delivered = set()
acks_received = {}

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
    msg = clientSock.recv(2048)
    PEERS = pickle.loads(msg)
    print('Got list of peers: ', PEERS)
    clientSock.close()
    return PEERS

class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        print('Handler is ready. Waiting for the handshakes...')
        global handShakeCount, lamport_clock, msgQueue, delivered, acks_received

        logList = []
        lamport_clock = 0
        msgQueue = []
        delivered = set()
        acks_received = {}

        while handShakeCount < N:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == 'READY':
                handShakeCount += 1
                print('--- Handshake received: ', msg[1])

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        stopCount = 0
        while True:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)

            if msg[0] == -1:
                stopCount += 1
                if stopCount == N:
                    break
                continue

            recv_clock = msg[-1]
            lamport_clock = max(lamport_clock, recv_clock) + 1

            if msg[0] == "DATA":
                sender_id, msg_number, msg_time = msg[1], msg[2], msg[3]
                print(f'Message {msg_number} from process {sender_id}')
                msgQueue.append((sender_id, msg_number, msg_time))

                lamport_clock += 1
                ack = pickle.dumps(("ACK", myself, (sender_id, msg_number), lamport_clock))
                sendSocket.sendto(ack, (PEERS[sender_id], PEER_UDP_PORT))

            elif msg[0] == "ACK":
                ack_sender = msg[1]
                data_id = msg[2]
                if data_id not in acks_received:
                    acks_received[data_id] = set()
                acks_received[data_id].add(ack_sender)

            # Entrega ordenada com log correto
            msgQueue.sort(key=lambda x: (x[2], x[0]))  # (timestamp, sender_id)
            while msgQueue:
                sender_id, msg_number, msg_time = msgQueue[0]
                key = (sender_id, msg_number)
                if len(acks_received.get(key, set())) == (N - 1):
                    if key not in delivered:
                        delivered.add(key)
                        print(f"Delivered message {msg_number} from process {sender_id}")
                        logList.append((sender_id, msg_number, msg_time))
                        msgQueue.pop(0)
                else:
                    break

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
    myself = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps('Peer process ' + str(myself) + ' started.'))
    conn.close()
    return (myself, nMsgs)

registerWithGroupManager()

while 1:
    print('Waiting for signal to start...')
    (myself, nMsgs) = waitToStart()
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

    print('Main Thread: Sent all handshakes. handShakeCount=', str(handShakeCount))

    while (handShakeCount < N):
        pass  # espera ativa

    for msgNumber in range(0, nMsgs):
        lamport_clock += 1
        msg = ("DATA", myself, msgNumber, lamport_clock)
        msgPack = pickle.dumps(msg)
        for addrToSend in PEERS:
            sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
            print('Sent message ' + str(msgNumber))

    for addrToSend in PEERS:
        msg = (-1, -1)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
