from socket import *
from constMPT import *  # -
import threading
import random
import pickle
from requests import get
import collections

logical_clock = 0
message_queue = collections.deque()

def update_logical_clock(received_timestamp):
    global logical_clock
    logical_clock = max(logical_clock, received_timestamp) + 1
    print(f'Logical clock updated to: {logical_clock}')

handShakeCount = 0
PEERS = []

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
    print('Connecting to group manager:', (GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    ipAddr = get_public_ip()
    req = {"op": "register", "ipaddr": ipAddr, "port": PEER_UDP_PORT}
    msg = pickle.dumps(req)
    print('Registering with group manager:', req)
    clientSock.send(msg)
    clientSock.close()

def getListOfPeers():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print('Connecting to group manager:', (GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    req = {"op": "list"}
    msg = pickle.dumps(req)
    print('Getting list of peers from group manager:', req)
    clientSock.send(msg)
    msg = clientSock.recv(2048)
    PEERS = pickle.loads(msg)
    print('Got list of peers:', PEERS)
    clientSock.close()
    return PEERS

class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        print('Handler is ready. Waiting for the handshakes...')
        global handShakeCount, logical_clock, logList, myself
        logList = []

        while handShakeCount < N:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            update_logical_clock(msg[2] if len(msg) > 2 else logical_clock)

            if msg[0] == 'READY':
                handShakeCount += 1
                print('--- Handshake received:', msg[1])

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        stopCount = 0
        while True:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            update_logical_clock(msg[2] if len(msg) > 2 else logical_clock)

            if msg[0] == -1:
                stopCount += 1
                if stopCount == N:
                    break
            else:
                message_queue.append(msg)
                print(f"Message {msg[1]} from process {msg[0]} received at logical time {msg[2]}")

                if msg[0] != 'ACK':
                    send_ack(msg[0], msg[1])

                process_message_queue()

        with open('logfile' + str(myself) + '.log', 'w') as logFile:
            logFile.writelines(str(logList))

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

while True:
    print('Waiting for signal to start...')
    (myself, nMsgs) = waitToStart()
    print('I am up, and my ID is:', str(myself))

    if nMsgs == 0:
        print('Terminating.')
        exit(0)

    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()
    print('Handler started')

    PEERS = getListOfPeers()

    for addrToSend in PEERS:
        print('Sending handshake to', addrToSend)
        global logical_clock
        logical_clock += 1
        msg = ('READY', myself, logical_clock)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    print('Main Thread: Sent all handshakes. handShakeCount=', str(handShakeCount))

    while handShakeCount < N:
        pass

    for msgNumber in range(0, nMsgs):
        logical_clock += 1
        msg = (myself, msgNumber, logical_clock)
        msgPack = pickle.dumps(msg)
        for addrToSend in PEERS:
            sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
        print(f'Sent message {msgNumber} at logical time {logical_clock}')

    for addrToSend in PEERS:
        logical_clock += 1
        msg = (-1, -1, logical_clock)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

def send_ack(target_peer_id, message_id):
    global logical_clock
    logical_clock += 1
    ack_msg = (
        'ACK',
        myself,
        target_peer_id,
        message_id,
        logical_clock
    )
    ack_msg_pack = pickle.dumps(ack_msg)
    target_addr = None
    for i, peer_addr in enumerate(PEERS):
        if i == target_peer_id:
            target_addr = peer_addr
            break
    if target_addr:
        sendSocket.sendto(ack_msg_pack, (target_addr, PEER_UDP_PORT))
        print(f'Sent ACK for message {message_id} to peer {target_peer_id} at logical time {logical_clock}')

def process_message_queue():
    global logical_clock
    while message_queue:
        msg = message_queue.popleft()
        logList.append(msg)
        print(f'Message {msg[1]} from process {msg[0]} delivered to application at logical time {msg[2]}')
