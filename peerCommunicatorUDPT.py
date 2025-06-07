from socket import *
from constMPT import *
import threading
import random
import time
import pickle
from requests import get

logical_clock = 0
message_queue = []  # Fila de mensagens recebidas aguardando entrega
ack_tracker = {}    # Chave: (origem, num_msg, timestamp) -> set de processos que enviaram ACK

lock = threading.Lock()  # Para evitar condições de corrida

handShakeCount = 0

PEERS = []

# UDP sockets para envio e recepção de dados
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# TCP socket para receber sinal de início do servidor de comparação
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
    peers = pickle.loads(msg)
    print('Got list of peers: ', peers)
    clientSock.close()
    return peers


class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        global handShakeCount, logical_clock, message_queue, ack_tracker

        print('Handler is ready. Waiting for the handshakes...')

        logList = []

        while handShakeCount < N:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == 'READY':
                handShakeCount += 1
                print('--- Handshake received: ', msg[1])

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        stopCount = 0
        while True:
            msgPack = self.sock.recv(2048)
            msg = pickle.loads(msgPack)

            with lock:
                # Atualiza relógio lógico
                msg_clock = msg[2] if len(msg) > 2 else 0
                logical_clock = max(logical_clock, msg_clock) + 1

                if msg[0] == -1:
                    stopCount += 1
                    if stopCount == N:
                        break
                elif msg[0] == 'ACK':
                    orig, msg_num, ts = msg[1]
                    key = (orig, msg_num, ts)
                    if key not in ack_tracker:
                        ack_tracker[key] = set()
                    ack_tracker[key].add(msg[3])  # Quem enviou o ACK
                else:
                    # Mensagem de dados
                    sender_id, msg_num, ts = msg
                    key = (sender_id, msg_num, ts)
                    if key not in [ (m[0], m[1], m[2]) for m in message_queue]:
                        message_queue.append((sender_id, msg_num, ts))

                    # Envia ACK para todos
                    ack = ('ACK', (sender_id, msg_num, ts), logical_clock, myself)
                    ackPack = pickle.dumps(ack)
                    for addrToSend in PEERS:
                        sendSocket.sendto(ackPack, (addrToSend, PEER_UDP_PORT))

            deliver_messages(logList)

        # Grava log de mensagens entregues
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


def deliver_messages(logList):
    global message_queue, ack_tracker

    with lock:
        # Ordena mensagens por timestamp e id do processo
        message_queue.sort(key=lambda x: (x[2], x[0]))

        i = 0
        while i < len(message_queue):
            msg = message_queue[i]
            key = (msg[0], msg[1], msg[2])
            if key in ack_tracker and len(ack_tracker[key]) == N:
                print('Message ' + str(msg[1]) + ' from process ' + str(msg[0]))
                logList.append((msg[0], msg[1]))
                message_queue.pop(i)
                del ack_tracker[key]
                i = 0  # reinicia pra checar fila novamente
            else:
                i += 1


def waitToStart():
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    global myself
    myself = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps('Peer process ' + str(myself) + ' started.'))
    conn.close()
    return (myself, nMsgs)


registerWithGroupManager()
while True:
    print('Waiting for signal to start...')
    (myself, nMsgs) = waitToStart()
    print('I am up, and my ID is: ', str(myself))

    if nMsgs == 0:
        print('Terminating.')
        exit(0)

    time.sleep(5)

    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()
    print('Handler started')

    PEERS = getListOfPeers()

    # Envia handshakes para todos os pares
    for addrToSend in PEERS:
        print('Sending handshake to ', addrToSend)
        msg = ('READY', myself)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    print('Main Thread: Sent all handshakes. handShakeCount=', str(handShakeCount))

    while handShakeCount < N:
        pass

    global logical_clock
    for msgNumber in range(0, nMsgs):
        logical_clock += 1
        msg = (myself, msgNumber, logical_clock)
        msgPack = pickle.dumps(msg)
        for addrToSend in PEERS:
            sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
            print('Sent message ' + str(msgNumber))

    for addrToSend in PEERS:
        msg = (-1, -1)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
