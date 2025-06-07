from socket import *
import threading
import random
import time
import pickle
from requests import get
import constMPT as const

handShakeCount = 0
PEERS = []

# UDP sockets para enviar e receber dados
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('', const.PEER_UDP_PORT))

# TCP socket para receber o sinal de início do comparisonServer
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', const.PEER_TCP_PORT))
serverSock.listen(1)

# Relógio lógico de Lamport
lamport_clock = 0
lamport_lock = threading.Lock()

def get_public_ip():
    ipAddr = get('https://api.ipify.org').content.decode('utf8')
    print('My public IP address is: {}'.format(ipAddr))
    return ipAddr

def registerWithGroupManager():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print('Connecting to group manager:', (const.GROUPMNGR_ADDR, const.GROUPMNGR_TCP_PORT))
    clientSock.connect((const.GROUPMNGR_ADDR, const.GROUPMNGR_TCP_PORT))
    ipAddr = get_public_ip()
    req = {"op": "register", "ipaddr": ipAddr, "port": const.PEER_UDP_PORT}
    msg = pickle.dumps(req)
    print('Registering with group manager:', req)
    clientSock.send(msg)
    clientSock.close()

def getListOfPeers():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print('Connecting to group manager:', (const.GROUPMNGR_ADDR, const.GROUPMNGR_TCP_PORT))
    clientSock.connect((const.GROUPMNGR_ADDR, const.GROUPMNGR_TCP_PORT))
    req = {"op": "list"}
    msg = pickle.dumps(req)
    print('Getting list of peers from group manager:', req)
    clientSock.send(msg)
    msg = clientSock.recv(2048)
    clientSock.close()
    peers = pickle.loads(msg)
    print('Got list of peers:', peers)
    return peers

class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock
        self.logList = []
        self.stopCount = 0

    def run(self):
        global handShakeCount
        global lamport_clock

        print('Handler is ready. Waiting for the handshakes...')
        while handShakeCount < const.N:
            msgPack, addr = recvSocket.recvfrom(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == 'READY':
                # Envia ACK de volta para confirmar handshake
                ack_msg = ('ACK', msg[1])
                ack_pack = pickle.dumps(ack_msg)
                sendSocket.sendto(ack_pack, (addr[0], const.PEER_UDP_PORT))

                handShakeCount += 1
                print('Handshake received from', msg[1], 'Total handshakes:', handShakeCount)

        print('Received all handshakes. Entering message loop.')

        while True:
            msgPack, addr = recvSocket.recvfrom(1024)
            msg = pickle.loads(msgPack)

            if msg[0] == -1:
                self.stopCount += 1
                if self.stopCount == const.N:
                    break
            else:
                # Atualiza relógio de Lamport
                with lamport_lock:
                    lamport_clock = max(lamport_clock, msg[2]) + 1

                print(f"Message {msg[1]} from process {msg[0]} with timestamp {msg[2]}, updated clock to {lamport_clock}")
                self.logList.append(msg)

                # Envia ACK de volta confirmando recebimento da mensagem
                ack_msg = ('ACK', msg[0], msg[1])
                ack_pack = pickle.dumps(ack_msg)
                sendSocket.sendto(ack_pack, (addr[0], const.PEER_UDP_PORT))

        # Salva log localmente
        with open(f'logfile{myself}.log', 'w') as logFile:
            logFile.writelines(str(self.logList))

        # Envia log para servidor para comparação
        print('Sending the list of messages to the server for comparison...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((const.SERVER_ADDR, const.SERVER_PORT))
        msgPack = pickle.dumps(self.logList)
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
    conn.send(pickle.dumps('Peer process '+str(myself)+' started.'))
    conn.close()
    return (myself, nMsgs)

def main():
    global handShakeCount
    global PEERS
    registerWithGroupManager()

    while True:
        print('Waiting for signal to start...')
        (myself, nMsgs) = waitToStart()

        print('I am up, and my ID is:', myself)

        if nMsgs == 0:
            print('Terminating.')
            exit(0)

        time.sleep(5)  # Aguarda os outros peers

        msgHandler = MsgHandler(recvSocket)
        msgHandler.start()
        print('Handler started')

        PEERS = getListOfPeers()

        # Envia handshakes e aguarda confirmação via ACK
        for addrToSend in PEERS:
            print('Sending handshake to', addrToSend)
            msg = ('READY', myself)
            msgPack = pickle.dumps(msg)
            sendSocket.sendto(msgPack, (addrToSend, const.PEER_UDP_PORT))

            # Espera ACK do handshake
            while True:
                try:
                    recvSocket.settimeout(2)
                    ack_pack, _ = recvSocket.recvfrom(1024)
                    ack = pickle.loads(ack_pack)
                    if ack[0] == 'ACK' and ack[1] == myself:
                        print('Received handshake ACK from', addrToSend)
                        break
                except timeout:
                    print('Timeout waiting for handshake ACK from', addrToSend)
                    sendSocket.sendto(msgPack, (addrToSend, const.PEER_UDP_PORT))
            recvSocket.settimeout(None)

        while handShakeCount < const.N:
            time.sleep(0.1)

        # Envio de mensagens com timestamp Lamport e espera de ACKs
        for msgNumber in range(nMsgs):
            time.sleep(random.uniform(0.01, 0.1))

            with lamport_lock:
                lamport_clock += 1
                ts = lamport_clock

            msg = (myself, msgNumber, ts)
            msgPack = pickle.dumps(msg)

            for addrToSend in PEERS:
                sendSocket.sendto(msgPack, (addrToSend, const.PEER_UDP_PORT))

            # Aguarda ACKs de todos os peers para essa mensagem
            for addrToSend in PEERS:
                while True:
                    try:
                        recvSocket.settimeout(2)
                        ack_pack, _ = recvSocket.recvfrom(1024)
                        ack = pickle.loads(ack_pack)
                        if ack[0] == 'ACK' and ack[1] == myself and ack[2] == msgNumber:
                            # ACK recebido, vai para próxima
                            break
                    except timeout:
                        print(f'Timeout esperando ACK de {addrToSend} para mensagem {msgNumber}, reenviando...')
                        sendSocket.sendto(msgPack, (addrToSend, const.PEER_UDP_PORT))
            recvSocket.settimeout(None)
            print(f'Sent message {msgNumber} with timestamp {ts} and received all ACKs')

        # Envia mensagens de parada (-1) para avisar que terminou
        stop_msg = (-1, -1, -1)
        stopPack = pickle.dumps(stop_msg)
        for addrToSend in PEERS:
            sendSocket.sendto(stopPack, (addrToSend, const.PEER_UDP_PORT))

if __name__ == '__main__':
    main()
