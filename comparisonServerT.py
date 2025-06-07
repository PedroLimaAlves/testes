from socket import *
import pickle
from constMPT import *
import time
import sys

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', SERVER_PORT))
serverSock.listen(6)

def mainLoop():
    cont = 1
    while True:
        nMsgs = promptUser()
        if nMsgs == 0:
            break
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
        req = {"op":"list"}
        msg = pickle.dumps(req)
        clientSock.send(msg)
        msg = clientSock.recv(2048)
        clientSock.close()
        peerList = pickle.loads(msg)
        print("List of Peers: ", peerList)
        startPeers(peerList, nMsgs)
        print('Now, wait for the message logs from the communicating peers...')
        waitForLogsAndCompare(nMsgs)
    serverSock.close()

def promptUser():
    nMsgs = int(input('Enter the number of messages for each peer to send (0 to terminate)=> '))
    return nMsgs

def startPeers(peerList, nMsgs):
    peerNumber = 0
    for peer in peerList:
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((peer, PEER_TCP_PORT))
        msg = (peerNumber, nMsgs)
        msgPack = pickle.dumps(msg)
        clientSock.send(msgPack)
        msgPack = clientSock.recv(512)
        print(pickle.loads(msgPack))
        clientSock.close()
        peerNumber += 1

def waitForLogsAndCompare(N_MSGS):
    numPeers = 0
    msgs = []

    while numPeers < N:
        (conn, addr) = serverSock.accept()
        msgPack = conn.recv(32768)
        print(f'Received log from peer {addr}')
        conn.close()
        msgs.append(pickle.loads(msgPack))
        numPeers += 1

    unordered = 0

    for j in range(0, N_MSGS):
        firstMsg = msgs[0][j]
        for i in range(1, N):
            if firstMsg != msgs[i][j]:
                unordered += 1
                break

    print(f'Found {unordered} unordered message rounds')

mainLoop()
