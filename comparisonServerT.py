from socket import *
import pickle
from constMPT import *
import time
import sys

# Cria o socket do servidor
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', SERVER_PORT))
serverSock.listen(6)

def mainLoop():
    while True:
        nMsgs = promptUser()
        if nMsgs == 0:
            break

        # Solicita a lista de peers ao gerenciador do grupo
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
        req = {"op": "list"}
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
    try:
        return int(input('Enter the number of messages for each peer to send (0 to terminate)=> '))
    except ValueError:
        print("Please enter a valid integer.")
        return promptUser()

def startPeers(peerList, nMsgs):
    for peerNumber, peer in enumerate(peerList):
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((peer, PEER_TCP_PORT))
        msg = (peerNumber, nMsgs)
        msgPack = pickle.dumps(msg)
        clientSock.send(msgPack)
        msgPack = clientSock.recv(512)
        print(pickle.loads(msgPack))
        clientSock.close()

def waitForLogsAndCompare(N_MSGS):
    numPeers = 0
    msgs = []  # cada elemento é uma lista de mensagens de um peer

    # Recebe os logs dos peers
    while numPeers < N:
        (conn, addr) = serverSock.accept()
        msgPack = b''
        while True:
            packet = conn.recv(4096)
            if not packet:
                break
            msgPack += packet
        print('Received log from peer')
        conn.close()

        try:
            peerLog = pickle.loads(msgPack)
        except Exception as e:
            print(f"Erro ao processar log do peer {numPeers}: {e}")
            return

        msgs.append(peerLog)
        numPeers += 1

    # Verifica se todos os peers enviaram a quantidade esperada de mensagens
    for idx, peerMsgs in enumerate(msgs):
        if len(peerMsgs) != N_MSGS:
            print(f"Aviso: Peer {idx} enviou {len(peerMsgs)} mensagens (esperado {N_MSGS}). Abortando comparação.\n")
            return

    unordered = 0

    # Compara as mensagens enviadas para garantir ordem igual entre os peers
    for j in range(N_MSGS):
        firstMsg = msgs[0][j]
        for i in range(1, N):
            if msgs[i][j] != firstMsg:
                unordered += 1
                break

    print(f'Found {unordered} unordered message rounds\n')

# Inicia o servidor
mainLoop()
