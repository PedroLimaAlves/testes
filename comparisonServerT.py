from socket import *
import pickle
from constMPT import * # Importa N de constMPT
import time
import sys

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', SERVER_PORT))
serverSock.listen(6)

def mainLoop():
    while 1:
        nMsgs = promptUser()
        if nMsgs == 0:
            break
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
        req = {"op":"list"}
        msg = pickle.dumps(req)
        clientSock.send(msg)
        msg = clientSock.recv(2048)
        clientSock.close()
        peerList = pickle.loads(msg)
        print("List of Peers: ", peerList)
        startPeers(peerList,nMsgs)
        print('Now, wait for the message logs from the communicating peers...')
        waitForLogsAndCompare(nMsgs)
    serverSock.close()

def promptUser():
    nMsgs = int(input('Enter the number of messages for each peer to send (0 to terminate)=> '))
    return nMsgs

def startPeers(peerList,nMsgs):
    # Conecta-se a cada um dos peers e envia o sinal 'initiate':
    peerNumber = 0
    for peer in peerList:
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((peer, PEER_TCP_PORT))
        msg = (peerNumber,nMsgs)
        msgPack = pickle.dumps(msg)
        clientSock.send(msgPack)
        msgPack = clientSock.recv(512)
        print(pickle.loads(msgPack))
        clientSock.close()
        peerNumber = peerNumber + 1

def waitForLogsAndCompare(N_MSGS):
    # Loop para aguardar os logs de mensagens para comparação:
    numPeers = 0
    msgs = [] # cada msg é uma lista de tuplas (com as mensagens originais recebidas pelos processos peer)

    # Recebe os logs de mensagens dos processos peer
    # O valor de N precisa ser o número total de peers configurados em constMPT.py
    while numPeers < N: # Usando o N de constMPT.py
        (conn, addr) = serverSock.accept()
        msgPack = conn.recv(32768)
        print ('Received log from peer')
        conn.close()
        msgs.append(pickle.loads(msgPack))
        numPeers = numPeers + 1

    unordered = 0

    # Compara as listas de mensagens
    # Se a implementação do relógio lógico e ordenação estiver correta,
    # 'unordered' deve ser sempre 0.
    if len(msgs) > 0: # Garante que há logs para comparar
        for j in range(0, N_MSGS): # Itera sobre o número de mensagens esperadas
            firstMsg = msgs[0][j] # Pega a j-ésima mensagem do primeiro peer
            for i in range(1, N): # Compara com os outros peers
                if firstMsg != msgs[i][j]:
                    unordered = unordered + 1
                    break # Se encontrou uma diferença para esta rodada, já é desordenado

    print ('Found ' + str(unordered) + ' unordered message rounds')


# Inicia o servidor:
mainLoop()
