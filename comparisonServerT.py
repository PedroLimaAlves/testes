from socket import *
import pickle
from constMPT import *
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
        
        # Conecta ao gerenciador de grupo para obter a lista de peers
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
        req = {"op":"list"}
        msg = pickle.dumps(req)
        clientSock.send(msg)
        msg = clientSock.recv(2048)
        clientSock.close()
        peerList = pickle.loads(msg)
        print("List of Peers: ", peerList)
        
        # Inicia os peers
        startPeers(peerList, nMsgs)
        
        print('Now, wait for the message logs from the communicating peers...')
        waitForLogsAndCompare(nMsgs, peerList) # Passa peerList para obter N dinamicamente
        
    serverSock.close()

def promptUser():
    nMsgs = int(input('Enter the number of messages for each peer to send (0 to terminate)=> '))
    return nMsgs

def startPeers(peerList, nMsgs):
    # Conecta a cada um dos peers e envia o sinal 'initiate'
    peerNumber = 0
    for peer_ip in peerList:
        clientSock = socket(AF_INET, SOCK_STREAM)
        try:
            clientSock.connect((peer_ip, PEER_TCP_PORT))
            msg = (peerNumber, nMsgs)
            msgPack = pickle.dumps(msg)
            clientSock.send(msgPack)
            msgPack = clientSock.recv(512)
            print(pickle.loads(msgPack))
            clientSock.close()
        except ConnectionRefusedError:
            print(f"Could not connect to peer at {peer_ip}:{PEER_TCP_PORT}. Is it running?")
        except Exception as e:
            print(f"Error starting peer {peer_ip}: {e}")
        peerNumber = peerNumber + 1

def waitForLogsAndCompare(N_MSGS, peerList):
    numPeers = 0
    msgs = [] # Cada msg é uma lista de tuplas (com as mensagens originais recebidas pelo processo peer)
    
    # N é agora o número de peers registrados, não uma constante fixa se desejar flexibilidade
    # Se N é uma constante em constMPT.py, você pode usá-la diretamente.
    # Caso contrário, você pode usar len(peerList)
    num_expected_peers = len(peerList) 

    # Recebe os logs de mensagens dos processos peer
    while numPeers < num_expected_peers:
        (conn, addr) = serverSock.accept()
        msgPack = conn.recv(32768)
        print ('Received log from peer')
        conn.close()
        msgs.append(pickle.loads(msgPack))
        numPeers = numPeers + 1

    unordered_rounds = 0

    # Compara as listas de mensagens
    # Certifique-se de que cada mensagem na logList é uma tupla (sender_id, content)
    if not msgs:
        print("No message logs received to compare.")
        return

    # A comparação deve ser feita mensagem a mensagem, considerando o conteúdo
    # e que as mensagens são tuplas (sender_id, content) ou (sender_id, seq_num, content)
    # Se o logList contiver (sender_id, content), compare o content.
    # Se contiver (sender_id, original_msg_number, content), compare o original_msg_number e o content.
    
    # Para o propósito de ordenamento, estamos interessados se a ordem dos conteúdos recebidos é a mesma.
    # A estrutura atual do logList no PEERCOMMUNICATORUDPT.py é [(sender_id, content), ...].
    # Vamos comparar o conteúdo das mensagens na mesma posição do log de cada peer.

    # Assumimos que todos os peers enviaram N_MSGS mensagens e as logLists terão o mesmo tamanho
    for j in range(N_MSGS): # Itera sobre cada "rodada" de mensagens
        if not msgs[0] or j >= len(msgs[0]):
            print(f"Peer 0 did not send {N_MSGS} messages.")
            break # Ou lidar com o erro de forma mais robusta

        first_msg_content = msgs[0][j][1] # Pega o conteúdo da mensagem do primeiro peer

        for i in range(1, num_expected_peers): # Itera sobre os outros peers
            if not msgs[i] or j >= len(msgs[i]):
                print(f"Peer {i} did not send {N_MSGS} messages.")
                unordered_rounds += 1 # Considera como desordenado se um peer não tem a mensagem
                break
            
            current_peer_msg_content = msgs[i][j][1] # Pega o conteúdo da mensagem do peer atual

            if first_msg_content != current_peer_msg_content:
                unordered_rounds += 1
                print(f"Mismatch found at round {j}: Peer 0 has '{first_msg_content}', Peer {i} has '{current_peer_msg_content}'")
                break # Se uma rodada está desordenada, não precisamos comparar com os outros peers para esta rodada

    print('Found ' + str(unordered_rounds) + ' unordered message rounds')


# Inicia o servidor:
mainLoop()
