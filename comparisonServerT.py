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
        waitForLogsAndCompare(nMsgs, peerList) # Passar peerList para saber o número de peers
    serverSock.close()

def promptUser():
    nMsgs = int(input('Enter the number of messages for each peer to send (0 to terminate)=> '))
    return nMsgs

def startPeers(peerList, nMsgs):
    peerNumber = 0
    # Connect to each of the peers and send the 'initiate' signal:
    for peer in peerList:
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((peer, PEER_TCP_PORT))
        # msg = (peerNumber, nMsgs) # Original
        # AQUI: O myself (peerNumber) será o ID do processo.
        # Estamos passando o IP, mas para o COMPARISONSERVER, um índice pode ser útil.
        # Vamos usar o índice da lista como o 'myself' temporariamente para a comunicação.
        msg = (peerNumber, nMsgs) # (myself, nMsgs)
        msgPack = pickle.dumps(msg)
        clientSock.send(msgPack)
        msgPack = clientSock.recv(512)
        print(pickle.loads(msgPack))
        clientSock.close()
        peerNumber = peerNumber + 1

def waitForLogsAndCompare(N_MSGS, peerList):
    # N agora será o número de peers na lista
    num_expected_peers = len(peerList)
    msgs = [] # each msg is a list of tuples (with the original messages received by the peer processes)

    # Receive the logs of messages from the peer processes
    numPeersReceived = 0
    while numPeersReceived < num_expected_peers: # N aqui é o número de peers esperados
        (conn, addr) = serverSock.accept()
        msgPack = conn.recv(32768 * 2) # Aumenta o buffer para logs maiores
        print ('Received log from peer')
        conn.close()
        msgs.append(pickle.loads(msgPack))
        numPeersReceived = numPeersReceived + 1

    unordered = 0

    # Compare the lists of messages
    # O formato de log agora é (sender_id, message_number, clock)
    # Precisamos comparar a ordem das mensagens (message_number) de cada remetente,
    # e verificar se todos os peers receberam a mesma sequência.
    
    # Exemplo de verificação simples: verificar se a primeira mensagem de cada peer
    # é igual em todos os logs, depois a segunda, etc.
    # Isso assume que todos os peers recebem *todos* as mensagens e *na mesma ordem*.
    # Com relógios lógicos e ACKs, a ordem *deve* ser a mesma.

    # Reorganiza os logs para facilitar a comparação por "rodada" de mensagem original
    # (mensagem original do peer X, message_number Y)
    # Vamos agrupar as mensagens pelo message_number
    
    # Criar um dicionário onde a chave é o message_number e o valor é uma lista
    # de tuplas (sender_id, received_clock) de todos os peers que receberam essa mensagem.
    
    # msgs é uma lista de logs, onde cada log é uma lista de (sender_id, message_number, clock)
    
    # Dicionário para armazenar as mensagens recebidas por "rodada" (message_number original)
    messages_by_round = {} # Key: (sender_id_original, message_number_original), Value: list of (receiving_peer_id, received_clock)

    for peer_log in msgs:
        for msg_entry in peer_log:
            sender_id, message_number, received_clock = msg_entry
            # A chave da rodada é (quem enviou, qual mensagem enviou)
            round_key = (sender_id, message_number)
            
            if round_key not in messages_by_round:
                messages_by_round[round_key] = []
            # Adiciona quem recebeu e qual o relógio lógico do receptor ao receber
            # Aqui, "peer_id" do receptor não está diretamente disponível no loop externo.
            # Podemos assumir que a ordem na lista `msgs` corresponde aos `peerNumber` de `startPeers`.
            # Isso é frágil; o ideal seria cada peer enviar seu próprio ID com o log.
            # Por enquanto, vamos usar o índice.
            
            # Para uma comparação mais robusta, cada peer deve incluir seu ID no log que envia
            # Ex: (my_id, log_list)
            # Para este exemplo, vamos simplificar e comparar a ordem de recebimento de todos os logs.

    # A forma mais simples de verificar "ordem" é ver se todos os logs são idênticos.
    # Se os logs estão em ordem causal e total, e todos receberam tudo, eles devem ser iguais.
    
    if len(msgs) == 0:
        print("No logs received for comparison.")
        return

    # Compara o primeiro log com todos os outros. Se forem diferentes, houve desordem.
    # Isso funciona porque, com Lamport e ACKs, a ordem final de recebimento *deve* ser a mesma em todos os processos.
    first_log = msgs[0]
    
    for i in range(1, len(msgs)):
        if first_log != msgs[i]:
            print(f"Discrepancy found between log 0 and log {i}. Messages are unordered or incomplete.")
            unordered += 1
            # Para depuração, você pode imprimir as diferenças:
            # print("Log 0:", first_log)
            # print(f"Log {i}:", msgs[i])
            # break # Quebra para não contar múltiplos unordered para o mesmo par de logs

    if unordered == 0:
        print('All message rounds were received in the same order by all peers.')
    else:
        print('Found ' + str(unordered) + ' discrepancies in message ordering across peers.')
    
    # Para uma análise mais granular, você poderia iterar sobre as mensagens e verificar se
    # a mensagem no índice `j` do log `i` é a mesma que no log `k`.
    # Mas a comparação de listas inteiras é mais direto para "ordem" garantida.
    
# Initiate server:
mainLoop()
