from socket import *
from constMPT import *
import threading
import pickle
from requests import get
import heapq
from collections import defaultdict
import time
import sys

# Variáveis globais
handShakeCount = 0
PEERS = []
lamport_clock = 0
myself = -1
N = 4  # default, mas vai ser ajustado dinamicamente

# Estruturas para ordenação
hold_back_queue = defaultdict(list)
expected_seq = defaultdict(int)
delivered_log = []  # <== Adicionado: log de mensagens entregues

# Sockets
send_socket = socket(AF_INET, SOCK_DGRAM)
recv_socket = socket(AF_INET, SOCK_DGRAM)
recv_socket.settimeout(5.0)  # Timeout para evitar bloqueio eterno
recv_socket.bind(('0.0.0.0', PEER_UDP_PORT))

# Constantes para tipos de mensagem
MSG_TYPES = {
    'READY': 0,
    'DATA': 1,
    'ACK': 2,
    'END': 3
}

def get_public_ip():
    return get('https://api.ipify.org').text

def get_peer_list():
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    req = {"op":"list"}
    msg = pickle.dumps(req)
    clientSock.send(msg)
    msg = clientSock.recv(2048)
    clientSock.close()
    peerList = pickle.loads(msg)
    return peerList

def get_unique_peers(peers_list):
    """Remove peers duplicados mantendo a ordem"""
    seen = set()
    return [p for p in peers_list if not (p in seen or seen.add(p))]

def create_message(msg_type, msg_id=0, ack_for=0):
    global lamport_clock
    lamport_clock += 1
    return {
        'type': msg_type,
        'sender': myself,
        'id': msg_id,
        'ack_for': ack_for,
        'timestamp': lamport_clock
    }

class PeerSystem:
    def _init_(self):
        self.running = True
        self.handshake_received = set()
        self.peer_lock = threading.Lock()
        
    def reset(self):
        with self.peer_lock:
            global handShakeCount, lamport_clock
            handShakeCount = 0
            lamport_clock = 0
            self.handshake_received.clear()
            hold_back_queue.clear()
            expected_seq.clear()
            delivered_log.clear()

system = PeerSystem()

def message_handler():
    print(f"Peer {myself} message handler started")
    
    while system.running:
        try:
            data, addr = recv_socket.recvfrom(2048)
            msg = pickle.loads(data)
            
            # Atualização do relógio lógico
            system.peer_lock.acquire()
            lamport_clock = max(lamport_clock, msg['timestamp']) + 1
            
            # Processamento de mensagens
            if msg['type'] == 'READY':
                if msg['sender'] not in system.handshake_received:
                    system.handshake_received.add(msg['sender'])
                    ack_msg = create_message('ACK', ack_for=msg['id'])
                    send_socket.sendto(pickle.dumps(ack_msg), (addr[0], PEER_UDP_PORT))
                    
            elif msg['type'] == 'DATA':
                heapq.heappush(hold_back_queue[msg['sender']], (msg['timestamp'], msg))
                ack_msg = create_message('ACK', ack_for=msg['id'])
                send_socket.sendto(pickle.dumps(ack_msg), (addr[0], PEER_UDP_PORT))
                
            elif msg['type'] == 'ACK':
                pass  # Tratamento opcional de ACKs
                
            elif msg['type'] == 'END':
                system.running = False
            
            # Verificar mensagens prontas para entrega
            deliver_messages()
            
            system.peer_lock.release()
            
        except timeout:
            continue
        except Exception as e:
            print(f"Error: {str(e)}")
            continue

def deliver_messages():
    for sender in list(hold_back_queue.keys()):
        while hold_back_queue[sender] and hold_back_queue[sender][0][1]['id'] == expected_seq[sender]:
            _, msg = heapq.heappop(hold_back_queue[sender])
            print(f"Peer {myself} delivered message {msg['id']} from {msg['sender']} (ts: {msg['timestamp']})")
            delivered_log.append((msg['sender'], msg['id'], msg['timestamp']))  # <== Adicionado ao log
            expected_seq[sender] += 1

# NOVA FUNÇÃO: servidor TCP para receber 'initiate'
def peer_tcp_server():
    global myself, N
    tcp_sock = socket(AF_INET, SOCK_STREAM)
    tcp_sock.bind(('0.0.0.0', PEER_TCP_PORT))
    tcp_sock.listen(1)
    print(f"Peer {myself} TCP server listening on port {PEER_TCP_PORT}")
    conn, addr = tcp_sock.accept()
    msgPack = conn.recv(512)
    peerNumber, nMsgs = pickle.loads(msgPack)
    print(f"Peer {myself} received initiate: peerNumber={peerNumber}, nMsgs={nMsgs}")
    conn.send(pickle.dumps("ACK: ready to start"))
    conn.close()
    
    # Após receber 'initiate', começa a enviar mensagens
    # Fase de handshake
    ready_msg = create_message('READY')
    for peer in PEERS:
        send_socket.sendto(pickle.dumps(ready_msg), (peer, PEER_UDP_PORT))
    
    # Espera handshakes
    while len(system.handshake_received) < N-1 and system.running:
        time.sleep(0.1)
    
    print(f"Peer {myself} completed handshakes with {len(system.handshake_received)} peers")
    
    # Envio de mensagens
    for i in range(nMsgs):
        data_msg = create_message('DATA', msg_id=i)
        for peer in PEERS:
            send_socket.sendto(pickle.dumps(data_msg), (peer, PEER_UDP_PORT))
        time.sleep(0.5)  # Intervalo entre mensagens
    
    # Finalização
    end_msg = create_message('END')
    for peer in PEERS:
        send_socket.sendto(pickle.dumps(end_msg), (peer, PEER_UDP_PORT))

def main():
    global myself, PEERS, N
    
    # Registro e obtenção de peers
    peers = get_unique_peers(get_peer_list())
    if not peers:
        print("No valid peers found!")
        return
    
    PEERS = [p for p in peers if p != get_public_ip()]
    N = len(PEERS) + 1  # Ajuste dinâmico do número esperado de peers
    
    # Configuração inicial
    myself = peers.index(get_public_ip()) if get_public_ip() in peers else -1
    if myself == -1:
        print("Peer not in peers list!")
        return
    
    print(f"Peer {myself} started with {len(PEERS)} other peers")
    
    # Thread de tratamento de mensagens UDP
    handler_thread = threading.Thread(target=message_handler)
    handler_thread.daemon = True
    handler_thread.start()
    
    # Thread servidor TCP (para receber 'initiate')
    tcp_server_thread = threading.Thread(target=peer_tcp_server)
    tcp_server_thread.start()
    
    # Espera a thread TCP terminar (peer_tcp_server inclui o envio das mensagens)
    tcp_server_thread.join()
    
    # Depois de tudo, enviar log para ComparisonServer
    print(f"Peer {myself} sending log to ComparisonServer...")
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((SERVER_ADDR, SERVER_PORT))
    clientSock.send(pickle.dumps(delivered_log))
    clientSock.close()
    print(f"Peer {myself} finished and sent log")
    
    # Terminar
    system.running = False
    handler_thread.join(timeout=5)
    print(f"Peer {myself} fully terminated.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nShutting down...")
        system.running = False
        sys.exit(0)
