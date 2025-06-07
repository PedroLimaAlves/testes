# constMPT.py

N = 2  # número de peers
PORT_BASE = 5000
PORT_LOG = 6000
PORT_SERVER = 7000

MSG_COUNT = 2  # número de mensagens que cada peer deve enviar

PEER_TIMEOUT = 10  # segundos para espera de handshakes
SERVER_IP = '172.31.89.212'  # IP do servidor coletor de logs

# groupMngrT.py

import socket
import threading
import constMPT as const

peers = []

def handle_peer(conn, addr):
    ip = addr[0]
    if ip not in peers:
        peers.append(ip)
        print(f"Peer {len(peers)-1} registered: {ip}")
    conn.close()

def main():
    sock = socket.socket()
    sock.bind(('', const.PORT_BASE))
    sock.listen(const.N)
    print(f"Listening for {const.N} peers...")

    while len(peers) < const.N:
        conn, addr = sock.accept()
        threading.Thread(target=handle_peer, args=(conn, addr)).start()

    print("All peers registered:", peers)
    sock.close()

if __name__ == '__main__':
    main()

# comparisonServerT.py

import socket
import threading
import constMPT as const

logs = []

def handle_logs(conn):
    while True:
        data = conn.recv(1024)
        if not data:
            break
        logs.append(data.decode())
        print(data.decode())
    conn.close()

def main():
    sock = socket.socket()
    sock.bind(('', const.PORT_LOG))
    sock.listen(const.N)
    print(f"Server waiting for logs from {const.N} peers...")

    for _ in range(const.N):
        conn, _ = sock.accept()
        threading.Thread(target=handle_logs, args=(conn,)).start()

if __name__ == '__main__':
    main()

# peerCommunicatorUDPT.py

import socket
import threading
import time
import constMPT as const

server_ip = const.SERVER_IP

my_ip = socket.gethostbyname(socket.gethostname())
print(f"My IP is {my_ip}")

lamport_clock = 0
peers = []


def listener(sock):
    global lamport_clock
    while True:
        data, addr = sock.recvfrom(1024)
        msg = data.decode()
        sender_clock, sender_id = map(int, msg.split(':'))
        lamport_clock = max(lamport_clock, sender_clock) + 1
        log_msg = f"Received {msg} from {addr[0]} | Clock updated to {lamport_clock}"
        print(log_msg)
        send_log(log_msg)


def send_log(message):
    log_sock = socket.socket()
    log_sock.connect((server_ip, const.PORT_LOG))
    log_sock.sendall(message.encode())
    log_sock.close()


def handshake(sock):
    for peer_ip in peers:
        sock.sendto(b'HS', (peer_ip, const.PORT_BASE))


def wait_handshakes(sock):
    count = 0
    while count < const.N-1:
        data, addr = sock.recvfrom(1024)
        if data == b'HS':
            count += 1
    print("Received all handshakes")


def message_loop(sock):
    global lamport_clock
    for i in range(const.MSG_COUNT):
        time.sleep(1)
        lamport_clock += 1
        for peer_ip in peers:
            msg = f"{lamport_clock}:{i}"
            sock.sendto(msg.encode(), (peer_ip, const.PORT_BASE))
        log_msg = f"Sent message {i} | Clock is {lamport_clock}"
        print(log_msg)
        send_log(log_msg)


def main():
    global peers

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', const.PORT_BASE))

    mgr_sock = socket.socket()
    mgr_sock.connect((server_ip, const.PORT_BASE))
    mgr_sock.close()

    time.sleep(2)

    peers = []
    with open('peers.txt') as f:
        for line in f:
            ip = line.strip()
            if ip != my_ip:
                peers.append(ip)

    print("Peers:", peers)

    threading.Thread(target=listener, args=(sock,), daemon=True).start()

    handshake(sock)
    wait_handshakes(sock)

    message_loop(sock)

    time.sleep(5)

if __name__ == '__main__':
    main()
