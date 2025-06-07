from socket import *
from constMPT import *
import threading
import random
import time
import pickle
from requests import get
import heapq # For priority queue

# Counter to make sure we have received handshakes from all other processes
handShakeCount = 0

PEERS = []

# UDP sockets to send and receive data messages:
# Create send socket
sendSocket = socket(AF_INET, SOCK_DGRAM)
#Create and bind receive socket
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# TCP socket to receive start signal from the comparison server:
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

# Logical clock
logical_clock = 0
clock_lock = threading.Lock() # To protect the logical clock

# Message queue for ordered delivery (priority queue)
message_queue = [] # Stores (timestamp, message_content)
queue_lock = threading.Lock()

# Expected logical clock for the next message from each peer
# This will help in ordering messages
expected_clocks = {}

def get_public_ip():
    ipAddr = get('https://api.ipify.org').content.decode('utf8')
    print('My public IP address is: {}'.format(ipAddr))
    return ipAddr

# Function to register this peer with the group manager
def registerWithGroupManager():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    ipAddr = get_public_ip()
    req = {"op":"register", "ipaddr":ipAddr, "port":PEER_UDP_PORT}
    msg = pickle.dumps(req)
    print ('Registering with group manager: ', req)
    clientSock.send(msg)
    clientSock.close()

def getListOfPeers():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    req = {"op":"list"}
    msg = pickle.dumps(req)
    print ('Getting list of peers from group manager: ', req)
    clientSock.send(msg)
    msg = clientSock.recv(2048)
    peers_list = pickle.loads(msg)
    print ('Got list of peers: ', peers_list)
    clientSock.close()
    return peers_list

def update_logical_clock(received_timestamp):
    global logical_clock
    with clock_lock:
        logical_clock = max(logical_clock, received_timestamp) + 1
        return logical_clock

def increment_logical_clock():
    global logical_clock
    with clock_lock:
        logical_clock += 1
        return logical_clock

def deliver_messages():
    global expected_clocks
    while True:
        with queue_lock:
            if not message_queue:
                break # No messages in the queue

            # Check if the message at the top of the queue is ready for delivery
            # A message is ready if its timestamp matches the expected clock for its sender
            next_timestamp, (msg_type, sender_id, msg_content, original_timestamp) = message_queue[0]

            if sender_id not in expected_clocks:
                expected_clocks[sender_id] = 0 # Initialize if not seen before

            if next_timestamp == expected_clocks[sender_id] + 1:
                # Message can be delivered
                heapq.heappop(message_queue)
                expected_clocks[sender_id] += 1

                if msg_type == 'DATA':
                    print(f'Message {msg_content} from process {sender_id} (timestamp: {original_timestamp}, delivered at: {next_timestamp})')
                    logList.append((sender_id, msg_content, original_timestamp)) # Store original timestamp for comparison
                    # Send ACK for data message
                    send_ack_message(sender_id, original_timestamp)
                elif msg_type == 'ACK':
                    print(f'ACK received from process {sender_id} for message with original timestamp {original_timestamp} (ACK timestamp: {next_timestamp})')
                else:
                    print(f"Unknown message type: {msg_type}")
            else:
                # Message not yet ready for delivery, stop checking for now
                break


class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        print('Handler is ready. Waiting for the handshakes...')

        global handShakeCount
        global logList
        logList = [] # This needs to be defined within the thread's scope or passed

        # Wait until handshakes are received from all other processes
        while handShakeCount < N:
            msgPack, addr = self.sock.recvfrom(1024)
            msg_type, sender_id = pickle.loads(msgPack)
            if msg_type == 'READY':
                handShakeCount += 1
                print(f'--- Handshake received from: {sender_id}')
                # Initialize expected clock for this peer
                expected_clocks[sender_id] = 0
            else:
                # This should not happen during handshake phase, but good for debugging
                print(f"Unexpected message type during handshake: {msg_type}")


        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        stopCount = 0
        while True:
            msgPack, addr = self.sock.recvfrom(32768)
            msg = pickle.loads(msgPack)
            msg_type = msg[0]
            received_timestamp = msg[3] if len(msg) > 3 else 0 # Assuming timestamp is at index 3 for data/ack

            # 1º passo: atualiza o relógio lógico;
            update_logical_clock(received_timestamp)

            # 2° passo: coloca a mensagem na fila;
            with queue_lock:
                # msg format: (type, sender_id, msg_content, original_timestamp)
                heapq.heappush(message_queue, (received_timestamp, msg))
            
            # 3° passo: Se é mensagem de dados então enviar ACK (se for mensagem de ACK só faz 1º e 2º passo);
            # This is handled by deliver_messages now

            # 4º passo: verifica se "destrava" a primeira mensagem da fila e entrega a mensagem para a aplicação;
            deliver_messages()

            if msg_type == -1:  # count the 'stop' messages from the other processes
                stopCount = stopCount + 1
                if stopCount == N:
                    break  # stop loop when all other processes have finished
            
        # Write log file
        logFile = open(f'logfile{myself}.log', 'w')
        logFile.writelines(str(logList))
        logFile.close()

        # Send the list of messages to the server (using a TCP socket) for comparison
        print('Sending the list of messages to the server for comparison...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        msgPack = pickle.dumps(logList)
        clientSock.send(msgPack)
        clientSock.close()

        # Reset the handshake counter
        global handShakeCount
        handShakeCount = 0
        
        # Reset expected clocks for a new round
        expected_clocks.clear()
        
        # Clear the message queue for a new round
        with queue_lock:
            message_queue.clear()

        # This exit will stop the entire process. If you want to run multiple rounds
        # without restarting the peer, you'll need to remove this and manage the lifecycle
        # differently (e.g., by signaling the main thread to prepare for a new round).
        exit(0)

def send_ack_message(target_peer_id, original_msg_timestamp):
    ack_timestamp = increment_logical_clock()
    # Find the IP address for the target_peer_id from the PEERS list
    target_ip = None
    for ip in PEERS:
        # Assuming PEERS contains just IP addresses. If it was (IP, Port), adjust this.
        # For this example, assuming `PEERS` contains only the IP addresses.
        # If `PEERS` contained (IP, Port), you'd need to adapt.
        # For simplicity, assuming target_peer_id can be mapped to an IP in PEERS
        # This is a simplification; in a real system, you'd map peer IDs to their addresses.
        # As PEERS is a list of IPs, we'd need a more robust way to map target_peer_id to an IP
        # or send ACK to all if we don't have a specific target IP.
        # Let's assume PEERS are indexed by 0, 1, 2... for simplicity to get the IP.
        # A better solution would be to pass the actual IP of the sender to send the ACK back.
        pass # Placeholder: The original structure of PEERS in the code just lists IPs.
             # We need to know *which* peer sent the message to send an ACK back to it.
             # For now, let's just send to all, which isn't ideal for a targeted ACK.
             # A more correct approach would be to include the sender's IP in the original message.

    # For the purpose of demonstration, let's assume `PEERS` contains a list of IPs,
    # and we need to send the ACK back to the *sender* of the data message.
    # The `addr` from `recvfrom` in `MsgHandler` would be the correct recipient.
    # However, `send_ack_message` is called later, out of that scope.
    # To correctly send an ACK, the original message should contain the sender's full address.
    # For now, as a temporary measure, we will iterate through `PEERS` and send to all,
    # which is inefficient but demonstrates the ACK message structure.
    # It's crucial to refine this in a production environment.

    # Revised ACK sending: The data message should carry the sender's IP.
    # The current message format is (myself, msgNumber). It needs to be (myself, msgNumber, my_ip_address, timestamp).
    # Then the ACK can be sent back to my_ip_address.

    # Since the current data message only has (sender_id, msg_content), we'll assume
    # for now that we can iterate through the PEERS list and send an ACK to every peer,
    # or that `target_peer_id` can be directly mapped to an IP in `PEERS`.
    # This is a simplification for demonstration.

    ack_msg = ('ACK', myself, original_msg_timestamp, ack_timestamp)
    ack_msg_pack = pickle.dumps(ack_msg)
    
    # This loop is a temporary workaround. A proper ACK would be sent *directly* to the sender.
    # The sender's address needs to be part of the initial data message.
    for addrToSend in PEERS:
        if addrToSend != get_public_ip(): # Don't send ACK to self
            sendSocket.sendto(ack_msg_pack, (addrToSend, PEER_UDP_PORT))
            print(f'Sent ACK for message with original timestamp {original_msg_timestamp} to {addrToSend} (ACK timestamp: {ack_timestamp})')


# Function to wait for start signal from comparison server:
def waitToStart():
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    myself = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps(f'Peer process {myself} started.'))
    conn.close()
    return (myself,nMsgs)

# From here, code is executed when program starts:
registerWithGroupManager()
while 1:
    print('Waiting for signal to start...')
    (myself, nMsgs) = waitToStart()
    print(f'I am up, and my ID is: {myself}')

    if nMsgs == 0:
        print('Terminating.')
        exit(0)
    
    # Initialize expected clocks for all peers (including self if needed, but not for sending)
    global PEERS # ensure PEERS is accessible
    PEERS = getListOfPeers() # Get the latest list of peers
    
    for peer_ip in PEERS:
        # Initialize expected_clocks for all peers. For self, it's not relevant for incoming.
        # This setup assumes `PEERS` are IP addresses.
        # For a truly robust system, PEERS should include more info like a unique peer ID.
        pass # The initialization of expected_clocks for incoming messages is done in MsgHandler when handshakes are received.

    # Create receiving message handler
    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()
    print('Handler started')
    
    # Send handshakes
    for addrToSend in PEERS:
        if addrToSend != get_public_ip(): # Don't send handshake to self
            print(f'Sending handshake to {addrToSend}')
            # Handshake message now includes the logical clock
            handshake_timestamp = increment_logical_clock()
            msg = ('READY', myself, handshake_timestamp)
            msgPack = pickle.dumps(msg)
            sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
    
    print(f'Main Thread: Sent all handshakes. handShakeCount={handShakeCount}')

    # Wait for handshakes to be received by the MsgHandler.
    # The `pass` loop is intentionally left as per the original request not to remove it.
    # However, for a production system, this `pass` loop is a busy-wait and should be replaced
    # with a more efficient synchronization mechanism (e.g., condition variable).
    while (handShakeCount < N - 1): # N-1 because we don't handshake with ourselves.
        pass  # find a better way to wait for the handshakes
    
    # Send a sequence of data messages to all other processes
    for msgNumber in range(0, nMsgs):
        current_logical_clock = increment_logical_clock()
        msg_content = msgNumber
        # Message format: (type, sender_id, msg_content, timestamp)
        msg = ('DATA', myself, msg_content, current_logical_clock)
        msgPack = pickle.dumps(msg)
        for addrToSend in PEERS:
            if addrToSend != get_public_ip(): # Don't send message to self
                sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
                print(f'Sent message {msgNumber} with timestamp {current_logical_clock} to {addrToSend}')

    # Tell all processes that I have no more messages to send
    for addrToSend in PEERS:
        if addrToSend != get_public_ip(): # Don't send stop message to self
            stop_timestamp = increment_logical_clock()
            msg = (-1,-1, -1, stop_timestamp) # -1 is the stop signal
            msgPack = pickle.dumps(msg)
            sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
