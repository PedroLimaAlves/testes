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

# Make myself (peer ID) a global variable to be accessible in MsgHandler
myself = -1 
logList = [] # Make logList a global variable to be easily accessible for appending and sending

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
    global myself # Declare myself as global to use it
    global logList # Declare logList as global to use it

    while True:
        with queue_lock:
            if not message_queue:
                break # No messages in the queue

            # Check if the message at the top of the queue is ready for delivery
            # A message is ready if its timestamp matches the expected clock for its sender
            # Structure: (timestamp, (msg_type, sender_id, msg_content, original_timestamp))
            next_timestamp, (msg_type, sender_id, msg_content, original_timestamp) = message_queue[0]

            if sender_id not in expected_clocks:
                # This should ideally be initialized when handshakes are received
                expected_clocks[sender_id] = 0 

            # Condition for ordered delivery: the message's timestamp must be 
            # exactly one greater than the last delivered message from that sender.
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
                elif msg_type == -1: # Handle the stop message
                    print(f'Stop message received from process {sender_id} (timestamp: {original_timestamp}, delivered at: {next_timestamp})')
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
        # Mova todas as declarações global para o topo do método
        global handShakeCount
        global logList
        global myself # 'myself' will be set by the main thread, and MsgHandler needs to access it
        global expected_clocks
        global message_queue # Ensure message_queue is accessible for clearing

        print('Handler is ready. Waiting for the handshakes...')
        
        # Wait until handshakes are received from all other processes
        while handShakeCount < N - 1: # N-1 because we don't handshake with ourselves
            msgPack, addr = self.sock.recvfrom(1024)
            msg = pickle.loads(msgPack)
            msg_type = msg[0]
            sender_id = msg[1]
            # timestamp = msg[2] # Not strictly needed for handshake processing, but good to acknowledge it's there.

            if msg_type == 'READY':
                handShakeCount += 1
                print(f'--- Handshake received from: {sender_id}')
                # Initialize expected clock for this peer when handshake is received
                expected_clocks[sender_id] = 0
            else:
                # This should not happen during handshake phase, but good for debugging
                print(f"Unexpected message type during handshake: {msg_type}")

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        stopCount = 0
        while True:
            msgPack, addr = self.sock.recvfrom(32768)
            msg_data = pickle.loads(msgPack)
            msg_type = msg_data[0]
            
            # Ensure msg_data has enough elements before accessing index 3
            received_timestamp = msg_data[3] if len(msg_data) > 3 else 0 

            # 1º passo: atualiza o relógio lógico;
            update_logical_clock(received_timestamp)

            # 2° passo: coloca a mensagem na fila;
            with queue_lock:
                # Store (received_timestamp, original_message_tuple) for priority queue
                heapq.heappush(message_queue, (received_timestamp, msg_data))
            
            # 3° passo: Se é mensagem de dados então enviar ACK (se for mensagem de ACK só faz 1º e 2º passo);
            # 4º passo: verifica se "destrava" a primeira mensagem da fila e entrega a mensagem para a aplicação;
            deliver_messages()

            if msg_type == -1:  # count the 'stop' messages from the other processes
                stopCount = stopCount + 1
                if stopCount == N - 1: # N-1 because we wait for stop signals from other peers, not ourselves
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

        # Reset the handshake counter for a new round
        handShakeCount = 0
        
        # Reset expected clocks for a new round
        expected_clocks.clear()
        
        # Clear the message queue for a new round
        with queue_lock:
            message_queue.clear()
        
        logList.clear() # Clear the log list for a new round
        
        # This exit will stop the entire process. If you want to run multiple rounds
        # without restarting the peer, you'll need to remove this and manage the lifecycle
        # differently (e.g., by signaling the main thread to prepare for a new round).
        exit(0)

def send_ack_message(sender_id, original_msg_timestamp):
    global myself # Declare myself as global to use it
    global PEERS # Declare PEERS as global to use it

    ack_timestamp = increment_logical_clock()
    
    # Message format: (type, sender_id_of_ack, original_msg_timestamp, ack_timestamp)
    ack_msg = ('ACK', myself, original_msg_timestamp, ack_timestamp)
    ack_msg_pack = pickle.dumps(ack_msg)
    
    # In a real scenario, you would send the ACK only to the `sender_id`
    # You would need a mapping from `sender_id` to `IP_address` if `PEERS` only contains IPs.
    # For now, as a demonstration, we will send to all peers except ourselves.
    # This is not efficient for a targeted ACK but shows the ACK message.
    
    # To properly send an ACK to a specific sender, the initial DATA message
    # should carry the sender's IP address.
    # For instance, if msg_data was ('DATA', myself, msg_content, current_logical_clock, my_ip),
    # then you could use my_ip here.
    
    # Assuming for this simplified example that we need to find the IP of the original sender.
    # If the PEERS list is just IPs, this part is tricky without a proper peer-ID-to-IP mapping.
    # For simplicity, let's assume we iterate all peers and send to all *other* peers.
    
    # IMPORTANT: THIS IS A SIMPLIFICATION. A proper ACK targets the sender.
    # You need a mechanism to map `sender_id` back to their IP address if `PEERS` is just a list of IPs.
    # For now, it sends to all other peers, demonstrating the ACK structure.
    for addrToSend in PEERS:
        if addrToSend != get_public_ip(): # Don't send ACK to self
            sendSocket.sendto(ack_msg_pack, (addrToSend,PEER_UDP_PORT))
            print(f'Sent ACK for message with original timestamp {original_msg_timestamp} to {addrToSend} (ACK timestamp: {ack_timestamp})')


# Function to wait for start signal from comparison server:
def waitToStart():
    global myself # Declare myself as global here too

    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    myself = msg[0] # Set the global myself
    nMsgs = msg[1]
    conn.send(pickle.dumps(f'Peer process {myself} started.'))
    conn.close()
    return (myself,nMsgs)

# From here, code is executed when program starts:
registerWithGroupManager()
while 1:
    print('Waiting for signal to start...')
    (myself, nMsgs) = waitToStart() # myself is set here
    print(f'I am up, and my ID is: {myself}')

    if nMsgs == 0:
        print('Terminating.')
        exit(0)
    
    # Initialize expected clocks for all peers (including self if needed, but not for sending)
    global PEERS # ensure PEERS is accessible
    PEERS = getListOfPeers() # Get the latest list of peers
    
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
            # Message format: (type, sender_id, dummy_content, timestamp)
            msg = (-1, myself, -1, stop_timestamp) # -1 is the stop signal
            msgPack = pickle.dumps(msg)
            sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
