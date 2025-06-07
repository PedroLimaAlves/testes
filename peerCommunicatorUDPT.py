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

PEERS = [] # Declared globally here

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

# Variable to hold the peer's own ID
myself = -1
# Variable to hold the number of messages to send in the current round
nMsgs = 0

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
    global PEERS # Declare global if you're modifying it here
    clientSock = socket(AF_INET, SOCK_STREAM)
    print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    req = {"op":"list"}
    msg = pickle.dumps(req)
    print ('Getting list of peers from group manager: ', req)
    clientSock.send(msg)
    msg = clientSock.recv(2048)
    PEERS = pickle.loads(msg) # Assignment, so global is needed
    print ('Got list of peers: ', PEERS)
    clientSock.close()
    return PEERS

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

# global logList to store messages received and delivered
logList = [] # Initialized globally

def deliver_messages():
    global expected_clocks
    global logList # Ensure logList is accessible globally
    global myself # Used for printing 'myself'

    while True:
        with queue_lock:
            if not message_queue:
                break # No messages in the queue

            # Peek at the message at the top of the queue without removing it
            next_timestamp, (msg_type, sender_id, msg_content, original_timestamp) = message_queue[0]

            if sender_id not in expected_clocks:
                # Initialize expected clock for this peer.
                # Assuming timestamps start from 1, the first message from a peer will have a timestamp of 1.
                # So we expect expected_clocks[sender_id] + 1 == 1, meaning expected_clocks[sender_id] should be 0.
                expected_clocks[sender_id] = 0

            if next_timestamp == expected_clocks[sender_id] + 1:
                # Message can be delivered
                _, delivered_msg = heapq.heappop(message_queue) # Pop the message
                msg_type, sender_id, msg_content, original_timestamp = delivered_msg

                expected_clocks[sender_id] += 1

                if msg_type == 'DATA':
                    print(f'Message {msg_content} from process {sender_id} (timestamp: {original_timestamp}, delivered at: {next_timestamp})')
                    logList.append((sender_id, msg_content, original_timestamp)) # Store original timestamp for comparison
                    # Send ACK for data message
                    send_ack_message(sender_id, original_timestamp)
                elif msg_type == 'ACK':
                    print(f'ACK received from process {sender_id} for message with original timestamp {original_timestamp} (ACK timestamp: {next_timestamp})')
                elif msg_type == -1: # Stop message
                    print(f'Stop message received from process {sender_id} (timestamp: {original_timestamp})')
                    # Stop messages are just consumed and updated, no specific logging here
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
        # Declare global variables at the very beginning of the function
        global handShakeCount
        global logList
        global expected_clocks
        global myself # Needed to print myself when writing log file
        global PEERS # Might need to access PEERS list
        
        print('Handler is ready. Waiting for the handshakes...')
        
        # logList is already initialized globally. No need to re-initialize here unless it's thread-specific.
        # Assuming one logList for the entire peer process.

        # Wait until handshakes are received from all other processes
        # N-1 because we don't send handshake to ourselves
        while handShakeCount < N - 1:
            try:
                msgPack, addr = self.sock.recvfrom(1024)
                msg_content = pickle.loads(msgPack)
                
                # Handshake message format: ('READY', sender_id, timestamp)
                if isinstance(msg_content, tuple) and len(msg_content) == 3 and msg_content[0] == 'READY':
                    msg_type, sender_id, received_timestamp = msg_content
                    update_logical_clock(received_timestamp) # Update clock on handshake
                    handShakeCount += 1
                    print(f'--- Handshake received from: {sender_id}')
                    
                    # Initialize expected clock for this peer if not already
                    # We initialize expected_clocks[sender_id] to 0, assuming first data message will have timestamp 1
                    if sender_id not in expected_clocks:
                        expected_clocks[sender_id] = 0 # Expecting their first *data* message after handshake to have timestamp 1
                else:
                    print(f"Received non-handshake message during handshake phase: {msg_content}")
            except Exception as e:
                print(f"Error during handshake reception: {e}")
                # Consider breaking or handling specific errors

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        stopCount = 0
        while True:
            try:
                msgPack, addr = self.sock.recvfrom(32768)
                msg = pickle.loads(msgPack)
                
                # Ensure msg is a tuple and has enough elements
                if not isinstance(msg, tuple) or len(msg) < 3:
                    print(f"Received malformed message: {msg}. Skipping.")
                    continue

                msg_type = msg[0]
                # Timestamp is at index 3 for DATA/ACK/STOP
                received_timestamp = msg[3] if len(msg) > 3 else 0 

                # 1º passo: atualiza o relógio lógico;
                update_logical_clock(received_timestamp)

                # 2° passo: coloca a mensagem na fila;
                with queue_lock:
                    # msg format: (type, sender_id, msg_content, original_timestamp)
                    # We store the received_timestamp (Lamport's timestamp) as the priority
                    heapq.heappush(message_queue, (received_timestamp, msg))
                
                # 3° passo: Se é mensagem de dados então enviar ACK (se for mensagem de ACK só faz 1º e 2º passo);
                # This is handled by deliver_messages now

                # 4º passo: verifica se "destrava" a primeira mensagem da fila e entrega a mensagem para a aplicação;
                deliver_messages()

                if msg_type == -1:  # count the 'stop' messages from the other processes
                    stopCount = stopCount + 1
                    # Check if all peers have sent a stop message.
                    # N-1 because we don't count our own stop message if we sent it
                    # (or N if we expect one from ourselves for symmetry, depending on design)
                    # Assuming N-1 other peers will send stop messages.
                    if stopCount == N - 1:
                        # All other peers have finished sending.
                        # Now, ensure all messages in the queue are delivered before stopping.
                        print("All stop messages received. Delivering remaining messages in queue...")
                        # Loop until the message_queue is empty and no more messages can be delivered
                        while True:
                            initial_queue_size = len(message_queue)
                            deliver_messages() # Attempt to deliver all possible messages
                            with queue_lock:
                                # If the queue is empty after delivery attempt, we are done
                                if not message_queue:
                                    break
                                # If the queue size didn't change, it means messages are stuck
                                # (e.g., waiting for an earlier timestamp not yet received)
                                # This could indicate a problem, but for now, we assume they will eventually arrive
                                if len(message_queue) == initial_queue_size:
                                    # This indicates no more messages can be delivered at this moment
                                    # (waiting for an earlier timestamp not yet arrived, or N is not accurate)
                                    # For a robust system, you might add a timeout here.
                                    # For this exercise, we can assume that if all stop messages are received,
                                    # then all data messages eventually will be delivered.
                                    # Break to avoid infinite busy-wait if there's a logic error or packet loss
                                    break # Exit inner while true loop if no progress made

                        print("All messages delivered from queue. MsgHandler is ready to finish.")
                        break  # Exit the main while True loop (for receiving messages)

            except Exception as e:
                print(f"Error during message reception: {e}")
                # In a real system, you might want more sophisticated error handling or a graceful shutdown.
                break # Break on error to prevent infinite loop on bad data

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

        # Reset for the next round
        handShakeCount = 0
        expected_clocks.clear()
        with queue_lock:
            message_queue.clear()
        logList.clear() # Clear logList for the next round

        print("MsgHandler thread finished its round.")
        # Do not exit(0) here, as it kills the entire process.
        # The main thread should manage the lifecycle.
        # Instead, signal the main thread that this round is complete.
        # For simplicity in this example, we'll let the main thread loop.
        # If the main thread relies on this thread exiting to know a round is done,
        # you might need a threading.Event.

# The main loop needs to manage the MsgHandler thread's lifecycle if it doesn't exit(0).
# For now, it will just keep waiting for the next signal.

def send_ack_message(target_peer_id, original_msg_timestamp):
    # To send a proper ACK back, the original DATA message should ideally contain the sender's IP.
    # The current PEERS list contains only IPs without a direct mapping to sender_id.
    # This is a simplification. For now, it will send the ACK to all peers except itself.
    # In a production system, you'd want to store a mapping of peer_id to (IP, Port)
    # or ensure the DATA message includes the sender's full address.

    ack_timestamp = increment_logical_clock()
    ack_msg = ('ACK', myself, original_msg_timestamp, ack_timestamp) # Format: (type, sender_id, original_msg_timestamp, ack_timestamp)
    ack_msg_pack = pickle.dumps(ack_msg)
    
    # Iterate through the global PEERS list to send the ACK.
    # This assumes PEERS contains only IP addresses.
    my_ip = get_public_ip() # Get current peer's IP once for comparison
    for addrToSend in PEERS:
        if addrToSend != my_ip: # Don't send ACK to self
            try:
                sendSocket.sendto(ack_msg_pack, (addrToSend, PEER_UDP_PORT))
                print(f'Sent ACK for message with original timestamp {original_msg_timestamp} to {addrToSend} (ACK timestamp: {ack_timestamp})')
            except Exception as e:
                print(f"Error sending ACK to {addrToSend}: {e}")

# Function to wait for start signal from comparison server:
def waitToStart():
    global myself # Will be assigned here
    global nMsgs # Will be assigned here
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
while True: # Changed from while 1 for clarity
    print('Waiting for signal to start...')
    (myself, nMsgs) = waitToStart() # myself and nMsgs are updated globally here
    print(f'I am up, and my ID is: {myself}')

    if nMsgs == 0:
        print('Terminating.')
        # Clean up sockets before exiting
        sendSocket.close()
        recvSocket.close()
        serverSock.close()
        exit(0)
    
    # Get the latest list of peers before each round
    # The getListOfPeers function already updates the global PEERS list.
    PEERS = getListOfPeers()
    
    # Create receiving message handler thread
    # Only start a new thread if the previous one is not alive (or if it terminated naturally).
    # If MsgHandler.run() doesn't exit(0), then the thread will finish and can be joined.
    # For this current setup, it will re-create.
    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()
    print('Handler started')
    
    # Send handshakes
    my_ip = get_public_ip() # Get current peer's IP once for comparison
    for addrToSend in PEERS:
        if addrToSend != my_ip: # Don't send handshake to self
            print(f'Sending handshake to {addrToSend}')
            # Handshake message now includes the logical clock
            handshake_timestamp = increment_logical_clock()
            # Format: ('READY', sender_id, timestamp)
            msg = ('READY', myself, handshake_timestamp)
            msgPack = pickle.dumps(msg)
            sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
    
    print(f'Main Thread: Sent all handshakes. handShakeCount={handShakeCount}')

    # Wait for handshakes to be received by the MsgHandler.
    # The `pass` loop is a busy-wait and should be replaced with a better sync mechanism
    # like a threading.Event or Condition Variable in production.
    # N-1 because we don't expect a handshake from ourselves.
    while (handShakeCount < N - 1):
        pass
    
    print('Main Thread: All handshakes received by handler. Starting data messages...')

    # Send a sequence of data messages to all other processes
    for msgNumber in range(0, nMsgs):
        current_logical_clock = increment_logical_clock()
        msg_content = msgNumber
        # Message format: (type, sender_id, msg_content, timestamp)
        msg = ('DATA', myself, msg_content, current_logical_clock)
        msgPack = pickle.dumps(msg)
        for addrToSend in PEERS:
            if addrToSend != my_ip: # Don't send message to self
                sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
                print(f'Sent message {msgNumber} with timestamp {current_logical_clock} to {addrToSend}')

    # Tell all processes that I have no more messages to send
    for addrToSend in PEERS:
        if addrToSend != my_ip: # Don't send stop message to self
            stop_timestamp = increment_logical_clock()
            # Format: (type, sender_id, msg_content, timestamp)
            # Use -1 for type and msg_content to signify a stop message, include my ID for logging/debug
            msg = (-1, myself, -1, stop_timestamp) 
            msgPack = pickle.dumps(msg)
            sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))

    # Wait for the MsgHandler thread to finish processing messages for this round.
    # This is important before the main thread loops back to waitToStart().
    msgHandler.join()
    print("Main Thread: MsgHandler finished for this round. Preparing for next round...")
