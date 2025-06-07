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
                # This peer might have joined late or hasn't sent a handshake yet.
                # For ordering, we need an initial expected clock.
                # Initialize it to original_timestamp - 1, expecting the next message to have this timestamp
                # or a value derived from the first message's timestamp.
                # A more robust solution might involve peers announcing their initial clock.
                expected_clocks[sender_id] = original_timestamp - 1 # Expecting the first message to be 0 or 1.
                                                                    # If timestamps start at 1, this should be 0.
                                                                    # This logic might need refinement based on exact timestamping.


            if next_timestamp == expected_clocks[sender_id] + 1:
                # Message can be delivered
                _, delivered_msg = heapq.heappop(message_queue) # Pop the message
                msg_type, sender_id, msg_content, original_timestamp = delivered_msg

                expected_clocks[sender_id] += 1

                if msg_type == 'DATA':
                    print(
