from socket import *
from constMPT import *
import threading
import random
import time
import pickle
import sys # Import sys for sys.exit()
from requests import get # Using requests for simplicity to get public IP

# Global variables for logical clock and message handling
logical_clock = 0
# Stores messages from other peers that are awaiting delivery
# Format: {sender_id: [(type, sender_id, msg_number, timestamp), ...]}
incoming_message_queues = {} 
# Stores the next expected message number from each peer
# Format: {sender_id: next_expected_msg_number}
expected_sequence_numbers = {} 

# Counter to make sure we have received handshakes from all other processes
handShakeCount = 0

PEERS = [] # List of peer IP addresses, populated by getListOfPeers()
myself = -1 # ID of this peer
nMsgs = 0   # Number of messages this peer will send

# UDP sockets to send and receive data messages:
# Create send socket
sendSocket = socket(AF_INET, SOCK_DGRAM)
# Create and bind receive socket
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# TCP socket to receive start signal from the comparison server:
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

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
    # Update the global PEERS list
    global PEERS
    PEERS = pickle.loads(msg)
    print ('Got list of peers: ', PEERS)
    clientSock.close()
    return PEERS

def update_logical_clock(received_timestamp=None):
    global logical_clock
    if received_timestamp is not None:
        logical_clock = max(logical_clock, received_timestamp) + 1
    else:
        logical_clock += 1

def send_ack(target_ip, original_sender_id, original_msg_number):
    update_logical_clock() # Update clock for sending ACK
    # ACK message format: ('ACK', my_id, original_sender_id, original_msg_number, my_logical_clock)
    ack_msg = ('ACK', myself, original_sender_id, original_msg_number, logical_clock)
    ack_msg_pack = pickle.dumps(ack_msg)
    sendSocket.sendto(ack_msg_pack, (target_ip, PEER_UDP_PORT))
    # print(f"Sent ACK for msg {original_msg_number} from {original_sender_id} to {target_ip} with clock {logical_clock}")

def try_deliver_messages(sender_id, logList):
    """
    Attempts to deliver messages from a specific sender that are in order.
    Messages are delivered if their sequence number matches the expected one.
    """
    global expected_sequence_numbers

    # Ensure the sender has an entry in expected_sequence_numbers and incoming_message_queues
    if sender_id not in expected_sequence_numbers:
        expected_sequence_numbers[sender_id] = 0
    if sender_id not in incoming_message_queues:
        incoming_message_queues[sender_id] = []

    # Sort the queue by (message_number) to ensure we always pick the lowest sequence number
    # This is important if messages arrive out of order and we want to deliver them sequentially.
    incoming_message_queues[sender_id].sort(key=lambda x: x[2]) # Sort by message_number

    while incoming_message_queues[sender_id] and \
          incoming_message_queues[sender_id][0][2] == expected_sequence_numbers[sender_id]:
        
        msg_to_deliver = incoming_message_queues[sender_id].pop(0) # Get and remove the first message
        # msg_to_deliver format: (type, sender_id, message_number, timestamp)

        # Deliver to application (in this case, print and add to logList)
        print('Message ' + str(msg_to_deliver[2]) + ' from process ' + str(msg_to_deliver[1]) + \
              ' at logical time ' + str(msg_to_deliver[3]))
        logList.append(msg_to_deliver) # Add the full message (including timestamp) to log
        expected_sequence_numbers[sender_id] += 1
    
# Only one MsgHandler class definition
class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock
        self.logList = []

    def run(self):
        print('Handler is ready. Waiting for the handshakes...')
        
        global handShakeCount
        global N # N is not defined globally, leading to NameError if not provided by external constMPT.py
        
        # Wait until handshakes are received from all other processes
        while handShakeCount < N:
            try:
                # Use recvfrom to get the sender's address
                msgPack, sender_addr_port = self.sock.recvfrom(1024)
                msg = pickle.loads(msgPack)
                sender_ip = sender_addr_port[0]

                if msg[0] == 'READY': # Handshake message type
                    handShakeCount += 1
                    print('--- Handshake received from: ', msg[1])
                    if msg[1] not in expected_sequence_numbers:
                        expected_sequence_numbers[msg[1]] = 0
                    if msg[1] not in incoming_message_queues:
                        incoming_message_queues[msg[1]] = []
            except Exception as e:
                print(f"Error receiving handshake: {e}")

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')
        
        stopCount = 0 
        while True:            
            try:
                msgPack, sender_addr_port = self.sock.recvfrom(1024)
                msg = pickle.loads(msgPack)
                sender_ip = sender_addr_port[0] # IP of the message sender

                # Update logical clock upon receiving any message
                # msg format for DATA/ACK: (type, sender_id, msg_number, timestamp)
                # msg format for READY: ('READY', sender_id) - no timestamp, use current clock
                # msg format for STOP: (-1, -1) - no timestamp, use current clock
                
                # Check message type to safely access timestamp
                if msg[0] in ['DATA', 'ACK']:
                    update_logical_clock(msg[3]) # msg[3] is the timestamp
                else:
                    update_logical_clock() # Just increment local clock

                if msg[0] == -1:    # 'stop' message
                    stopCount += 1
                    if stopCount == N: # All processes have finished
                        break
                elif msg[0] == 'DATA': # Data message received
                    sender_id = msg[1] # The ID of the peer that sent the DATA message
                    
                    # Add to queue
                    if sender_id not in incoming_message_queues:
                        incoming_message_queues[sender_id] = []
                    incoming_message_queues[sender_id].append(msg)
                    
                    # Send ACK back to the original sender's IP
                    send_ack(sender_ip, sender_id, msg[2]) # msg[2] is the message_number

                    # Attempt to deliver messages from this sender
                    try_deliver_messages(sender_id, self.logList)
                elif msg[0] == 'ACK': # ACK message received
                    pass # Do nothing else for ACK for now as per instructions (only 1st and 2nd step)

            except Exception as e:
                print(f"Error in MsgHandler receiving loop: {e}")

        # After the loop, send the log
        # Write log file
        logFile = open('logfile'+str(myself)+'.log', 'w')
        logFile.writelines(str(self.logList))
        logFile.close()
        
        # Send the list of messages to the server (using a TCP socket) for comparison
        print('Sending the list of messages to the server for comparison...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        msgPack = pickle.dumps(self.logList)
        clientSock.send(msgPack)
        clientSock.close()
        
        # Reset the handshake counter for next round if this thread were to be reused.
        handShakeCount = 0
        sys.exit(0) # Terminate the peer process after a round

# Function to wait for start signal from comparison server:
def waitToStart():
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    
    global myself, nMsgs
    myself = msg[0] # ID of this peer
    nMsgs = msg[1]  # Number of messages to send
    
    conn.send(pickle.dumps('Peer process '+str(myself)+' started.'))
    conn.close()
    return (myself, nMsgs)

# From here, code is executed when program starts:
registerWithGroupManager()
while 1:
    print('Waiting for signal to start...')
    (myself, nMsgs) = waitToStart()
    print('I am up, and my ID is: ', str(myself))
    
    if nMsgs == 0:
        print('Terminating.')
        # Cleanly close sockets before exiting
        sendSocket.close()
        recvSocket.close()
        serverSock.close()
        sys.exit(0)

    # Initialize expected sequence numbers for all peers for the new round
    # This must be done for all peers, not just those from whom we've received handshakes.
    # We need the total number of peers (N) for this.
    # REMOVED THE 'global' KEYWORD HERE
    expected_sequence_numbers = {i: 0 for i in range(N) if i != myself} 
    incoming_message_queues = {i: [] for i in range(N) if i != myself}


    # Create receiving message handler
    # Pass the socket that will receive UDP messages
    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()
    print('Handler started')
    
    PEERS = getListOfPeers() # Populate the global PEERS list
    
    # Send handshakes
    # Send to all peers except myself
    for addrToSend in PEERS:
        if addrToSend != get_public_ip(): # Don't send handshake to myself
            print('Sending handshake to ', addrToSend)
            # Handshake message format: ('READY', my_id)
            msg = ('READY', myself)
            msgPack = pickle.dumps(msg)
            sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))

    print('Main Thread: Sent all handshakes. handShakeCount=', str(handShakeCount))
    
    # Wait for all handshakes to be received by the MsgHandler thread
    # The MsgHandler updates handShakeCount, so this loop waits for it.
    while (handShakeCount < N):
        pass # Spin-wait for handshakes. In a real system, use threading.Event or similar.
    
    # All peers are ready. Now, send data messages.
    # Send a sequence of data messages to all other processes 
    for msgNumber in range(0, nMsgs):
        # Update logical clock for sending a new data message
        update_logical_clock()
        # Data message format: ('DATA', my_id, message_number, current_logical_clock)
        msg = ('DATA', myself, msgNumber, logical_clock)
        msgPack = pickle.dumps(msg)
        for addrToSend in PEERS:
            if addrToSend != get_public_ip(): # Don't send messages to myself
                sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
                print('Sent message ' + str(msgNumber) + ' with clock ' + str(logical_clock) + ' to ' + addrToSend)
    
    # Tell all processes that I have no more messages to send
    # Stop message format: (-1, -1) - no timestamp, no specific data
    for addrToSend in PEERS:
        if addrToSend != get_public_ip():
            msg = (-1,-1)
            msgPack = pickle.dumps(msg)
            sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))

    pass
