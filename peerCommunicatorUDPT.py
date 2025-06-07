from socket import *
import threading
import random
import time
import pickle
from requests import get
from constMPT import * # Import N, N_VIRGINIA, N_OREGON

# Global variables
handShakeCount = 0
PEERS = []
lamport_clock = 0 # Initialize Lamport clock
message_queue = [] # To store messages received out of order
expected_messages = {} # To keep track of the next expected Lamport timestamp from each peer
my_ip = ""
myself_id = -1 # My unique ID assigned by the comparison server

# UDP sockets
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# TCP socket
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

def get_public_ip():
    global my_ip
    my_ip = get('https://api.ipify.org').content.decode('utf8')
    print('My public IP address is: {}'.format(my_ip))
    return my_ip

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
    global PEERS
    clientSock = socket(AF_INET, SOCK_STREAM)
    print ('Connecting to group manager to get peer list: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    req = {"op":"list"}
    msg = pickle.dumps(req)
    print ('Getting list of peers from group manager: ', req)
    clientSock.send(msg)
    msg = clientSock.recv(2048)
    PEERS = pickle.loads(msg)
    print ('Got list of peers: ', PEERS)
    clientSock.close()
    
    # Filter out my own IP from the PEERS list
    if my_ip in PEERS:
        PEERS.remove(my_ip)
    
    # Initialize expected_messages for each peer
    for peer_ip in PEERS:
        expected_messages[peer_ip] = 0 # Expecting a message with Lamport timestamp 0 or greater initially

    return PEERS

# Function to update Lamport clock
def update_lamport_clock(received_timestamp=None):
    global lamport_clock
    if received_timestamp is None: # Local event
        lamport_clock += 1
    else: # Message received
        lamport_clock = max(lamport_clock, received_timestamp) + 1
    return lamport_clock

class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock
        self.logList = [] # Log for this peer
        self.pending_messages = [] # Buffer for out-of-order messages

    def process_message(self, msg, sender_addr):
        global lamport_clock
        sender_id, msg_number, msg_lamport_time = msg[0], msg[1], msg[2]
        
        # Update Lamport clock based on received message
        update_lamport_clock(msg_lamport_time)
        print(f'[{lamport_clock}] Message {msg_number} from process {sender_id} received. Lamport: {msg_lamport_time}')
        
        # Add to pending messages
        self.pending_messages.append((sender_id, msg_number, msg_lamport_time, sender_addr))
        self.pending_messages.sort(key=lambda x: x[2]) # Sort by Lamport timestamp

        # Process messages that are now in order
        while self.pending_messages:
            # For simplicity, we process if the current message's lamport time is what's expected for that sender.
            # A more robust solution for total order would require a global ordering or a consensus algorithm.
            # Here, we're mainly demonstrating causal order with Lamport.
            
            # Let's try to process messages if they are the next expected message from ANY peer.
            # This is a simplification for a causal order.
            
            # Find the message with the smallest Lamport timestamp in the pending queue
            next_msg_to_process = self.pending_messages[0]
            
            # If the timestamp of the next message is the current clock + 1 OR if it's the first message from a sender,
            # then process it. This logic is a bit tricky for global ordering without a central sequencer.
            # For Lamport, you just update your clock and record the message.
            
            # For the purpose of this exercise and comparison, we'll simply append all received messages
            # to the log after updating the clock, and the comparison server will sort by Lamport.
            
            # The ACK mechanism helps with reliable delivery, not necessarily strict total order across all peers,
            # which is harder to achieve without further mechanisms.
            
            processed_msg = self.pending_messages.pop(0) # Process the message (remove from queue)
            self.logList.append(processed_msg[:-1]) # Add (sender_id, message_number, lamport_timestamp) to log
            
            # Send ACK
            ack_msg = ('ACK', processed_msg[0], processed_msg[1], update_lamport_clock()) # ACK with sender_id, msg_number, and current Lamport clock
            ack_msg_pack = pickle.dumps(ack_msg)
            # Send ACK back to the original sender
            sendSocket.sendto(ack_msg_pack, (processed_msg[3], PEER_UDP_PORT))
            print(f'[{lamport_clock}] Sent ACK for message {processed_msg[1]} from {processed_msg[0]} to {processed_msg[3]}.')


    def run(self):
        print('Handler is ready. Waiting for the handshakes and messages...')
        global handShakeCount
        
        stopCount = 0
        while True:
            try:
                msgPack, addr = self.sock.recvfrom(2048) # Receive data and sender address
                msg = pickle.loads(msgPack)
                
                # Handshake handling
                if msg[0] == 'READY':
                    handShakeCount += 1
                    print(f'--- Handshake received from {msg[1]} ({addr[0]}). Current handShakeCount: {handShakeCount}')
                    # Send ACK for handshake (optional, but good practice)
                    ack_handshake = ('ACK_HS', myself_id)
                    sendSocket.sendto(pickle.dumps(ack_handshake), addr)
                    print(f'Sent Handshake ACK to {addr[0]}')
                    
                elif msg[0] == 'ACK_HS':
                    # This peer sent a handshake and received an ACK for it.
                    # This means the other peer is ready. Not strictly needed for handShakeCount logic here.
                    pass 

                # Data message handling (sender_id, message_number, lamport_timestamp)
                elif isinstance(msg[0], int) and len(msg) == 3: # Check if it's a data message
                    self.process_message(msg, addr[0]) # Pass sender address for ACK
                    
                # Stop message handling
                elif msg[0] == -1: # count the 'stop' messages from the other processes
                    stopCount += 1
                    print(f'Received stop message. Current stopCount: {stopCount}. Total peers: {N}')
                    if stopCount == N - 1: # N-1 because I don't send a stop to myself
                        print('All other processes have finished. Stopping message handler.')
                        break # stop loop when all other processes have finished
                
                # ACK handling for data messages
                elif msg[0] == 'ACK':
                    # This means I sent a message and received an ACK for it.
                    # This logic would be more complex if we were waiting for ACKs before sending next message.
                    # For now, we'll just print it.
                    print(f'[{lamport_clock}] Received ACK for message {msg[2]} from peer {msg[1]} with Lamport time {msg[3]}')
                    
                else:
                    print(f"Unknown message type received: {msg}")

            except Exception as e:
                print(f"Error in MsgHandler run: {e}")
                break

        # Write log file
        logFile = open('logfile'+str(myself_id)+'.log', 'w')
        pickle.dump(self.logList, logFile) # Use pickle.dump to save the list
        logFile.close()
        print(f'Log file logfile{myself_id}.log written with {len(self.logList)} entries.')

        # Send the list of messages to the server (using a TCP socket) for comparison
        print('Sending the list of messages to the server for comparison...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        try:
            clientSock.connect((SERVER_ADDR, SERVER_PORT))
            msgPack = pickle.dumps(self.logList)
            clientSock.sendall(msgPack) # Use sendall to ensure all data is sent
            clientSock.close()
            print('Log sent to comparison server.')
        except Exception as e:
            print(f"Error sending log to comparison server: {e}")
        
        # Reset the handshake counter for the next round
        global handShakeCount
        handShakeCount = 0
        
        exit(0) # Terminate this thread when done

# Function to wait for start signal from comparison server:
def waitToStart():
    global myself_id
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    myself_id = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps('Peer process '+str(myself_id)+' started.'))
    conn.close()
    return (myself_id,nMsgs)

# From here, code is executed when program starts:
registerWithGroupManager()
while 1:
    print('Waiting for signal to start...')
    (myself_id, nMsgs) = waitToStart()
    print('I am up, and my ID is: ', str(myself_id))

    if nMsgs == 0:
        print('Terminating.')
        exit(0)
    
    # Create receiving message handler
    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()
    print('Handler started')

    PEERS = getListOfPeers() # Refresh peer list to ensure it's up-to-date and without my own IP
    
    # Wait a bit for all handlers to start and for other peers to get the updated list
    time.sleep(5) 

    # Send handshakes
    print(f"Sending handshakes to {len(PEERS)} peers: {PEERS}")
    for addrToSend in PEERS:
        msg = ('READY', myself_id)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
        print(f'Sent handshake to {addrToSend}:{PEER_UDP_PORT}')

    print('Main Thread: Sent all handshakes. Waiting for all handshakes to be received.')
    
    # Wait for all handshakes to be received by this peer
    while (handShakeCount < N -1): # N-1 because I don't handshake with myself
        time.sleep(0.1) # Small delay to avoid busy-waiting

    print('Main Thread: All handshakes received. Starting message exchange.')
    
    # Send a sequence of data messages to all other processes
    for msgNumber in range(0, nMsgs):
        # Wait some random time between successive messages
        time.sleep(random.randrange(10,100)/1000)
        
        # Increment Lamport clock for sending a message
        current_lamport_time = update_lamport_clock()
        
        msg = (myself_id, msgNumber, current_lamport_time) # Message now includes Lamport timestamp
        msgPack = pickle.dumps(msg)
        
        for addrToSend in PEERS:
            sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
            print(f'[{current_lamport_time}] Sent message {msgNumber} to {addrToSend}.')
            
            # For robust ACK, you would block here and wait for an ACK from addrToSend
            # or put it in a retry queue. For this exercise, we'll assume UDP
            # usually delivers and let the comparison server detect if something went wrong.
            # A full reliable ordered delivery system is complex.

    # Tell all processes that I have no more messages to send
    print('Sending stop messages to all peers.')
    for addrToSend in PEERS:
        msg = (-1,-1) # Use the existing stop signal format
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
    
    # Wait for the MsgHandler thread to finish its work (sending logs to comparison server)
    msgHandler.join()
    print(f"Peer {myself_id} completed its cycle.")
