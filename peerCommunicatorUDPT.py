from socket import *
from constMP import *
import threading
import random
import time
import pickle
from requests import get

# Global variables
handShakeCount = 0
PEERS = []
myself = -1  # My own ID
nMsgs = 0    # Number of messages to send

# Logical clock
logical_clock = 0

# Message queues for causal ordering
# Stores (sender_id, sequence_number, logical_timestamp, message_content)
# We'll use a dictionary of lists: {sender_id: [(seq, ts, content), ...]}
received_messages_queue = {}
# To keep track of messages we are waiting ACKs for
sent_messages_awaiting_ack = {} # {(receiver_id, msg_id): message_data}

# UDP sockets
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# TCP socket for comparison server
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

# Lock for updating logical_clock and accessing shared resources
clock_lock = threading.Lock()

def get_public_ip():
    ipAddr = get('https://api.ipify.org').content.decode('utf8')
    print(f'My public IP address is: {ipAddr}')
    return ipAddr

def registerWithGroupManager():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print(f'Connecting to group manager: {(GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT)}')
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    ipAddr = get_public_ip()
    req = {"op": "register", "ipaddr": ipAddr, "port": PEER_UDP_PORT}
    msg = pickle.dumps(req)
    print(f'Registering with group manager: {req}')
    clientSock.send(msg)
    clientSock.close()

def getListOfPeers():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print(f'Connecting to group manager: {(GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT)}')
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    req = {"op": "list"}
    msg = pickle.dumps(req)
    print(f'Getting list of peers from group manager: {req}')
    clientSock.send(msg)
    msg = clientSock.recv(2048)
    peers_list = pickle.loads(msg)
    print(f'Got list of peers: {peers_list}')
    clientSock.close()
    return peers_list

def update_logical_clock(received_timestamp=0):
    global logical_clock
    with clock_lock:
        logical_clock = max(logical_clock, received_timestamp) + 1
        return logical_clock

def deliver_message(msg):
    """
    Simulates delivering the message to the application.
    In a real application, this would involve processing the message content.
    """
    print(f"DELIVERED to application: Message {msg[2]} from Process {msg[1]} (Timestamp: {msg[3]})")
    # Here you would add the message to the logList
    # For now, we'll just print it.

class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock
        self.logList = [] # Log of messages delivered to the application
        self.expected_seq_numbers = {peer_ip: 0 for peer_ip, _ in PEERS} # Track next expected sequence number for each peer

    def run(self):
        print('Handler is ready. Waiting for handshakes and messages...')
        global handShakeCount
        global PEERS
        global N # N from constMP

        # Wait until handshakes are received from all other processes
        # (to make sure that all processes are synchronized before they start exchanging messages)
        while handShakeCount < N - 1: # We don't handshake with ourselves, so N-1 others
            try:
                msgPack, addr = self.sock.recvfrom(1024)
                msg = pickle.loads(msgPack)
                if msg[0] == 'READY':
                    with clock_lock:
                        update_logical_clock(msg[3]) # Update clock based on handshake timestamp
                    handShakeCount += 1
                    print(f'--- Handshake received from {addr[0]} (ID: {msg[1]}, Timestamp: {msg[3]})')
                    # Send ACK for handshake
                    ack_msg = ('ACK_HANDSHAKE', myself, -1, update_logical_clock())
                    ack_msg_pack = pickle.dumps(ack_msg)
                    sendSocket.sendto(ack_msg_pack, (addr[0], PEER_UDP_PORT))
            except Exception as e:
                print(f"Error receiving handshake: {e}")
                pass

        print(f'Secondary Thread: Received all handshakes. Entering the loop to receive messages. Handshake count: {handShakeCount}')

        stopCount = 0
        while True:
            try:
                msgPack, addr = self.sock.recvfrom(1024)
                msg = pickle.loads(msgPack) # msg format: (type, sender_id, seq_num, timestamp)

                # 1st thing: Update logical clock
                received_timestamp = msg[3] if len(msg) > 3 else 0 # Handle cases where old messages might not have timestamp
                current_logical_clock = update_logical_clock(received_timestamp)
                print(f"Received message. My clock: {current_logical_clock}. Received timestamp: {received_timestamp}")

                # 2nd thing: Place the message in the queue
                msg_type = msg[0]
                sender_id = msg[1]
                seq_num = msg[2]

                if sender_id not in received_messages_queue:
                    received_messages_queue[sender_id] = []
                received_messages_queue[sender_id].append(msg)
                received_messages_queue[sender_id].sort(key=lambda x: x[2]) # Sort by sequence number

                if msg_type == 'DATA':
                    print(f'Message {seq_num} from process {sender_id} received. Current queue for {sender_id}: {[m[2] for m in received_messages_queue[sender_id]]}')

                    # 3rd thing: If data message -> send ACK
                    ack_msg = ('ACK_DATA', myself, seq_num, update_logical_clock())
                    ack_msg_pack = pickle.dumps(ack_msg)
                    sendSocket.sendto(ack_msg_pack, (addr[0], PEER_UDP_PORT))
                    print(f"Sent ACK for message {seq_num} from {sender_id} to {addr[0]}")

                elif msg_type == 'ACK_DATA':
                    # This ACK is for a message *I* sent.
                    # Remove it from sent_messages_awaiting_ack
                    if (sender_id, seq_num) in sent_messages_awaiting_ack:
                        del sent_messages_awaiting_ack[(sender_id, seq_num)]
                        print(f"Received ACK for my message {seq_num} from {sender_id}")
                    else:
                        print(f"Received unexpected ACK for message {seq_num} from {sender_id}")

                elif msg_type == -1: # Stop message
                    stopCount += 1
                    print(f"Received stop signal from {sender_id}. Total stop signals: {stopCount}")
                    if stopCount == N:
                        print("All peers have sent stop signals. Terminating message reception.")
                        break # Stop loop when all other processes have finished
                    continue # Don't process stop messages further in delivery logic

                # 4th thing: Check if "unlocks" the first message in the queue and deliver
                self.check_and_deliver_messages()

            except ConnectionResetError:
                # This can happen if a peer closes its socket before we read from it
                print(f"Connection reset by peer {addr}. Skipping.")
                continue
            except Exception as e:
                print(f"Error receiving message: {e}")
                # You might want to log this error or handle it more gracefully
                continue

        # Write log file (of delivered messages)
        logFile = open(f'logfile{myself}.log', 'w')
        logFile.writelines(str(self.logList))
        logFile.close()

        # Send the list of delivered messages to the server (using a TCP socket) for comparison
        print('Sending the list of messages to the server for comparison...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        msgPack = pickle.dumps(self.logList)
        clientSock.send(msgPack)
        clientSock.close()

        # Reset the handshake counter for potential next round (though this demo exits)
        global logical_clock
        with clock_lock:
            logical_clock = 0
        handShakeCount = 0

        print("MsgHandler thread exiting.")
        exit(0) # Terminate this thread

    def check_and_deliver_messages(self):
        """
        Checks if any messages in the queue are ready for delivery.
        A message (sender_id, seq_num, timestamp) is ready if:
        1. It's the next expected sequence number from that sender.
        2. All ACKs for messages up to this timestamp from this sender have been received (this part is hard for causal).
           For this demo, we'll simplify: just check sequential order and ensure all peers sent handshakes.
        """
        global PEERS # Access the global list of peers

        # Iterate through each sender's queue
        for sender_id, queue in received_messages_queue.items():
            # Find the actual sender IP from PEERS list (assuming PEERS contains IDs)
            sender_ip = None
            for ip, _ in PEERS: # PEERS is a list of IPs from GroupManager
                if ip == sender_id: # This won't work directly as sender_id is peer ID, not IP. Need to map IDs to IPs or use IPs directly in PEERS.
                                    # Let's assume for simplicity, sender_id is the IP for now or we map them.
                                    # Correction: PEERS is a list of IPs, but msg[1] is the peer ID.
                                    # We need to map peer IDs to their IPs if not using IP directly in messages.
                pass # This logic needs refinement based on how 'sender_id' (msg[1]) maps to PEERS

            # Simplified delivery logic: Deliver if it's the next expected sequence number
            while queue and queue[0][2] == self.expected_seq_numbers.get(queue[0][1], 0):
                msg_to_deliver = queue.pop(0)
                self.logList.append(msg_to_deliver)
                deliver_message(msg_to_deliver)
                self.expected_seq_numbers[msg_to_deliver[1]] = msg_to_deliver[2] + 1
            # If the queue is not empty, but the first message is not the next expected, it means we are waiting for an earlier message.
            # No action needed here, just keep waiting.


# Function to wait for start signal from comparison server:
def waitToStart():
    global myself, nMsgs # Update global variables
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    myself = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps(f'Peer process {myself} started.'))
    conn.close()
    return (myself, nMsgs)

# From here, code is executed when program starts:
registerWithGroupManager()
while True: # Keep running until explicitly terminated
    print('Waiting for signal to start from Comparison Server...')
    (myself, nMsgs) = waitToStart()
    print(f'I am up, and my ID is: {myself}. Will send {nMsgs} messages.')

    if nMsgs == 0:
        print('Comparison Server requested termination. Terminating.')
        exit(0)

    # Instead of time.sleep, we rely on handshakes for synchronization.
    # The MsgHandler will wait for all handshakes before processing data messages.
    # It's crucial that all peers start their MsgHandler before sending handshakes.

    PEERS = getListOfPeers()
    # Filter out my own IP from the PEERS list for sending messages
    my_ip = get_public_ip() # Need to get my IP again or store it globally
    PEERS_filtered = [peer for peer in PEERS if peer != my_ip]
    global N # Update N based on actual number of peers
    N = len(PEERS) # N should reflect the total number of peers, including myself

    # Initialize expected sequence numbers for all peers
    for peer_ip in PEERS:
        if peer_ip != my_ip:
            received_messages_queue[peer_ip] = []


    # Create receiving message handler (UDP listener)
    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()
    print('Handler started. Waiting for handshakes...')

    # Send handshakes
    # We should send handshakes to all OTHER peers, not to myself.
    for addrToSend in PEERS_filtered:
        print(f'Sending handshake to {addrToSend}')
        # msg format: ('READY', sender_id, sequence_number, logical_timestamp)
        # For handshake, sequence_number is not relevant, so we use -1.
        msg = ('READY', myself, -1, update_logical_clock())
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    print(f'Main Thread: Sent all handshakes. Waiting for handshakes to complete in MsgHandler...')

    # The main thread should wait until handshakes are acknowledged by the MsgHandler.
    # This is handled implicitly by the MsgHandler's internal while loop for handShakeCount.
    # The main thread can proceed with sending messages once the MsgHandler indicates readiness.
    # For now, we'll just wait for the MsgHandler to complete its initial handshake phase.
    # This will be done by the `handShakeCount` global variable being updated by the `MsgHandler`.
    while handShakeCount < N -1:
        pass # Wait for the MsgHandler to receive all handshakes. This is a busy-wait.
             # A more robust solution would be a threading.Event or similar.
    print(f'Main Thread: All handshakes acknowledged ({handShakeCount}). Starting to send data messages.')


    # Send a sequence of data messages to all other processes
    for msgNumber in range(0, nMsgs):
        # Increment clock before sending each message
        current_ts = update_logical_clock()
        # msg format: ('DATA', sender_id, sequence_number, logical_timestamp)
        msg = ('DATA', myself, msgNumber, current_ts)
        msgPack = pickle.dumps(msg)
        for addrToSend in PEERS_filtered:
            sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
            print(f'Sent message {msgNumber} to {addrToSend} (Timestamp: {current_ts})')
            # Add message to our awaiting ACK list
            # We need to track ACKs for each message sent to each peer
            # For simplicity in this demo, we won't track ACKs per message per peer exhaustively for delivery.
            # The causal order relies on sending the timestamp and updating the clock.
            # The ACK mechanism here is more for basic reliability rather than strict causal ordering conditions.

    # Tell all processes that I have no more messages to send
    # This is the termination signal, sent after all data messages.
    for addrToSend in PEERS_filtered:
        msg = (-1, myself, -1, update_logical_clock()) # -1 for type and sequence
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
        print(f"Sent termination signal to {addrToSend}")

    # Wait for the MsgHandler thread to finish processing all messages and send logs
    msgHandler.join() # This will block the main thread until MsgHandler finishes and exits.
    print("Main thread: MsgHandler finished. Restarting main loop for next round.")
    # Reset for next round if the Comparison Server initiates again
    global handShakeCount
    handShakeCount = 0
    # Clear queues for next round
    received_messages_queue.clear()
    sent_messages_awaiting_ack.clear()
