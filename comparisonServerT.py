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

        if len(peerList) != N: # Check if all peers are registered
            print(f"ERROR: Expected {N} peers, but got {len(peerList)}. Please ensure all peers are running and registered.")
            continue # Try again

        startPeers(peerList,nMsgs)
        print('Now, wait for the message logs from the communicating peers...')
        waitForLogsAndCompare(nMsgs, peerList) # Pass peerList to infer N

    serverSock.close()

def promptUser():
    nMsgs = int(input('Enter the number of messages for each peer to send (0 to terminate)=> '))
    return nMsgs

def startPeers(peerList,nMsgs):
    peerNumber = 0
    for peer_ip in peerList: # peerList contains only IPs
        clientSock = socket(AF_INET, SOCK_STREAM)
        try:
            clientSock.connect((peer_ip, PEER_TCP_PORT))
            msg = (peerNumber, nMsgs)
            msgPack = pickle.dumps(msg)
            clientSock.send(msgPack)
            msgPack = clientSock.recv(512)
            print(pickle.loads(msgPack))
            clientSock.close()
            peerNumber = peerNumber + 1
        except Exception as e:
            print(f"Error connecting to peer {peer_ip}:{PEER_TCP_PORT}: {e}")

def waitForLogsAndCompare(N_MSGS, peerList):
    numPeers = 0
    msgs = [] # each msg is a list of tuples (with the original messages received by the peer processes)

    # Receive the logs of messages from the peer processes
    while numPeers < N:
        try:
            (conn, addr) = serverSock.accept()
            msgPack = conn.recv(32768)
            print ('Received log from peer')
            conn.close()
            msgs.append(pickle.loads(msgPack))
            numPeers = numPeers + 1
        except Exception as e:
            print(f"Error receiving log from peer: {e}")
            # Consider a timeout mechanism here to prevent infinite waiting if a peer fails

    print(f"Received logs from {numPeers} peers.")

    unordered = 0

    # Compare the lists of messages
    # Each log entry will be (sender_id, message_number, lamport_timestamp)
    # We want to check if the sequence of (sender_id, message_number) is the same across all logs,
    # regardless of the Lamport timestamp, as Lamport is for ordering within a process.
    # The crucial part is that each peer should have received the same set of messages in the same causal order.
    # We will sort messages by Lamport timestamp for comparison to reflect the causal order.

    # First, let's sort each peer's log by Lamport timestamp to ensure causal order.
    # Assuming each message in the log is (sender_id, message_number, lamport_timestamp)
    sorted_msgs = []
    for peer_log in msgs:
        # Filter out ACK messages if they were logged - we only care about data messages for comparison.
        # Assuming data messages are (sender_id, message_number, lamport_timestamp)
        # and ACK messages are ('ACK', original_sender_id, original_msg_number, lamport_timestamp)
        data_messages = [m for m in peer_log if m[0] != 'ACK']
        sorted_msgs.append(sorted(data_messages, key=lambda x: x[2])) # Sort by lamport_timestamp

    # Now compare the sorted lists
    if not sorted_msgs:
        print("No messages received for comparison.")
        return

    # Assuming all peers should receive the same number of messages
    # If not, it indicates a problem in message delivery
    expected_msg_count_per_peer = N_MSGS * (N - 1) # Each peer sends N_MSGS to N-1 other peers

    # Check if all logs have the expected number of messages
    for i, log in enumerate(sorted_msgs):
        if len(log) != expected_msg_count_per_peer:
            print(f"WARNING: Peer {i} received {len(log)} messages, but expected {expected_msg_count_per_peer}.")
            # This indicates messages were lost or not sent correctly.
            # We can still proceed with comparison for messages received.

    # For comparison, we will compare the content of the messages (sender_id, message_number)
    # The order is determined by the Lamport timestamp within each log.
    # We want to see if the sequence of (sender_id, message_number) is consistent across all peers.
    # This comparison assumes a total order (all peers receive messages in the exact same sequence).
    # If using only Lamport, it guarantees causal order, not necessarily total order.
    # For total order, a more complex algorithm like a centralized sequencer or a distributed consensus
    # would be needed. For now, we will compare the ordered sequence from each peer.

    # Compare message content (sender_id, message_number)
    # We'll use the first peer's sorted log as the reference
    if sorted_msgs:
        reference_log = [(m[0], m[1]) for m in sorted_msgs[0]] # Extract (sender_id, message_number)
        
        for i in range(1, len(sorted_msgs)):
            current_log = [(m[0], m[1]) for m in sorted_msgs[i]]
            if len(reference_log) != len(current_log):
                print(f"Mismatch in number of messages between peer 0 ({len(reference_log)}) and peer {i} ({len(current_log)})")
                unordered += 1
                continue
            
            for j in range(len(reference_log)):
                if reference_log[j] != current_log[j]:
                    print(f"Disorder found! Peer 0 message {j}: {reference_log[j]} vs Peer {i} message {j}: {current_log[j]}")
                    unordered += 1
                    break # Break inner loop, move to next peer

    print ('Found ' + str(unordered) + ' unordered message rounds')
    if unordered == 0:
        print("All messages were received in the same causal order across all peers.")
    else:
        print("Disorder detected in message delivery.")


# Initiate server:
mainLoop()
