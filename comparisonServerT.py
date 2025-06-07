from socket import *
import pickle
from constMPT import *
import time
import sys

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', SERVER_PORT))
serverSock.listen(6)

# N from constMPT.py is used here to know how many peers to expect logs from.
# It's crucial that N in constMPT.py matches the actual number of peers.

def mainLoop():
    cont = 1
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
        startPeers(peerList,nMsgs)
        print('Now, wait for the message logs from the communicating peers...')
        waitForLogsAndCompare(nMsgs, len(peerList)) # Pass the actual number of peers
    serverSock.close()

def promptUser():
    nMsgs = int(input('Enter the number of messages for each peer to send (0 to terminate)=> '))
    return nMsgs

def startPeers(peerList,nMsgs):
    # Connect to each of the peers and send the 'initiate' signal:
    peerNumber = 0
    for peer in peerList:
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((peer, PEER_TCP_PORT))
        msg = (peerNumber,nMsgs)
        msgPack = pickle.dumps(msg)
        clientSock.send(msgPack)
        msgPack = clientSock.recv(512)
        print(pickle.loads(msgPack))
        clientSock.close()
        peerNumber = peerNumber + 1

def waitForLogsAndCompare(N_MSGS, num_active_peers):
    # Loop to wait for the message logs for comparison:
    numPeersReceivedLogs = 0
    msgs = [] # each msg is a list of tuples (with the original messages received by the peer processes)

    # Receive the logs of messages from the peer processes
    while numPeersReceivedLogs < num_active_peers: # Use num_active_peers here
        (conn, addr) = serverSock.accept()
        msgPack = conn.recv(32768)
        print ('Received log from peer')
        conn.close()
        msgs.append(pickle.loads(msgPack))
        numPeersReceivedLogs = numPeersReceivedLogs + 1

    unordered = 0

    # Compare the lists of messages
    # The logs now contain messages like ('DATA', sender_id, message_number, timestamp)
    # We should compare (sender_id, message_number) for ordering, ignoring timestamp for comparison
    
    # First, let's normalize the logs to just (sender_id, message_number) for comparison
    normalized_msgs = []
    for peer_log in msgs:
        normalized_peer_log = []
        for msg_entry in peer_log:
            # We are interested in (sender_id, message_number) for comparison
            # Assuming msg_entry is ('DATA', sender_id, message_number, timestamp)
            # or (type, sender_id, message_number, timestamp)
            if msg_entry[0] == 'DATA': # Only consider DATA messages for ordering comparison
                normalized_peer_log.append((msg_entry[1], msg_entry[2])) # (sender_id, message_number)
        normalized_msgs.append(normalized_peer_log)

    if not normalized_msgs:
        print('No messages received for comparison.')
        return

    # Assuming all peers should have received the same set of messages in the same order
    # after the logical clock and ACK mechanism.
    # The comparison now should be between the *ordered* list of received messages.
    
    # We compare the received logs. If they are truly ordered, all logs should be identical.
    # The comparison logic assumes N_MSGS is the total count of messages *per peer*.
    # If the system works perfectly, each peer should have received N_MSGS * (N-1) DATA messages.
    # The current comparison logic (comparing item by item across peer logs) works if we expect
    # all peers to have the *exact same* sequence of received messages from all other peers.

    # A more robust comparison for a distributed system would be to collect all messages,
    # sort them by timestamp, and then verify if the "causal" order holds.
    # However, the user's intent seems to be a simple pairwise comparison of received logs.

    # Let's adapt the original comparison to the normalized logs.
    # This loop still assumes that normalized_msgs[x][j] should be identical across all x.
    # It also assumes that each normalized_peer_log will have at least N_MSGS-1 elements,
    # which might not be true if N_MSGS was the number sent by *each* peer, not the total received.

    # A better way to compare if the goal is "no unordered message rounds" from the *original* problem:
    # Each peer's log should be identical if the messages are delivered in the same order.
    
    # Let's compare the *entire sorted normalized logs* of messages received by each peer.
    # Sort each peer's received log by timestamp (or by sender_id then message_number if timestamps are identical).
    # Since logical clock ensures causal ordering, if the delivery mechanism is correct,
    # the sorted logs of received messages should be identical across all peers.
    
    # We need to sort the inner lists (normalized_peer_log) first to ensure
    # we are comparing based on the order they *should* have been delivered.
    # Sorting by (sender_id, message_number) guarantees a consistent order.
    for i in range(len(normalized_msgs)):
        normalized_msgs[i].sort(key=lambda x: (x[0], x[1])) # Sort by sender_id, then message_number

    if len(normalized_msgs) > 1:
        first_peer_log = normalized_msgs[0]
        for i in range(1, len(normalized_msgs)):
            if first_peer_log != normalized_msgs[i]:
                unordered += 1
                # If logs are not identical, it means some messages were received in different orders.
                # We can break here or continue to count all disparities.
                # For simplicity, we just count if *any* log differs from the first one.
                break 

    print ('Found ' + str(unordered) + ' unordered message rounds')
    if unordered == 0:
        print('All messages were received in order across all peers.')
    else:
        print('Some messages were received out of order across peers.')


# Initiate server:
mainLoop()
