from socket import *
import pickle
from constMPT import *

port = GROUPMNGR_TCP_PORT
membership = []

def serverLoop():
    serverSock = socket(AF_INET, SOCK_STREAM)
    serverSock.bind(('0.0.0.0', port))
    serverSock.listen(6)
    print(f"Group Manager listening on port {port}")
    while(1):
        (conn, addr) = serverSock.accept()
        msgPack = conn.recv(2048)
        req = pickle.loads(msgPack)
        if req["op"] == "register":
            if (req["ipaddr"], req["port"]) not in membership:
                membership.append((req["ipaddr"],req["port"]))
                print ('Registered peer: ', req)
            else:
                print(f"Peer {req['ipaddr']}:{req['port']} already registered.")
        elif req["op"] == "list":
            # list = [] # This variable name masks the built-in list type. Let's rename it.
            peer_list_to_send = []
            for m in membership:
                peer_list_to_send.append(m[0]) # Only send IP addresses
            print ('List of peers sent to server: ', peer_list_to_send)
            conn.send(pickle.dumps(peer_list_to_send))
        else:
            print(f"Unknown operation: {req['op']}")
            # fix (send back an answer in case of unknown op)
            conn.send(pickle.dumps({"status": "error", "message": "Unknown operation"}))

        conn.close()

serverLoop()
