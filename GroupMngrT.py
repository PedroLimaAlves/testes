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
    
    while True:
        conn, addr = serverSock.accept()
        msgPack = conn.recv(2048)
        req = pickle.loads(msgPack)

        if req["op"] == "register":
            peer_entry = (req["ipaddr"], req["port"])
            if peer_entry not in membership:
                membership.append(peer_entry)
            print('Registered peer: ', req)
            conn.send(pickle.dumps("ACK: Registered"))
            
        elif req["op"] == "list":
            peer_list = [m[0] for m in membership]
            print('List of peers sent to server: ', peer_list)
            conn.send(pickle.dumps(peer_list))
            
        else:
            print(f"Unknown operation requested: {req.get('op', 'N/A')}")
            conn.send(pickle.dumps("ERROR: Unknown operation"))
        
        conn.close()

serverLoop()
