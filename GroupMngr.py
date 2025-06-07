from socket import *
import pickle
from constMP import *

port = GROUPMNGR_TCP_PORT
membership = []

def serverLoop():
  serverSock = socket(AF_INET, SOCK_STREAM)
  serverSock.bind(('0.0.0.0', port))
  serverSock.listen(6)
  while(1):
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(2048)
    req = pickle.loads(msgPack)
    if req["op"] == "register":
      membership.append((req["ipaddr"],req["port"]))
      print ('Registered peer: ', req)
    elif req["op"] == "list":
      list = []
      for m in membership:
        list.append(m[0])
      print ('List of peers sent to server: ', list)
      conn.send(pickle.dumps(list))
    else:
      pass # fix (send back an answer in case of unknown op

  conn.close()

serverLoop()
