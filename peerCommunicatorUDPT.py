from socket  import *
from constMPT import *
import threading
import random
import pickle
from requests import get

handShakeCount = 0
PEERS = []

sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

logicalClock = 0
msgQueue = []
ackTracker = {}
deliveredMsgs = set()

def updateClock(receivedClock):
  global logicalClock
  logicalClock = max(logicalClock, receivedClock) + 1

def get_public_ip():
  ipAddr = get('https://api.ipify.org').content.decode('utf8')
  print('My public IP address is: {}'.format(ipAddr))
  return ipAddr

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
  global PEERS
  PEERS = pickle.loads(msg)
  print ('Got list of peers: ', PEERS)
  clientSock.close()
  return PEERS

class MsgHandler(threading.Thread):
  def __init__(self, sock):
    threading.Thread.__init__(self)
    self.sock = sock

  def run(self):
    global handShakeCount, logicalClock, msgQueue, ackTracker, deliveredMsgs
    print('Handler is ready. Waiting for the handshakes...')

    logList = []

    while handShakeCount < N:
      msgPack = self.sock.recv(1024)
      msg = pickle.loads(msgPack)
      if msg[0] == 'READY':
        handShakeCount += 1
        print('--- Handshake received: ', msg[1])

    print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

    stopCount = 0
    while True:
      msgPack = self.sock.recv(1024)
      msg = pickle.loads(msgPack)

      if msg[0] == "DATA":
        _, senderId, msgId, timestamp = msg
        updateClock(timestamp)
        msgKey = (senderId, msgId)
        print(f"Received DATA: {msgKey} with timestamp {timestamp}")
        msgQueue.append((timestamp, msg))
        logicalClock += 1
        ackMsg = ("ACK", myself, senderId, msgId, logicalClock)
        ackPack = pickle.dumps(ackMsg)
        sendSocket.sendto(ackPack, (PEERS[senderId], PEER_UDP_PORT))

      elif msg[0] == "ACK":
        _, _, senderId, msgId, timestamp = msg
        updateClock(timestamp)
        ackTracker[(senderId, msgId)] = True
        print(f"Received ACK for {(senderId, msgId)}")

      elif msg[0] == "END":
        _, senderId, _, timestamp = msg
        updateClock(timestamp)
        stopCount += 1
        if stopCount == N:
          break

      msgQueue.sort()
      i = 0
      while i < len(msgQueue):
        _, queuedMsg = msgQueue[i]
        _, senderId, msgId, _ = queuedMsg
        msgKey = (senderId, msgId)
        if msgKey in ackTracker and msgKey not in deliveredMsgs:
          print(f'Message {msgId} from process {senderId} delivered to application.')
          logList.append((senderId, msgId))
          deliveredMsgs.add(msgKey)
          msgQueue.pop(i)
          continue
        i += 1

    logFile = open('logfile'+str(myself)+'.log', 'w')
    logFile.writelines(str(logList))
    logFile.close()

    print('Sending the list of messages to the server for comparison...')
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((SERVER_ADDR, SERVER_PORT))
    msgPack = pickle.dumps(logList)
    clientSock.send(msgPack)
    clientSock.close()

    handShakeCount = 0
    exit(0)

def waitToStart():
  (conn, addr) = serverSock.accept()
  msgPack = conn.recv(1024)
  msg = pickle.loads(msgPack)
  global myself
  myself = msg[0]
  nMsgs = msg[1]
  conn.send(pickle.dumps('Peer process '+str(myself)+' started.'))
  conn.close()
  return (myself,nMsgs)

registerWithGroupManager()
while 1:
  print('Waiting for signal to start...')
  (myself, nMsgs) = waitToStart()
  print('I am up, and my ID is: ', str(myself))

  if nMsgs == 0:
    print('Terminating.')
    exit(0)

  msgHandler = MsgHandler(recvSocket)
  msgHandler.start()
  print('Handler started')

  PEERS = getListOfPeers()

  for addrToSend in PEERS:
    print('Sending handshake to ', addrToSend)
    msg = ('READY', myself)
    msgPack = pickle.dumps(msg)
    sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))

  while (handShakeCount < N):
    pass

  for msgNumber in range(0, nMsgs):
    logicalClock += 1
    msg = ("DATA", myself, msgNumber, logicalClock)
    msgPack = pickle.dumps(msg)
    for addrToSend in PEERS:
      sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
      print('Sent message ' + str(msgNumber))

  for addrToSend in PEERS:
    logicalClock += 1
    msg = ("END", myself, -1, logicalClock)
    msgPack = pickle.dumps(msg)
    sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
