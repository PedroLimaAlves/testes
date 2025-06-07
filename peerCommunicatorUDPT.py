from socket import *
from constMPT import *
import threading
import random
import time
import pickle
from requests import get
import heapq

handShakeCount = 0
PEERS = []
clock = 0
msgBuffer = []
ackDict = {}

sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

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
  PEERS = pickle.loads(msg)
  print ('Got list of peers: ', PEERS)
  clientSock.close()
  return PEERS

def update_clock(received_ts):
  global clock
  clock = max(clock, received_ts) + 1

def try_deliver_messages(logList):
  global msgBuffer, ackDict
  delivered = True
  while msgBuffer and delivered:
    ts, sender, msgNum, msg = msgBuffer[0]
    key = (ts, sender)
    if len(ackDict.get(key, set())) >= N - 1:
      heapq.heappop(msgBuffer)
      logList.append((sender, msgNum))
      print('Message ' + str(msgNum) + ' from process ' + str(sender))
    else:
      delivered = False

class MsgHandler(threading.Thread):
  def __init__(self, sock):
    threading.Thread.__init__(self)
    self.sock = sock

  def run(self):
    global handShakeCount, msgBuffer, ackDict

    print('Handler is ready. Waiting for the handshakes...')
    logList = []

    while handShakeCount < N:
      msgPack = self.sock.recv(1024)
      msg = pickle.loads(msgPack)
      if msg[0] == 'READY':
        handShakeCount = handShakeCount + 1
        print('--- Handshake received: ', msg[1])

    print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

    stopCount = 0
    while True:
      msgPack = self.sock.recv(2048)
      msg = pickle.loads(msgPack)

      if msg[0] == -1:
        stopCount += 1
        if stopCount == N:
          break

      elif msg[0] == 'ACK':
        sender, original_ts, original_sender = msg[1:]
        key = (original_ts, original_sender)
        ackDict.setdefault(key, set()).add(sender)
        update_clock(original_ts)
        try_deliver_messages(logList)

      else:
        sender, msgNum, ts = msg
        update_clock(ts)
        key = (ts, sender)
        heapq.heappush(msgBuffer, (ts, sender, msgNum, msg))

        ackMsg = ('ACK', myself, ts, sender)
        ackPack = pickle.dumps(ackMsg)
        for peer in PEERS:
          sendSocket.sendto(ackPack, (peer, PEER_UDP_PORT))

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
    sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

  print('Main Thread: Sent all handshakes. handShakeCount=', str(handShakeCount))

  while (handShakeCount < N):
    pass

  for msgNumber in range(0, nMsgs):
    clock += 1
    msg = (myself, msgNumber, clock)
    msgPack = pickle.dumps(msg)
    for addrToSend in PEERS:
      sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
      print('Sent message ' + str(msgNumber))

  for addrToSend in PEERS:
    msg = (-1,-1)
    msgPack = pickle.dumps(msg)
    sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
