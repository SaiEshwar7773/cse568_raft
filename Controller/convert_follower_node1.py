import json
import socket
import traceback
import time
import threading
# Wait following seconds below sending the controller request

from os import environ
time.sleep(5)

# Read Message Template
msg = json.load(open("Message.json"))

# Initialize
sender = "Controller"
target = "Node1"
port = 5555
extra_logs = True

# Request
msg['sender_name'] = sender


# msg['request'] = "LEADER_INFO"
# msg['request'] = "CONVERT_FOLLOWER"
# msg['request'] = "SHUTDOWN"
# msg['request'] = "TIMEOUT"
msg['request'] = "STORE"

entry = { 
            "id": 4, 
            "score": 3901
        } 
msg['entry'] = entry

print(f"Request Created : {msg}")


def custom_print(*x):
    global extra_logs
    if extra_logs:
        print(*x)

def set_raft_leader(raft_leader):
    environ['raft_leader'] = raft_leader
    custom_print(environ.get("hostname") ," set leader to: " , environ.get("raft_leader"))

def process_msg(response_msg, skt):
    if response_msg["key"] == "LEADER":
        set_raft_leader(response_msg["value"])
        skt.sendto(json.dumps(request_msg).encode('utf-8'), (environ.get("raft_leader"), port))    


# Listener -- Controller
def listener(skt):
    print(f"Starting Listener")
    while True:
        try:
            msg, addr = skt.recvfrom(1024)
        except:
            print(f"ERROR while fetching from socket : {traceback.print_exc()}")

        # Decoding the Message received from Node 1
        decoded_msg = json.loads(msg.decode('utf-8'))
        # print(f"Message Received : {decoded_msg} From : {addr}")

        print(decoded_msg)
        process_msg(decoded_msg, skt)
    print("Exiting Listener Function")

# Socket Creation and Binding
skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
skt.bind((sender, port))

# Send Message
try:
    # Encoding and sending the message
    global request_msg
    request_msg = msg
    skt.sendto(json.dumps(request_msg).encode('utf-8'), (target, port))
    
    # Calling Listener
    listen_thread = threading.Thread(target=listener, args=[skt])
    listen_thread.start()

except:
    #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")


