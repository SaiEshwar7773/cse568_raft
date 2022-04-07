import json
import socket
import traceback
import threading
import time

from os import environ

# Wait following seconds below sending the controller request
time.sleep(10)

# Read Message Template
msg = json.load(open("Message.json"))

# Initialize
sender = "Controller"
targets = ["Node1","Node2","Node3"]
port = 5555


# Initialize for Canditate
votes_recived = 0

# Request
msg['sender_name'] = sender
msg['request'] = "CONVERT_FOLLOWER"
print(f"Request Created : {msg}")



def process_msg(msg, skt):
    message_type = msg["type"]
    message_body = msg["body"]
    if message_type == "CastVote":
        if "sender_name" in message_body:
            receive_vote(skt, message_body["sender_name"])


#Request Vote
def request_vote(skt):
    global votes_received 
    votes_received = 0 
    msg = {
        "type" : "RequestVote",
        "body":
            {
                "candidate_name": environ.get("hostname"),
                "term": int(environ.get("term"))+1
            }   
        }
    msg_bytes = json.dumps(msg).encode('utf-8')

    for target in targets:
        skt.sendto(msg_bytes, (target, 5555))
    time.sleep(1)

#receive vote
def receive_vote(skt,voter):
    global votes_received 
    votes_received += 1
    print("Recived vote from ", voter)
    print("vote count", votes_received)
    if votes_received>=3: 
        threading.Thread(target=set_leader_msg_all_nodes, args=[sender_skt]).start()
    



# Send Message From Leader
def set_leader_msg_all_nodes(skt):
    msg = {
        "type" : "AppendEntry",
        "body":
            {
                "leader_name": environ.get("hostname")
            }   
        }
    msg_bytes = json.dumps(msg).encode('utf-8')
    for target in targets:
        sender_skt.sendto(msg_bytes, (target, 5555))
    time.sleep(1)
        


# Listener --- Universal i.e. for Leader, Follower, Candidate
def listener(skt):
    print(f"Starting Listener")
    counter=0
    while True:
        try:
            msg, addr = skt.recvfrom(1024)
        except:
            print(f"ERROR while fetching from socket : {traceback.print_exc()}")

        # Decoding the Message received from Node 1
        decoded_msg = json.loads(msg.decode('utf-8'))
        print(f"Message Received : {decoded_msg} From : {addr}")
        threading.Thread(target=process_msg, args=[decoded_msg, skt]) #processing thread is separate
        if counter >= 4:
            break
        counter+=1

    print("Exiting Listener Function")

# Socket Creation and Binding
sender_skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
sender_skt.bind((sender, port))

# Send Message
try:
    # Encoding and sending the message
    threading.Thread(target=request_vote, args=[sender_skt]).start()

    #Starting listener
    listen_thread = threading.Thread(target=listener, args=[sender_skt])
    listen_thread.start()
    listen_thread.join()

    


except:
    #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")

