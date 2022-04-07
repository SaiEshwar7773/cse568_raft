import socket
import time
import threading
import json
import traceback

from os import environ 

import time

# Constants
MAJORITY = 3
STD_TIMEOUT = 1


# Initialize
sender = "Controller"
targets = environ.get("targets").split(",")
port = 5555
heartbeat_timer = time.time()


# Initialize for Canditate
votes_received = 0
voting_completed = False
 
state_info = {
    "current_term": 0,
    "voted_for": None,
    "log": [],
    "timeout": STD_TIMEOUT,
    "heartbeat": STD_TIMEOUT
}


# Send Message From Candidate -- Request Vote
def request_vote(skt):
    global votes_received 
    votes_received = 1 
    global voting_completed 
    voting_completed = False
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



# Send Message From --- Leader --- to followers
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
        skt.sendto(msg_bytes, (target, 5555))

# Send heartbeats From --- Leader --- to followers
def send_heartbeat(skt):
    msg = {
        "type" : "AppendEntry",
        "body":
            {
            }   
        }
    msg_bytes = json.dumps(msg).encode('utf-8')
    while True:
        for target in targets:
            skt.sendto(msg_bytes, (target, 5555))
        time.sleep(STD_TIMEOUT)



# Listener -- Universal
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

        process_msg(decoded_msg, skt)
    print("Exiting Listener Function")



# Listener  -- Universal Process Message 
def process_msg(msg, skt):
    message_type = msg["type"]
    message_body = msg["body"]
    if message_type == "AppendEntry":#Message from Leader -- valid for follower
        if "leader_name" in message_body:
            set_raft_leader(message_body["leader_name"])
        if  not message_body:
            receive_heartbeat()
    if message_type == "RequestVote":
        if "candidate_name" in message_body:
            cast_vote(skt, message_body["candidate_name"], message_body["term"])
    if message_type == "CastVote":
        if "sender_name" in message_body:
            receive_vote(skt, message_body["sender_name"])
 
    
# Listen --- Candidate ---- Receive vote
def receive_vote(skt,voter):
    global votes_received
    global voting_completed 
    if not voting_completed:
        votes_received += 1
        print(environ.get("hostname"),"Recieved vote from", voter, ". The Vote count is now ", votes_received)
        if votes_received >= MAJORITY: 
            voting_completed = True
            set_raft_state("raft_leader")

# Listen --- Follower --- Heartbeat
def receive_heartbeat():
    global heartbeat_timer
    print( "Heartbeat received from: " , environ.get("raft_leader")," at ",environ.get("hostname"))
    heartbeat_timer = time.time()

# Listen --- Follower --- Cast vote
def cast_vote(skt,target,term):
    if term>int(environ.get("term")):
        msg = {
            "type" : "CastVote",
            "body":
                {
                    "sender_name": environ.get("hostname")
                }   
            }
        msg_bytes = json.dumps(msg).encode('utf-8')
        skt.sendto(msg_bytes, (target, 5555))
        environ['votedFor'] = target
        environ['term'] = str(term)

#Listen -- follower
def set_raft_leader(raft_leader):
    environ['raft_leader'] = raft_leader
    print(environ.get("hostname") ," set leader to: " , environ.get("raft_leader"))


def lookout_for_heartbeats():
    global heartbeat_timer
    
    while True:
        curr_time = time.time()
        if (curr_time - heartbeat_timer > 3*STD_TIMEOUT):
            set_raft_state("raft_candidate")
            break
        time.sleep(STD_TIMEOUT)


#Listen -- Universal
def set_raft_state(raft_state):
    environ['raft_state'] = raft_state
    print(environ.get("hostname") ," set to raft state: " , environ.get("raft_state"))
    if raft_state == "raft_follower":
        threading.Thread(target=lookout_for_heartbeats, args=[]).start()
    if raft_state == "raft_candidate":
        threading.Thread(target=request_vote, args=[UDP_Socket]).start()
    if raft_state == "raft_leader":
        notify_thread = threading.Thread(target=set_leader_msg_all_nodes, args=[UDP_Socket])
        notify_thread.start()
        notify_thread.join()
        heart_beat_thread = threading.Thread(target=send_heartbeat, args=[UDP_Socket])
        heart_beat_thread.start()
        heart_beat_thread.join()


# Creating Socket and binding it to the target container IP and port
UDP_Socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

sender = environ.get("hostname")
# Bind the node to sender ip and port
UDP_Socket.bind((sender, 5555))

if __name__ == "__main__":
    print("starting ",environ.get("hostname"))

    set_raft_state("raft_follower")
    
    

    #Starting thread 1
    listen_thread = threading.Thread(target=listener, args=[UDP_Socket])
    listen_thread.start()