from email import message
from glob import glob
from pickle import TRUE
import socket
import time
import threading
import json
import traceback

from os import environ 

import time
import random



# Constants
MAJORITY = len(environ.get("targets").split(","))//2 + 1
STD_TIMEOUT = 0.15
current_term_timeout = STD_TIMEOUT*random.uniform(2,3)



# Initialize
sender = "Controller"
targets = environ.get("targets").split(",")
port = 5555
heartbeat_timer = time.time()
extra_logs = True


# Initialize for Canditate
votes_received = 0
voting_completed = False
vote_casted = {}
 
state_info = {
    "current_term": 0,
    "voted_for": None,
    "log": [],
    "timeout": STD_TIMEOUT,
    "heartbeat": STD_TIMEOUT
}

def custom_print(*x):
    global extra_logs
    if extra_logs:
        print(*x)

# # Send Message From Candidate -- Request Vote
def seek_votes(skt):
    global candidate_timer 
    candidate_timer = time.time()
    while environ["raft_state"]=="raft_candidate":
        request_vote(skt)
        custom_print(voting_completed)
        time.sleep(STD_TIMEOUT*random.uniform(2,3))

# Send Message From Candidate -- Request Vote
def request_vote(skt):
    global votes_received 
    votes_received = 1 
    global voting_completed 
    voting_completed = False
    environ["term"] = str(int(environ.get("term")) + 1)
    msg = {
            "request" : "RequestVoteRPC",
            "term" : environ.get("term"),
            "candidate_id": environ.get("hostname"),
            "prevLogIndex" : -1,
            "prevLogTerm" : -1 } 
    msg_bytes = json.dumps(msg).encode('utf-8')
    for target in targets:
        try:
            custom_print(environ.get("hostname"), "requesting vote from", target, "Term ", environ.get("term"))
            skt.sendto(msg_bytes, (target, 5555))
        except Exception as e:
            pass

# Send Leader INFO --- Contoller Funtion
def send_leader_info(msg, skt):
    msg["key"] = "LEADER"
    msg["value"] = environ.get("raft_leader")
    msg_bytes = json.dumps(msg).encode('utf-8')
    custom_print(msg,environ.get("raft_leader"))
    skt.sendto(msg_bytes, ("Controller", 5555))


# Send Message From --- Leader --- to followers
def set_leader_msg_all_nodes(skt):
    msg = {
            "request" : "AppendEntry",
            "term" : environ.get("term"),
            "leaderId": environ.get("hostname"),
            "entries" : ["leader_update"],
            "prevLogIndex" : -1,
            "prevLogTerm" : -1
        }
    msg_bytes = json.dumps(msg).encode('utf-8')
    for target in targets:
        try:
            skt.sendto(msg_bytes, (target, 5555))
        except:
            pass

# Send heartbeats From --- Leader --- to followers
def send_heartbeat(skt):
    msg = {
        "request" : "AppendEntry",
        "term" : environ.get("term"),
        "leaderId": environ.get("hostname"),
        "entries" : [],
        "prevLogIndex" : -1,
        "prevLogTerm" : -1
        }
    msg_bytes = json.dumps(msg).encode('utf-8')
    while environ.get("raft_state") == "raft_leader":
        for target in targets:
            try:
                skt.sendto(msg_bytes, (target, 5555))
            except:
                pass
        time.sleep(STD_TIMEOUT)



# Listener -- Universal
def listener(skt):
    custom_print(f"Starting Listener")
    while environ.get("raft_state")!="raft_shutdown":
        try:
            msg, addr = skt.recvfrom(1024)
        except:
            custom_print(f"ERROR while fetching from socket : {traceback.print_exc()}")

        # Decoding the Message received from Node 1
        decoded_msg = json.loads(msg.decode('utf-8'))
        # custom_print(f"Message Received : {decoded_msg} From : {addr}")

        process_msg(decoded_msg, skt)
    custom_print("Exiting Listener Function")



# Listener  -- Universal Process Message 
def process_msg(msg, skt):
    global vote_casted
    global heartbeat_timer
    message_type = msg["request"]
    if message_type == "AppendEntry":#Message from Leader -- valid for follower
        message_entries = msg["entries"]
        if msg["term"]>environ.get("term"):
            heartbeat_timer = time.time()
            environ["term"] = str(msg["term"])
            set_raft_state("raft_follower")
        if message_entries:
                set_raft_leader(msg["leaderId"])
        elif  not msg["entries"]:
            receive_heartbeat()
    if message_type == "RequestVoteRPC":
        if "candidate_id" in msg:
            cast_vote(skt, msg["candidate_id"], int(msg["term"]))
    if message_type == "ResponseVoteRPC": 
        if "follower_id" in msg:
            receive_vote(skt, msg["follower_id"],msg['term'])
    if message_type == "LEADER_INFO": 
        # send_leader_info(msg, skt)
        msg["key"] = "LEADER"
        msg["value"] = environ.get("raft_leader")
        print(msg)
    if message_type == "CONVERT_FOLLOWER": 
        set_raft_state("raft_follower")
        print("Controller converted ",environ.get("hostname"), " to follower.")
    if message_type == "TIMEOUT":
        if environ.get("raft_state") == "raft_follower":
            set_raft_state("raft_candidate")
            print("Controller converted ",environ.get("hostname"), " to Candidate.")
    if message_type == "SHUTDOWN":
        set_raft_state("raft_shutdown")


# Listen --- Candidate ---- Receive vote
def receive_vote(skt,voter,term):
    global votes_received
    global voting_completed
    if not voting_completed and term == int(environ.get("term")):
        votes_received += 1
        custom_print(environ.get("hostname"),"Recieved vote from", voter, "in term ",environ.get("term"),". The Vote count is now ", votes_received)
        if votes_received >= MAJORITY: 
            voting_completed = True
            set_raft_state("raft_leader")

# Listen --- Follower --- Heartbeat
def receive_heartbeat():
    global heartbeat_timer
    # custom_print( "Heartbeat received from: " , environ.get("raft_leader")," at ",environ.get("hostname"))
    heartbeat_timer = time.time()

# Listen --- Follower --- Cast vote
def cast_vote(skt,target,term):
    global vote_casted
    global heartbeat_timer 
    heartbeat_timer = time.time()
    custom_print("cast_vote -- 3rd line", vote_casted, term , int(environ.get("term")))
    if term>int(environ.get("term")) and (term not in vote_casted) :
        msg = {
            "request" : "ResponseVoteRPC",
            "term" : term,
            "follower_id": environ.get("hostname"),
            "prevLogIndex" : -1,
            "prevLogTerm" : -1
        }
        msg_bytes = json.dumps(msg).encode('utf-8')
        skt.sendto(msg_bytes, (target, 5555))
        environ['votedFor'] = target
        vote_casted[term] = target
        #Update the timeout for this term
        global current_term_timeout
        current_term_timeout = STD_TIMEOUT*random.uniform(2,3)
        

#Listen -- follower
def set_raft_leader(raft_leader):
    environ['raft_leader'] = raft_leader
    custom_print(environ.get("hostname") ," set leader to: " , environ.get("raft_leader"))


def lookout_for_heartbeats():
    global heartbeat_timer
    global curr_time
    global vote_casted
    while environ.get("raft_state") == "raft_follower":
        curr_time = time.time() 
        if (curr_time - heartbeat_timer > current_term_timeout) : 
            if int(environ.get("term"))+1 not in vote_casted:
                set_raft_state("raft_candidate")
                break
        time.sleep(STD_TIMEOUT)


#Listen -- Universal
def set_raft_state(raft_state):
    environ['raft_state'] = raft_state
    custom_print(environ.get("hostname") ," set to raft state: " , environ.get("raft_state"))
    if raft_state == "raft_follower":
        threading.Thread(target=lookout_for_heartbeats, args=[]).start()
    if raft_state == "raft_candidate":
        threading.Thread(target=seek_votes, args=[UDP_Socket]).start()
    if raft_state == "raft_leader":
        environ['raft_leader'] = environ.get("hostname")
        notify_thread = threading.Thread(target=set_leader_msg_all_nodes, args=[UDP_Socket])
        notify_thread.start()
        notify_thread.join()
        heart_beat_thread = threading.Thread(target=send_heartbeat, args=[UDP_Socket])
        heart_beat_thread.start()


# Creating Socket and binding it to the target container IP and port
UDP_Socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

sender = environ.get("hostname")
# Bind the node to sender ip and port
UDP_Socket.bind((sender, 5555))

if __name__ == "__main__":
    custom_print("starting ",environ.get("hostname"))
    set_raft_state("raft_follower")
    #Starting thread 1
    listen_thread = threading.Thread(target=listener, args=[UDP_Socket])
    listen_thread.start()