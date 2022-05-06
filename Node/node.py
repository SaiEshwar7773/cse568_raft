from email import message
from glob import glob
from pickle import TRUE
import socket
import time
import threading
import json
import traceback

from os import environ 
from os.path import exists

import time
import random


#Log indices start from 1 --- not zero



# Constants
MAJORITY = len(environ.get("targets").split(","))//2 + 1
STD_TIMEOUT =  1.5
current_term_timeout = STD_TIMEOUT*random.uniform(2,3)
FILE_NAME = "./logs.json"
 
# Empty File      
with open(FILE_NAME, "w") as file:
    empty = []
    json.dump(empty, file) 

#Constants for Leaders --- Log Replication
next_index_dict = {}
match_index_dict = {}
pending_entries_queue = []
commit_votes_dict = {} 




# Phase 4 --- Volatile State
commit_index = 0    # Both for follower and leader
last_applied = 0    # Unused for now



# Initialize
targets = environ.get("targets").split(",")
port = 5555
heartbeat_timer = time.time()
extra_logs = False


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
    if extra_logs :
        print(*x)

# # Send Message From Candidate -- Request Vote
def seek_votes(skt):
    global candidate_timer 
    candidate_timer = time.time()
    while environ["raft_state"]=="raft_candidate":
        request_vote(skt)
        # custom_print(voting_completed)
        time.sleep(STD_TIMEOUT*random.uniform(2,3))

def reset_heartbeat_timer(location):
    global heartbeat_timer
    # custom_print(time.time()-heartbeat_timer,location)
    heartbeat_timer = time.time()

def timeout():
    if (time.time() - heartbeat_timer > current_term_timeout):
        custom_print(time.time() - heartbeat_timer, "check_timeout")
        return True
    return False
    

# Send Message From Candidate -- Request Vote
def request_vote(skt):
    global votes_received 
    votes_received = 1 
    global voting_completed 
    voting_completed = False
    environ["term"] = str(int(environ.get("term")) + 1)
    reset_heartbeat_timer("request_vote")
    custom_print(environ.get("hostname") ," candidate, seeking votes for term: " , environ.get("term"))
    msg = {
            "request" : "RequestVoteRPC",
            "term" : environ.get("term"),
            "candidate_id": environ.get("hostname"),
            "prevLogIndex" : commit_index,
            "prevLogTerm" : 0 
        } 
    msg_bytes = json.dumps(msg).encode('utf-8')
    for target in targets:
        try:
            skt.sendto(msg_bytes, (target, 5555))
            custom_print(environ.get("hostname"), "requesting vote from", target, "Term ", environ.get("term"))
            # skt.sendto(msg_bytes, (target, 5555))
        except Exception as e:
            # pass
            print(e)


    

# Send Leader INFO --- Contoller Funtion
def send_leader_info(skt):
    msg = {}
    msg["sender_name"] = environ.get("hostname")
    msg["term"] = environ.get("term")
    msg["request"] = "LEADER_INFO"
    msg["key"] = "LEADER"
    msg["value"] = environ.get("raft_leader")
    msg_bytes = json.dumps(msg).encode('utf-8')
    custom_print(msg,environ.get("raft_leader"))
    skt.sendto(msg_bytes, ("Controller", 5555))


# Send Message From --- Leader --- to followers
def set_leader_msg_all_nodes(skt):
    global next_index_dict
    global match_index_dict
    global commit_index
    msg = {
            "request" : "AppendEntry",
            "term" : environ.get("term"),
            "leaderId": environ.get("hostname"),
            "entries" : [],
            "prevLogIndex" : commit_index,
            "prevLogTerm" : get_prev_log_term()
        }
    msg_bytes = json.dumps(msg).encode('utf-8')
    next_index_dict = {}
    match_index_dict = {}
    for target in targets:
            next_index_dict[target] = commit_index + 1 
            match_index_dict[target] = 0 
    for target in targets:
        try:
            msg["prevLogIndex"] = next_index_dict[target] - 1
            skt.sendto(msg_bytes, (target, 5555))
        except Exception as e:
            print(e)
            # pass

def get_prev_log_term():
    with open(FILE_NAME, "r") as file:
        logs = json.load(file)
        if len(logs)>0:
            return logs[-1]["term"]
        else:
            return 0
        
#
def save_logs(entry,next_index):
    log = {}
    log["Entry"] = entry
    log["term"] = environ.get("term")
    with open(FILE_NAME, "r") as file:
        logs = json.load(file)
    with open( FILE_NAME , "w") as file:
        if next_index<len(logs):
            #if no of logs are greater than the next index we have to place the new log into the next index and delete the following logs
            #Receive implementation case 3
            logs=logs[:next_index]
        logs.append(log)
        json.dump(logs, file)
    commit_index = next_index

# Send heartbeats From --- Leader --- to followers
def send_heartbeat(skt):
    reset_heartbeat_timer("send_heartbeat")
    global commit_votes_dict
    global pending_entries_queue
    msg = {
        "request" : "AppendEntry",
        "term" : environ.get("term"), # leaders's term
        "leaderId": environ.get("hostname"), # leaders's id
        "entries" : []
        }
    while environ.get("raft_state") == "raft_leader":
        # print("commit_votes_count::",commit_votes_count)
        # if commit_votes_count> 0: # log replication happened in last heartbeat
        custom_print(" targets::", targets)
        for target in targets:
            msg["leaderCommit"] = commit_index
            msg["prevLogIndex"] = next_index_dict[target] - 1  
            msg["prevLogTerm"] = get_prev_log_term()
            # print("prevLogTermkk", msg["prevLogTerm"])
            eetime = time.time()
            with open(FILE_NAME, "r") as file:
                logs = json.load(file)
            if next_index_dict[target] <= commit_index:
                msg["entries"] = [] if next_index_dict[target]-1>len(logs) else 0
            else:
                if pending_entries_queue:
                    msg["entries"] = [pending_entries_queue[0]]#dequeue will happen if the majority votes 
                    new_log_no = len(logs)+1
                    commit_votes_dict[new_log_no] = 1 #includes leader itself
                # print(target, "send_heartbeat", "else case" , commit_index, next_index_dict)
            # print(msg["entries"])
            msg_bytes = json.dumps(msg).encode('utf-8')
            try:
                print(msg, " The entry creation", target)
                skt.sendto(msg_bytes, (target, 5555))
            except Exception as e:
                print("node probably delted",  e, target)
                # pass
        time.sleep(STD_TIMEOUT)



# Listener -- Universal
def listener(skt):
    # custom_print(f"Starting Listener")
    while True:
        if environ.get("raft_state")!="raft_shutdown":
            try:
                msg, addr = skt.recvfrom(1024)
            except:
                custom_print(f"ERROR while fetching from socket : {traceback.print_exc()}")

            # Decoding the Message received from Node 1
            decoded_msg = json.loads(msg.decode('utf-8'))
            # custom_print(f"Message Received : {decoded_msg} From : {addr}")

            process_msg(decoded_msg, skt)
        
        #When controller sends msg "convert follower" listern should able to convert the node state to follower 
        if decoded_msg["request"] == "CONVERT_FOLLOWER":
            process_msg(decoded_msg, skt)
    custom_print("Exiting Listener Function")



# Listener  -- Universal Process Message 
def process_msg(msg, skt):
    global vote_casted
    
    message_type = msg["request"]
    # print("Phase4")
    if message_type == "AppendEntry":#Message from Leader -- valid for follower
        message_entries = msg["entries"]
        if msg["term"]>environ.get("term"):#New leader
            set_raft_state("raft_follower")
        if msg["leaderId"] != environ.get("raft_leader"):
            set_raft_leader(msg["leaderId"], msg["term"])
        custom_print("receive heartbeat")
        # if environ.get("raft_state")!="raft_leader":
        if msg["entries"]:
            print("entries(proces_message):: ", msg["entries"], environ["raft_state"])
        reset_heartbeat_timer("process_msg")
        receive_heartbeat(skt, msg)
    if message_type == "RequestVoteRPC":
        if "candidate_id" in msg:
            candidate_log_index = msg["prevLogIndex"]
            cast_vote(skt, msg["candidate_id"], int(msg["term"]), candidate_log_index)
    if message_type == "ResponseVoteRPC": 
        if "follower_id" in msg:
            receive_vote(skt, msg["follower_id"],msg['term'])
    if message_type == "ResponseEntryRPC": 
        follower_next_log = msg["nextlog"] 
        follower_target = msg["target"] 
        receive_heartbeat_response(msg["success"], follower_next_log , follower_target )
    if message_type == "LEADER_INFO": 
        send_leader_info(skt) # no need to respond to the controller -- we are just printing in the console
        msg["key"] = "LEADER"
        msg["value"] = environ.get("raft_leader")
        custom_print(msg)
    if message_type == "CONVERT_FOLLOWER": 
        set_raft_state("raft_follower")
        custom_print("Controller converted ",environ.get("hostname"), " to follower.")
    if message_type == "TIMEOUT":
        if environ.get("raft_state") == "raft_follower":
            set_raft_state("raft_candidate")
            custom_print("Controller converted ",environ.get("hostname"), " to Candidate.")
    if message_type == "SHUTDOWN":
        set_raft_state("raft_shutdown")
    if message_type == "STORE":
        if environ.get("raft_state") == "raft_leader":
            global pending_entries_queue
            global commit_votes_dict 
            pending_entries_queue.append(msg["entries"]) 
        else:
            send_leader_info(skt)
            # print("Phase4::::::::")

def receive_heartbeat_response(success_flag ,follower_next_log , follower_target):
    if success_flag:
        global commit_votes_dict
        if(follower_next_log > next_index_dict[follower_target]): #Always true
            next_index_dict[follower_target] =  follower_next_log
        followers_cur_log = follower_next_log-1
        if followers_cur_log == 0:
            return
        print(followers_cur_log, commit_votes_dict) 
        commit_votes_dict[followers_cur_log] += 1 # Increase the log vote by one
        if commit_votes_dict[followers_cur_log] >= MAJORITY and pending_entries_queue:
            save_logs(pending_entries_queue.pop(0),followers_cur_log)
    else:
        next_index_dict[follower_target] = max(next_index_dict[follower_target]-1, 1 )
    

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
def receive_heartbeat(skt, request_msg):
    # custom_print( "Heartbeat received from: " , environ.get("raft_leader")," at ",environ.get("hostname"))
    with open(FILE_NAME, "r") as file:
        logs = json.load(file)
    commit_index = len(logs)
    print("In receive heartbeat, commit_index", commit_index)
    invalid_term = environ.get("term") > request_msg["term"]
    custom_print("logs", logs)
    if len(logs)>0:
        prev_log_term_local = logs[-1]["term"]
    else:
        prev_log_term_local = 0

    print( "PREV LOG TERM(local):: ", prev_log_term_local , "PREV LOG TERM(Heartbeat):: ",request_msg["prevLogTerm"] , "PREV LOG_LEN:: ", len(logs) , "PREV LOG INDEX(Heartbeat):: ", request_msg["prevLogIndex"])
    invalid_prev = prev_log_term_local != request_msg["prevLogTerm"] or len(logs) != request_msg["prevLogIndex"]
    print("invalid_prev",invalid_prev , "invalid_term",invalid_term)
    if invalid_prev or invalid_term:
        print("Writing logs before",logs)
        if request_msg["prevLogIndex"] < commit_index: # excess log deletion
            logs = logs[:request_msg["prevLogIndex"]+1]
            with open(FILE_NAME, "w") as file:
                json.dump(logs, file)
        
        elif prev_log_term_local != request_msg["prevLogTerm"]:
            logs.pop()
            with open(FILE_NAME, "w") as file:
                json.dump(logs, file)
        print("Writing logs after",logs)
        
        response_msg = {
            "request" : "ResponseEntryRPC",
            "term" : environ.get("term"),
            "success" : False,
            "target" : environ.get("hostname"),
            "nextlog" : len(logs)+1
            }
        msg_bytes = json.dumps(response_msg).encode('utf-8')
        print("Receive heartbeat", "if case")
        skt.sendto(msg_bytes, (environ.get("raft_leader"), 5555))

    else: 
        print("Receive heartbeat", "else case")
        num_logs = len(logs)
        if request_msg["entries"]:
            save_logs(request_msg["entries"],request_msg["prevLogIndex"]+1)
            num_logs +=1
        print("Writing logs else",logs)
        response_msg = {
            "request" : "ResponseEntryRPC",
            "term" : environ.get("term"),
            "success" : True,
            "target" : environ.get("hostname"),
            "nextlog" : num_logs+1
            }
        msg_bytes = json.dumps(response_msg).encode('utf-8')
        skt.sendto(msg_bytes, (environ.get("raft_leader"), 5555))
        ######
        print("Reuest msg",request_msg)

    print("logs(receive_heartbeat):: ", logs)
        

    

# Listen --- Follower --- Cast vote
def cast_vote(skt,target,candidate_term, candidate_log_index):
    global vote_casted 
    with open(FILE_NAME, "r") as file:
        logs = json.load(file)
    follower_logs_length = len(logs)
    if (candidate_log_index < follower_logs_length):
        return
    reset_heartbeat_timer("cast_vote")
    if candidate_term>int(environ.get("term")) and (candidate_term not in vote_casted) :
        custom_print(environ.get("hostname"),"casting vote to,", target ,"for term" , candidate_term ,". Its current term is ", environ.get("term"))
        msg = {
            "request" : "ResponseVoteRPC",
            "term" : candidate_term,
            "follower_id": environ.get("hostname"),
            "prevLogIndex" : 0,
            "prevLogTerm" : 0
        }
        msg_bytes = json.dumps(msg).encode('utf-8')
        skt.sendto(msg_bytes, (target, 5555))
        environ['votedFor'] = target
        # environ["term"] = str(int(environ.get("term")) + 1)
        vote_casted[candidate_term] = target
        #Update the timeout for this term
        global current_term_timeout
        current_term_timeout = STD_TIMEOUT*random.uniform(2,3)
        

#Listen -- follower
def set_raft_leader(raft_leader, term):
    environ['raft_leader'] = raft_leader
    environ['term'] = term
    custom_print(environ.get("hostname") ," set leader to: " , environ.get("raft_leader"))


def lookout_for_heartbeats():
    global heartbeat_timer
    global curr_time
    global vote_casted
    while environ.get("raft_state") == "raft_follower": 
        if timeout() : 
            custom_print("vote_casted::: ", vote_casted, "next_term::", int(environ.get("term"))+1)
            if int(environ.get("term"))+1 not in vote_casted: 
                set_raft_state("raft_candidate")
                break
        time.sleep(STD_TIMEOUT)


#Listen -- Universal
def set_raft_state(raft_state):
    global commit_index
    with open(FILE_NAME, "r") as file:
        logs = json.load(file)
        commit_index = len(logs)
    environ['raft_state'] = raft_state
    # custom_print(environ.get("hostname") ," set to raft state: " , environ.get("raft_state"))
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