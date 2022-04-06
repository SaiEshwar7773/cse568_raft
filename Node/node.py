import socket
import time
import threading
import json
import traceback


from os import environ 

# Listener
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

        if counter >= 4:
            break
        counter+=1

    print("Exiting Listener Function")

#Sending funtion
def create_msg(skt):
    for i in range(5):
        msg={"sender_name": environ.get("hostname"),
        "request": "VOTE_REQUEST",
        "term": environ.get("term"),
        "key": 'VOTE_REQUEST',
        "value": "Recived"
        }
        # msg = {"msg": f"Hi, I am Node", "counter":counter}
        msg_bytes = json.dumps(msg).encode()
        targets = environ.get("targets").split(",")
        for target in targets:
            skt.sendto(msg_bytes, (target, 5555))
        time.sleep(1)
    # return msg_bytes

# Dummy Function
def function_to_demonstrate_multithreading():
    for i in range(5):
        print(f"Hi Executing Dummy function : {i}")
        time.sleep(2)


if __name__ == "__main__":
    print("starting ",environ.get("hostname"))

    sender = environ.get("hostname")

    # Creating Socket and binding it to the target container IP and port
    UDP_Socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

    # Bind the node to sender ip and port
    UDP_Socket.bind((sender, 5555))

    #Starting thread 1
    threading.Thread(target=listener, args=[UDP_Socket]).start()

    #Starting thread 2
    # threading.Thread(target=function_to_demonstrate_multithreading).start()
    threading.Thread(target=create_msg, args=[UDP_Socket]).start()

    print("Started both functions, Sleeping on the main thread for 10 seconds now")
    time.sleep(15)
    print(f"Completed Node Main Thread Node 2")