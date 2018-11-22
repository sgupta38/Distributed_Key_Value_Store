#!/usr/bin/python
# -*- coding: utf-8 -*-
import logging
import logging.config
import ast
import os
import sys
import json
import socket
import threading
import time
import random
sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import kv_pb2
from threading import Lock
from pathlib import Path

KV_FILE = "write-ahead.db"
br_name = ""
br_balance = 0
br_list = []
replica_dict = {}
replica_names_list = []
other_replicas_dict = {}
BUFFER_SIZE = 1024
doTransfer = False
currentSnapId = 9999
snapshot_map = {}
marker_count = 0
isCapture = False
balance_updated = True
received_amount = 0
isInitiator = False
rBalance = 0
my_client_conn_obj = []
sock_obj = []
# Locking Variable
lock = Lock()
lock2 = Lock()
lock3 = Lock()
ip_to_replica = {}
key_value_store = {}
# need to declare variable for caputuring so that marker and tranx are not done at same time
# discuss with sonu

def setup_logging(
    default_path='logging.json',
    defalt_level=logging.DEBUG,
    env_key='LOG_CFG'
    ):
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open('logging.json', 'r') as f:
            config = json.load(f)
            logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=defalt_level)

def parseFile(filename):
    logger.debug('parseFile() entry')
    try:
        replica_dict = {}
        replica_info = []
        f = open(filename)
        values = f.read().split()
        print('values are: ' + str(values))
        i=0
        while i != len(values):
            replica_info.append(values[i+1])
            replica_info.append(values[i+2])
            replica_dict[str(values[i])] = replica_info

            if str(values[i]) != sys.argv[1]:
                replica_names_list.append(str(values[i])) # Just names

            ## ip->port Map
            ip_port = values[i+1] +':'+str(values[i+2])
            ip_to_replica[ip_port]= values[i]

            replica_info = [] # since, added to dictionary, reset it otherwise old list will be appended.
            i = i+3

        f.close()
        logger.debug('Replica Dictionary:%s' ,replica_dict)
        logger.debug('parseFile() exit')

        return replica_dict
    except:
        logger.exception('Some Internal Error occured')

def getIpPortOfReplica(replica_name):
    replica_info = replica_dict.get(replica_name)
    ip = replica_info[0]
    port = int(replica_info[1])
    return ip, port

def InitializeDataStructures():
    global replica_dict
    global other_replicas_dict

    logger.debug('InitializeDataStructures() entry')

    replica_dict =  parseFile(sys.argv[3])
    other_replicas_dict = replica_dict.copy()
    other_replicas_dict.pop(sys.argv[1], None) # Storing other replica info, removing itself
    logger.debug("All Replica info: %s", str(replica_dict))
    logger.debug("Local Replica info: %s", str(other_replicas_dict))

    logger.debug('InitializeDataStructures() exit')

def InitializeKVStore():
    global KV_FILE
    global key_value_store

    logger.debug('InitializeKVStore() entry')

    log_file = Path(KV_FILE)

    if log_file.is_file(): # if file exits, open and populate dictionary
        logger.debug("KV file exists")
        with open(KV_FILE, 'r') as f:
            s = f.read()
            key_value_store = ast.literal_eval(s)
            logger.debug("Previous KV store was : " + str(key_value_store))
    else:
        logger.debug("KV file does not exists. Creating..")
        open(KV_FILE, 'w')
        key_value_store = {}

    logger.debug('InitializeKVStore() exit')

def startTxn():
    global br_name
    global br_balance
    global br_list
    global replica_dict
    global marker_count
    global rBalance
    global isCapture

    logger.debug('startTxn() entry: Record state: %d', isCapture)
    while doTransfer:


        ## Randomly selected any 'BRANCH' to transfer upto 5% of money.
        random_entry = random.choice(br_list)
        client_info = replica_dict.get(random_entry)
        #print("client_info:/n" + str(client_info))
        
        random_ip = client_info[1]
        random_port = int(client_info[2])
        if br_balance > 50: # discuss this value with sonu
            # @todo: Add try-catch here
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((random_ip, random_port))


            ## Creating transfer mesage here
            message = bank_pb2.BranchMessage()
            transfer = bank_pb2.Transfer()
            transfer.money = int((br_balance * random.randrange(1, 5)) / 100)

            ## src & dst branch data is not filled here?
            transfer.src_branch = br_name
            transfer.dst_branch = getBranchName(random_ip, str(random_port))
            message.transfer.CopyFrom(transfer)

            old_balance = br_balance
            lock.acquire()
            br_balance = br_balance - transfer.money
            lock.release()

            if isCapture:
                logger.debug('Last recorded balance at %s is : %d', br_name, br_balance)
                rBalance = br_balance
            else:
                logger.debug('isCapture is OFF')

            print('Sending transfer message: '+str(message))
            print('Updated balance is: '+ str(br_balance))
            logger.debug('SENDING %d to %s', transfer.money, getBranchName(random_ip, str(random_port)))
            logger.debug('Trnasfer Message: %s', message)
            logger.debug('@%s Old balance: %d, New Balance: %d', br_name, old_balance, br_balance)
            s.sendall(message.SerializeToString())
            s.close()

        
            # discuss with sonu		
        time.sleep(2) ## Thread will sleep


def getReplicaName(ip, port):
    global ip_to_replica

    logger.debug('getReplicaName() entry')
    ip_port = ip+':'+ str(port)
    logger.debug('Looking for name  %s ', ip_port)
    logger.debug('Data structure for Dict: %s', str(ip_to_replica))
    replica_name = ip_to_replica.get(str(ip_port))
    if replica_name is not None:
        logger.debug('Its Name is: %s', replica_name)
        return replica_name
    else:
        logger.debug('%s Not found in %s', ip_port, str(ip_to_replica))

    logger.debug('getReplicaName() exit')


if __name__ == '__main__':
    if(len(sys.argv)!=4):
        print('Invalid arguments passed')
        sys.exit(0)
    
    setup_logging()
    logger = logging.getLogger('replica')

    logger.debug("Main() Entry")
    setup_logging()
    logger = logging.getLogger('replica')  # setting confguartion as per 'branch' logger specification
    ip = socket.gethostbyname(socket.getfqdn())
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((ip, int(sys.argv[2])))
    server_socket.listen(5)

    ###
    InitializeDataStructures()
    
    ### Once whole setup is done, load the 'write-ahead log file'
    InitializeKVStore()
    logger.debug('Local Db is: %s', str(key_value_store))


    logger.debug('\nListening on %s:%s\n' % (str(ip), sys.argv[2]))
    print('\n Listening on ' + str(ip) + ' : '+ sys.argv[2])
    try:
        while True:
            kv_message = kv_pb2.KVMessage()

            client_conn, client_addr = server_socket.accept()
            data = client_conn.recv(BUFFER_SIZE)  # dont print data directly, its binary
            kv_message.ParseFromString(data)

            logger.debug('Message Received is: %s', str(kv_message))

            client_ip, client_port = client_conn.getsockname()
            logger.debug("Connected from : %s:%s", client_ip, client_port)

            if kv_message.HasField('get_request'):
                logger.debug('get_request() entry')

                get_request = kv_message.get_request
                logger.debug('  requested key is: %d Consistency_level is: %d', get_request.key, get_request.consistency_level)
                
                ## Based on consistency level, read key and return value to client

                ## todo: send response client based on protocol, preplica handling
                value = key_value_store.get(str(get_request.key)) 
                print('Value is: ' + value)

                logger.debug('get_request() exit')

            elif kv_message.HasField('put_request'):
                logger.debug('put_request() entry')
                ## Based on consistency level, insert key & value and return TRUE to client else FALSE
                
                put_request = kv_message.put_request
                logger.debug('  key is: %d, value is: %d', put_request.key, put_request.value)

                logger.debug('put_request() exit')

            elif kv_message.HasField('replica_request'):
                logger.debug('replica_request() entry')
                ## This message came from 'Co-ordinator'. So get/put based on consistency level

                logger.debug('replica_request() exit')

            elif kv_message.HasField('replica_response'):
                logger.debug('replica_response() entry')
                ## This message came from 'REPLICA'. Based on consistency level criteria, return data to CLIENT.

                logger.debug('replica_response() exit')

            else:
                print('Invalid Message received.')
                logger.debug("Invalid Message received.")


            '''
            branchMessage = bank_pb2.BranchMessage()
            client_conn, client_addr = server_socket.accept()
            data = client_conn.recv(BUFFER_SIZE)
            branchMessage.ParseFromString(data)
            client_ip, client_port = client_conn.getsockname()
            logger.debug("Connected from : %s:%s", client_ip, client_port)

            logger.debug('From %s : Message Received is: %s', getBranchName(client_ip, client_port), str(branchMessage))

            if branchMessage.HasField('init_branch'):
                logger.debug('init_branch() entry')
                br_name = sys.argv[1]
                init_branch = branchMessage.init_branch
                br_balance = init_branch.balance

                doTransfer = True
                storeBranchList(init_branch)
                if len(br_list) > 0:
                    thread = threading.Thread(target=startTxn, args=())
                    thread.setDaemon(True)
                    thread.start()
                
                ###########################################
                logger.debug('My name is: %s', sys.argv[1])
                logger.debug('br_name:' + br_name)
                logger.debug('br_balanace:' + str( br_balance))
                logger.debug('I know Branches: %s', str(replica_dict))
                logger.debug('List of branches is: %s', str(br_list))
                
                ###########################################
                client_conn.shutdown(socket.SHUT_RDWR)
                client_conn.close()

                logger.debug('init_branch() exit')

                
            elif branchMessage.HasField('transfer'):
                logger.debug('Transfer() entry')
                logger.debug('Transfer Message received is: %s', str(branchMessage))
                print('Transfer Message received is: '+str(branchMessage))

                # if transfer message come, it means we are going to increase out balance.
                # Also, we will record it in our snapshot map only if 'isCapture' is true [ i.e, only when recording is ON] 

                old_balance = br_balance
                received_amount = branchMessage.transfer.money
                logger.debug('RECEIVE AMNT IS: %d', received_amount)

                lock2.acquire()
                br_balance = br_balance + branchMessage.transfer.money
                lock2.release()
                balance_updated = True

                print('Updated balance: ' + str(br_balance))
                ## This is new updated balance after adding money

                if isCapture:
                   #final_nav = branchMessage.transfer.src_branch + '->' + branchMessage.transfer.dst_branch
                    if currentSnapId != 9999:
                        snapshot_map[currentSnapId][str(branchMessage.transfer.src_branch)] = branchMessage.transfer.money
                        logger.debug('Map is updated at %s, Since RECORDING is ON, map is: %s', br_name, snapshot_map)
                        rBalance = br_balance
                else:
                    logger.debug('Sorry, Recording has stopped. Map is not updated for this transfer. rBalance is: %d', rBalance)

                ## here, i was supposed to get src and destination branch name in protobuff message.

                logger.debug('Transfer: %d %s->%s', branchMessage.transfer.money, branchMessage.transfer.src_branch, branchMessage.transfer.dst_branch)
                logger.debug('Old balance: %d   New Balance is: %d', old_balance, br_balance)

                client_conn.shutdown(socket.SHUT_RDWR)
                client_conn.close()
                logger.debug('Transfer() exit')

            elif branchMessage.HasField('init_snapshot'):
                logger.debug('init_snapshot() entry')
                marker_count = marker_count + 1 # Since this is also sending its marker for the first time.
                currentSnapId = branchMessage.init_snapshot.snapshot_id

                ## Local state is saved.
                snapshot_map[currentSnapId] = {}
                ## Marking incoming for other branches as zero, initially.

                for branch in replica_dict.keys():
                    snapshot_map[currentSnapId][str(branch)] = 0

                rBalance = br_balance

                logger.debug('SNAPSHOT is :%s', str(snapshot_map))

                for branch in replica_dict.keys():
                    # Sending Marker message to all branches.
                    marker = bank_pb2.Marker()
                    marker.snapshot_id = branchMessage.init_snapshot.snapshot_id
                    marker.src_branch = br_name
                    marker.dst_branch = branch
                    message = bank_pb2.BranchMessage()
                    message.marker.CopyFrom(marker)
                    logger.debug('Marker message to be SENT is: %s', str(message))
                    list1 = replica_dict[branch]
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect((list1[1], list1[2]))
                    sock.sendall(message.SerializeToString())
                    sock.close()
                    logger.debug('Marker message is sent to: %s', branch)

                isCapture = True
                logger.debug('Initiator: RECORDING IS ON for s-id: %d, balance is: %d', currentSnapId, br_balance)
                isInitiator = True 
                client_conn.shutdown(socket.SHUT_RDWR)
                client_conn.close()
                logger.debug('init_snapshot() exit')
            
            elif branchMessage.HasField('marker'):
                logger.debug('Marker messaget() entry')

                currentSnapId = branchMessage.marker.snapshot_id
                src_branch = branchMessage.marker.src_branch
                dst_branch = branchMessage.marker.dst_branch
                logger.debug('Marker Count Is: %d, Total_expected_is: %d', marker_count, len(replica_dict))

                if marker_count == 0:  ## If seeing marker message for the first time, Start 'Recording'
                    
                    logger.debug('Marker Count Is: %d, Total_expected_is: %d', marker_count, len(replica_dict)-1)
                    logger.debug('FIRST MARKER, adding to snapshot_map')
                    marker_count = marker_count + 1

                    # Start 'Recording'
                    currentSnapId = branchMessage.marker.snapshot_id
                    rBalance = br_balance
                    snapshot_map[currentSnapId] = {}
                    ## Here, we have to mark incoming stage to empty
                    snapshot_map[currentSnapId][src_branch] = 0
                    logger.debug('Initial Snapshot:%s', snapshot_map)

                    isCapture = True
                    logger.debug('Non-Initiator: RECORDING IS ON for s_id : %d, balance is: %d', currentSnapId, br_balance)

                    for branch in replica_dict.keys():
                        if branch != src_branch:
                            snapshot_map[currentSnapId][str(branch)] = 0
                    
                    logger.debug('SNAPSHOT@ is :%s', str(snapshot_map))

                    
                    # Sending Marker message to all branches.
                    for branch in replica_dict.keys():
                        marker = bank_pb2.Marker()
                        marker.snapshot_id = currentSnapId
                        marker.src_branch = br_name
                        marker.dst_branch = branch
                        message = bank_pb2.BranchMessage()
                        message.marker.CopyFrom(marker)
                        logger.debug('Marker message to be SENT is: %s', str(message))
                        list1 = replica_dict[branch]
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.connect((list1[1], list1[2]))
                        sock.sendall(message.SerializeToString())
                        sock.close()
                        logger.debug('Marker message is sent to: %s', branch)

                elif marker_count == len(replica_dict): ## If 'ALL' marker messages received, RESET all variables. Algorithm finishes here.
                    logger.debug('Initiator and LAST MARKER')
                    isCapture = False
                    logger.debug('Initiator: RECORDING IS STOP for s_id: %d', currentSnapId)
                    marker_count = 0
                    isInitiator = False
                    balance_updated = False
#                    logger.debug('socket objects are: %s', str(my_client_conn_obj))
                    logger.debug(" Branch Name: %s Balance: %d, Recorded Balance: %d snapshot_map: %s", br_name, int(br_balance),int(rBalance), snapshot_map)
                    #logger.debug('Snapshot captured: %s \n\n' % str(snapshot_map[data.marker.snapshot_id]))
                elif isInitiator is False and marker_count == len(replica_dict)-1:
                    logger.debug('Not Initiator and LAST MARKER for s_id: %d', currentSnapId)
                    isCapture = False
                    marker_count = 0
                    logger.debug('Non-Initiator: RECORDING IS STOP for s_id: %d', currentSnapId)
                    balance_updated = False
                    logger.debug(" Branch Name: %s Balance: %d, Recorded Balance: %d, snapshot_map: %s", br_name, int(br_balance),int(rBalance), snapshot_map)
 #                   logger.debug('socket objects are: %s', str(my_client_conn_obj))

                else:                                 ## Just increment count here.
                    logger.debug('IN-BETWEEN MARKER. Balance: %d, Snapshot is: %s', br_balance, str(snapshot_map))
                    marker_count = marker_count + 1

                client_conn.shutdown(socket.SHUT_RDWR)
                client_conn.close()
                logger.debug('Marker() exit')

            elif branchMessage.HasField('retrieve_snapshot'):
                logger.debug('retrieve_snapshot() entry')
                logger.debug('@%s SONU_RAKESH: %s', br_name,snapshot_map)
                logger.debug('SONU_RAKESH rBalance: %s', rBalance)

                ## parse message to see, 'snapshot_id'
                retrieve_snapshot = branchMessage.retrieve_snapshot
                look_snapid = retrieve_snapshot.snapshot_id 

                ## create returnsnapshot msg here
                branch_message = bank_pb2.BranchMessage()
                return_snapshot = branch_message.return_snapshot
                return_snapshot.local_snapshot.snapshot_id = look_snapid
                return_snapshot.local_snapshot.balance = rBalance

                counter_list = []
                ## look in our dictionary, fill all data in sorted form
                for key in snapshot_map.keys():
                    if look_snapid == key:
                        sids = snapshot_map[key]
                        for branch in sorted(sids.keys()):
                            return_snapshot.local_snapshot.channel_state.append(sids[branch])
                            counter_list.append(sids[branch])
                            
                logger.debug('channel List is: %s ', str(counter_list))	
                logger.debug('return_snapshot: %s', branch_message)
                ##send back 'return snapshot msg'
                client_conn.sendall(branch_message.SerializeToString())
                client_conn.shutdown(socket.SHUT_RDWR)
                client_conn.close()
                logger.debug('retrieve_snapshot() exit')
            '''

    except KeyboardInterrupt:
        print("\nServer Stopped\n")
        server_socket.close()
