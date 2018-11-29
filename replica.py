'''
    @Author: Sonu Gupta
    @Purpose: This file acts as a 'Co-rdinator' and 'Replica'. It handles requests from other clients/Co-ordinator
'''
#!/usr/bin/python
# -*- coding: utf-8 -*-
import logging
import logging.config
import ast
import re
import os
import sys
import json
import socket
import threading
import time
import operator
import random
sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import kv_pb2
from threading import Lock
from pathlib import Path

KV_FILE = ""
replica_dict = {}
replica_names_list = []
other_replicas_dict = {}
BUFFER_SIZE = 1024
# Locking Variable
lock = Lock()
ip_to_replica = {}
key_value_store = {}
my_name = ''
timestamp = 0.0
read_counter = 0
write_counter = 0
hinted_list = []
CO_ORDINATOR = 1
REPLICA = 2
GET = 0
PUT = 1
CONSISTENCY_LEVEL_QUORUM = 2
CONSISTENCY_LEVEL_ONE = 1
READ_REPAIR = 1
HINTED_HANDOFF = 2
configuration = 0
hint_dict = {}

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
    logger.debug('getIpPortOfReplica() entry')

    replica_info = replica_dict.get(replica_name)
    ip = replica_info[0]
    port = int(replica_info[1])

    logger.debug('Relica name: %s, ip:%s, port:%d', replica_name, ip, port)
    logger.debug('getIpPortOfReplica() exit')

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

    try:
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
            s = f.write(str(key_value_store))
    except SyntaxError:
        logger.error('Please DELETE old DB file.')
        print('Error: Please DELETE old DB file.')
    except ValueError:
        logger.error('Please DELETE old DB file.')
        print('Error: Please DELETE old DB file.')
    except:
        print('Some Internal Error Occured while initilising KV STORE')

    logger.debug('InitializeKVStore() exit')

# returns ID based on passed name. replica1-->1 replica0-->0 etc
def getCurentReplicaID():
    logger.debug('getCurentReplicaID() entry')

    s = [my_name]
    ID = re.findall('\d+', s[0])

    logger.debug('ID is: %s', ID[0])

    logger.debug('getCurentReplicaID() exit')
    return ID[0]

def getReplicaIDFromName(r_name):
    logger.debug('getReplicaIDFromName() entry')

    s = [r_name]
    ID = re.findall('\d+', s[0])

    logger.debug('ID is: %s', ID[0])

    logger.debug('getReplicaIDFromName() exit')
    return ID[0]

def getReplicNameFromID(ID):
    logger.debug('getReplicNameFromID() entry')

    ## just append ID to 'replica'. @Note: Its assumed that replica name will always be specified as 'replicaX'
    return 'replica'+str(ID)
    logger.debug('getReplicNameFromID() exit')


# @note: List is hardcoded becos its assumed that there 4 nodes and no failure.
# This might chnage in future based on dynamic approach.

def getReplicaList(key):
    logger.debug('getReplicaList() entry')

    if 0<=int(key)<=63:
        logger.debug('ReplicaList is: %s', str([0,1,2]))
        return ['0','1','2']
    elif 64<=int(key)<=127:
        logger.debug('ReplicaList is: %s', str([1,2,3]))
        return ['1','2','3']
    elif 128<=int(key)<=191:
        logger.debug('ReplicaList is: %s', str([2,3,0]))
        return ['2','3','0']
    elif 192<=int(key)<=255:
        logger.debug('ReplicaList is: %s', str([3,0,1]))
        return ['3','0','1']
    else:
        logger.debug('Invalid Key Passed')
        return []

def AmIReplica(partioner_ring):
    logger.debug('AmIReplica() entry')

    ID = getCurentReplicaID()
    logger.debug('ID is: %s partioner_ring is: %s', ID, str(partioner_ring))

    if ID in partioner_ring:
        logger.debug('ID belongs to partioner_ring')
        return True
    else:
        logger.debug('ID DOES NOT belongs to partioner_ring')
        return False
    
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

def update_key_value_store(key, value, p_timestamp):
        global key_value_store
        global KV_FILE
        global timestamp
        
        logger.debug('update_key_value_store() entry')

        write_timestamp = p_timestamp

        try:
            ## If key doesnt exist, add it.
            if str(key) not in key_value_store:
                logger.debug('key does not exist, adding it')
                lock.acquire()
                key_value_store[str(key)] = [value, write_timestamp]
                lock.release()
            else:
                ## update a key only when its present timestamp is less than timestamp of the update.
                logger.debug('old timestamp(%f), new timestamp(%f)', key_value_store[str(key)][1], p_timestamp)

                if  key_value_store[str(key)][1] < write_timestamp:
                    lock.acquire()
                    key_value_store[str(key)] = [value, write_timestamp]
                    lock.release()

            ## Write to write-ahead file
            f = open(KV_FILE, "w")
            f.write(str(key_value_store))
            f.close()
        except:
            logger.exception('Some Internal Error occured')
            return False

        logger.debug('Write timestamp is: %f', write_timestamp)
        logger.debug('update_key_value_store() exit')

        return True

def send_ack_to_client(client_conn, request):
        logger.debug('send_ack_to_client() entry')

        ## Preparing protobuf resonse structure
        response_kv_message = kv_pb2.KVMessage()
        cord_response = response_kv_message.cord_response
        cord_response.key = request.key
        cord_response.status = True
        cord_response.value = request.value

        client_conn.sendall(response_kv_message.SerializeToString())
        logger.debug('Response sent to client: %s', response_kv_message)
        logger.debug('send_ack_to_client() exit')

def send_errmsg_to_client(client_conn, msg):
        logger.debug('send_errmsg_to_client() entry')

        ## Preparing protobuf resonse structure
        response_kv_message = kv_pb2.KVMessage()
        error_message = response_kv_message.error_message
        error_message.msg = msg 

        client_conn.sendall(response_kv_message.SerializeToString())
        logger.debug('Response sent to client: %s', response_kv_message)
        logger.debug('send_errmsg_to_client() exit')

def get_most_recent_kv(kvlist_of_tuple):
    logger.debug('get_most_recent_kv() entry')
    kvlist_of_tuple.sort(key=operator.itemgetter(3), reverse=True) # sort in ascending

    logger.debug(' sorted list is: %s', str(kvlist_of_tuple))

    return kvlist_of_tuple[0]
    logger.debug('get_most_recent_kv() exit')

def eventually_update_replicas(client_conn, partioner_list, request, approach, op_type):
    global write_counter
    global read_counter
    global hinted_list
    global timestamp
    global configuration
    global hint_dict
    logger.debug('eventually_update_replicas() entry')

    kvlist_of_tuple = []

    ## @todo: DO i need to add 'own' details here?
    if op_type == GET:

        ## before getting, check if I belong here or not
        if AmIReplica(partioner_list):
            if str(request.key) in key_value_store:
                my_data = key_value_store[str(request.key)]
                kvlist_of_tuple = [tuple((getCurentReplicaID(), request.key, my_data[0], my_data[1]))]

    hinted_list = []

    try:
        logger.debug(' parms: partioner_list: %s, op_type = %s, approach = %s, request = %s ', str(partioner_list), str(op_type), str(approach), request)

        ## Preparing protobuf resonse structure
        response_kv_message = kv_pb2.KVMessage()
        replica_request = response_kv_message.replica_request
        replica_request.key = request.key
        replica_request.id = request.id

        if op_type == PUT:
            replica_request.value = request.value

        replica_request.timestamp = timestamp

        if op_type == PUT:
            replica_request.operation = PUT                   # put-->write
        elif op_type == GET:
            replica_request.operation = GET                   # get-->read

        for ID in partioner_list:
            if ID != getCurentReplicaID():
                replica_name = getReplicNameFromID(ID)
                replica_info = replica_dict[replica_name]
                ip = replica_info[0]
                port = replica_info[1]

                logger.debug('Connecting to %s ip:%s port:%s', replica_name, ip, port)

                ## Connect to respective replica 
                try:
                    sock = socket.socket()
                    sock.connect((ip, int(port))) # Connect to branch and send message.
                    sock.sendall(response_kv_message.SerializeToString()) # .encode('ascii')
                    data = sock.recv(BUFFER_SIZE) 
                except:
                    logger.error('Error while connecting. Trying next to connect next replica')
                    sock.close()
                    # Add id to failed node list
                    hinted_list.append(ID)

                    ## add key, value,ts to hint_dict[id], only if its PUT/Write
                    if op_type == PUT and configuration == HINTED_HANDOFF:
                        hint_dict[ID].append(tuple((request.key, request.value, timestamp)))
                        logger.debug('hint is: %s', str(hint_dict))

                    ## @todo: during RR shall i add this to kvlist??
                    ## kvlist_of_tuple.append(tuple((ID, 0, 0, 0)))       ### Just add the blank details with proper ID
                    continue

                ## Here, parse the 'Response' received from REPLICA. Know whether success/false.
                response_from_replica = kv_pb2.KVMessage()
                response_from_replica.ParseFromString(data)

                logger.debug('Replica response is: %s', response_from_replica)                

                if response_from_replica.HasField('replica_response'):
                    replica_response = response_from_replica.replica_response

                    if configuration == HINTED_HANDOFF:
                        ## Before Doing anything, first store all 'hints'
                        for hinted_handoff in replica_response.hinted_handoff:
                            logger.debug('Received HINT from %s', getReplicNameFromID(hinted_handoff.id))
                            logger.debug('kv = [%d, %s, %f]', hinted_handoff.key, hinted_handoff.value, hinted_handoff.timestamp)
                            update_key_value_store(hinted_handoff.key, hinted_handoff.value, hinted_handoff.timestamp)

                    ## Only care about 'STATUS' Here.
                    if replica_response.status is True:                # Replica successfully updated

                        if op_type == PUT:
                            
                            write_counter = write_counter + 1
                            
                            ## If write_counter is greater than 2 and its QUORUM based approach
                            if write_counter >= 2 and approach == CONSISTENCY_LEVEL_QUORUM:
                                send_ack_to_client(client_conn, request)
                            elif write_counter == 1 and approach == CONSISTENCY_LEVEL_ONE:          # co-rdinator != replica [by client]
                                send_ack_to_client(client_conn, request)
                        
                        elif op_type == GET:

                            read_counter = read_counter + 1

                            ## Read response and store it in tuple. For 'QUORUM', we need to return recent value based on time stamp
                            kvlist_of_tuple.append(tuple((ID, replica_response.key, replica_response.value, replica_response.timestamp)))

                            if read_counter == 1 and approach == CONSISTENCY_LEVEL_ONE:          # co-rdinator != replica [by client]
                                request.value = replica_response.value
                                send_ack_to_client(client_conn, request)

                            ''' 
                                For a read request with consistency level QUORUM, the coordinator will return the most recent
                                data from two replicas
                            '''
                            if read_counter == 2 and approach == CONSISTENCY_LEVEL_QUORUM:

                                # sort the tuple based on time stamp and return the latest value.
                                replica_id, key, value, timestamp = get_most_recent_kv(kvlist_of_tuple)

                                logger.debug('replica_id: %s, key: %d, value: %s, timestamp: %f', replica_id, key, value, timestamp)

                                ## Based on 'latest' timestamp, modify request and send it to client
                                ## @workaround: here, 'request' aka. getrequest has no 'value'. We need struct with 'value' here.
                                request.key = key
                                request.value = value
                                logger.debug('Latest value: %s', request)
                                send_ack_to_client(client_conn, request)

                    else: # Replica has failed to update so inc failed_list. Since, that replica doesnt had KEY, it replid with FALSE status.
                        hinted_list.append(ID) ## It means replica has no key. @discuss

                sock.close()

        ## SPECIAL CASE: After all attempts--QUORUM and Hinted off is selected
        if op_type == PUT:
            if approach == CONSISTENCY_LEVEL_QUORUM and write_counter < 2 :
                logger.debug('QUORUM rules are not satisfied')
                send_errmsg_to_client(client_conn, 'Exception: Not Enough Replica Available')

        if op_type == GET:
            if approach == CONSISTENCY_LEVEL_QUORUM and read_counter < 2:
                logger.debug('QUORUM rules are not satisfied')
                send_errmsg_to_client(client_conn, 'Exception: Not Enough Replica Available')

            ## Read Repair Done here
            if configuration == READ_REPAIR:
                logger.debug('READ-REPAIR enabled, repairing the replicas')
                repair_failed_replicas(kvlist_of_tuple)

    except KeyError:
        logger.error('Invalid Replica Name. Please check replicas.txt')
    except:
        logger.exception('Some Internal Error occured')

    logger.debug('Configuration type: %d', configuration)
    logger.debug('Final writer_count: %d', write_counter)
    logger.debug('Final read_count: %d', read_counter)
    logger.debug('HH list: %s', str( list( set(hinted_list)) ) )   ## @todo:Since this is global list, fire this to read-repair function and reset it.
    logger.debug('kvlist_of_tuple: %s', str(kvlist_of_tuple))
    logger.debug('hint dict: %s', str(hint_dict))
    logger.debug('eventually_update_replicas() exit')

def repair_failed_replicas(kvlist_of_tuple):
    logger.debug('repair_failed_replicas() entry')

    ## While repair, making sure we UPDATE with latest timestamp value.
    kvlist_of_tuple.sort(key=operator.itemgetter(3), reverse=True) # sort in ascending

    failed_list = []
    key = kvlist_of_tuple[0][1]
    latest_value = kvlist_of_tuple[0][2]
    latest_timestamp = kvlist_of_tuple[0][3]

    logger.debug('latest key: %s, value: %s, timestamp: %f', str(key), latest_value, timestamp)

    i = 1
    while i != len(kvlist_of_tuple):
        if kvlist_of_tuple[i][3] != latest_timestamp:
            failed_list.append(kvlist_of_tuple[i][0])
        i = i + 1

    logger.debug('Spawning a thread for READ-REPAIR')
    threading.Thread(target = perform_read_repair, args=(failed_list, key, latest_value, latest_timestamp)).start()

    logger.debug('RR list: %s', str(failed_list))
    logger.debug('repair_failed_replicas() exit')

def perform_read_repair(failed_list, key, value, timestamp):
    logger.debug('perform_read_repair() entry')

    ## Prepare Read-Repair 'Request' Message here.
    RR_request = kv_pb2.KVMessage()
    replica_request = RR_request.replica_request
    replica_request.id = int(getCurentReplicaID())
    replica_request.key = key
    replica_request.value = value
    replica_request.timestamp = timestamp
    replica_request.operation = PUT

    for ID in failed_list:
        replica_name = getReplicNameFromID(ID)
        info = replica_dict[replica_name]
        ip = info[0]
        port = int(info[1])

        ## Just send message here. Dont wait for response.
        logger.debug('Connecting to %s ip:%s port:%s', replica_name, ip, str(port))

        sock = socket.socket()
        sock.connect((ip, int(port))) # Connect to replica and send message.
        sock.sendall(RR_request.SerializeToString())
        sock.close()

    logger.debug('perform_read_repair() exit')
    return True

def  handle_ONE_approach(client_conn, request, op_type):
    global write_counter
    global read_counter
    global timestamp

    logger.debug('handle_ONE_approach() entry')

    # get partioner list i.e, replicas responsible for this operation
    partioner_list = getReplicaList(request.key)

    logger.debug('Ring is: %s', str(partioner_list))

    #check whether current node/Co-ordinator is one of the replcias?
    value = AmIReplica(partioner_list)
    if value is True:
        if op_type == PUT:     # Write operation
            write_counter = write_counter + 1
            update_key_value_store(request.key, request.value, timestamp) #Its co-ordinator who is updating
        elif op_type == GET:   # Read operation
            ### Check whether key exists or not
            if str(request.key) not in key_value_store:
                logger.debug('Checking key existence')
                send_errmsg_to_client(client_conn, 'Key Does not exist')
                return False
            else:
                read_counter = read_counter + 1
                request.value = key_value_store[str(request.key)][0] ## Fill value here, so client can display

        # For ONE, send ACK to client
        send_ack_to_client(client_conn, request)
        ## Remove its ID from partioner List.
        partioner_list.remove(getCurentReplicaID())

    # Here, partioner list will definitely not have 'this' ID, Because, if it was 'this', it has already done its work
    # and has removed itself from 'partioner_list'

    eventually_update_replicas(client_conn, partioner_list, request, CONSISTENCY_LEVEL_ONE, op_type)
    logger.debug('handle_ONE_approach() exit')

def handle_QUORUM_approach(client_conn, request, op_type): # QUORUM
    global write_counter
    global read_counter
    global timestamp

    logger.debug('handle_QUORUM_approach() entry')

    # get partioner list i.e, replicas responsible for this operation
    partioner_list = getReplicaList(request.key)
    logger.debug('Ring is: %s', str(partioner_list))

    # @note: For 'QUORUM' based approach, we are not sending 'ACK' to client immediately.
    if AmIReplica(partioner_list):
        if op_type == PUT:
            write_counter = write_counter + 1
            update_key_value_store(request.key, request.value, timestamp)
        elif op_type == GET:
            ### Check whether key exists or not
            read_counter = read_counter + 1

        ## Remove its ID from partioner List.
        partioner_list.remove(getCurentReplicaID())

    eventually_update_replicas(client_conn, partioner_list, request, CONSISTENCY_LEVEL_QUORUM, op_type)
    logger.debug('handle_QUORUM_approach() exit')

if __name__ == '__main__':
    if(len(sys.argv)!=5):
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
    my_name = sys.argv[1]


    ### setting configuration type:
    if sys.argv[4] == 'READ-REPAIR':
        configuration = READ_REPAIR
        logger.debug('Configuration Type is: READ_REPAIR ')    
    elif sys.argv[4] == 'HINTED-HANDOFF':
        configuration = HINTED_HANDOFF
        logger.debug('Configuration Type is: HINTED_HANDOFF')    
    else:
        logger.debug('Invalid Configuration Type : ')
        print(' Please select appropriate Configuration Type')
        sock.close()
        sys.exit(0)

    ###
    InitializeDataStructures()
    
    ### Once whole setup is done, load the 'write-ahead log file'
    KV_FILE = 'write-ahead{}.db'.format(getCurentReplicaID())
    logger.debug('DB FileName: %s', KV_FILE)

    InitializeKVStore()
    logger.debug('Local Db is: %s', str(key_value_store))

    ########## creating the hint_dictionary data structure  ####
    for key in other_replicas_dict.keys():
        hint_dict[getReplicaIDFromName(key)] = []

    logger.debug('Initially blank, hint_list: %s', str(hint_dict))

    logger.debug('\nListening on %s:%s\n', str(ip), sys.argv[2])
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

                read_counter = 0
                get_request = kv_message.get_request
                get_request.id = int(getCurentReplicaID())
                logger.debug('  requested key is: %d Consistency_level is: %d', get_request.key, get_request.consistency_level)
                
                ## Based on consistency level, read key and return value to client

                ## Check Consistency level:
                if get_request.consistency_level == 1:              # ONE
                    handle_ONE_approach(client_conn, get_request, GET)
                elif get_request.consistency_level == 2:
                    handle_QUORUM_approach(client_conn, get_request, GET) # QUORUM

                logger.debug('get_request() exit')

            elif kv_message.HasField('put_request'):
                logger.debug('put_request() entry')
                write_counter = 0
                
                '''
                 the coordinator should record the time at which the request was received and include this as a
                 timestamp when contacting replica servers for writing
                '''
                timestamp = time.time()

                ## Based on consistency level, insert key & value and return TRUE to client else FALSE
                
                put_request = kv_message.put_request
                put_request.id = int(getCurentReplicaID())
                logger.debug('  key is: %d, value is: %s', put_request.key, put_request.value)
                
                ## Before directly updating, first check whether 'this' is in partioner list or not
                
                ## Check Consistency level:
                if put_request.consistency_level == 1:              # ONE
                    handle_ONE_approach(client_conn, put_request, PUT)
                elif put_request.consistency_level == 2:
                    handle_QUORUM_approach(client_conn, put_request, PUT) # QUORUM

                logger.debug('put_request() exit')

            elif kv_message.HasField('replica_request'):
                global hint_dict
                logger.debug('replica_request() entry')

                logger.debug('Hint dictionary : %s',str(hint_dict)) 

                current_id =  int(getCurentReplicaID())

                ## This message came from 'Co-ordinator'. So get/put based on consistency level
                replica_request = kv_message.replica_request

                ## create response
                replica_response_message = kv_pb2.KVMessage()
                replica_response = replica_response_message.replica_response
                replica_response.id = current_id

                if configuration == HINTED_HANDOFF:
                    #########################################################

                    # For hinted-handoff, first check whether given id exists in hint dict
                    # if present, prepare hinted_message and send along with replica_response message

                    #########################################################

                    ## if id ID in dictionary, iterate all the fields and keep adding messages
                    if str(replica_request.id) in hint_dict:
                        logger.debug('Hint Exists for : %s', getReplicNameFromID(replica_request.id))

                        stale_data_list = hint_dict.get(str(replica_request.id))
                        ## Fill the protobuf based on stale_list

                        for data in stale_data_list:
                            logger.debug('kvt = %s', str(data)), 

                            hh_message = replica_response.hinted_handoff.add()
                            hh_message.id = current_id
                            hh_message.operation = PUT
                            hh_message.key, hh_message.value, hh_message.timestamp = data # tuples will be extracted here

                if 0 == replica_request.operation:              # READ
                    logger.debug('READ request')
                    ## Preparing protobuf resonse structure
                    replica_response.key = replica_request.key

                    if str(replica_request.key) in key_value_store:
                        value = key_value_store.get(str(replica_request.key))  # Fetching value of key
                        replica_response.status = True
                        replica_response.value = value[0]
                        replica_response.timestamp = float(value[1])
                    else:
                        replica_response.status = False

                elif 1 == replica_request.operation:              # WRITE
                    logger.debug('WRITE request')
                    ## Preparing protobuf resonse structure
                    replica_response.key = replica_request.key
                    if True == update_key_value_store(replica_request.key, replica_request.value, replica_request.timestamp):
                        replica_response.status = True
                    else:
                        replica_response.status = False

                client_conn.sendall(replica_response_message.SerializeToString())
                logger.debug('Replica --> co-ordinator: %s', replica_response_message)

                ## Since, message is sent. Delete hints here.
                ## if id ID in dictionary, iterate all the fields and keep adding messages
                if str(replica_request.id) in hint_dict:
                    logger.debug('Restting hint dict, since previous data sent:')
                    hint_dict[str(replica_request.id)] = []

                logger.debug('hint dict after submission is: %s', str(hint_dict))
                logger.debug('replica_request() exit')
            else:
                print('Invalid Message received.')
                logger.debug("Invalid Message received.")

    except KeyboardInterrupt:
        print("\nServer Stopped\n")
    finally:
        server_socket.shutdown(socket.SHUT_RDWR)
        server_socket.close()

