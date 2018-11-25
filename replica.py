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
write_timestamp = 0.0
read_counter = 0
write_counter = 0
hinted_list = []
CO_ORDINATOR = 1
REPLICA = 2

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
    except SyntaxError:
        logger.debug('Please DELETE old DB file.')
        print('Please DELETE old DB file.')
    except ValueError:
        logger.debug('Please DELETE old DB file.')
        print('Please DELETE old DB file.')
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
        return True
    else:
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

def update_key_value_store(key, value, timestamp, ROLE):
        global key_value_store
        global write_timestamp
        global KV_FILE
        
        logger.debug('update_key_value_store() entry')

        #todo: add try catch
        ## Here, we need to insert the key into DB and then convey it to other replicas.

        if ROLE == CO_ORDINATOR:
            write_timestamp = time.time()
        elif ROLE == REPLICA:
            write_timestamp = timestamp

        try:
            key_value_store[str(key)] = [value, write_timestamp]
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

def send_ack_to_client(client_conn, put_request):
        logger.debug('send_ack_to_client() entry')

        ## Preparing protobuf resonse structure
        response_kv_message = kv_pb2.KVMessage()
        cord_response = response_kv_message.cord_response
        cord_response.key = put_request.key
        cord_response.status = True
        cord_response.value = put_request.value

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

def eventually_update_replicas(client_conn, partioner_list, put_request, approach):
    global write_timestamp
    global write_counter
    global hinted_list

    try:
        logger.debug('eventually_update_replicas() entry')

        ## Preparing protobuf resonse structure
        response_kv_message = kv_pb2.KVMessage()
        replica_request = response_kv_message.replica_request
        replica_request.key = put_request.key
        replica_request.value = put_request.value
        replica_request.timestamp = write_timestamp
        replica_request.operation = 1                   # put-->write

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
                     continue

                ## Here, parse the 'Resonse' received from REPLICA. Know whether success/false.
                response_from_replica = kv_pb2.KVMessage()
                response_from_replica.ParseFromString(data)

                logger.debug('Replica response is: %s', response_from_replica)

                if response_from_replica.HasField('replica_response'):
                    replica_response = response_from_replica.replica_response

                    ## Only care about 'STATUS' Here.
                    if replica_response.status is True:                # Replica successfully updated
                        write_counter = write_counter + 1

                        ## If write_counter is greater than 2 and its QUORUM based approach
                        if write_counter >= 2 and approach == 2:
                            send_ack_to_client(client_conn, put_request)
                        elif write_counter == 1 and approach == 1:          # co-rdinator != replica [by client]
                            send_ack_to_client(client_conn, put_request)


                    else:           # Replica has failed to update.
                        ## IF its 'HINTED-HANDOFF', store list of failed replicas.
                        if sys.argv[4] == 'HINTED-HANDOFF':
                            hinted_list.append[ID]

                ## @todo: Similar if-else will go for READ-REPAIRS here.
            
                sock.close()

        ## SPECIAL CASE: After all attempts--
        if write_counter == 1 and approach == 2:
            send_errmsg_to_client(client_conn, 'Not Enough Replica Available')

    except KeyError:
        logger.error('Invalid Replica Name. Please check replicas.txt')
    except:
        logger.exception('Some Internal Error occured')

    logger.debug('Final writer_count: %d', write_counter)
    logger.debug('HH list: %s', str(hinted_list))
    logger.debug('eventually_update_replicas() exit')

def  handle_ONE_approach(client_conn, put_request):
    global write_counter

    logger.debug('handle_ONE_approach() entry')

    # get partioner list i.e, replicas responsible for this operation
    partioner_list = getReplicaList(put_request.key)

    logger.debug('Ring is: %s', str(partioner_list))

    #check whether current node/Co-ordinator is one of the replcias?
    if AmIReplica(partioner_list):
        write_counter = write_counter + 1
        update_key_value_store(put_request.key, put_request.value, 0, CO_ORDINATOR) #Its co-ordinator who is updating
        # For ONE, send ACK to client
        send_ack_to_client(client_conn, put_request)
        ## Remove its ID from partioner List.
        partioner_list.remove(getCurentReplicaID())

    # Here, partioner list will definitely not have 'this' ID, Because, if it was 'this', it has already done its work
    # and has removed itself from 'partioner_list'

    eventually_update_replicas(client_conn, partioner_list, put_request, 1)
    logger.debug('handle_ONE_approach() exit')

def handle_QUORUM_approach(client_conn, put_request): # QUORUM
    global write_counter

    logger.debug('handle_QUORUM_approach() entry')

    # get partioner list i.e, replicas responsible for this operation
    partioner_list = getReplicaList(put_request.key)
    logger.debug('Ring is: %s', str(partioner_list))

    # @note: For 'QUORUM' based approach, we are not sending 'ACK' to client immediately.
    if AmIReplica(partioner_list):
        write_counter = write_counter + 1
        update_key_value_store(put_request.key, put_request.value, 0, CO_ORDINATOR)
        ## Remove its ID from partioner List.
        partioner_list.remove(getCurentReplicaID())

    eventually_update_replicas(client_conn, partioner_list, put_request, 2)
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

    ###
    InitializeDataStructures()
    
    ### Once whole setup is done, load the 'write-ahead log file'
    KV_FILE = 'write-ahead{}.db'.format(getCurentReplicaID())
    logger.debug('DB FileName: %s', KV_FILE)

    InitializeKVStore()
    logger.debug('Local Db is: %s', str(key_value_store))

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
                global read_counter
                logger.debug('get_request() entry')

                read_counter = 0
                get_request = kv_message.get_request
                logger.debug('  requested key is: %d Consistency_level is: %d', get_request.key, get_request.consistency_level)
                
                ## Based on consistency level, read key and return value to client

                ## Preparing protobuf resonse structure
                response_kv_message = kv_pb2.KVMessage()
                cord_response = response_kv_message.cord_response
                cord_response.key = get_request.key

                if str(get_request.key) in key_value_store:
                    value = key_value_store.get(str(get_request.key))  # Fetching value of key
                    cord_response.status = True
                    cord_response.value = value[0]
                    cord_response.timestamp = float(value[1])
                else:
                    cord_response.status = False

                client_conn.sendall(response_kv_message.SerializeToString())
                logger.debug('Response sent to client: %s', response_kv_message)
                logger.debug('get_request() exit')

            elif kv_message.HasField('put_request'):
                logger.debug('put_request() entry')
                global write_counter
                write_counter = 0
                ## Based on consistency level, insert key & value and return TRUE to client else FALSE
                
                put_request = kv_message.put_request
                logger.debug('  key is: %d, value is: %s', put_request.key, put_request.value)
                
                ## Before directly updating, first check whether 'this' is in partioner list or not
                
                ## Check Consistency level:
                if put_request.consistency_level == 1:              # ONE
                    handle_ONE_approach(client_conn, put_request)
                elif put_request.consistency_level == 2:
                    handle_QUORUM_approach(client_conn, put_request) # QUORUM

                logger.debug('put_request() exit')

            elif kv_message.HasField('replica_request'):
                logger.debug('replica_request() entry')

                ## This message came from 'Co-ordinator'. So get/put based on consistency level
                replica_request = kv_message.replica_request

                ## create response
                replica_response_message = kv_pb2.KVMessage()
                replica_response = replica_response_message.replica_response
                replica_response.id = int(getCurentReplicaID())

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
                    if True == update_key_value_store(replica_request.key, replica_request.value, replica_request.timestamp, REPLICA):
                        replica_response.status = True
                    else:
                        replica_response.status = False

                client_conn.sendall(replica_response_message.SerializeToString())
                logger.debug('Replica --> co-ordinator: %s', replica_response_message)

                logger.debug('replica_request() exit')
            else:
                print('Invalid Message received.')
                logger.debug("Invalid Message received.")

    except KeyboardInterrupt:
        print("\nServer Stopped\n")
    finally:
        server_socket.shutdown(socket.SHUT_RDWR)
        server_socket.close()

