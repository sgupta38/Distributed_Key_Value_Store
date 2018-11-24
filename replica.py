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

KV_FILE = "write-ahead.db"
replica_dict = {}
replica_names_list = []
other_replicas_dict = {}
BUFFER_SIZE = 1024
# Locking Variable
lock = Lock()
ip_to_replica = {}
key_value_store = {}
my_name = ''

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

# returns ID based on passed name. replica1-->1 replica0-->0 etc
def getCurentReplicaID():
    logger.debug('getCurentReplicaID() entry')

    s = [my_name]
    ID = re.findall('\d+', s[0])

    logger.debug('ID is: %s', ID[0])
    logger.debug('getCurentReplicaID() exit')

    return ID[0]

def getReplicaList(key):
    logger.debug('getReplicaList() entry')

    if 0<=int(key)<=63:
        logger.debug('ReplicaList is: %s', str([0,1,2]))
        return [0,1,2]
    elif 64<=int(key)<=127:
        logger.debug('ReplicaList is: %s', str([1,2,3]))
        return [1,2,3]
    elif 128<=int(key)<=191:
        logger.debug('ReplicaList is: %s', str([2,3,0]))
        return [2,3,0]
    elif 192<=int(key)<=255:
        logger.debug('ReplicaList is: %s', str([3,0,1]))
        return [3,0,1]
    else:
        logger.debug('Invalid Key Passed')
        return []
    
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
    my_name = sys.argv[1]

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
                ## Based on consistency level, insert key & value and return TRUE to client else FALSE
                
                put_request = kv_message.put_request
                logger.debug('  key is: %d, value is: %s', put_request.key, put_request.value)

                ## Here, we need to insert the key into DB and then convey it to other replicas.
                key_value_store[str(put_request.key)] = [put_request.value, time.time()]

                getCurentReplicaID()
                getReplicaList(put_request.key)

                ## Write to write-ahead file
                f = open(KV_FILE, "w")
                f.write(str(key_value_store))
                f.close()

                 ## Preparing protobuf resonse structure
                response_kv_message = kv_pb2.KVMessage()
                cord_response = response_kv_message.cord_response
                cord_response.key = put_request.key
                cord_response.status = True
                cord_response.value = put_request.value

                client_conn.sendall(response_kv_message.SerializeToString())
                logger.debug('Response sent to client: %s', response_kv_message)
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

    except KeyboardInterrupt:
        print("\nServer Stopped\n")
    finally:
        server_socket.shutdown(socket.SHUT_RDWR)
        server_socket.close()

