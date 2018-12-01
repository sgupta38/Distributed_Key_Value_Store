'''
    @Author: Sonu Gupta
    @Purpose: This file acts as a 'Client'
'''
import logging
import logging.config
import json
import sys
sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import os
import socket
import kv_pb2
import random
from random import randint
import time
import struct

# globals
channels = zip()
br_name = ""
branch_info = []
BUFFER_SIZE = 1024
branch_dict = {}
REPLICA_FILE = 'replicas.txt'

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
    global branch_info
    global branch_dict
    try:
        branch_dict = {}
        branch_info = []
        f = open(filename)
        values = f.read().split()
        i=0
        while i != len(values):
            branch_info.append(values[i+1])
            branch_info.append(values[i+2])
            branch_dict[str(values[i])] = branch_info
            branch_info = [] # since, added to dictionary, reset it otherwise old list will be appended.
            i = i+3
    
        f.close()
        logger.debug('Branch Dictionary:%s' ,branch_dict)
        logger.debug('parseFile() exit')

        return branch_dict
    except:
        logger.exception('Some Internal Error occured')

def sendDataOverSocket(ip, port, message):
    logger.debug('sendDataOverSocket() entry')
    logger.debug('ip: %s port:%s message %s', ip, str(port), message)

    sock = socket.socket()
    sock.connect((ip, int(port))) # Connect to branch and send message.
    sock.sendall(message.SerializeToString()) # .encode('ascii')
    data = sock.recv(BUFFER_SIZE) 
    kv_message = kv_pb2.KVMessage()
    kv_message.ParseFromString(data)
    logger.debug('Received KV is: %s', kv_message)

    ## Since, co-ordinator will response either True or False and based on that client will just print
    ## o/p on console
    if kv_message.HasField('cord_response'):
        ## check status 
        if kv_message.cord_response.status is True:
            print('=====================================================================')
            print('Operation Successfully Completed....!!!')
            print(' Key: ' + str(kv_message.cord_response.key))
            print(' Value: ' + kv_message.cord_response.value)
            print('=====================================================================')
            print()
        else:
            print('=====================================================================')
            print("Error: Either Key is not Valid or Some error in writing key to DB.")
            print('=====================================================================')

    elif kv_message.HasField('error_message'):
            print('=====================================================================')
            print("Error:" + str(kv_message.error_message.msg))
            print('=====================================================================')

    logger.debug('sendDataOverSocket() exit')
    return kv_message.ParseFromString(data)

def get_request(key, c_level):
    # creating probuf based message here and returning to send over socket
    logger.debug('get_request() entry')
    logger.debug('Looking for key:%s, level: %s', key, c_level)
    kv_message = kv_pb2.KVMessage()
    get_request = kv_message.get_request
    get_request.key = int(key)
    get_request.consistency_level = int(c_level)

    logger.debug('get_request() exit')
    return kv_message

def put_request(key, value, c_level):
    # creating probuf based message here and returning to send over socket
    logger.debug('put_request() entry')
    logger.debug('Putting key:%s,  value: %s,  level: %s', key, value, c_level)
    kv_message = kv_pb2.KVMessage()
    put_request = kv_message.put_request
    put_request.key = int(key)
    put_request.value = value
    put_request.consistency_level = int(c_level)

    logger.debug('put_request() exit')
    return kv_message    

def display_kv_store():
    logger.debug('display_kv_store() entry')

    kv_message = kv_pb2.KVMessage()
    display_request = kv_message.display_kvstore
    display_request.status = True

    logger.debug('display_kv_store() exit')
    return kv_message


def main():
    # Controller will parse the text file and fill branch info in local structure
    logger.debug('main() entry')

    try:
        ## Create socket to send requests to 'branch'.
        # sock = socket.socket()

        if len(sys.argv)  != 3:
            logger.error('Invalid parameters Passed')
            print('Invalid parameters passed.')

        ## parse file
        replica_dict =  parseFile(REPLICA_FILE)

        ## Display all entity involved here.
        logger.debug('Node info: %s', str(branch_dict))

        ## Extract co-ordinators ip-port here.
        ip = sys.argv[1]
        port = int(sys.argv[2])

        while True:

            print('Enter the action to perform:')
            print('1. Get Key ')
            print('2. Put Key ')
            print('3. Display ')
            print('4. Exit ')
            choice = input()

        #####  GET REQUEST
            if 1 == int(choice):
                # call get routine
                try:
                    print('Enter the key to search')
                    key = input()
                    if 0<=int(key)<=255:
                        print('Enter the CONSISTENCY LEVEL you would like.')
                        print('  1. ONE')
                        print('  2. QUORUM')
                        c_level = input()
                        req = get_request(key, c_level)
                        response = sendDataOverSocket(ip, port, req)
                    else:
                        print('Key if Out-of Range. Please Enter between range 0-255')
                except ValueError:
                    print('Error: Please Enter Valid \'Integer Value\' ')
                except:
                    print('Error: Internal Error Occured')

        #####  PUT REQUEST

            elif 2 == int(choice):
                # call put routine 
                try:
                    print('Enter the key  ')
                    key = input()
                    if 0<=int(key)<=255:
                        print('Enter the value  ')
                        value = input()
                        print('Enter the CONSISTENCY LEVEL you would like. ')
                        print('  1. ONE')
                        print('  2. QUORUM')
                        c_level = input()
                        req = put_request(key, value, c_level)
                        response = sendDataOverSocket(ip, port, req)
                    else:
                        print('Key if Out-of Range. Please Enter between range 0-255')
                except ValueError:
                    print('Error: Please Enter Valid \'Integer Value\' ')
                except:
                    print('Error: Internal Error Occured')

            elif 3 == int(choice):
                # call put routine 
                req = display_kv_store()
                logger.debug('Sending Msg: %s', str(req))
                # request all replcas for showing the data.
                for key in branch_dict:
                    info = branch_dict[key]
                    try:
                        sock = socket.socket()
                        sock.connect((info[0], int(info[1]))) # Connect to branch and send message.
                        sock.sendall(req.SerializeToString()) # .encode('ascii')
                        sock.close()
                    except:
                        logger.error('Connection Error. Connecting to next client')
                        sock.close()
                        continue

        ####  QUIT
            elif 4 == int(choice):
                exit(0)
                # call exit routine here
            else:
                print('Invalid Choice. Exiting..!!')

            ## Build messages get/put based on user input.

    except KeyboardInterrupt:
        logger.debug('Ctr + C is pressed exiting controller')
    finally:
        logger.debug('main() exit')

# Main routine
if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger('client')
    main()
