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
    logger.debug('Received Data is: %s', data)

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

def parse_get_response(kv_message):
    ## parsing the received response here and priting requested 'key', 'value' on console
    logger.debug('parse_get_response() entry')
    logger.debug(' get_response: %s', kv_message)

    if kv_message.HasField('cord_response'):
        cord_response_data = kv_message.cord_response
        if 1 == cord_response_data.status: #print only if success else display error
            print(' key: ' + str(cord_response_data.key))
            print(' value: ' + cord_response_data.value)
        else:
            print('Some internal error occured.')

    logger.debug('parse_get_response() exit')

def parse_put_response(kv_message):
    ## parsing the received response here and priting requested 'key', 'value' on console
    logger.debug('parse_put_response() entry')
    logger.debug(' put_response: %s', kv_message)

    if kv_message.HasField('cord_response'):
        cord_response_data = kv_message.cord_response
        if 1 == cord_response_data.status: #print only if success else display error
            print(' Successfully Entered..!!')
        else:
            print('Some internal error occured.')

    logger.debug('parse_put_response() exit')

def main():
    # Controller will parse the text file and fill branch info in local structure
    logger.debug('main() entry')

    try:
        ## Create socket to send requests to 'branch'.
        # sock = socket.socket()

        if len(sys.argv)  != 3:
            logger.error('Invalid parameters Passed')
            print('Invalid parameters passed.')

        ## Extract co-ordinators ip-port here.
        ip = sys.argv[1]
        port = int(sys.argv[2])

        while True:

            print('Enter the action to perform:')
            print('1. Get Key ')
            print('2. Put Key ')
            print('3. Exit ')
            choice = input()

        #####  GET REQUEST
            if 1 == int(choice):
                # call get routine
                print('Enter the key to search')
                key = input()
                print('Enter the CONSISTENCY LEVEL you would like.')
                print('  1. ONE')
                print('  2. QUORUM')
                c_level = input()
                req = get_request(key, c_level)
                response = sendDataOverSocket(ip, port, req)
                parse_get_response(response)

        #####  PUT REQUEST

            elif 2 == int(choice):
                # call put routine 
                print('Enter the key  ')
                key = input()
                print('Enter the value  ')
                value = input()
                print('Enter the CONSISTENCY LEVEL you would like. ')
                print('  1. ONE')
                print('  2. QUORUM')
                c_level = input()
                req = put_request(key, value, c_level)
                response = sendDataOverSocket(ip, port, req)
                parse_put_response(response)

        ####  QUIT
            elif 3 == int(choice):
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
