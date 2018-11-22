import logging
import logging.config
import json
import sys
#sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import os
import socket
#import bank_pb2
import random
from random import randint
import time
import struct

# globals
channels = zip()
br_name = ""
branch_info = []

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

def Create_InitBranch_Message(balance, branch_info):
        logger.debug('Create_InitBranch_Message() entry')
        branch_message = bank_pb2.BranchMessage()
        init_branch = branch_message.init_branch
        init_branch.balance = int(balance)

        for key, value in branch_info.items():
            branch = init_branch.all_branches.add()
            branch.name = key
            branch.ip = value[0]
            branch.port = int(value[1])
           # logger.debug(f' balance: {balance} name:{key} ip:{value[0]} port:{value[1]}')

        logger.debug('InitBranch Message:%s',branch_message)

        logger.debug('Create_InitBranch_Message() exit')
        return branch_message

def Create_InitSnapshot_Message(snapshot_id):

        logger.debug('Create_Initsnapshot_Message() entry')
        branch_message = bank_pb2.BranchMessage()
        init_snapshot = branch_message.init_snapshot
        init_snapshot.snapshot_id = snapshot_id

        logger.debug('Snapshot id is: %d', snapshot_id)

        logger.debug('InitSnapshot Message:%s', branch_message)

        logger.debug('Create_Initsnapshot_Message() exit')
        return branch_message

def Create_RetrieveSnapshot_Message(snapshot_id):

        logger.debug('Create_RetrieveSnapshot_Message() entry')
        branch_message = bank_pb2.BranchMessage()
        retrieve_snapshot = branch_message.retrieve_snapshot
        retrieve_snapshot.snapshot_id = snapshot_id

        logger.debug('Snapshot id is: %d', snapshot_id)

        logger.debug('RetrieveSnapshot Message: %s',branch_message)

        logger.debug('Create_RetrieveSnapshot_Message) exit')
        return branch_message

def get_request():
    print('get_reuqest')

def put_request():
    print('put_request')

def get_response():
    print('get_response')

def put_response():
    print('put_response')


def main():
    # Controller will parse the text file and fill branch info in local structure
    logger.debug('main() entry')
    br_name = sys.argv[1]

    try:
        ## Create socket to send requests to 'branch'.
       # sock = socket.socket()

        if len(sys.argv)  != 3:
            logger.error('Invalid parameters Passed')
            print('Invalid parameters passed.')

        ## Extract co-ordinators ip-port here.
        ip = sys.argv[1]
        port = int(sys.argv[2])

        print('Enter the action to perform:')
        print('1. Get Key')
        print('2. Put Key')
        print('3. Exit')
        choice = input()

        if 1 == choice:
            # call get routine
            get_request()
        elif 2 == choice:
            # call put routine 
            put_request()
        elif 3 == choice:
            exit(0)
            # call exit routine here
        else:
            print('Invalid Choice. Exiting..!!')

        ## Build messages get/put based on user input.

        while True:
            sock = socket.socket()
            sock.connect((ip, port)) 
            sock.sendall(init_snapshot_msg.SerializeToString()) # .encode('ascii')?
            data = sock.recv(1024)
            sock.close()

            # based on recived RESPONSE, functions will be called here.
            
            ## parse protobuf format
            #branchMessage.ParseFromString(data)
                
    except KeyboardInterrupt:
        logger.debug('Ctr + C is pressed exiting controller')
        sock.close()
    finally:
        sock.close()
        logger.debug('main() exit')


# Main routine
if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger('client')
    main()