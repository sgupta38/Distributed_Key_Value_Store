# Distributed Key-Value Store with Configurable Consistency

This is a distributed key-value store that borrows designs from *Dynamo* and *Cassandra*. Here, *Google protobuf* is used for RPC calls.


### Key-Value Store

Each replica server will be a key-value store. Keys are unsigned integers between **0 and 255**. Values are strings.
Each replica server supports the following key-value operations:

- get key – 		given a key, return its corresponding value

- put key-value – 	if the key does not already exist, create a new key-value pair; otherwise, update the key to the new value

For simplicity, each replica only needs to store key-value pairs in its memory. That is, there is no need to flush
the memory content to persistent storage.

###  Configurable Consistency
This distributed key-value store currently supports four replicas. 
Each replica server is pre-configured with information about all other replicas.

Keys are assigned to replica servers using a partitioner similar to the **_ByteOrderedPartitioner_** in Cassandra. Each replica server is expected to be assigned equal portions of the key space. The replication factor will be 3 – every key-value pair should be stored on three out of four replicas. 
Three replicas are selected as follows: the first replica is determined by the partitioner, and the second and third replicas are determined by going
clockwise on the partitioner ring.

- Keys 0-63 will be stored on replica “0”, and also on replica “1” and “2”.

- Keys 64-127 will be stored on replica “1”, and also on replica “2” and “3”.

- Keys 128-191 will be stored on replica “2”, and also on replica “3” and “0”.

- Keys 192-255 will be stored on replica “3”, and also on replica “0” and “1”.


Every client request (get or put) is handled by a coordinator. Client can select any replica server as the coordinator.
Therefore, any replica can be a coordinator

### Consistency level. 
Similar to Cassandra, consistency level is configured by the client. When issuing a request, put or get, the client explicitly specifies the desired consistency level: **ONE or QUORUM**.

For example, 
**_Write request_** with consistency level _QUORUM_, the coordinator will send the request to all replicas for a key (may or may
not include itself). It will respond successful to the client once the write has been written to quorum replicas – i.e.,
two in our setup. 

**_Read request_** with consistency level _QUORUM_, the coordinator will return the most recent data from two replicas. 
When the consistency level is set to QUORUM during both get and put, we have strong consistency. However,
this is not the case when the client uses _ONE_. 
With **_ONE_** consistency level, different replicas may be inconsistent.
However, replicas will eventually become consistent using either 'Read-Repair' or 'Hinted handoff' mechanisms.

- **Read repair :** When handling read requests, the coordinator contacts all replicas. If it finds inconsistent data, it will
perform “read repair” in the background.

- **Hinted handoff:** During write, the coordinator tries to write to all replicas. As long as enough replicas have
succeeded, ONE or QUORUM, it will respond successful to the client. However, if not all replicas succeeded, e.g.,
two have succeeded but one replica server has failed, the coordinator would store a “hint” locally. If at a later time
the failed server has recovered, it might be selected as coordinator for another client’s request. This will allow other
replica servers that have stored “hints” for it to know it has recovered and send over the stored hints.

### Client
Once started, the client acts as a console, allowing users to issue a stream of requests. The client selects one replica server
as the coordinator for all its requests. That is, all requests from a single client are handled by the same coordinator.


### How to Run:

You should have information about replicas, stored inn replicas.txt in following format.

 > replica0 128.226.114.201 9090
 > 
 > replica1 128.226.114.202 9091
 > 
 > replica2 128.226.114.203 9092
 > 
 > replica3 128.226.114.204 9093


#### Client 

	> python3 client.py  coordinator_ip coordinator_port
	
#### Replica/Co-ordinator:

	For READ-REPAIR:
	> python3 replica.py nodename PORTNUM replicas.txt READ-REPAIR
	
	For HINTED-HANDOFF:
	> python3 replica.py nodename PORTNUM replicas.txt HINTED-HANDOFF