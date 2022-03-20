A toy Distributed hash table using chord DHT ideas. 

https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf


## Demo

Open demo.mp4

## Install the dependencies

Within the virtual environment.

`pip install -r requirements.txt` 

## Steps

1. Run ` sh clean_up_dirs.sh`  if node directories already exist
2. Run ` python generate_testbed.py` to generate the testbed for running. Currently creates 100 key value pairs.
3. Run `python consistent_hashing.py` to run the consistent hashing service.
4. Run `python start_nodes.py` , Brings up 10 nodes and add them to the hash ring.
5. Run `python run_test_bed.py` to test with the test bed data. 

## Features added and commentary.

[1] A set of nodes are created whose ID values are mapped to keys on a Chord DHT. Each node is a program that has an interface to submit read or write requests. E.g. a user can submit a request (READ, <nodeid>, <data_key>) and (WRITE, <nodeid>, <data key, value>).  

> Each node is an isolated process that comes up in a different port number. This process exposes REST endpoints
  which allows for insertion and reading of key value pairs. Storage nodes are backed by separate directories.
> We use pickledb to efficiently serialize and store the key value pairs. 

[2] When a data item write request is made to a node, it hashes the data item (key-value pair) and sends the data item to the node that should be storing the data, e.g. node Ni. A copy of the data is also replicated on successor of node N on the DHT.

> Data doesn't get inserted directly into the client node where the request reaches. Hash function which is implemented through `md5` maps the key to a point in the hash ring. We do binary search through the bisect module to find the right node 
where the data should go. This is very effective since binary search completes in O(logn) time.

> Data is also replicated and stored in a replica server. 

[3] When a read for the data item is received by a node Nj, it routes the request using Chord DHT routing (using finger tables) and sends the request to the target node Nk. Nk then sends the data back to the requester.

> For read also since data is distributed, we might not find keys directly in the client node. We again do binary search to land up in the correct node which will have the key.  Complexity O(logn)

[4] If Nk has failed, then the request should be served from the replica. Assume 2 consecutive nodes do not fail at the same time.

> While insertion we insert data into the primary node and replica node. Replica node is the successor in the hash ring.  When the current node has failed and effectively removed from the hash ring, process will look for the key in the successor node. 

> Endpoint /remove_node

[5]  Now suppose a new node is introduced into the system, the system re-distributes data and mappings using Chord DHT techniques.

> For every new node added in hash ring , there is a predecessor and successor. Redistribution only needs to happen for keys in the predecessor and successor nodes. 

> Endpoint /add_new_node_with_redistribution


## Swagger Schemas

1. Nodes

http://0.0.0.0:3000/docs
http://0.0.0.0:3001/docs 
http://0.0.0.0:3002/docs 
http://0.0.0.0:3003/docs 
http://0.0.0.0:3004/docs 
http://0.0.0.0:3005/docs 
http://0.0.0.0:3006/docs 
http://0.0.0.0:3007/docs 
http://0.0.0.0:3008/docs 
http://0.0.0.0:3009/docs 

2. Consistent hashing

http://0.0.0.0:9999/docs

## Demo video 

demo.mp4

