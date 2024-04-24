# Assignment-3 : Implementing a Write-Ahead Logging for consistency in Replicated Database with Sharding

Contributors:
- [Deepsikha Behera](https://github.com/deepsikhabehera) - 20CS10023
- [Prerit Paliwal](https://github.com/preritpaliwal/) - 20CS10046
- [Vibhu](https://github.com/vibhu-yadav) - 20CS30071

## Introduction
This assignment implements a sharded database system capable of distributing a single table, StudT, across multiple server containers. Each shard manages a limited number of entries, enabling scalability and parallel read capabilities. The system utilizes a `Write-Ahead Logging (WAL)` mechanism to maintain consistency among replicas of the shards distributed across various servers.

## Usage
- `sudo make`

### Client
- A client makes simple http requests to access the various endpoints of the application.
- We have utilised curl command to simulated the `GET` and `POST` requests during our testing.

## Database
- A MySQL database with two data tables following the following schemas:
1) `ShardT (Stud id low: Number, Shard id: Number, Shard size:Number, valid idx:Number)`
2) `MapT (Shard id: Number, Server id: Number)`
- There are 4 shards in the database, each shard has 3 replicas across the servers, and there are 6 servers having shards in configuration.

## Server
A HTTP server supporting the endpoints `/config`, `/copy`, `/read`, `/write`, `/update` and `/del`, It handles the shards of the `StudT (Stud id: Number, Stud name: String, Stud marks:Number)` table.
- `/config` endpoint supports `POST` requests and is utilized to initialize shard tables in the server database to configure the shards.
- `/copy` endpoint supports `GET` requests and returns all data entries corresponding to the specified shards in the payload.
- `/read` endpoint supports `POST` requests and reads all entries from the shard specified in the payload along with a range of Stud_ids.
- `/write` endpoint supports `POST` requests - for secondary servers, makes changes in the log and then writes the entries in a shard in the particular server container; for primary server, it first makes changes to it's log and then sends write request to other servers where same shard is present, once it gets confirmation from other secondary servers then it writes the data in its database.
- `/update` endpoint supports `PUT` requests and updates a specified data entry - expects only one entry to be updated in the server container along with Shard_id.
- `/del` endpoint supports `DELETE` requests and deletes a specified data entry - expects only one entry to be deleted in the server container along with Shard_id.

##HashRing
The HashRing class implements a distributed hash ring using consistent hashing - as implemented in Assignment 1. Consistent hashing is used to map keys to servers in this system efficiently. 

## Load Balancer
- The Load Balancer is the only container that we run ourselves and it manages the `N` server containers being run. Certain modifications have been made to integrate the new features needed in this assignment compared to the previous one.
- The Load Balancer manages the Stud_id -> Shard_id -> Server_id mapping. We have maintained consistent hashmaps for each of the shards - hashmaps can be identified with the Shard_id. 
- It still utilizes Consistent Hashing for mapping requests to `virtual servers`.
- The Load Balancer abstracts away the internal ports and endpoints of servers and handles all the requests at it's port 5000 and supports the following endpoints:
- `/init` : This endpoint supports `POST` requests and initializes distributed database across all the different shards and replicas.
- `/status` : This endpoint supports `GET` requests and returns a json reply containing the number and names of currently active servers.
- `/add` : This endpoint supports `POST` requests and allows addition of new servers to the application by specifying new instances, server names, and shard placements.
- `/rm` : This endpoint supports `POST` requests and removes server instances from load balancer. If a primary server is removed, Shard Manager triggers primary selection from available servers for that shard. Shard Manager exposes a /primary_elect (GET) endpoint to handle the request from Load Balancer after /rm is triggered.
- `/read` : This endpoint supports `POST` requests and reads data entries from the shard replicas across all server containers based on a given range of Stud_ids.
- `/write` : This endpoint supports `POST` requests and writes data entries to it's corresponding shard replicas across the distributed database.
- `/update` : This endpoint supports `PUT` requests and updates a specified data entry across the distributed database.
- `/del` : This endpoint supports `DELETE` requests and deletes a specified data entry across the distributed database.

# Design choices 

- Every server maintains log file for each shard.
- Entries in log file contain byte offset (as index), CRC and data (key?)

# Analysis
The `Analysis` folder contains scripts to do the following analysis:
Certainly! You can summarize this analysis process in your README file as follows:

`A-1: Default Configuration`: Report the read and write speed for 10,000 writes and 10,000 reads in the default configuration provided in task 2.

`A-2: Increase Shard Replicas`: Increase the number of shard replicas to 7 from the default configuration and report the write speed for 10,000 writes and read speed for 10,000 reads.

`A-3: Increase Servers and Shards`: Increase the number of servers to 10 and the number of shards to 6, with each shard having 8 replicas. Define the new configuration accordingly. Report the write speed for 10,000 writes and read speed for 10,000 reads.

`A-4: Endpoint Check and Server Drop Test`: Ensure correctness of all endpoints. Manually drop a server container to demonstrate the load balancer's ability to spawn a new container and copy shard entries from other replicas.
