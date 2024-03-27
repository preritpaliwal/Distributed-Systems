# Assignment-2 : Implementing a Scalable Database with Sharding

Contributors:
- [Deepsikha Behera](https://github.com/deepsikhabehera) - 20CS10023
- [Prerit Paliwal](https://github.com/preritpaliwal/) - 20CS10046
- [Vibhu](https://github.com/vibhu-yadav) - 20CS30071

## Usage
- `sudo make`

### Client
- A client makes simple http requests to access the various endpoints of the application.
- We have utilised curl command to simulated the `GET` and `POST` requests during our testing.

## Database
- A MySQL database with two data tables following the following schemas:
1) ShardT (Stud id low: Number, Shard id: Number, Shard size:Number, valid idx:Number)
2) MapT (Shard id: Number, Server id: Number)
- There are 4 shards in the database, each shard has 3 replicas across the servers, and there are 6 servers having shards in configuration.

## Server
- A HTTP server supporting the endpoints `/config`, `/heartbeat`, `/copy`, `/read`, `/write`, `/update` and `/del`.
- `/config` endpoint supports `POST` requests and is utilized to initialize shard tables in the server database to configure the shards.
- `/heartbeat`  endpoint supports `GET` requests and is useful for checking server health.
- `/copy` endpoint supports `GET` requests and returns all data entries corresponding to the specified shards in the payload.
- `/read` endpoint supports `POST` requests and reads all entries from the shard specified in the payload along with a range of Stud_ids.
- `/write` endpoint supports `POST` requests and writes data entries in the specified shard.
- `/update` endpoint supports `PUT` requests and updates a specified data entry.
- `/del` endpoint supports `DELETE` requests and deletes a specified data entry.

## Load Balancer
- The Load Balancer is the only container that we run ourselves and it manages the `N` server containers being run. Certain modifications have been made to integrate the new features needed in this assignment compared to the previous one.
- The Load Balancer acts as the parent in spawning the server containers initially as well as respawning new server containers on the failure of existing ones.
- It still utilizes Consistent Hashing for mapping requests to `virtual servers`.
- The Load Balancer abstracts away the internal ports and endpoints of servers and handles all the requests at it's port 5000 and supports the following endpoints:
- `/init` : This endpoint supports `POST` requests and initializes distributed database across all the different shards and replicas.
- `/status` : This endpoint supports `GET` requests and returns a json reply containing the number and names of currently active servers.
- `/add` : This endpoint supports `POST` requests and allows addition of new servers to the application by specifying new instances, server names, and shard placements.
- `/rm` : This endpoint supports `POST` requests and allows removal of existing servers.
- `/read` : This endpoint supports `POST` requests and reads data entries from the shard replicas across all server containers based on a given range of Stud_ids.
- `/write` : This endpoint supports `POST` requests and writes data entries to it's corresponding shard replicas across the distributed database.
- `/update` : This endpoint supports `PUT` requests and updates a specified data entry across the distributed database.
- `/del` : This endpoint supports `DELETE` requests and deletes a specified data entry across the distributed database.
- `/<path>` : This generic endpoint supporrts `GET` requests and returns the response from server if path is home else returns an error message.

# Design choices 

- Since request resolution is really quick, instead of using heartbeat to check server health, we simply go ahead with the request and use it's failure as an indication of server failure. 
