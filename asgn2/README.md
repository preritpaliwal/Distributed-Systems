# Assignment-1 : Implementing a Customizable Load Balancer

Contributors:
- [Vibhu](https://github.com/vibhu-yadav) - 20CS30071
- [Prerit Paliwal](https://github.com/preritpaliwal/) - 20CS10046
- [Deepsikha Behera](https://github.com/deepsikhabehera) - 20CS10023

## Usage
- `sudo make`

### Client
- A client makes simple http requests to access the various endpoints of the application.
- We have utilised curl command to simulated the `GET` and `POST` requests during our testing.

## Server
- A HTTP server supporting the endpoints `/home` and `/heartbeat`.
- `/home` endpoint supports `GET` requests and is utilised to inform the client what server serves the request.
- `/heartbeat`  endpoint supports `GET` requests and is useful for checking server health.

## Load Balancer
- The Load Balancer is the only container that we run overselves and it manages the `N` server containers being run.
- The Load Balancer acts as the parent in spawning the server containers initially as well as respawning new server containers on failure of existing ones.
- It utilises Consistent Hashing for mapping requests to `virtual servers`.
- The Load Balancer abstracts away the internal ports and endpoints of servers and handles all the requests at it's port 5000 and supports the following endpoints:
- `/rep` : This endpoint supports `GET` requests and returns a json reply containing the number and names of currently active servers.
- `/add` : This endpoint supports `POST` requests and allows addition of new servers to the application.
- `/rm` : This endpoint supports `POST` requests and allows removal of existing servers.
- `/<path>` : This generic endpoint supporrts `GET` requests and returns the response from server if path is home else returns an error message.

# Design choices 

- Since request resolution is really quick, instead of using heartbeat to check server health, we simply go ahead with the request and use it's failure as an indication of server failure. 
