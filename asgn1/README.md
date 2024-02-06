# Assignment-1 : Implementing a Customizable Load Balancer

Contributors:
- [Vibhu](https://github.com/vibhu-yadav) - 20CS30071
- [Prerit Paliwal](https://github.com/preritpaliwal/) - 20CS10046
- [Deepsikha Behera](https://github.com/deepsikhabehera) - 20CS10023

## Usage
- sudo make

### Client
- A client makes simple http requests to access the various endpoints of the application.
- We have utilised curl command to simulated the `GET` and `POST` requests during our testing.

## Server
- A HTTP server supporting the endpoints `/home` and `/heartbeat`.
- `/home` endpoint is utilised to inform the client what server serves the request.
- `/heartbeat` is an endpoint useful for checking server health.

## Load Balancer
- The Load Balancer manages the `N` servers being run.

# Details of server

Why K = log(M)

Since request resolution is really quick, instead of using heartbeat to check server health, we simply go ahead with the request and use it's failure as an indication of server failure. 