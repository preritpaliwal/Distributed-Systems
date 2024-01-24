from flask import Flask, request, json
from consistent_hashing import consistentHash
import sys, random, logging, requests

app = Flask(__name__)
app.config['DEBUG'] = True
log = logging.getLogger('werkzeug')
log.disabled = True

# curl -X POST "http://127.0.0.1:5000/add" -H "Content-Type: application/json" -d "@payload.json"

mapper = consistentHash(num_servers = 3, num_slots = 512, num_virtual_servers = 9)
load = {}

@app.route("/rep", methods=["GET"])
def rep():
    
    replicas = mapper.getReplicas()
    
    return app.response_class(
        response = json.dumps({
            "message" : {
                "N" : len(replicas),
                "replicas" : replicas
            }
        }),
        status = 200
    )


@app.route("/add", methods=["POST"])
def add():
    
    payload = json.loads(request.data)
    
    if('n' not in payload.keys() or 'hostnames' not in payload.keys()):
        return app.response_class(
            response = json.dumps({  
                "message" : "<Error> Payload not formatted correctly. Should contain n and hostnames field",
                "status" : "failure"
            }),
            status = 400
        )
        
    n = int(payload['n'])
    hostnames = payload['hostnames']
    
    if(n < len(hostnames)):
        return app.response_class(
            response = json.dumps({
                "message" : "<Error> Length of hostname list is more than newly added instances",
                "status" : "failure"
            }),
            status = 400
        )
        
    # TODO - add instances
    
    replicas = mapper.addServer(n, hostnames)
    
    return app.response_class(
        response = json.dumps({
            "message" : {
                "N" : len(replicas),
                "replicas" : replicas
            },
            "status" : "successful"
        }),
        status = 200
    )
    
@app.route("/rm", methods=["DELETE"])
def rm():
    
    payload = json.loads(request.data)
    
    if('n' not in payload.keys() or 'hostnames' not in payload.keys()):
        return app.response_class(
            response = json.dumps({  
                "message" : "<Error> Payload not formatted correctly. Should contain n and hostnames field",
                "status" : "failure"
            }),
            status = 400
        )
        
    n = int(payload['n'])
    hostnames = payload['hostnames']
    
    if(n < len(hostnames)):
        return app.response_class(
            response = json.dumps({
                "message" : "<Error> Length of hostname list is more than removable instances",
                "status" : "failure"
            }),
            status = 400
        )
        
    print("Calling deleteServers")
    replicas = mapper.deleteServer(n, hostnames)
    
    # TODO - delete instances
    
    return app.response_class(
        response = json.dumps({
            "message" : {
                "N" : len(replicas),
                "replicas" : replicas
            },
            "status" : "successful"
        }),
        status = 200
    )
    
@app.route("/<path>", methods = ["GET"])
def serveClient(path):
    
    requestID = random.randint(100000, 999999)
    server, rSlot = mapper.addRequest(requestID)
    
    if(rSlot is None):
        print("HashTable full", requestID)
        return app.response_class(
            response = json.dumps({
                "message" : "HashTable Full"
            }),
            status = 400
        )
    
    server2port = {
        "Server 1" : 5001,
        "Server 2" : 5002,
        "Server 3" : 5003,
    }
    
    if server in load.keys():
        load[server] += 1
    else:
        load[server] = 1
        
    with open("load.json", 'w') as f:
        json.dump(load, f)
    
    print(f"{requestID} served by {server}")
    
    try:
        response = requests.get(f"http://127.0.0.1:{server2port[server]}/{path}")
        mapper.clearRequest(rSlot)
    
    except:
        
        # TODO - Server Down, Respawn
        
        response = requests.get(f"http://127.0.0.1:{server2port[server]}/{path}")
        mapper.clearRequest(rSlot)
    
    
    return app.response_class(
        response = response.json(),
        status = response.status_code
    )

if __name__ == "__main__":
        
    app.run( host = "0.0.0.0", port = 5000, debug = True)
    