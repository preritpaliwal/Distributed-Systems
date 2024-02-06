from flask import Flask, request, json
from consistent_hashing import consistentHash
import random, logging, requests, os

app = Flask(__name__)
app.config['DEBUG'] = True
log = logging.getLogger('werkzeug')
log.disabled = True

# curl -X POST "http://127.0.0.1:5000/add" -H "Content-Type: application/json" -d @add.json

os.popen(f'docker run --name Server_1 --network mynet --network-alias Server_1 -e serverID=1 -d server:latest').read()
os.popen(f'docker run --name Server_2 --network mynet --network-alias Server_2 -e serverID=2 -d server:latest').read()

mapper = consistentHash(num_servers = 2, num_slots = 512, num_virtual_servers = 9)
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
    
    currentReplicas = mapper.getReplicas()
    newNames = []
    for hostname in hostnames:
        if hostname not in currentReplicas:
            newNames.append(hostname)
    
    for i in range(n-len(newNames)):
        for j in range(n+len(currentReplicas)):
            if f"Server_{j}" not in currentReplicas and f"Server_{j}" not in newNames:
                newNames.append(f"Server_{j}")
                break
    
    # sanity check
    print(len(newNames), newNames, flush=True)
    for name in newNames:
        _,server_id= name.split("_")
        container = os.popen(f'docker run --name {name} --network mynet --network-alias {name} -e serverID={server_id} -d server:latest').read()
        
    replicas = mapper.addServer(n, newNames)
    
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
    
    for hostname in hostnames:
        print(hostname, flush=True)
        os.system(f'docker stop {hostname} && docker rm {hostname}')
    
    currentReplicas = mapper.getReplicas()
    for i in range(n - len(hostnames)):
        os.system(f'docker stop {currentReplicas[-1]} && docker rm {currentReplicas[-1]}')
        hostnames.append(currentReplicas[-1])
    
    replicas = mapper.deleteServer(n, hostnames)
    
    
    
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
    if(path == "favicon.ico"):
        
        return app.response_class(
            response = None,
            status = 200
        )
    
    requestID = random.randint(100000, 999999)
    server, rSlot = mapper.addRequest(requestID)
    print(path, server, rSlot, flush=True)
    if(rSlot is None):
        print("HashTable full", requestID)
        return app.response_class(
            response = json.dumps({
                "message" : "HashTable Full"
            }),
            status = 400
        )
    
    if server in load.keys():
        load[server] += 1
    else:
        load[server] = 1
        
    with open("load.json", 'w') as f:
        json.dump(load, f)
    
    print(f"#########\n\n{requestID} served by {server}\n\n\n###############\n\n",flush=True)
    try:
        req = f"http://{server}:5000/{path}"
        print(req, flush=True)
        response = requests.get(req)
        print(response, flush=True)
        mapper.clearRequest(rSlot)
        return response.json(), response.status_code
    
    except:
        
        req = f"http://{server}:5000/{path}"
        response = requests.get(req)
        print(response, flush=True)
        mapper.clearRequest(rSlot)
    
    
    return app.response_class(
        response = response.json(),
        status = response.status_code
    )

if __name__ == "__main__":
        
    app.run( host = "0.0.0.0", port = 5000, debug = True)
    