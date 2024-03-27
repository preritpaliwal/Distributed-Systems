from flask import Flask, request, json, jsonify
from consistent_hashing import consistentHash
import random, logging, requests, os

app = Flask(__name__)
app.config['DEBUG'] = True
log = logging.getLogger('werkzeug')
log.disabled = True

mapper = consistentHash(num_servers = 2, num_slots = 512, num_virtual_servers = 9)
bookkeeping = {"N": 0, "schema": {}, "shards": [], "servers": {}}

def has_keys(json_data : dict, keys : list):
    
    for key in keys:
        if key not in json_data.keys():
            return False
    
    return True
     

@app.route("/init", methods=["POST"])
def init():
    payload = json.loads(request.data)
    
    if not has_keys(payload, ["N","schema", "shards", "servers"]) or not has_keys(payload["schema"], ["columns", "dtypes"]) or not has_keys(payload["shards"], ["Stud_id_low", "Shard_id","Shard_size"]):
        return jsonify({
            "message" : "<Error> Payload not formatted correctly.",
            "status" : "failure"
        }), 200
    N = payload["N"]
    schema = payload["schema"]
    shards = payload["shards"]
    servers = payload["servers"]
    
    bookkeeping["schema"]=schema
    bookkeeping["shards"]=shards
    
    # TODO: Start N servers with mentioned shards and schema
    
    cnt=0
    for s,v in servers:
        if s.contains('['):
            name = "server"+random.randint(0,1000)
        else:
            name = s
        while name in bookkeeping["servers"].keys():
            name = "server"+random.randint(0,1000)
        bookkeeping["servers"][name] = v
        bookkeeping["N"]+=1
        os.popen(f'docker run --name {name} --network mynet --network-alias {name} -d server:latest').read()
        data_payload = {}
        data_payload["schema"] = schema
        data_payload["shards"] = v
        r = requests.post(f"http://{name}:5000/config", data=data_payload)
        cnt+=1
    
    while cnt<N:
        name = "server"+random.randint(0,1000)
        while name in bookkeeping["servers"].keys():
            name = "server"+random.randint(0,1000)
        cur_shards = ["sh" + str(random.randint(1,len(shards))),"sh"+str(random.randint(1,len(shards)))]
        bookkeeping["servers"][name] = cur_shards
        bookkeeping["N"]+=1
        # TODO - add new server and hit its config endpoint
        os.popen(f'docker run --name {name} --network mynet --network-alias {name} -d server:latest').read()
        data_payload = {}
        data_payload["schema"] = schema
        data_payload["shards"] = cur_shards
        r = requests.post(f"http://{name}:5000/config", data=data_payload)
        cnt+=1

    return jsonify({
        "message" : "Configured Database",
        "status" : "success"
    }) , 200

@app.route("/status", methods=["GET"])
def status():
    return jsonify(bookkeeping) , 200

@app.route("/add",methods=["POST"])
def add():
    payload = json.loads(request.data)
    
    if not has_keys(payload, ["n", "new_shards", "servers"]) or not has_keys(payload["shards"], ["Stud_id_low", "Shard_id","Shard_size"]):
        return jsonify({
            "message" : "<Error> Payload not formatted correctly.",
            "status" : "failure"
        }), 200
    
    n = payload["n"]
    new_shards = payload["new_shards"]
    servers = payload["servers"]
    
    if n > len(servers):
        return jsonify({
            "message" : "<Error> Number of new servers (n) is greater than newly added instances",
            "status" : "failure"
        }), 400
    
    new_servers = []
    cnt=0
    for s,v in servers:
        if s.contains('['):
            name = "server"+random.randint(0,1000)
        else:
            name = s
        while name in bookkeeping["servers"].keys():
            name = "server"+random.randint(0,1000)
        new_servers.append(name)
        bookkeeping["servers"][name] = v
        bookkeeping["N"]+=1
        # TODO - add new server and hit its config endpoint
        os.popen(f'docker run --name {name} --network mynet --network-alias {name} -d server:latest').read()
        data_payload = {}
        data_payload["schema"] = bookkeeping["schema"]
        data_payload["shards"] = v
        r = requests.post(f"http://{name}:5000/config", data=data_payload)
        
        cnt+=1
        if cnt == n:
            break
        
    bookkeeping["shards"] += new_shards

@app.route("/rm", methods=["DELETE"])
def rm():
    pass
    # TODO - remove a server from the database

@app.route("/read", methods=["POST"])
def read():
    pass
    # TODO - pick what servers to ping for each data shard
    # TODO - return data from all servers to client


@app.route("/write", methods=["POST"])
def write():
    pass

    # TODO - update all servers containing the shard
    # TODO - figure out use of curr_idx

@app.route("/update", methods=["PUT"])
def update():
    pass

    # TODO - update a single data row in all servers containting the shard

@app.route("/del", methods=["DELETE"])
def delete():
    pass

    # TODO - delete a record in all servers containing the shard

