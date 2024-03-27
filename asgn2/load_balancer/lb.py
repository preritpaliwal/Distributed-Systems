from flask import Flask, request, json, jsonify
from consistent_hashing import consistentHash
import random, logging, requests, os, time

app = Flask(__name__)
app.config['DEBUG'] = True
# log = logging.getLogger('werkzeug')
# log.disabled = True

num_replicas = 3

# mapper = consistentHash(num_servers = 2, num_slots = 512, num_virtual_servers = 9)
bookkeeping = {"N": 0, "schema": {}, "shards": [], "servers": {}}
shard_mappers = {}

def has_keys(json_data : dict, keys : list):
    
    for key in keys:
        if key not in json_data.keys():
            return False
    
    return True
     

@app.route("/init", methods=["POST"])
def init():
    payload = json.loads(request.data)
    
    if not has_keys(payload, ["N","schema", "shards", "servers"]) or not has_keys(payload["schema"], ["columns", "dtypes"]) :
        return jsonify({
            "message" : "<Error> Payload not formatted correctly.",
            "status" : "failure"
        }), 200
    N = payload["N"]
    schema = payload["schema"]
    shards = payload["shards"]
    servers = payload["servers"]
    
    for shard in shards:
        shard_mappers[shard["Shard_id"]] = {
            "mapper" : consistentHash(0, 512, 9),
            "servers" : []
        }
    
    bookkeeping["schema"]=schema
    bookkeeping["shards"]=shards
    
    # TODO: Start N servers with mentioned shards and schema
    
    cnt=0
    for s,v in servers.items():
        if '[' in s:
            name = "Server"+str(random.randint(0,1000))
        else:
            name = s
        while name in bookkeeping["servers"].keys():
            name = "Server"+str(random.randint(0,1000))
        bookkeeping["servers"][name] = v
        bookkeeping["N"]+=1
        os.popen(f'docker run --name {name} --network mynet --network-alias {name} -d server:latest').read()
        # print(name,flush=True)
        time.sleep(2)
        data_payload = {}
        data_payload["schema"] = schema
        data_payload["shards"] = v
        response = requests.post(f"http://{name}:5000/config", data=data_payload)
        cnt+=1
    
    while cnt<N:
        name = "Server"+str(random.randint(0,1000))
        while name in bookkeeping["servers"].keys():
            name = "Server"+str(random.randint(0,1000))
        cur_shards = ["sh" + str(random.randint(1,len(shards))),"sh"+str(random.randint(1,len(shards)))]
        bookkeeping["servers"][name] = cur_shards
        bookkeeping["N"]+=1
        # TODO - add new server and hit its config endpoint
        os.popen(f'docker run --name {name} --network mynet --network-alias {name} -d -p 5000 server:latest').read()
        time.sleep(4)
        data_payload = {}
        data_payload["schema"] = schema
        data_payload["shards"] = cur_shards
        r = requests.post(f"http://{name}:5000/config", data=data_payload)
        cnt+=1
        
    for server, shards in bookkeeping["servers"].items():
        
        for shard_id in shards:
            
            shard_mappers[shard_id]["servers"].append(server)
            

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
    
    if not has_keys(payload, ["n", "new_shards", "servers"]):
        return jsonify({
            "message" : "<Error> Payload not formatted correctly.",
            "status" : "failure"
        }), 200
    
    n = int(payload["n"])
    new_shards = payload["new_shards"]
    servers = payload["servers"]
    
    if n > len(servers):
        return jsonify({
            "message" : "<Error> Number of new servers (n) is greater than newly added instances",
            "status" : "failure"
        }), 400
    
    cnt=0
    msg = "Added "
    for s,v in servers.items():
        if '[' in s:
            name = "Server"+str(random.randint(0,1000))
        else:
            name = s
        while name in bookkeeping["servers"].keys():
            name = "Server"+str(random.randint(0,1000))
        bookkeeping["servers"][name] = v
        bookkeeping["N"]+=1
        # TODO - add new server and hit its config endpoint
        os.popen(f'docker run --name {name} --network mynet --network-alias {name} -d server:latest').read()
        time.sleep(4)
        data_payload = {}
        data_payload["schema"] = bookkeeping["schema"]
        data_payload["shards"] = v
        r = requests.post(f"http://{name}:5000/config", data=data_payload)
        msg+=name
        cnt+=1
        if cnt == n:
            break
        else:
            msg+=" and "
    bookkeeping["shards"] += new_shards
    
    return jsonify({"N":bookkeeping["N"],
                    "message": msg,
                    "status": "successful"
                    }),200

@app.route("/rm", methods=["DELETE"])
def rm():

    payload = json.loads(request.data)
    
    if not has_keys(payload, ["n", "servers"]):
        return jsonify({
            "message" : "<Error> Payload not formatted correctly.",
            "status" : "failure"
        }), 200
        
    n = int(payload["n"])
    servers = payload["servers"]
    
    
    if n < len(servers):
        return jsonify({
            "message" : "<Error> wrong input n < len(servers).",
            "status" : "failure"
        }), 200
        
    
    # TODO - remove a server from the database

@app.route("/read", methods=["POST"])
def read():

    payload = json.loads(request.data)
    
    if not has_keys(payload, ["Stud_id"]) or not has_keys(payload["Stud_id"], ["low", "high"]):
        return jsonify({
            "message" : "<Error> Payload not formatted correctly.",
            "status" : "failure"
        }), 200
        
    low = int(payload["Stud_id"]["low"])
    high = int(payload["Stud_id"]["high"])
    
    for idx in range(low, high+1):
        for shard in bookkeeping["shards"]:
            if shard["Stud_id_low"] <= idx and shard["Stud_id_low"] + shard["Shard_size"] >= idx:
                # Make request to shard idx
                shard_id = shard["Shard_id"]
                requestID = random.randint(100000, 999999)
                server, rSlot = shard_mappers[shard_id].addRequest(requestID)
                print(idx, shard_id, server, rSlot, flush = True)
                
                
                if rSlot is None:
                    print("Hashtable full", requestID)
                    return jsonify({
                        "message" : "Hashtable full"
                    }), 400
                    
                data_payload = 
                try:
                     r = requests.post(f"http://{server}:5000/config", data=data_payload)
                    
                except:
                    
    
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

if __name__ == "__main__":
        
    app.run( host = "0.0.0.0", port = 5000, debug = True)
    