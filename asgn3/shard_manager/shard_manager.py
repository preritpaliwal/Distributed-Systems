from flask import Flask, json, jsonify, request
from threading import Thread
import time,os ,requests

app  = Flask(__name__)
app.config["DEBUG"] = True

shard_info = {}
server_info = {}

def has_keys(json_data : dict, keys : list):
    for key in keys:
        if key not in json_data.keys():
            return False
    return True

def replicate_shard(shard_id, server_name):
    primary_server = shard_info[shard_id]["primary_server"]
    if primary_server == None :
        return 0
    r = requests.get(f"http://{primary_server}:5000/get_log/{shard_id}")
    if r.status_code!=200:
        return 1
    logs = r.json()["logs"]
    r = requests.post(f"http://{server_name}:5000/apply_log",json={"shard":shard_id, "logs":logs})
    if r.status_code!=200:
        return 2
    return 0

def spawn_and_config_server_contianer(name, schema, shard_ids):
    try:
        os.popen(f'docker run --name {name} --network mynet --network-alias {name} -d server:latest').read()
    except Exception as e:
        print(e,flush=True)
    time.sleep(2)
    
    data_payload = {}
    data_payload["schema"] = schema
    data_payload["shards"] = shard_ids
    url=f"http://{name}:5000/config"
    r = requests.post(url, json=data_payload)
    print("r = ", r,flush=True)
    time.sleep(2)
    if r.status_code != 200:
        return 1
    return 0
    
@app.route("/add_server",methods = ["POST"])
def add_server():
    payload = json.loads(request.data)
    if not has_keys(payload, ["name","shard_ids", "schema"]):
        return jsonify({
            "message" : "<Error> Payload not formatted correctly.",
            "status" : "failure"
        }), 201
    
    name = payload["name"]
    shard_ids = payload["shard_ids"]
    schema = payload["schema"]
    
    if spawn_and_config_server_contianer(name,schema,shard_ids)!=0:
        return jsonify({
            "message" : "<Error> Failed to spawn and configure server.",
            "status" : "failure"
        }), 201
    
    for shard_id in shard_ids:
        if shard_id in shard_info.keys():
            shard_info[shard_id]["servers"].append(name)
            if replicate_shard(shard_id, name)!=0:
                return jsonify({
                    "message" : f"<Error> Failed to replicate {shard_id}.",
                    "status" : "failure"
                }), 201
        else:
            shard_info[shard_id] = {"servers":[name],"primary_server":None}
    
    server_info[name] = {"shards":shard_ids,"schema":schema}
    
    respawn_thread = Thread(target=respawn_loop, args=(name))
    respawn_thread.start()
    
    return jsonify({
        "message" : "Server added successfully and replicated data.",
        "status" : "success"
    }), 200

@app.route("/election",methods = ["POST"])
def election():
    payload = json.loads(request.data)
    if not has_keys(payload, ["shard_id"]):
        return jsonify({
            "message" : "<Error> Payload not formatted correctly.",
            "status" : "failure"
        }), 201
    shard_id = payload["shard_id"]
    servers = shard_info[shard_id]["servers"]
    election_indexes = {}
    max_votes = 0
    for server in servers:
        r = requests.get(f"http://{server}:5000/election_index/{shard_id}")
        if r.status_code != 200:
            print(f"Failed to get election index from {server}.",flush=True)
            continue
        election_indexes[server] = r.json()["election_index"]
        max_votes = max(max_votes, election_indexes[server])
    for server in servers:
        if max_votes == election_indexes[server]:
            shard_info[shard_id]["primary_server"] = server
            break
    return jsonify({"primary_server":shard_info[shard_id]["primary_server"]}),200

@app.route("/rm/<server_name>",methods = ["DELETE"])
def rm(server_name):
    # kill the respawn thread as well
    try:
        os.popen(f'docker rm -f {server_name}').read()
    except:
        print(f"Server {server_name} not found", flush = True)
    del server_info[server_name]
    for shard_id in shard_info.keys():
        if server_name in shard_info[shard_id]["servers"]:
            shard_info[shard_id]["servers"].remove(server_name)
    return jsonify({"message":"Server removed successfully."}),200

@app.route("/respawn/<server_name>",methods = ["POST"])
def respawn(server_name):
    respwan_server(server_name)
    return jsonify({"message":"Server respawned successfully."}),200

def respwan_server(server_name):
    r = requests.get(f"http://{server_name}:5000/heartbeat")
    if r.status_code == 200:
        return 
    while spawn_and_config_server_contianer(server_name, server_info[server_name]["schema"], server_info[server_name]["shards"]) != 0:
        print(f"Failed to respawn {server_name}.",flush=True)
    for shard_id in server_info[server_name]["shards"]:
        while replicate_shard(shard_id, server_name)!=0:
            print(f"Failed to replicate {shard_id}.",flush=True)

def respawn_loop(server_name):
    while True:
        time.sleep(5)
        respwan_server(server_name)

if __name__ == "__main__":
    app.run( host = "0.0.0.0", port = 5000, debug = True)