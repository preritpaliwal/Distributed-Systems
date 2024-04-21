from flask import Flask, json, jsonify, request
from threading import Thread, Lock
import time, os , requests

app  = Flask(__name__)
app.config["DEBUG"] = True

shard_info = {}
shard_info_lock = Lock()
server_info = {}
server_info_lock = Lock()

def has_keys(json_data : dict, keys : list):
    for key in keys:
        if key not in json_data.keys():
            return False
    return True

def replicate_shard(shard_id, server_name):
    shard_info_lock.acquire()
    primary_server = shard_info[shard_id]["primary_server"]
    shard_info_lock.release()
    print(f"replicating shard {shard_id} from ps: {primary_server} to new_s: {server_name}",flush=True)
    if primary_server == None :
        return 0
    url = f"http://{primary_server}:5000/get_log/{shard_id}"
    print(url)
    r = requests.get(url)
    if r.status_code!=200:
        print(f"Failed to get log from primary server {primary_server}, {r}",flush=True)
        return 1
    logs = r.json()["logs"]
    if len(logs)==0:
        return 0
    r = requests.post(f"http://{server_name}:5000/apply_log",json={"shard":shard_id, "logs":logs})
    if r.status_code!=200:
        print(f"Failed to apply log to new server {server_name}, {r.json()},{r.status_code}",flush=True)
        return 2
    return 0

def spawn_and_config_server_contianer(name, schema, shard_ids):
    try:
        os.popen(f'docker rm -f {name}').read()
    except Exception as e:
        print(e,flush=True)
    time.sleep(1)
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
        }), 202
    
    for shard_id in shard_ids:
        shard_info_lock.acquire()
        if shard_id in shard_info.keys():
            shard_info[shard_id]["servers"].append(name)
            shard_info_lock.release()
            if replicate_shard(shard_id, name)!=0:
                return jsonify({
                    "message" : f"<Error> Failed to replicate {shard_id}.",
                    "status" : "failure"
                }), 203
        else:
            shard_info[shard_id] = {"servers":[name],"primary_server":None}
            shard_info_lock.release()
    
    server_info_lock.acquire()
    server_info[name] = {"shards":shard_ids,"schema":schema}
    server_info[name]["alive"] = True
    server_info_lock.release()
    Thread(target=lambda: respawn_loop(name)).start()
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
    shard_info_lock.acquire()   
    servers = shard_info[shard_id]["servers"]
    primary_server = shard_info[shard_id]["primary_server"]
    shard_info_lock.release()
    try:
        r = requests.get(f"http://{primary_server}:5000/heartbeat")
        if r.status_code == 200:
            return jsonify({"primary_server":primary_server}),200
    except Exception as e:
        # print(e,flush=True)
        pass
    election_indexes = {}
    max_votes = 0
    print(f"getting votes for shard {shard_id} from servers: ",servers,flush=True)
    for server in servers:
        try:
            r = requests.get(f"http://{server}:5000/election_index/{shard_id}")
            if r.status_code != 200:
                print(f"Failed to get election index from {server}.",flush=True)
                continue
            election_indexes[server] = r.json()["election_index"]
            max_votes = max(max_votes, election_indexes[server])
        except Exception as e:
            print(e,flush=True)
    for server, votes in election_indexes.items():
        if max_votes == votes:
            shard_info_lock.acquire()
            shard_info[shard_id]["primary_server"] = server
            shard_info_lock.release()
            primary_server = server
            break
    return jsonify({"primary_server":primary_server}),200

@app.route("/rm/<server_name>",methods = ["DELETE"])
def rm(server_name):
    # kill the respawn thread as well
    try:
        os.popen(f'docker rm -f {server_name}').read()
    except:
        print(f"Server {server_name} not found", flush = True)
    server_info_lock.acquire()
    server_info[server_name]["alive"] = False
    server_info_lock.release()
    shard_info_lock.acquire()
    for shard_id in shard_info.keys():
        if server_name in shard_info[shard_id]["servers"]:
            shard_info[shard_id]["servers"].remove(server_name)
    shard_info_lock.release()
    return jsonify({"message":"Server removed successfully."}),200

@app.route("/respawn/<server_name>",methods = ["POST"])
def respawn(server_name):
    respwan_server(server_name)
    return jsonify({"message":"Server respawned successfully."}),200

def respwan_server(server_name):
    try:
        r = requests.get(f"http://{server_name}:5000/heartbeat")
        if r.status_code == 200:
            return 
    except Exception as e:
        print(e,flush=True)
        tries = 5
        server_info_lock.acquire()
        shard_ids = server_info[server_name]["shards"]
        schema = server_info[server_name]["schema"]
        server_info_lock.release()
        for shard_id in shard_ids:
            shard_info_lock.acquire()
            primary_server = shard_info[shard_id]["primary_server"]
            shard_info_lock.release()
            if primary_server == server_name:
                r = requests.post(f"http://sm:5000/election",json={"shard_id":shard_id})
                if r.status_code != 200:
                    print(f"Failed to elect primary server for {shard_id}.",flush=True)
                    return
                print(f"Primary server for {shard_id} is {r.json()['primary_server']}",flush=True)
                r = requests.post(f"http://load_balancer:5000/set_primary",json={"shard_id":shard_id,"primary_server":r.json()["primary_server"]})
                if r.status_code != 200:
                    print(f"Failed to set primary server for {shard_id}.",flush=True)
                    return
        while spawn_and_config_server_contianer(server_name, schema, shard_ids) != 0 and tries>0:
            print(f"Failed to respawn {server_name}. tries = {tries}",flush=True)
            tries -= 1
        for shard_id in shard_ids:
            while replicate_shard(shard_id, server_name)!=0  and tries>0:
                print(f"Failed to replicate {shard_id}. tries = {tries}",flush=True)
                tries -= 1

def respawn_loop(server_name):
    server_info_lock.acquire()
    while server_info[server_name]["alive"]:
        server_info_lock.release()
        time.sleep(5)
        respwan_server(server_name)
        server_info_lock.acquire()
    del server_info[server_name]
    server_info_lock.release()

if __name__ == "__main__":
    app.run( host = "0.0.0.0", port = 5000, debug = True)