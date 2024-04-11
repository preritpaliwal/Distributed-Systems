from flask import Flask, request, json, jsonify
from consistent_hashing import consistentHash
from threading import Lock, Semaphore
import random, logging, requests, os, time, threading

app = Flask(__name__)
app.config['DEBUG'] = True
# log = logging.getLogger('werkzeug')
# log.disabled = True

num_replicas = 3

bookkeeping = {"N": 0, "schema": {}, "shards": [], "servers": {}}
shard_mappers = {}

def has_keys(json_data : dict, keys : list):
    
    for key in keys:
        if key not in json_data.keys():
            return False
    
    return True

def elect_primary_server(shard_id):
    # TODO: implement better logic and do we need any mutual exclusion here?
    shard_mappers[shard_id]["primary_server"] = shard_mappers[shard_id]["servers"][0]

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
            "hash_ring" : consistentHash(0, 512, 9),
            "primary_server" : None,
            "servers" : [],
            "curr_idx" : 0,
            "old_readers_cnt" : 0,
            "new_readers_cnt" : 0,
            "writer_cnt" : 0,
            "or_lock" : Lock(),
            "nr_lock" : Lock(),
            "w_lock" : Lock(),
            "data_lock" : Lock()
        }
    
    bookkeeping["schema"]=schema
    bookkeeping["shards"]=shards
    
    
    cnt=0
    for s,v in servers.items():
        if '[' in s:
            name = "Server"+str(random.randint(0,1000))
        else:
            name = s
        while name in bookkeeping["servers"].keys():
            name = "Server"+str(random.randint(0,1000))
        os.popen(f'docker run --name {name} --network mynet --network-alias {name} -d server:latest').read()
        time.sleep(2)
        data_payload = {}
        data_payload["schema"] = schema
        data_payload["shards"] = v
        url=f"http://{name}:5000/config"
        try:
            r = requests.post(url, json=data_payload)
            print(r,flush=True)
        except:
            print("Server not reachable", flush = True)
        cnt+=1
        bookkeeping["N"]+=1
        bookkeeping["servers"][name] = v
    
    while cnt<N:
        name = "Server"+str(random.randint(0,1000))
        while name in bookkeeping["servers"].keys():
            name = "Server"+str(random.randint(0,1000))
        cur_shards = ["sh" + str(random.randint(1,len(shards))),"sh"+str(random.randint(1,len(shards)))]
        bookkeeping["servers"][name] = cur_shards
        bookkeeping["N"]+=1
        os.popen(f'docker run --name {name} --network mynet --network-alias {name} -d -p 5000 server:latest').read()
        time.sleep(2)
        data_payload = {}
        data_payload["schema"] = schema
        data_payload["shards"] = cur_shards
        url=f"http://{name}:5000/config"
        try:
            r = requests.post(url, json=data_payload)
            print(r,flush=True)
        except:
            print("Server not reachable", flush = True)
        cnt+=1
        
    for server, shards in bookkeeping["servers"].items():
        for shard_id in shards:
            shard_mappers[shard_id]["servers"].append(server)
            shard_mappers[shard_id]["hash_ring"].addServer(server)

    for shard_id in shards:
        elect_primary_server(shard_id)
    
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
        }), 200
    
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
        os.popen(f'docker run --name {name} --network mynet --network-alias {name} -d server:latest').read()
        time.sleep(2)
        data_payload = {}
        data_payload["schema"] = bookkeeping["schema"]
        data_payload["shards"] = v
        try:
            r = requests.post(f"http://{name}:5000/config", json=data_payload)
            print(r,flush=True)
        except:
            print("Server not reachable", flush = True)
        
        msg+=name
        cnt+=1
        if cnt == n:
            break
        else:
            msg+=" and "
    
    bookkeeping["shards"] += new_shards
    
    for shard in new_shards:
        shard_mappers[shard["Shard_id"]] = {
            "hash_ring" : consistentHash(0, 512, 9),
            "primary_server" : None,
            "servers" : [],
            "curr_idx" : 0,
            "old_readers_cnt" : 0,
            "new_readers_cnt" : 0,
            "writer_cnt" : 0,
            "or_lock" : Lock(),
            "nr_lock" : Lock(),
            "w_lock" : Lock(),
            "data_lock" : Lock()
        }
    
    for server, shards in bookkeeping["servers"].items():
        for shard_id in shards:
            if server not in shard_mappers[shard_id]["servers"]:
                shard_mappers[shard_id]["servers"].append(server)
                shard_mappers[shard_id]["hash_ring"].addServer(server)
    
    for shard_id in new_shards:
        elect_primary_server(shard_id)
    
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
        
    
    deleted_servers = []
    cnt=0
    for server in servers:
        try:
            if(server not in bookkeeping["servers"].keys()):
                continue
            os.popen(f'docker rm -f {server}').read()
        except:
            print(f"Server {server} not found", flush = True)
        bookkeeping["N"]-=1
        for shard_id in bookkeeping["servers"][server]:
            shard_mappers[shard_id]["servers"].remove(server)
            shard_mappers[shard_id]["hash_ring"].deleteServer(1,[server])
        del bookkeeping["servers"][server]
        cnt+=1
        deleted_servers.append(server)
        
    while cnt<n:
        server = random.choice(list(bookkeeping["servers"].keys()))
        try:
            os.popen(f'docker rm -f {server}').read()
        except:
            print(f"Server {server} not found", flush = True)
        bookkeeping["N"]-=1
        for shard_id in bookkeeping["servers"][server]:
            shard_mappers[shard_id]["servers"].remove(server)
            shard_mappers[shard_id]["hash_ring"].deleteServer(1,[server])
        del bookkeeping["servers"][server]
        cnt+=1
        deleted_servers.append(server)
    
    for shard_ids in shard_mappers.keys():
        if shard_mappers[shard_id]["primary_server"] in deleted_servers:
            elect_primary_server(shard_ids)
    
    return jsonify({
            "message" : { "N" : n, "servers" : deleted_servers },
            "status" : "successful"
        }),200

def add_reader(shard_id):
    shard_mappers[shard_id]["nr_lock"].acquire()
    shard_mappers[shard_id]["new_readers_cnt"] += 1
    shard_mappers[shard_id]["nr_lock"].release()
    
    shard_mappers[shard_id]["w_lock"].acquire()
    while shard_mappers[shard_id]["writer_cnt"] > 0:
        shard_mappers["Shard_id"]["w_lock"].release()
        time.sleep(0.1)
        shard_mappers["Shard_id"]["w_lock"].acquire()
    shard_mappers[shard_id]["or_lock"].acquire()
    shard_mappers[shard_id]["old_readers_cnt"] += 1
    shard_mappers[shard_id]["or_lock"].release()
    
    shard_mappers[shard_id]["nr_lock"].acquire()
    shard_mappers[shard_id]["new_readers_cnt"] -= 1
    shard_mappers[shard_id]["nr_lock"].release()
    
    shard_mappers[shard_id]["w_lock"].release()

def remove_reader(shard_id):
    shard_mappers[shard_id]["or_lock"].acquire()
    shard_mappers[shard_id]["old_readers_cnt"] -= 1
    shard_mappers[shard_id]["or_lock"].release()

def read_target(shard_id, ql, qhi, data):
    requestID = random.randint(100000, 999999)
    server, rSlot = shard_mappers[shard_id]["hash_ring"].addRequest(requestID)
    data_payload = {"shard":shard_id, "Stud_id":{"low":ql, "high":qhi}}
    add_reader(shard_id)
    try:
        url=f"http://{server}:5000/read"
        r = requests.post(url, json=data_payload)
        print(r,flush=True)
        data[shard_id] = r.json()["data"]
    except:
        print("Server not reachable", flush = True)
    remove_reader(shard_id)
    shard_mappers[shard_id]["hash_ring"].clearRequest(rSlot)

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
    
    shards_used = []
    data = {}
    threads = []
    for shard in bookkeeping["shards"]:
        lo = shard["Stud_id_low"]
        hi = shard["Stud_id_low"] + shard["Shard_size"]
        if not (lo>high or hi<low):
            shards_used.append(shard["Shard_id"])
            shard_id = shard["Shard_id"]
            ql = max(lo, low)
            qhi = min(hi, high)
            
            threads.append(threading.Thread(target=read_target, args=(shard_id, ql, qhi, data)))
            threads[-1].start()
    
    for thread in threads:
        thread.join()
    
    ret_data = []
    for shard_id in shards_used:
        ret_data.extend(data[shard_id])
    
    return jsonify({
            "shards_queried": shards_used,
            "data": ret_data,
            "status": "success"
        }),200

def add_writer(shard_id):
    shard_mappers[shard_id]["or_lock"].acquire()
    while shard_mappers[shard_id]["old_readers_cnt"] > 0 :
        shard_mappers[shard_id]["or_lock"].release()
        time.sleep(0.1)
        shard_mappers[shard_id]["or_lock"].acquire()
    shard_mappers[shard_id]["or_lock"].release()
    
    shard_mappers[shard_id]["w_lock"].acquire()
    shard_mappers[shard_id]["writer_cnt"] += 1
    shard_mappers[shard_id]["w_lock"].release()
    
def remove_writer(shard_id):
    shard_mappers[shard_id]["w_lock"].acquire()
    shard_mappers[shard_id]["writer_cnt"] -= 1
    shard_mappers[shard_id]["w_lock"].release()

def write_target(shard_id, records):
    data_payload = {
        "shard" : shard_id,
        "curr_idx" : shard_mappers[shard_id]["curr_idx"],
        "data" : records
    }
    add_writer(shard_id)
    primary_server = shard_mappers[shard_id]["primary_server"]
    data_payload_log = data_payload
    data_payload_log["mode"] = "log"
    r = requests.post(f"http://{primary_server}:5000/write", json=data_payload_log)
    for server in shard_mappers[shard_id]["servers"]:
            if server == primary_server:
                continue
            r = requests.post(f"http://{server}:5000/write", json=data_payload)
    data_payload_log["mode"] = "exec"
    r = requests.post(f"http://{primary_server}:5000/write", json=data_payload_log)
    remove_writer(shard_id)
    shard_mappers[shard_id]["curr_idx"] += len(records)

@app.route("/write", methods=["POST"])
def write():
    payload = json.loads(request.data)
    data = payload["data"]
    shard_data = {}
    
    for record in data:
        for shard in bookkeeping["shards"]:
            if record["Stud_id"] >= shard["Stud_id_low"] and record["Stud_id"] < shard["Stud_id_low"] + shard["Shard_size"]:
                shard_id = shard["Shard_id"]
                
                if shard_id in shard_data.keys():
                    shard_data[shard_id].append(record)
                else:
                    shard_data[shard_id] = [ record ]
    
    threads = []
    for shard_id, records in shard_data.items():
        # do in a parallel thread
        threads.append(threading.Thread(target=write_target, args=(shard_id, records)))
        threads[-1].start()
    
    for thread in threads:
        thread.join()
    
    return jsonify({
        "message" : f"{len(data)} Data entries added",
        "status" : "success"
        }), 200

@app.route("/update", methods=["PUT"])
def update():
    
    payload = json.loads(request.data) 
    stud_id = payload["Stud_id"]
    data = payload["data"]
    
    for shard in bookkeeping["shards"]:
        if stud_id >= shard["Stud_id_low"] and stud_id < shard["Stud_id_low"] + shard["Shard_size"]:
            
            shard_id = shard["Shard_id"]
            data_payload = {"shard":shard_id,"Stud_id":stud_id, "data":data}
            
            add_writer(shard_id)
            primary_server = shard_mappers[shard_id]["primary_server"]
            data_payload_log = data_payload
            data_payload_log["mode"] = "log"
            r = requests.post(f"http://{primary_server}:5000/update", json=data_payload_log)
            for server in shard_mappers[shard_id]["servers"]:
                    if server == primary_server:
                        continue
                    r = requests.post(f"http://{server}:5000/update", json=data_payload)
            data_payload_log["mode"] = "exec"
            r = requests.post(f"http://{primary_server}:5000/update", json=data_payload_log)
            remove_writer(shard_id)
            
    return jsonify({
            "message" : f"Data updated for Stud_id : {stud_id} updated",
            "status" : "success"
        }),200
    
@app.route("/del", methods=["DELETE"])
def delete():
    
    payload = json.loads(request.data)
    stud_id = payload["Stud_id"]
    
    for shard in bookkeeping["shards"]:
        if stud_id >= shard["Stud_id_low"] and stud_id < shard["Stud_id_low"] + shard["Shard_size"]:
            
            shard_id = shard["Shard_id"]
            data_payload = {"shard":shard_id,"Stud_id":stud_id}
            
            add_writer(shard_id)
            primary_server = shard_mappers[shard_id]["primary_server"]
            data_payload_log = data_payload
            data_payload_log["mode"] = "log"
            r = requests.post(f"http://{primary_server}:5000/del", json=data_payload_log)
            for server in shard_mappers[shard_id]["servers"]:
                    if server == primary_server:
                        continue
                    r = requests.post(f"http://{server}:5000/del", json=data_payload)
            data_payload_log["mode"] = "exec"
            r = requests.post(f"http://{primary_server}:5000/del", json=data_payload_log)
            remove_writer(shard_id)
    
    return jsonify({
        "message" : f"Data entry with Stud_id:{stud_id} removed from all replicas",
        "status" : "success"
    }), 200

def respawn_server():
    while True:
        
        for server, shards in bookkeeping["servers"].items():
            try:
                r = requests.get(f"http://{server}:5000/heartbeat")
                # print(r,flush=True)
            except:
                print(f"Server {server} not reachable, respawning", flush = True)
                try:
                    os.popen(f'docker rm -f {server}').read()
                except:
                    print(f"{server} not found", flush = True)
                
                try:
                    os.popen(f'docker run --name {server} --network mynet --network-alias {server} -d server:latest').read()
                except:
                    print(f"Could not respawn {server}", flush = True)
                time.sleep(2)
                try:
                    r = requests.post(f"http://{server}:5000/config", json={"schema":bookkeeping["schema"], "shards":shards})
                except:
                    print(f"Could not configure {server}", flush = True)
                time.sleep(2)
                # print(r.json(),r.status_code,flush=True
                
                for shard_id in shards:
                    for another_server in shard_mappers[shard_id]["servers"]:
                        if another_server != server:
                            try:
                                r = requests.get(f"http://{another_server}:5000/copy",json={"shards":[shard_id]})
                            except:
                                print(f"{another_server} not reachable", flush = True)
                            # print(r.json(),r.status_code,flush=True)
                            
                            # try:
                            data  = r.json()[shard_id]
                            
                            r = requests.post(f"http://{server}:5000/write",json={"shard":shard_id, "curr_idx":0, "data":data})
                            # except Exception as e:
                                # print(str(e), f"{server} not reachable", flush = True)
                            
                            # print(r.json(),r.status_code,flush=True)
                            print(f"Shard {shard_id} copied from {another_server} to {server}",flush=True)
                            break
        time.sleep(10)

if __name__ == "__main__":
    respawn_thread = threading.Thread(target=respawn_server)
    respawn_thread.start()
    app.run( host = "0.0.0.0", port = 5000, debug = True)