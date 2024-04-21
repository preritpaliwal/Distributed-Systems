from flask import Flask, request, json, jsonify
from consistent_hashing import consistentHash
from threading import Lock, Semaphore
import random, logging, requests, os, time, threading

app = Flask(__name__)
app.config['DEBUG'] = True

# TODO: put a lock on these also
bookkeeping = {"N": 0, "schema": {}, "shards": [], "servers": {}}
shard_mappers = {}

def has_keys(json_data : dict, keys : list):
    for key in keys:
        if key not in json_data.keys():
            return False
    return True

def elect_primary_server(shard_id):
    r = requests.post(f"http://sm:5000/election", json={"shard_id":shard_id})
    if r.status_code == 200:
        shard_mappers[shard_id]["primary_server"] = r.json()["primary_server"]
        for shard in bookkeeping["shards"]:
            if shard["Shard_id"] == shard_id:
                shard["primary_server"] = shard_mappers[shard_id]["primary_server"]
        return 0
    return 1

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
        # Get name
        if '[' in s:
            name = "Server"+str(random.randint(0,1000))
        else:
            name = s
        while name in bookkeeping["servers"].keys():
            name = "Server"+str(random.randint(0,1000))
        
        # add sever
        data_payload = {"name":name,"shard_ids":v,"schema":schema}
        r = requests.post("http://sm:5000/add_server",json=data_payload)
        if r.status_code == 200:
            cnt+=1
            bookkeeping["N"]+=1  
            bookkeeping["servers"][name] = v
        else:
            print(f"failed to start {name}",flush=True)
            return jsonify({"message":f"failed to start {name}","status":"failed"}),201
    
    while cnt<N:
        name = "Server"+str(random.randint(0,1000))
        while name in bookkeeping["servers"].keys():
            name = "Server"+str(random.randint(0,1000))
        cur_shards = ["sh" + str(random.randint(1,len(shards))),"sh"+str(random.randint(1,len(shards)))]
        data_payload = {"name":name,"shard_ids":cur_shards,"schema":schema}
        r = requests.post("http://sm:5000/add_server",json=data_payload)
        if r.status_code==200:
            cnt+=1
            bookkeeping["servers"][name] = cur_shards
            bookkeeping["N"]+=1
        else:
            print(f"failed to start {name}",flush=True)
            return jsonify({"message":f"failed to start {name}","status":"failed"}),201
        
    for server, shard_list in bookkeeping["servers"].items():
        for shard_id in shard_list:
            shard_mappers[shard_id]["servers"].append(server)
            shard_mappers[shard_id]["hash_ring"].addServer(server)

    for shard in bookkeeping["shards"]:
        if elect_primary_server(shard["Shard_id"])!=0:
            print(f"Failed to elect primary server for {shard['Shard_id']}",flush=True)
    
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
        
        r = requests.post("http://sm:5000/add_server",json={"name":name,"shard_ids":v,"schema":bookkeeping["schema"]})
        if r.status_code == 200:
            msg+=name
            bookkeeping["servers"][name] = v
            bookkeeping["N"]+=1
            cnt+=1
            if cnt == n:
                break
            else:
                msg+=" and "
        else:
            print(f"failed to start {name}",flush=True)
    
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
    
    for shard in bookkeeping["shards"]:
        if shard["primary_server"] is None:
            elect_primary_server(shard["Shard_id"])

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
        if(server not in bookkeeping["servers"].keys()):
            continue
        
        r = requests.delete(f"http://sm:5000/rm/{server}")
        if r.status_code != 200:
            print(f"Failed to remove {server}",flush=True)
            continue
        
        bookkeeping["N"]-=1
        for shard_id in bookkeeping["servers"][server]:
            if shard_mappers[shard_id]["primary_server"] == server:
                elect_primary_server(shard_id)
            shard_mappers[shard_id]["servers"].remove(server)
            shard_mappers[shard_id]["hash_ring"].deleteServer(1,[server])
        del bookkeeping["servers"][server]
        cnt+=1
        deleted_servers.append(server)
        
    
    while cnt<n:
        server = random.choice(list(bookkeeping["servers"].keys()))
        
        r = requests.delete(f"http://sm:5000/rm/{server}")
        if r.status_code != 200:
            print(f"Failed to remove {server}",flush=True)
            continue
        
        bookkeeping["N"]-=1
        for shard_id in bookkeeping["servers"][server]:
            if shard_mappers[shard_id]["primary_server"] == server:
                elect_primary_server(shard_id)
            shard_mappers[shard_id]["servers"].remove(server)
            shard_mappers[shard_id]["hash_ring"].deleteServer(1,[server])
        del bookkeeping["servers"][server]
        cnt+=1
        deleted_servers.append(server)
    
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
        shard_mappers[shard_id]["w_lock"].release()
        time.sleep(0.1)
        shard_mappers[shard_id]["w_lock"].acquire()
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

def read_target(t_id, shard_id, ql, qhi, data):
    requestID = random.randint(100000, 999999)
    server, rSlot = shard_mappers[shard_id]["hash_ring"].addRequest(requestID)
    data_payload = {"shard":shard_id, "Stud_id":{"low":ql, "high":qhi}}
    add_reader(shard_id)
    url=f"http://{server}:5000/read"
    r = requests.post(url, json=data_payload)
    data[shard_id] = r.json()["data"]
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
    id=0
    for shard in bookkeeping["shards"]:
        lo = shard["Stud_id_low"]
        hi = shard["Stud_id_low"] + shard["Shard_size"]
        if not (lo>high or hi<low):
            shard_id = shard["Shard_id"]
            shards_used.append(shard_id)
            ql = max(lo, low)
            qhi = min(hi, high)
            
            threads.append(threading.Thread(target=read_target, args=(id,shard_id, ql, qhi, data)))
            threads[id].start()
            id+=1
    
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

@app.route("/read/<server>", methods=["GET"])
def read_server(server):
    shards = bookkeeping["servers"][server]
    r = requests.get(f"http://{server}:5000/copy", json={"shards":shards})
    data = r.json()
    return jsonify(data), 200

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
    shard_mappers[shard_id]["data_lock"].acquire()
    
def remove_writer(shard_id):
    shard_mappers[shard_id]["data_lock"].release()
    shard_mappers[shard_id]["w_lock"].acquire()
    shard_mappers[shard_id]["writer_cnt"] -= 1
    shard_mappers[shard_id]["w_lock"].release()

def write_target(shard_id, records):
    data_payload = {
        "shard" : shard_id,
        "curr_idx" : shard_mappers[shard_id]["curr_idx"],
        "data" : records,
        "mode" : "log"
    }
    add_writer(shard_id)
    primary_server = shard_mappers[shard_id]["primary_server"]
    success_count = 0.0
    success_status = {}
    r = requests.post(f"http://{primary_server}:5000/write", json=data_payload)
    if r.status_code == 200:
        success_count += 0.5
    data_payload["mode"] = "both"
    for server in shard_mappers[shard_id]["servers"]:
        if server == primary_server:
            continue
        r = requests.post(f"http://{server}:5000/write", json=data_payload)
        success_status[server] = r.status_code
        if r.status_code == 200:
            success_count += 1
    data_payload["mode"] = "exec"
    r = requests.post(f"http://{primary_server}:5000/write", json=data_payload)
    success_status[primary_server] = r.status_code
    if r.status_code == 200:
        success_count += 0.5
    success = False
    if success_count >= len(shard_mappers[shard_id]["servers"])/2:
        success = True
    if success:
        # make it write on servers it failed
        if success_status[primary_server]!=200:
            # elect new primary
            if elect_primary_server(shard_id)!=0:
                print("failed to elect new primary",flush=True)
        
        for server, status in success_status.items():
            if status!=200:
                r = requests.post(f"http://sm:5000/respawn/{server}")
                if r.status_code!=200:
                    print(f"failed to respawn {server}",flush=True)
            
        shard_mappers[shard_id]["curr_idx"] += len(records)
    else:
        # roll back
        stud_ids = [record["Stud_id"] for record in records]
        for server, status in success_status.items():
            if status==200:
                for stud_id in stud_ids:
                    r = requests.delete(f"http://{server}:5000/del", json={"shard":shard_id,"Stud_id":stud_id, "mode":"both"})
                    if r.status_code!=200:
                        print(f"failed to rollback {server}",flush=True)
    remove_writer(shard_id)

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
    id = 0
    for shard_id, records in shard_data.items():
        threads.append(threading.Thread(target=write_target, args=(id,shard_id, records)))
        threads[id].start()
        id+=1
    
    for thread in threads:
        thread.join()
    
    return jsonify({
        "message" : f"{len(data)} Data entries added",
        "status" : "success"
        }), 200

def handle_rollback(endpoint, shard_id, data_payload):
    
    read_payload = {"Stud_id":{"low":data_payload["Stud_id"],"high":data_payload["Stud_id"]}}
    r = requests.post(f"http://localhost:5000/read", json=read_payload)
    record = r.json()["data"][0]
    
    add_writer(shard_id)
    primary_server = shard_mappers[shard_id]["primary_server"]
    success_count = 0.0
    success_status = {}
    r = requests.put(f"http://{primary_server}:5000/{endpoint}", json=data_payload)
    if r.status_code == 200:
        success_count += 0.5
    data_payload["mode"]="both"
    for server in shard_mappers[shard_id]["servers"]:
        if server == primary_server:
            continue
        r = requests.put(f"http://{server}:5000/{endpoint}", json=data_payload)
        if r.status_code == 200:
            success_count += 1
        success_status[server] = r.status_code
    data_payload["mode"] = "exec"
    r = requests.put(f"http://{primary_server}:5000/{endpoint}", json=data_payload)
    if r.status_code == 200:
        success_count += 0.5
    success_status[primary_server] = r.status_code
    success = False
    if success_count >= len(shard_mappers[shard_id]["servers"])/2:
        success = True
    if success:
        # make it write on servers it failed
        if success_status[primary_server]!=200:
            # elect new primary
            if elect_primary_server(shard_id)!=0:
                print("failed to elect new primary",flush=True)
        
        for server, status in success_status.items():
            if status!=200:
                r = requests.post(f"http://sm:5000/respawn/{server}")
                if r.status_code!=200:
                    print(f"failed to respawn {server}",flush=True)
    else:
        # roll back
        for server, status in success_status.items():
            if status == 200:
                if endpoint.startswith("update"):
                    r = requests.put(f"http://{server}:5000/{endpoint}", json={"shard":shard_id,"Stud_id":record["Stud_id"],"data":record,"mode":"both"})
                    if r.status_code!=200:
                        print(f"failed to rollback {server}",flush=True)
                else:
                    r = requests.post(f"http://{server}:5000/write",json={"shard":shard_id,"data":[record]})
                    if r.status_code!=200:
                        print(f"failed to rollback {server}",flush=True)
    remove_writer(shard_id)

@app.route("/update", methods=["PUT"])
def update():
    
    payload = json.loads(request.data) 
    stud_id = payload["Stud_id"]
    data = payload["data"]
    
    for shard in bookkeeping["shards"]:
        if stud_id >= shard["Stud_id_low"] and stud_id < shard["Stud_id_low"] + shard["Shard_size"]:
            
            shard_id = shard["Shard_id"]
            data_payload = {"shard":shard_id,"Stud_id":stud_id, "data":data, "mode":"log" }
            
            handle_rollback("update", shard_id, data_payload)
            
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
            data_payload = {"shard":shard_id,"Stud_id":stud_id,"mode":"log"}
            
            handle_rollback("del", shard_id, data_payload)
    
    return jsonify({
        "message" : f"Data entry with Stud_id:{stud_id} removed from all replicas",
        "status" : "success"
    }), 200

if __name__ == "__main__":
    app.run( host = "0.0.0.0", port = 5000, debug = True)