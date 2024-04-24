from flask import Flask, request, json, jsonify
from consistent_hashing import consistentHash
from threading import Lock
import random, requests, time, threading

app = Flask(__name__)
app.config['DEBUG'] = True

# TODO: put a lock on these also
bookkeeping = {"N": 0, "schema": {}, "shards": [], "servers": {}}
bookkeeping_lock = Lock()
shard_mappers = {}
shard_mappers_lock = Lock()

def has_keys(json_data : dict, keys : list):
    for key in keys:
        if key not in json_data.keys():
            return False
    return True

def update_primary_server_info(shard_id, primary_server):
    shard_mappers_lock.acquire()
    shard_mappers[shard_id]["primary_server"] = primary_server
    shard_mappers_lock.release()
    bookkeeping_lock.acquire()
    for shard in bookkeeping["shards"]:
        if shard["Shard_id"] == shard_id:
            shard["primary_server"] = primary_server
    bookkeeping_lock.release()

def elect_primary_server(shard_id):
    try:
        r = requests.post(f"http://sm:5000/election", json={"shard_id":shard_id})
        if r.status_code == 200:
            update_primary_server_info(shard_id, r.json()["primary_server"])
            return 0
        return 1
    except Exception as e:
        print(e,flush=True)
        return 1

@app.route("/set_primary", methods=["POST"])
def set_primary():
    payload = json.loads(request.data)
    shard_id = payload["shard_id"]
    primary_server = payload["primary_server"]
    update_primary_server_info(shard_id, primary_server)
    return jsonify({
        "message" : f"Primary server for {shard_id} set to {primary_server}",
        "status" : "success"
    }), 200

def add_server(t_id,s,v,schema, result):
    if '[' in s:
        name = "Server"+str(random.randint(0,1000))
    else:
        name = s
    
    bookkeeping_lock.acquire()
    while name in bookkeeping["servers"].keys():
        bookkeeping_lock.release()
        name = "Server"+str(random.randint(0,1000))
        bookkeeping_lock.acquire()
    bookkeeping_lock.release()
    
    # add sever
    data_payload = {"name":name,"shard_ids":v,"schema":schema}
    try:
        r = requests.post("http://sm:5000/add_server",json=data_payload)
        if r.status_code == 200:
            bookkeeping_lock.acquire()
            bookkeeping["N"]+=1  
            bookkeeping["servers"][name] = v
            bookkeeping_lock.release()
        else:
            print(f"failed to start {name}",flush=True)
            result[t_id]=1
    except Exception as e:
        print(e,flush=True)
        result[t_id] = 1
    
    # update shard info
    shard_mappers_lock.acquire()
    for shard_id in v:
        if shard_id not in shard_mappers.keys():
            shard_mappers[shard_id] = {
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
        
        shard_mappers[shard_id]["servers"].append(name)
        shard_mappers[shard_id]["hash_ring"].addServer(name)   
        
        if shard_mappers[shard_id]["primary_server"] is None:
            shard_mappers_lock.release()
            if elect_primary_server(shard_id)!= 0:
                print(f"failed to elect primary server for shard {shard_id}",flush=True)
                result[t_id] = 1
            shard_mappers_lock.acquire()
    shard_mappers_lock.release()
    result[t_id]=0


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
    
    bookkeeping_lock.acquire()
    bookkeeping["schema"]=schema
    bookkeeping["shards"]=shards
    bookkeeping_lock.release()
    cnt=0
    threads = []
    result={}
    for s,v in servers.items():
        threads.append(threading.Thread(target=lambda: add_server(cnt,s,v,schema,result)))
        threads[cnt].start()
        cnt+=1
    
    while cnt<N:
        threads.append(threading.Thread(target=lambda: add_server(cnt,"[]",v,schema,result)))
        threads[cnt].start()
        cnt+=1

    for thread in threads:
        thread.join()
    
    status_code=200
    for t_id,res in result.items():
        if res!=0:
            status_code=res
            break
        
    
    return jsonify({
        "message" : "Configured Database",
        "status" : "success"
    }) , status_code

@app.route("/status", methods=["GET"])
def status():
    bookkeeping_lock.acquire()
    bookkeeping_copy = bookkeeping.copy()
    bookkeeping_lock.release()
    return jsonify(bookkeeping_copy) , 200

@app.route("/add",methods=["POST"])
def add():
    payload = json.loads(request.data)
    
    if not has_keys(payload, ["n", "new_shards", "servers"]):
        return jsonify({
            "message" : "<Error> Payload not formatted correctly.",
            "status" : "failure"
        }), 200
    
    n = int(payload["n"])
    servers = payload["servers"]
    new_shards = payload["new_shards"]
    
    if n > len(servers):
        return jsonify({
            "message" : "<Error> Number of new servers (n) is greater than newly added instances",
            "status" : "failure"
        }), 200
    
    cnt=0
    threads = []
    bookkeeping_lock.acquire()
    schema = bookkeeping["schema"]
    bookkeeping_lock.release()
    result = {}
    for s,v in servers.items():
        threads.append(threading.Thread(target=lambda: add_server(cnt,s,v,schema,result)))
        threads[cnt].start()
        cnt+=1
    
    for thread in threads:
        thread.join()

    status_code=200
    for t_id,res in result.items():
        if res!=0:
            status_code=res
            break
        
    bookkeeping_lock.acquire()
    N = bookkeeping["N"]
    bookkeeping["shards"]+=new_shards
    bookkeeping_lock.release()
    return jsonify({"N":N,
                    "message": "Added new servers",
                    "status": "successful"
                    }),status_code

def remove_server(server):
    try:
        r = requests.delete(f"http://sm:5000/rm/{server}")
        if r.status_code != 200:
            print(f"Failed to remove {server}",flush=True)
            return 1
    except Exception as e:
        print(e,flush=True)
        return 1
    
    bookkeeping_lock.acquire()
    bookkeeping["N"]-=1
    shard_ids = bookkeeping["servers"][server]
    bookkeeping_lock.release()
    
    shard_mappers_lock.acquire()
    for shard_id in shard_ids:
        if shard_mappers[shard_id]["primary_server"] == server:
            shard_mappers_lock.release()
            if elect_primary_server(shard_id)!=0:
                print(f"failed to elect primary server for shard {shard_id}",flush=True)
            shard_mappers_lock.acquire()
        shard_mappers[shard_id]["servers"].remove(server)
        shard_mappers[shard_id]["hash_ring"].deleteServer(1,[server])
    shard_mappers_lock.release()
    
    bookkeeping_lock.acquire()
    del bookkeeping["servers"][server]
    bookkeeping_lock.release()
    return 0


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
    status_code=200
    for server in servers:
        bookkeeping_lock.acquire()
        existing_servers1 = bookkeeping["servers"].keys()
        bookkeeping_lock.release()
        if(server not in existing_servers1):
            continue
        if remove_server(server)!=0:
            status_code=201
        deleted_servers.append(server)
        cnt+=1
    
    while cnt<n:
        bookkeeping_lock.acquire()
        existing_servers2 = bookkeeping["servers"].keys()
        bookkeeping_lock.release()
        server = random.choice(list(existing_servers2))
        if remove_server(server)!=0:
            status_code=201
        deleted_servers.append(server)
        cnt+=1
    
    return jsonify({
            "message" : { "N" : n, "servers" : deleted_servers },
            "status" : "successful"
        }),status_code

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

def read_target(t_id, shard_id, ql, qhi, data, result):
    requestID = random.randint(100000, 999999)
    shard_mappers_lock.acquire()
    server, rSlot = shard_mappers[shard_id]["hash_ring"].addRequest(requestID)
    shard_mappers_lock.release()
    data_payload = {"shard":shard_id, "Stud_id":{"low":ql, "high":qhi}}
    add_reader(shard_id)
    url=f"http://{server}:5000/read"
    try:
        r = requests.post(url, json=data_payload)
    except Exception as e:
        print(e,flush=True)
        result[t_id]=201
        return
    data[shard_id] = r.json()["data"]
    remove_reader(shard_id)
    shard_mappers_lock.acquire()
    shard_mappers[shard_id]["hash_ring"].clearRequest(rSlot)
    shard_mappers_lock.release()
    result[t_id]=r.status_code


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
    result = {}
    bookkeeping_lock.acquire()
    shards = bookkeeping["shards"]
    bookkeeping_lock.release()
    for shard in shards:
        lo = shard["Stud_id_low"]
        hi = shard["Stud_id_low"] + shard["Shard_size"]
        if not (lo>high or hi<low):
            shard_id = shard["Shard_id"]
            shards_used.append(shard_id)
            ql = max(lo, low)
            qhi = min(hi, high)
            
            threads.append(threading.Thread(target=lambda: read_target(id, shard_id, ql, qhi, data, result)))
            threads[id].start()
            id+=1
    
    for thread in threads:
        thread.join()
    
    ret_data = []
    for shard_id in shards_used:
        ret_data.extend(data[shard_id])
    
    status_code = 200
    for t_id,res in result.items():
        if res != 200:
            status_code = res
            break
    
    return jsonify({
            "shards_queried": shards_used,
            "data": ret_data,
            "status": "success"
        }),status_code

@app.route("/read/<server>", methods=["GET"])
def read_server(server):
    bookkeeping_lock.acquire()
    shards = bookkeeping["servers"][server]
    bookkeeping_lock.release()
    try:
        r = requests.get(f"http://{server}:5000/copy", json={"shards":shards})
    except:
        return jsonify({
            "message" : f"Failed to read from {server}",
            "status" : "failure"
        }), 201
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

def write_target(t_id, shard_id, records, result):
    data_payload = {
        "shard" : shard_id,
        "curr_idx" : shard_mappers[shard_id]["curr_idx"],
        "data" : records,
        "mode" : "log"
    }
    add_writer(shard_id)
    shard_mappers_lock.acquire()
    primary_server = shard_mappers[shard_id]["primary_server"]
    shard_mappers_lock.release()
    success_count = 0.0
    success_status = {}
    try:
        r = requests.post(f"http://{primary_server}:5000/write", json=data_payload)
    except Exception as e:
        print(e,flush=True)
        result[t_id] = 201
        return
    if r.status_code == 200:
        success_count += 0.5
    data_payload["mode"] = "both"
    
    shard_mappers_lock.acquire()
    servers = shard_mappers[shard_id]["servers"]
    shard_mappers_lock.release()
    for server in servers:
        if server == primary_server:
            continue
        try:
            r = requests.post(f"http://{server}:5000/write", json=data_payload)
        except Exception as e:
            print(e,flush=True)
            result[t_id] = 201
            return
        success_status[server] = r.status_code
        if r.status_code == 200:
            success_count += 1
    data_payload["mode"] = "exec"
    try:
        r = requests.post(f"http://{primary_server}:5000/write", json=data_payload)
    except Exception as e:
        print(e,flush=True)
        result[t_id] = 201
        return
    success_status[primary_server] = r.status_code
    if r.status_code == 200:
        success_count += 0.5
    success = False
    if success_count >= len(servers)/2:
        success = True
    if success:
        # make it write on servers it failed
        if success_status[primary_server]!=200:
            # elect new primary
            if elect_primary_server(shard_id)!=0:
                print("failed to elect new primary",flush=True)
        
        for server, status in success_status.items():
            if status!=200:
                try:
                    r = requests.post(f"http://sm:5000/respawn/{server}")
                except Exception as e:
                    print(e,flush=True)
                if r.status_code!=200:
                    print(f"failed to respawn {server}",flush=True)
            
    else:
        # roll back
        stud_ids = [record["Stud_id"] for record in records]
        for server, status in success_status.items():
            if status==200:
                for stud_id in stud_ids:
                    try:
                        r = requests.delete(f"http://{server}:5000/del", json={"shard":shard_id,"Stud_id":stud_id, "mode":"both"})
                    except Exception as e:
                        print(e,flush=True)
                    if r.status_code!=200:
                        print(f"failed to rollback {server}",flush=True)
    remove_writer(shard_id)
    if success:
        result[t_id] = 200
    else:
        result[t_id] = 201

@app.route("/write", methods=["POST"])
def write():
    payload = json.loads(request.data)
    data = payload["data"]
    shard_data = {}
    
    for record in data:
        bookkeeping_lock.acquire()
        shards = bookkeeping["shards"]
        bookkeeping_lock.release()
        for shard in shards:
            if record["Stud_id"] >= shard["Stud_id_low"] and record["Stud_id"] < shard["Stud_id_low"] + shard["Shard_size"]:
                shard_id = shard["Shard_id"]
                if shard_id in shard_data.keys():
                    shard_data[shard_id].append(record)
                else:
                    shard_data[shard_id] = [ record ]
    
    threads = []
    id = 0
    result = {}
    for shard_id, records in shard_data.items():
        threads.append(threading.Thread(target=lambda: write_target(id,shard_id, records, result)))
        threads[id].start()
        id+=1
    
    for thread in threads:
        thread.join()
    
    status_code = 200
    for t_id,res in result.items():
        if res != 200:
            status_code = res
            break
    
    return jsonify({
        "message" : f"{len(data)} Data entries added",
        "status" : "success"
        }), status_code

def handle_rollback(endpoint, shard_id, data_payload):
    
    read_payload = {"Stud_id":{"low":data_payload["Stud_id"],"high":data_payload["Stud_id"]}}
    try:
        r = requests.post(f"http://load_balancer:5000/read", json=read_payload)
    except Exception as e:
        print(e,flush=True)
        return 201
    record = r.json()["data"][0]
    
    add_writer(shard_id)
    shard_mappers_lock.acquire()
    primary_server = shard_mappers[shard_id]["primary_server"]
    shard_mappers_lock.release()
    success_count = 0.0
    success_status = {}
    try:
        if endpoint.startswith("update"):
            r = requests.put(f"http://{primary_server}:5000/{endpoint}", json=data_payload)
        else:
            r = requests.delete(f"http://{primary_server}:5000/{endpoint}", json=data_payload)
    except Exception as e:
        print(e,flush=True)
        return 201
    if r.status_code == 200:
        success_count += 0.5
    data_payload["mode"]="both"
    shard_mappers_lock.acquire()
    servers = shard_mappers[shard_id]["servers"]
    shard_mappers_lock.release()
    for server in servers:
        if server == primary_server:
            continue
        try:
            if endpoint.startswith("update"):
                r = requests.put(f"http://{server}:5000/{endpoint}", json=data_payload)
            else:
                r = requests.delete(f"http://{server}:5000/{endpoint}", json=data_payload)
        except Exception as e:
            print(e,flush=True)
            return 201
        if r.status_code == 200:
            success_count += 1
        success_status[server] = r.status_code
    data_payload["mode"] = "exec"
    try:
        if endpoint.startswith("update"):
            r = requests.put(f"http://{primary_server}:5000/{endpoint}", json=data_payload)
        else:
            r = requests.delete(f"http://{primary_server}:5000/{endpoint}", json=data_payload)
    except Exception as e:
        print(e,flush=True)
        return 201
    if r.status_code == 200:
        success_count += 0.5
    success_status[primary_server] = r.status_code
    success = False
    if success_count >= len(servers)/2:
        success = True
    if success:
        # make it write on servers it failed
        if success_status[primary_server]!=200:
            # elect new primary
            if elect_primary_server(shard_id)!=0:
                print("failed to elect new primary",flush=True)
        
        for server, status in success_status.items():
            if status!=200:
                try:
                    r = requests.post(f"http://sm:5000/respawn/{server}")
                except Exception as e:
                    print(e,flush=True)
                    return 201
                if r.status_code!=200:
                    print(f"failed to respawn {server}",flush=True)
    else:
        # roll back
        for server, status in success_status.items():
            if status == 200:
                try:
                    if endpoint.startswith("update"):
                        r = requests.put(f"http://{server}:5000/{endpoint}", json={"shard":shard_id,"Stud_id":record["Stud_id"],"data":record,"mode":"both"})
                    else:
                        r = requests.post(f"http://{server}:5000/write",json={"shard":shard_id,"data":[record]})
                except Exception as e:
                    print(e,flush=True)
                    return 201
                if r.status_code!=200:
                    print(f"failed to rollback {server}",flush=True)
    remove_writer(shard_id)
    if success:
        return 200
    else :
        return 201

@app.route("/update", methods=["PUT"])
def update():
    
    payload = json.loads(request.data) 
    stud_id = payload["Stud_id"]
    data = payload["data"]
    
    bookkeeping_lock.acquire()
    shards = bookkeeping["shards"]
    bookkeeping_lock.release()
    for shard in shards:
        if stud_id >= shard["Stud_id_low"] and stud_id < shard["Stud_id_low"] + shard["Shard_size"]:
            
            shard_id = shard["Shard_id"]
            data_payload = {"shard":shard_id,"Stud_id":stud_id, "data":data, "mode":"log" }
            
            ret = handle_rollback("update", shard_id, data_payload)
            
    return jsonify({
            "message" : f"Data updated for Stud_id : {stud_id} updated",
            "status" : "success"
        }),ret
    
@app.route("/del", methods=["DELETE"])
def delete():
    
    payload = json.loads(request.data)
    stud_id = payload["Stud_id"]
    
    bookkeeping_lock.acquire()
    shards = bookkeeping["shards"]
    bookkeeping_lock.release()
    for shard in shards:
        if stud_id >= shard["Stud_id_low"] and stud_id < shard["Stud_id_low"] + shard["Shard_size"]:
            
            shard_id = shard["Shard_id"]
            data_payload = {"shard":shard_id,"Stud_id":stud_id,"mode":"log"}
            
            ret = handle_rollback("del", shard_id, data_payload)
    
    return jsonify({
        "message" : f"Data entry with Stud_id:{stud_id} removed from all replicas",
        "status" : "success"
    }), ret

if __name__ == "__main__":
    app.run( host = "0.0.0.0", port = 5000, debug = True)