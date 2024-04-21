from flask import Flask, json, jsonify, request
from threading import Lock
import mysql.connector

app = Flask(__name__)
app.config['DEBUG'] = True

name_to_dataType = {"Number":"int","String":"varchar(255)"}

def connect_to_db():
    # mysql parser
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Mysql@123",
        auth_plugin="mysql_native_password",
        autocommit=True,
        database = "studentdb"
    )
    cur = conn.cursor()
    return cur, conn

def disconnect_from_db(cur,conn):
    cur.close()
    conn.close()
class Logger:
    # TODO: if two same queries are executed back to back it will update the uncommited enteies dictionary which might give unexpected results
    def __init__(self, log_file) -> None:
        self.log_file = log_file
        self.election_index = 0
        self.index = 0
        self.uncommited_entries = {}
        self.lock = Lock()
    
    def append(self, log_entry:str,increment = 1):
        with self.lock:
            self.election_index+=increment
            with open(self.log_file, "a") as f:
                f.write(log_entry + f"^{self.index}"+"\n")
            self.uncommited_entries[log_entry]=self.index
            self.index+=1
            return self.index-1
    
    def read(self):
        uncommited_logs = {}
        committed_logs = []
        with self.lock:
            with open(self.log_file, "r") as f:
                while True:
                    line = f.readline().split("\n")[0]
                    if not line:
                        break
                    query, index = line.split("^")
                    if query.startswith("committed"):
                        committed_logs.append(uncommited_logs[int(index)])
                    else:
                        uncommited_logs[int(index)] = query
        return committed_logs

    def reset(self):
        with self.lock:
            open(self.log_file, 'w').close()
            self.election_index = 0
            self.index = 0
            self.uncommited_entries = {}
    
    def commit(self, query):
        with self.lock:
            index = self.uncommited_entries[query]
            del self.uncommited_entries[query]
            self.election_index+=1
            log = f"committed^{index}"
            with open(self.log_file, "a") as f:
                f.write(log +"\n")

logger = {}

def has_keys(json_data : dict, keys : list):
    for key in keys:
        if key not in json_data.keys():
            return False
    return True


def execute_query(query:str, shard_id, mode="both"):
    cur,conn = connect_to_db()
    try:
        if mode == "log":
            logger[shard_id].append(query)
        elif mode == "exec":
            cur.execute(query)
            logger[shard_id].commit(query)
        else:
            logger[shard_id].append(query)
            cur.execute(query)
            logger[shard_id].commit(query)
    except Exception as e:
        disconnect_from_db(cur,conn)
        print(f"Error: {e}",flush=True)
        return False
    disconnect_from_db(cur,conn)
    return True

@app.route("/election_index/<shard_id>", methods=["GET"])
def election_index(shard_id):
    return jsonify({"election_index":logger[shard_id].election_index}),200

@app.route("/get_log/<shard_id>", methods=["GET"])
def get_log(shard_id):
    logs = logger[shard_id].read()
    return jsonify({"logs":logs}),200

@app.route("/apply_log",methods=["POST"])
def apply_log():
    payload = json.loads(request.data)
    if not has_keys(payload, ["shard", "logs"]):
        return jsonify({"status" : "failure"}), 400
    shard = payload["shard"]
    logs = payload["logs"]
    logger[shard].reset()
    for log in logs:
        if not execute_query(log,shard):
            return jsonify({"message":"failed to execute query","status" : "failure"}), 402
    return jsonify({"status" : "success"}), 200

# curl -X POST "http://127.0.0.1:5000/config" -H "Content-Type: application/json" -d @config.json
@app.route("/config",methods=["POST"])
def config():
    # initialize the shard tables 
    try:
        payload = json.loads(request.data)
    except:
        return jsonify({"status" : "failure"}), 400 # Bad Request
    if not has_keys(payload, ["shards", "schema"]) or not has_keys(payload["schema"], ["columns", "dtypes"]):
        return jsonify({"status" : "failure"}), 400 # Bad Request
    
    schema = payload["schema"]
    shards = payload["shards"]
    
    cols = schema["columns"]
    dtypes = schema["dtypes"]
    
    query = "("
    
    for i in range(len(cols)-1):
        query += f"{cols[i]} {name_to_dataType[dtypes[i]]}, "
    query += f"{cols[-1]} {name_to_dataType[dtypes[-1]]})"
    
    msg = ""
    for sh in shards:
        final_query = f"CREATE TABLE studT_{sh} {query};"
        if not execute_query(final_query,sh):
            return jsonify({"status" : "failure"}), 402 # Bad Request
        msg += f"{sh}, "
        logger[sh] = Logger(f"shard_{sh}.log")
    msg+=" configured"
    return jsonify({
                    "message" : msg,
                    "status" : "success"
    }), 200

@app.route("/tables",methods=["GET"])
def tables():
    cur,conn = connect_to_db()
    cur.execute("Show tables;")
    tabs = cur.fetchall()
    disconnect_from_db(cur,conn)
    return jsonify({"data":tabs}),200


# curl http://127.0.0.1:5000/heartbeat
@app.route("/heartbeat", methods=["GET"])
def heartbeat():
    return jsonify({}), 200

# curl -X GET http://127.0.0.1:5000/copy -H "Content-Type: application/json" -d @copy.json
@app.route("/copy", methods=["GET"])
def copy():

    payload = json.loads(request.data)
    
    if not has_keys(payload, ["shards"]):
        return jsonify({"status" : "failure"}), 400 # Bad Request
    
    shards = payload["shards"]
    data = {}
    cur,conn = connect_to_db()
    for shard in shards:
        cur.execute(f"SELECT * FROM studT_{shard}")
        data[shard] = [ {"Stud_id" : record[0], "Stud_name" : record[1], "Stud_marks" : record[2]}  for record in  cur.fetchall() ]
    disconnect_from_db(cur,conn)
    data["status"] = "success"
    return jsonify(data), 200

# curl -X POST http://127.0.0.1:5000/read -H "Content-Type: application/json" -d @read.json
@app.route("/read", methods=["POST"])
def read():

    payload = json.loads(request.data)

    if not has_keys(payload, ["shard", "Stud_id"]) or not has_keys(payload["Stud_id"], ["low", "high"]):
        return jsonify({"status" : "failure"}), 400 # Bad Request

    shard = payload["shard"]
    lo = payload["Stud_id"]["low"]
    hi = payload["Stud_id"]["high"]

    cur,conn = connect_to_db()
    query = f"SELECT * FROM studT_{shard} WHERE Stud_id >= {lo} AND Stud_id <= {hi};"
    cur.execute(query)
    data = cur.fetchall()
    disconnect_from_db(cur,conn)
    return jsonify({
                   "data" : data,
                   "status" : "success"
    }), 200


# curl -X POST http://127.0.0.1:5000/write -H "Content-Type: application/json" -d @write.json
@app.route("/write", methods=["POST"])
def write():
    payload = json.loads(request.data)

    if not has_keys(payload, ["shard", "data"]):
        return jsonify({"status" : "failure"}), 400

    shard = payload["shard"]
    
    mode = "both"
    if "mode" in payload.keys():
        mode = payload["mode"]
    
    data = payload["data"]
    for record in data:
        query = f"INSERT INTO studT_{shard} VALUES ({record['Stud_id']}, '{record['Stud_name']}', {record['Stud_marks']});"
        if not execute_query(query,shard,mode):
            return jsonify({
                    "message" : "Data entries not added",
                    "status" : "failure"
            }), 401

    return jsonify({
                   "message" : "Data entries added",
                   "status" : "success"
    }), 200


# curl -X PUT http://127.0.0.1:5000/update -H "Content-Type: application/json" -d @update.json
@app.route("/update", methods=["PUT"])
def update():

    payload = json.loads(request.data)

    if not has_keys(payload, ["shard", "Stud_id", "data"]):
        return jsonify({"status" : "failure"}), 400 # Bad Request

    
    shard = payload["shard"]
    stud_id = payload["Stud_id"]
    data = payload["data"]
    stud_name=data["Stud_name"]
    stud_marks=data["Stud_marks"]
    query = f"UPDATE studT_{shard} SET Stud_name=\"{stud_name}\",Stud_marks={stud_marks} WHERE Stud_id = {stud_id};"
    mode = "both"
    if "mode" in payload.keys():
        mode = payload["mode"]
    if not execute_query(query,shard,mode):
        return jsonify({
                    "message" : f"Data entry for Stud_id : {stud_id} not updated",
                    "status" : "failure"
        }), 401
    
    return jsonify({
                    "message" : f"Data entry for Stud_id : {stud_id} updated",
                    "status" : "success"
    }), 200



# curl -X DELETE http://127.0.0.1:5000/del -H "Content-Type: application/json" -d @delete.json
@app.route("/del", methods=["DELETE"])
def delete():

    payload = json.loads(request.data)

    if not has_keys(payload, ["shard", "Stud_id"]):
        return jsonify({"status" : "failure"}), 400 # Bad Request

    shard = payload["shard"]
    stud_id = payload["Stud_id"]

    query = f"DELETE FROM studT_{shard} WHERE Stud_id = {stud_id};"
    mode = "both"
    if "mode" in payload.keys():
        mode = payload["mode"]
    if not execute_query(query,shard,mode):
        return jsonify({
                    "message" : f"Data entry with Stud_id : {stud_id} not removed",
                    "status" : "failure"
        }), 401

    return jsonify({
                    "message" : f"Data entry with Stud_id : {stud_id} removed",
                    "status" : "success"
    }), 200

@app.route("/<path>", methods = ["GET"])
def other(path):
    return jsonify({'message': f"<Error> '/{path}' endpoint does not exist in server", 'status': 'successful'}), 200

if __name__ == "__main__":
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Mysql@123",
        auth_plugin="mysql_native_password",
        autocommit=True
    )
    cur = conn.cursor()
    cur.execute("show databases;")
    databases = cur.fetchall()
    db_exists = False
    for db in databases:
        db = db[0]
        if db == "studentdb":
            db_exists = True
            break
    if not db_exists:
        cur.execute("create database studentdb;")
    cur.execute("use studentdb;")
    disconnect_from_db(cur,conn)
    app.run( host = "0.0.0.0", port = 5000, debug = True)
