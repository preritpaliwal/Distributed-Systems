from flask import Flask, json, jsonify, request
from threading import Lock
import sys, os, socket
import mysql.connector
serverID = 1

app = Flask(__name__)
app.config['DEBUG'] = True


curr_idx_shards = {}

name_to_dataType = {
    "Number":"int",
    "String":"varchar(255)"
}

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
    def __init__(self, log_file) -> None:
        self.log_file = log_file
        self.curr_offset = 0
    
    def append(self, log_entry:str):
        self.curr_offset+=len(str(log_entry))
        with open(self.log_file, "a") as f:
            f.write(log_entry+"\n")
    
    def read(self):
        logs = []
        with open(self.log_file, "r") as f:
            while True:
                line = f.readline().split("\n")[0]
                if not line:
                    break
                logs.append(line)
        return logs
    
logger = Logger("log.txt")
# database_lock = Lock()
print("logger created",flush=True)  

def has_keys(json_data : dict, keys : list):
    for key in keys:
        if key not in json_data.keys():
            return False
    return True


def execute_query(query:str, mode="both"):
    cur,conn = connect_to_db()
    try:
        if mode == "log":
            logger.append(query)
        elif mode == "exec":
            cur.execute(query)
        else:
            logger.append(query)
            cur.execute(query)
    except Exception as e:
        # database_lock.release()
        disconnect_from_db(cur,conn)
        print(f"Error: {e}",flush=True)
        return False
    disconnect_from_db(cur,conn)
    # database_lock.release()
    return True
        # raise e

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
        if not execute_query(final_query):
            return jsonify({"status" : "failure"}), 402 # Bad Request
        msg += f"{serverID}:{sh}, "
        curr_idx_shards[sh] = 0
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
    """
    Return all the data belonging to the shards in the request, 
    the load balancer ensures a server is only requested for the shards it holds
    """
    
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
    """
    Return all data in a range of student id's from a given shard
    """

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

    if not has_keys(payload, ["shard", 'curr_idx', "data"]):
        return jsonify({"status" : "failure"}), 400

    shard = payload["shard"]
    curr_idx = payload["curr_idx"]
    
    mode = "both"
    if "mode" in payload.keys():
        mode = payload["mode"]
    
    if(curr_idx_shards[shard] != curr_idx):
        return jsonify({
                    "message" : "Data entries not added, curr_idx mismatch",
                    "current_idx" : curr_idx_shards[shard],
                    "status" : "failure"
        }), 400
    
    data = payload["data"]
    for record in data:
        query = f"INSERT INTO studT_{shard} VALUES ({record['Stud_id']}, '{record['Stud_name']}', {record['Stud_marks']});"
        if not execute_query(query,mode):
            return jsonify({
                    "message" : "Data entries not added",
                    "current_idx" : curr_idx_shards[shard],
                    "status" : "failure"
            }), 401
        if mode == "log":
            continue
        curr_idx_shards[shard] += 1

    return jsonify({
                   "message" : "Data entries added",
                   "current_idx" : curr_idx_shards[shard],
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
    if not execute_query(query,mode):
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
    if not execute_query(query,mode):
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
    # Get the results
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
    cur.execute("show tables;")
    tables = cur.fetchall()
    for table in tables:
        table = table[0].decode()
        shard = table.split("_")[1]
        cur.execute(f"SELECT * FROM {table}")
        curr_idx_shards[shard] = len(cur.fetchall())
    disconnect_from_db(cur,conn)
    app.run( host = "0.0.0.0", port = 5000, debug = True)
