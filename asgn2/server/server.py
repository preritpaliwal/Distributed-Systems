from flask import Flask, json, jsonify, request
import sys, os, socket
# import MySQLdb
import mysql.connector

# env variables
# serverID = os.environ['serverID']
serverID = 1

app = Flask(__name__)
app.config['DEBUG'] = True

# mysql parser
conn = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Mysql@123",
  auth_plugin="mysql_native_password",
  autocommit=True
)
cur = conn.cursor()
curr_idx_shards = {}

name_to_dataType = {
    "Number":"int",
    "String":"varchar(255)"
}

# creating the database
# cur.execute("CREATE DATABASE studentdb")


def has_keys(json_data : dict, keys : list):
    for key in keys:
        if key not in json_data.keys():
            return False
    return True
# curl -X POST "http://127.0.0.1:5000/config" -H "Content-Type: application/json" -d @config.json
@app.route("/config",methods=["POST"])
def config():
    # initialize the shard tables 
    try:
        payload = json.loads(request.data)
    except:
        return jsonify({"status" : "failure"}), 401
    print("received\n\\n\n\n",flush=True)
    print(payload,flush= True)
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
        print(final_query,flush=True)
        try:
            cur.execute(final_query)
        except:
            return jsonify({"status" : "failure"}), 402 # Bad Request
        msg += f"{serverID}:{sh}, "
        curr_idx_shards[sh] = 0
    msg+=" configured"
    print("tables created",flush=True)
    return jsonify({
                   "message" : msg,
                   "status" : "success"
    }), 200

# @app.route("/home", methods=["GET"])
# def home():
#     pass
#     # print("home function called", os.environ['serverID'],flush=True)
#     # message = 'Hello from Server: ' + str(os.environ['serverID'])
#     # return jsonify({'message': message, 'status': 'successful'}), 200

@app.route("/tables",methods=["GET"])
def tables():
    cur.execute("Show tables;")
    tabs = cur.fetchall()
    print(tabs,flush=True)
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
    
    for shard in shards:
        cur.execute(f"SELECT * FROM studT_{shard}")
        data[shard] = cur.fetchall()
    
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

    query = f"SELECT * FROM studT_{shard} WHERE Stud_id >= {lo} AND Stud_id <= {hi};"
    cur.execute(query)

    return jsonify({
                   "data" : cur.fetchall(),
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
    if(curr_idx_shards[shard] != curr_idx):
        return jsonify({
                    "message" : "Data entries not added, curr_idx mismatch",
                    "current_idx" : curr_idx_shards[shard],
                    "status" : "failure"
        }), 400
    
    data = payload["data"]
    for record in data:
        query = f"INSERT INTO studT_{shard} VALUES ({record['Stud_id']}, '{record['Stud_name']}', {record['Stud_marks']});"
        print(query,flush=True)
        cur.execute(query)
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
    cur.execute(query)
    
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
    cur.execute(query)

    return jsonify({
                   "message" : f"Data entry with Stud_id : {stud_id} removed",
                   "status" : "success"
    }), 200

    

@app.route("/<path>", methods = ["GET"])
def other(path):
    print(path)
    return jsonify({'message': f"<Error> '/{path}' endpoint does not exist in server", 'status': 'successful'}), 200


if __name__ == "__main__":
    
    # if(len(sys.argv) != 2):
    #     print("Error! Usage : python3 server.py <ServerID>")
    #     exit(0)
    
    # serverID = sys.argv[1]
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
    print(curr_idx_shards,flush=True)
    
    app.run( host = "0.0.0.0", port = 5000, debug = True)
    
