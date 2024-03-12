from flask import Flask, json, jsonify, request
import sys, os, socket
# import MySQLdb
import mysql.connector

# env variables
serverID = os.environ['serverID']

app = Flask(__name__)
app.config['DEBUG'] = True

# mysql parser
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password=""
)
cur = mydb.cursor()

curr_idx_shards = {}

# creating the database
cur.execute("CREATE DATABASE studentdb")

def has_keys(json_data : dict, keys : list):
    for key in keys:
        if key not in json_data.keys():
            return False
    return True

@app.route("/config",methods=["POST"])
def config():
    # initialize the shard tables 
    payload = json.loads(request.data)
    
    if not has_keys(payload, ["shards", "schema"]) or not has_keys(payload["schema"], ["columns", "dtypes"]):
        return jsonify({"status" : "failure"}), 400 # Bad Request
    
    schema = payload["schema"]
    shards = payload["shards"]
    
    cols = schema["columns"]
    dtypes = schema["dtypes"]
    
    query = "("
    
    for i in range(len(cols)-1):
        query += f"{cols[i]} {dtypes[i]}, "
    query += f"{cols[-1]} {dtypes[-1]})"
    
    msg = ""
    for sh in shards:
        cur.execute(f"CREATE TABLE studT_{sh} {query};")
        msg += f"{serverID}:{sh}, "
        curr_idx_shards[sh] = 0
    msg+=" configured"
    
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

@app.route("/heartbeat", methods=["GET"])
def heartbeat():
    return jsonify({}), 200

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
    
    # TODO - query the data from the database
    for shard in shards:
        cur.execute(f"SELECT * FROM studT_{shard}")
        data[shard] = cur.fetchall()
    
    data["status"] = "success"
    return jsonify(data), 200
    
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

    cur.execute(f"SELECT * FROM studT_{shard} Stud_id >= {lo} AND Stud_id <= {hi}")

    return jsonify({
                   "data" : cur.fetchall(),
                   "status" : "success"
    }), 200
    
@app.route("/write", methods=["POST"])
def write():
    
    payload = json.loads(request.data)

    if not has_keys(payload, ["shard", 'curr_idx', "data"]):
        return jsonify({"status" : "failure"}), 400

    # TODO - use eval to make data into a list of dict - No need ig
    # TODO - figure out use of curr_idx
    shard = payload["shard"]
    curr_idx = payload["curr_idx"]
    data = payload["data"]
    data = [ "(" + ",".join(record["Stud_id"])+ ",".join(record["Stud_name"])+ ",".join(record["Stud_marks"]) + ")" for record in data ] 

    query = f"INSERT INTO studT_{shard} VALUES " + ",".join(data) + ";"
    cur.execute(query)

    return jsonify({
                   "message" : "Data entries added",
                   "current_idx" : "",
                   "status" : "success"
    }), 200


@app.route("/update", methods=["PUT"])
def update():

    payload = json.loads(request.data)

    if not has_keys(payload, ["shard", "Stud_id", "data"]):
        return jsonify({"status" : "failure"}), 400 # Bad Request

    shard = payload["shard"]
    stud_id = payload["Stud_id"]
    data = payload["data"]

    query = f"UPDATE studT_{shard} SET " + ",".join([f"{k} = {v}" for k,v in data.items()]) + f" WHERE Stud_id = {stud_id};"
    cur.execute(query)
    
    return jsonify({
                   "message" : f"Data entry for Stud_id : {stud_id} updated",
                   "status" : "success"
    }), 200




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
    return jsonify({'message': f"<Error> '/{path}' endpoint does not exist in server replicas", 'status': 'successful'}), 200


if __name__ == "__main__":
    
    # if(len(sys.argv) != 2):
    #     print("Error! Usage : python3 server.py <ServerID>")
    #     exit(0)
    
    # serverID = sys.argv[1]
    

        
    app.run( host = "0.0.0.0", port = 5000, debug = True)
    
