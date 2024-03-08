from flask import Flask, json, jsonify, request
import sys, os, socket
import MySQLdb


app = Flask(__name__)
app.config['DEBUG'] = True
db = MySQLdb.connect(host="localhost", user="root", passwd="",db="studT") 
cur = db.cursor()


def has_keys(json_data : dict, keys : list):
    
    for key in keys:
        if key not in json_data.keys():
            return False
    return True

@app.route("/home", methods=["GET"])
def home():
    pass
    # print("home function called", os.environ['serverID'],flush=True)
    # message = 'Hello from Server: ' + str(os.environ['serverID'])
    # return jsonify({'message': message, 'status': 'successful'}), 200

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
        cur.execute(f"SELECT * FROM STUDENTS WHERE shard = {shard}")
        data[shard] = cur.fetcahll()
    
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

    cur.execute(f"SELECT * FROM STUDENTS WHERE shard = {shard} AND Stud_id >= {lo} AND Stud_id <= {hi}")

    return jsonify({
                   "data" : cur.fetchall(),
                   "status" : "success"
    }), 200
    
@app.route("/write", methods=["POST"])
def write():
    
    payload = json.loads(request.data)

    if not has_keys(payload, ["shard", 'curr_idx', "data"]):
        return jsonify({"status" : "failure"}), 400

    # TODO - use eval to make data into a list of dict
    # TODO - figure out use of curr_idx
    shard = payload["shard"]
    curr_idx = payload["curr_idx"]
    data = payload["data"]
    data = [ "(" + ",".join(record) + ")" for record in data ] 

    query = "INSERT INTO STUDENTS VALUES " + ",".join(data) + ";"
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

    query = "UPDATE STUDENTS SET " + ",".join([f"{k} = {v}" for k,v in data.items()]) + f" WHERE Stud_id = {stud_id} AND shard = {shard};"
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

    query = f"DELETE FROM STUDENTS WHERE Stud_id = {stud_id} and shard = {shard};"
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
    
