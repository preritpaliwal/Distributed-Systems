from flask import Flask, request, json, jsonify
from consistent_hashing import consistentHash
import random, logging, requests, os

app = Flask(__name__)
app.config['DEBUG'] = True
log = logging.getLogger('werkzeug')
log.disabled = True

mapper = consistentHash(num_servers = 2, num_slots = 512, num_virtual_servers = 9)
bookkeeping = {}

def has_keys(json_data : dict, keys : list):
    
    for key in keys:
        if key not in json_data.keys():
            return False
    
    return True
     

@app.route("/init", methods=["POST"])
def init():
    
    payload = json.loads(request.data)
    
    if not has_keys(payload, ["N","schema", "shards", "servers"]) or not has_keys(payload["schema"], ["columns", "dtypes"]) or not has_keys(payload["shards"], ["Stud_id_low", "Shard_id","Shard_size"]):
        return jsonify({
            "message" : "<Error> Payload not formatted correctly.",
            "status" : "failure"
        }), 200
    bookkeeping=payload.copy()
    N = payload["N"]
    schema = payload["schema"]
    shards = payload["shards"]
    servers = payload["servers"]
    
    # TODO: Start N servers with mentioned shards and schema
    
    return jsonify({
        "message" : "Configured Database",
        "status" : "success"
    }) , 200
    
@app.route("/status", methods=["GET"])
def status():
    return jsonify(bookkeeping) , 200

@app.route("/add",methods=["POST"])
def add():
    pass
    # TODO - add a new server to the database

@app.route("/rm", methods=["DELETE"])
def rm():
    pass
    # TODO - remove a server from the database

@app.route("/read", methods=["POST"])
def read():
    pass
    # TODO - pick what servers to ping for each data shard
    # TODO - return data from all servers to client


@app.route("/write", methods=["POST"])
def write():
    pass

    # TODO - update all servers containing the shard
    # TODO - figure out use of curr_idx

@app.route("/update", methods=["PUT"])
def update():
    pass

    # TODO - update a single data row in all servers containting the shard

@app.route("/del", methods=["DELETE"])
def delete():
    pass

    # TODO - delete a record in all servers containing the shard

