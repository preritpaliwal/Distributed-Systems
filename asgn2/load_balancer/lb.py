from flask import Flask, request, json, jsonify
from consistent_hashing import consistentHash
import random, logging, requests, os

app = Flask(__name__)
app.config['DEBUG'] = True
log = logging.getLogger('werkzeug')
log.disabled = True

mapper = consistentHash(num_servers = 2, num_slots = 512, num_virtual_servers = 9)

def has_keys(json_data : dict, keys : list):
    
    for key in keys:
        if key not in json_data.keys():
            return False
    
    return True
     

@app.route("/config", methods=["POST"])
def config():
    
    payload = json.loads(request.data)
    
    if not has_keys(payload, ["schema", "shards"]) or not has_keys(payload["schema"], ["columns", "dtypes"]):
        return jsonify({
            "message" : "<Error> Payload not formatted correctly.",
            "status" : "failure"
        }), 200
        
    schema = payload["schema"]
    shards = payload["shards"]
    
    # TODO - initialise a mysql database with columns, dtypes in schema
    # TODO - pick 
    
    return jsonify({
        # "message" : ""
    }) 
    
@app.route("/copy", methods=["GET"])
def copy():
    pass

    # TODO - pick what servers to ping for each data shard
    # TODO - return all data in a shard

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

