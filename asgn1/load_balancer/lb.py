from flask import Flask, request, json
import sys

app = Flask(__name__)
app.config['DEBUG'] = True

@app.route("/rep", methods=["GET"])
def rep():
    pass


# curl -X POST "http://127.0.0.1:5000/add" -H "Content-Type: application/json" -d "@payload.json"

@app.route("/add", methods=["POST"])
def add():
    
    payload = json.loads(request.data)
    
    if('n' not in payload.keys() or 'hostnames' not in payload.keys()):
        return app.response_class(
            response = json.dumps({  
                "message" : "<Error> Payload not formatted correctly. Should contain n and hostnames field",
                "status" : "failure"
            }),
            status = 400
        )
        
    n = int(payload['n'])
    hostnames = payload['hostnames']
    
    if(n != len(hostnames)):
        return app.response_class(
            response = json.dumps({
                "message" : "<Error> Length of hostname list is more than newly added instances",
                "status" : "failure"
            }),
            status = 400
        )
        
    
    # TODO - add instances
    
    return app.response_class(
        response = json.dumps({
            "message" : {
                "N" : -1, # Value of N
                "replicas" : [] # Server Names
            },
            "status" : "successful"
        }),
        status = 200
    )
    
@app.route("/rm", methods=["DELETE"])
def rm():
    
    payload = json.loads(request.data)
    
    if('n' not in payload.keys() or 'hostnames' not in payload.keys()):
        return app.response_class(
            response = json.dumps({  
                "message" : "<Error> Payload not formatted correctly. Should contain n and hostnames field",
                "status" : "failure"
            }),
            status = 400
        )
        
    n = int(payload['n'])
    hostnames = payload['hostnames']
    
    if(n != len(hostnames)):
        return app.response_class(
            response = json.dumps({
                "message" : "<Error> Length of hostname list is more than removable instances",
                "status" : "failure"
            }),
            status = 400
        )
    
    # TODO - delete instances

@app.route("/<path>", methods = ["GET"])
def other(path):
    print(path)
    
    return app.response_class(
        response = json.dumps({
            "message" : f"<Error> '/{path}' endpoint does not exist in server replicas",
            "status" : "failure"
            }),
        status = 400
    )

if __name__ == "__main__":
    
    app.run( host = "0.0.0.0", port = 5000, debug = True)
    