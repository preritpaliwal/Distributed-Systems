from flask import Flask, json
import sys, os, socket

# serverID = socket.gethostname()
serverID = os.environ['serverID']

app = Flask(__name__)
app.config['DEBUG'] = True

@app.route("/home", methods=["GET"])
def home():
    print("home function called", serverID,flush=True)
    return app.response_class(
        response = json.dumps({
            "message" : f"Hello from Server: [{serverID}]",
            "status" : "successful"
            }),
        status = 200
    )

@app.route("/heartbeat", methods=["GET"])
def heartbeat():
    
    return app.response_class(
        response = None,
        status = 201
    )
    
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
    
    # if(len(sys.argv) != 2):
    #     print("Error! Usage : python3 server.py <ServerID>")
    #     exit(0)
    
    # serverID = sys.argv[1]
        
    app.run( host = "0.0.0.0", port = 5000, debug = True)
    