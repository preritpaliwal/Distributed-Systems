from flask import Flask, json, jsonify
import sys, os, socket

# serverID = socket.gethostname()
# serverID = 

app = Flask(__name__)
app.config['DEBUG'] = True

@app.route("/home", methods=["GET"])
def home():
    print("home function called", os.environ['serverID'],flush=True)
    message = 'Hello from Server: ' + str(os.environ['serverID'])
    return jsonify({'message': message, 'status': 'successful'}), 200


@app.route("/heartbeat", methods=["GET"])
def heartbeat():

    return jsonify({'message': "", 'status': 'successful'}), 200
    
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
    