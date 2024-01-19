from flask import Flask, jsonify
import os

app = Flask(__name__)

# Retrieve server ID from environment variable
server_id = os.environ.get('SERVER_ID', 'Unknown')

@app.route('/home', methods=['GET'])
def home():
    response = {
        'message': f'Hello from Server: {server_id}',
        'status': 'successful'
    }
    return jsonify(response), 200

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    return '', 200

if __name__ == '__main__':
    app.run(port=5000)
