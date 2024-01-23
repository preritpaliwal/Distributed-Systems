import socket
import os
from consistent_hashing import consistentHash

# Fetch server_id from environment variable (0 for load balancer)
server_id = 0

# Thread function
def handle_client(c):
    message = c.recv(1024).decode()
    print(f"Received message: {message}")
    
    if message == 'EXIT':
        print("Closing connection!")
        return

    # Parse message: say request_type and request_method
    request_type, request_method = message.split(' ')
    
    if request_type == 'GET':
        if request_method == '/rep':
            response = rep()
        elif request_method[0] == '/':
            response = path()
        else:
            response = not_found()
    else:
        response = not_found()
    
    if request_type == 'POST':
        if request_method == '/add':
            response = add()
        elif request_method == '/rm':
            response = rm()
        else:
            response = not_found()

    print('Sending response:', response)
    c.send(response.encode())

    c.close()

def add():
    # add instance to consistent hash data structure
    
    for names in message:
        serverHash.addInstance(names)
    
    pass

def rm():
    # remove instance from consistent hash data structure
    pass

def rep():
    # print status of consistent hash data structure
    pass

def home():
    response = f"message: Hello from Server: {server_id}, status: successful, code: 200"
    return response

def path():
    response = "message: '', status: '', code: 200"
    return response

def not_found():
    response = "message: '', status: '', code: 404"
    return response

def main():
    host = '127.0.0.1'
    port = 5002

    # Create a socket object
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print("Socket successfully created")

    # Bind the socket to a specific address and port
    s.bind((host, port))
    print("Socket binded to %s:%s" % (host, port))

    # Put the socket into listening mode
    s.listen(5)
    print("Socket is listening")

    try:
        while True:
            # Wait for a connection
            c, addr = s.accept()
            print('Connected to:', addr[0], ':', addr[1])

            # Fork a new process to handle the client
            child_pid = os.fork()

            if child_pid == 0:
                # This is the child process
                s.close()  # Close the socket in the child process
                handle_client(c)
                os._exit(0)  # Exit the child process

            # This is the parent process
            c.close()  # Close the client socket in the parent process

    except KeyboardInterrupt:
        print("Server shutting down...")

    finally:
        s.close()

if __name__ == '__main__':
    main()
