import socket
import os

# Fetch server_id from environment variable
server_id = os.environ.get('SERVER_ID', 'Unknown')

# Thread function
def handle_client(c):
    while True:
        message = c.recv(1024).decode()
        print(f"Received message: {message}")
        
        if message == 'EXIT':
            print("Closing connection!")
            break

        # Parse message: say request_type and request_method
        request_type, request_method = message.split(' ')
        
        if request_type == 'GET':
            if request_method == '/home':
                response = home()
            elif request_method == '/heartbeat':
                response = heartbeat()
            else:
                response = not_found()
        else:
            response = not_found()

        print('Sending response:', response)
        c.send(response.encode())

    c.close()

def home():
    response = f"message: Hello from Server: {server_id}, status: successful, code: 200"
    return response

def heartbeat():
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
