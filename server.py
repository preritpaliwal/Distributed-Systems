import socket
import os

#fetch server_id from environment variable
server_id = os.environ.get('SERVER_ID', 'Unknown')

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
    port = 5002
    # Create a socket object
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print("Socket successfully created")   

    s.bind(('', port))         
    print ("socket binded to %s" %(port)) 
    
    # put the socket into listening mode 
    s.listen(5)     
    print ("socket is listening")        
    """
    TODO: convert iterative server to concurrent server
    """
    while True: 
        # Establish connection with client. 
        c, addr = s.accept()     
        print ('Received connection request from: ', addr )
        # c.send('/Connected!'.encode()) 
        message = c.recv(1024).decode()
        print('Received message: ', message)
        # parse message: say request_type and request_method
        request_type, request_method = message.split(' ')
        # TODO: handle EXIT message to close socket in concurrent server
        if(request_type == 'GET'):
            if(request_method == '/home'):
                response = home()
            elif(request_method == '/heartbeat'):
                response = heartbeat()
            else:
                response = not_found()
        else:
            response = not_found()
        
        print('Sending response: ', response)
        c.send(response.encode())       
        c.close()
        break

if __name__ == '__main__':
    main()