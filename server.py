import socket
import os

#fetch server_id from environment variable
server_id = os.environ.get('SERVER_ID', 'Unknown')

from _thread import *
import threading
 
print_lock = threading.Lock()
 
# thread function
def threaded(c):
    while True:
        message = c.recv(1024).decode()
        print('Received message: ', message)
        if(message == 'EXIT'):
            print("Closing connection!")
            print_lock.release()
            break

        # parse message: say request_type and request_method
        request_type, request_method = message.split(' ')
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

    while True: 
        c, addr = s.accept()   

        print_lock.acquire()
        print('Connected to :', addr[0], ':', addr[1])

        start_new_thread(threaded, (c,))
    # s.close()

if __name__ == '__main__':
    main()