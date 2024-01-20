import socket             

def main():         
    port = 5002
    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)

    s.connect(('127.0.0.1', port)) 
    print("Connected!")
    while True:
        print("1. GET /home \n" +
            "2. GET /rep \n" +
            "3. POST /add \n" +
            "4. POST /rm \n" +
            "5. Custom request \n" +
            "6. Exit \n"
            )
        message = input("Enter request number: ")
        if(message == '1'):
            s.send("GET /home".encode())
        elif(message == '2'):
            s.send("GET /rep".encode())
        elif(message == '3'):
            s.send("POST /add".encode())
        elif(message == '4'):
            s.send("POST /rm".encode())
        elif(message == '5'):
            message = input("Enter custom request: ")
            s.send(message.encode())
        elif(message == '6'):
            s.send("EXIT".encode())
            break
        else:
            print("Invalid Request!")
            continue
        print(s.recv(1024).decode())
    s.close()    

if __name__ == '__main__':
    main()