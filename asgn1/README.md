# Details of server

Why K = log(M)

Since request resolution is really quick, instead of using heartbeat to check server health, we simply go ahead with the request and use it's failure as an indication of server failure. 