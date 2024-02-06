import math, random
from hashlib import md5, sha1, sha256

class consistentHash():

    def __init__(self, num_servers, num_slots, num_virtual_servers = None):
        
        self.N = num_servers
        self.M = num_slots
        self.K = int(math.log2(num_slots) if (num_virtual_servers is None) else num_virtual_servers)
        self.H = self.__h
        self.Phi = self.__phi
        
        self.pServers = []
        self.serverRing = [ None for i in range(self.M) ]
        self.requestRing = [ None for i in range(self.M) ]
        
        self.addServer(self.N, [ f"Server_{i}" for i in range(1, self.N+1) ])
    
    def __power(self, a, exp, mod):
        res = 1
        while exp > 1:
            if exp & 1:
                res = (res * a) % mod
            a = a ** 2 % mod
            exp >>= 1
        return (a * res) % mod
    
    def __h(self, i):
        # return __power(37, i, self.M)
        return i*i + 2*i + 17
    
    def __phi(self, i, j):
        # return __power(37, __power(i, j, 97), self.M)
        return i ** 2 + (j+1) ** 2 + 24
    
    def getReplicas(self):
        
        return self.pServers
    
    def __addEntity(self, ring : list, slot, name):
        
        M = len(ring)
        for k in range(M):
            if ring[(k+slot) % M] is None:
                ring[(k+slot) % M] = name
                return (k+slot) % M
        
    def addRequest(self, RequestID):
        
        # rSlot = md5("Request_" + str(RequestID)) % self.M
        # rSlot = sha1("Request_    " + str(RequestID)) % self.M
        # rSlot = sha256("Request_" + str(RequestID)) % self.M
        
        rSlot = self.H(RequestID) % self.M
        
        rSlot = self.__addEntity(self.requestRing, rSlot, f"R_{RequestID}")
        
        if rSlot is None:
            return -1, None
        
        server = None
        
        for i in range(self.M):
            if self.serverRing[(rSlot+i) % self.M] is not None:
                _, number, _ = self.serverRing[(rSlot + i) % self.M].split("_")
                server = f"Server_{number}"
                break
            
        
        return server, rSlot
        
    def clearRequest(self, rSlot):
        
        self.requestRing[rSlot] = None
    
    def __getServerNumber(self, server : str):
        for i in range(len(server)):
            if server[i:].isnumeric():
                return int(server[i:])

    def addServer(self, n, serverList):
        # done in load balancer
        # TODO - check if desired names can be used for servers
        # TODO - start n - len(serverList) additional servers
        
        for server in serverList:
            
            self.pServers.append(server)
            i = self.__getServerNumber(server)
            
            for j in range(1, self.K+1):
                
                # sSlot = md5(server + "_" + str(j)) % self.M
                # sSlot = sha1(server + "_" + str(j)) % self.M
                # sSlot = sha256(server + "_" + str(j)) % self.M
        
                sSlot = self.Phi(i, j) % self.M
                self.__addEntity(self.serverRing, sSlot, f"{server}_{j}")
        
        return self.pServers
    
    def __removeServer(self, server):
        
        i = self.__getServerNumber(server)
                
        for j in range(1, self.K+1):
            
            sSlot = self.Phi(i, j) % self.M
            # sSlot = md5(server + "_" + st r(j)) % self.M
            # sSlot = sha1(server + "_" + str(j)) % self.M
            # sSlot = sha256(server + "_" + str(j)) % self.M
            
            vServerName = f"{server}_{j}"
    
            for k in range(self.M):
                if self.serverRing[(k+sSlot) % self.M] == vServerName:
                    self.serverRing[(k+sSlot) % self.M] = None
                    break

    def deleteServer(self, n, serverList):
        
        for server in serverList:
            
            if server in self.pServers:
                self.__removeServer(server)                
                n -= 1
                self.pServers.remove(server)
                print(f"Removed {server}")
                
        for i in range(n):
            
            server = random.choice(self.pServers)
            self.pServers.remove(server)
            self.__removeServer(server)
            print(f"Removed {server}")
            
            
        return self.pServers