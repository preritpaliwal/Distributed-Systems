class logEntry:
    def __init__(self, log:str = None) -> None:
        self.offset = -1
        self.crc = -1
        self.data = None
        if log != None:
            self.getLog(log)
    
    def __str__(self) -> str:
        return f"{self.offset}^{self.crc}^{self.data}\n"
    
    def getLog(self, log:str):
        log = log.split("^")
        self.offset = int(log[0])
        self.crc = int(log[1])
        self.data = log[2]
        return self

class Logger:
    def __init__(self, log_file) -> None:
        self.log_file = log_file
    
    def append(self, log_entry):
        with open(self.log_file, "a") as f:
            f.write(str(log_entry))
    
    def read(self):
        logs = []
        with open(self.log_file, "r") as f:
            while True:
                line = f.readline().split("\n")[0]
                if not line:
                    break
                logs.append(logEntry(line))
        return logs
    

def main():
    logger = Logger("log.txt")
    logger.append(logEntry("1^2^hello"))
    logger.append(logEntry("2^3^world"))
    logger.append(logEntry("3^4^!"))
    logger.append(logEntry("4^5^sdf"))
    logs = logger.read()
    for l in logs:
        print(l.offset, l.data, l.crc)


if __name__ == "__main__":
    main()