class Logger:
    def __init__(self, log_file) -> None:
        self.log_file = log_file
        self.curr_offset = 0
    
    def append(self, log_entry:str):
        self.curr_offset+=len(str(log_entry))
        with open(self.log_file, "a") as f:
            f.write(log_entry+"\n")
    
    def read(self):
        logs = []
        with open(self.log_file, "r") as f:
            while True:
                line = f.readline().split("\n")[0]
                if not line:
                    break
                logs.append(line)
        return logs
    

def main():
    logger = Logger("log.txt")
    logger.append("hello")
    logger.append("world")
    logger.append("!")
    logger.append("sdf")
    logs = logger.read()
    print(logs)


if __name__ == "__main__":
    main()