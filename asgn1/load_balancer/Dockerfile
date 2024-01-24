FROM ubuntu:latest

RUN apt-get update && apt-get install -y build-essential git cmake
RUN apt-get install -y python3
RUN apt-get install -y python3-pip

RUN mkdir /src
WORKDIR /src
COPY load_balancer.py /src/load_balancer.py
COPY consistent_hashing.py /src/consistent_hashing.py
COPY requirements.txt /src/requirements.txt

RUN pip install -r requirements.txt

ENTRYPOINT  [ "python3" ]
CMD [ "load_balancer.py"]

# docker run --rm -p 5001:5000 --name server1 -e serverID=111 server