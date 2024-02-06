FROM ubuntu:latest

RUN apt-get update && apt-get install -y build-essential git cmake
RUN apt-get update
RUN apt-get install -y python3
RUN apt-get install -y python3-pip
RUN apt-get update
RUN apt-get install -y docker.io
RUN apt-get -y install sudo
RUN pip install --upgrade pip


COPY ./load_balancer.py ./load_balancer.py
COPY ./consistent_hashing.py ./consistent_hashing.py
COPY ./requirements.txt ./requirements.txt

RUN pip install -r requirements.txt

EXPOSE 5000

ENTRYPOINT  [ "python3" ]
CMD [ "load_balancer.py"]

# docker run --rm -p 5001:5000 --name server1 -e serverID=111 server