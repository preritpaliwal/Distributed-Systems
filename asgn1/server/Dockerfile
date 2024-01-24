FROM ubuntu:latest

RUN apt-get update && apt-get install -y build-essential git cmake
RUN apt-get install -y python3
RUN apt-get install -y python3-pip

RUN mkdir /src
WORKDIR /src
COPY server.py /src/server.py
COPY requirements.txt /src/requirements.txt

RUN pip install -r requirements.txt

ENV serverID="101"

ENTRYPOINT  [ "python3" ]
CMD [ "server.py"]

# docker run --rm -p 5001:5000 --name server1 -e serverID=111 server