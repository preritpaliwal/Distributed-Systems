FROM python:latest

RUN apt-get update
RUN apt-get install -y python3
RUN apt-get install -y python3-pip
RUN pip install --upgrade pip

RUN mkdir /src
WORKDIR /src
COPY server.py /src/server.py
COPY server_init.sh /src/server_init.sh
COPY requirements.txt /src/requirements.txt

RUN pip install -r requirements.txt
RUN apt-get install -y default-mysql-server

# ENV serverID="101"
EXPOSE 5000


ENTRYPOINT  [ "bash" ]
CMD [ "server_init.sh"]

# docker run --rm -p 5001:5000 --name server1 -e serverID=111 server
# cat /proc/self/cgroup