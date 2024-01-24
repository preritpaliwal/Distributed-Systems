FROM ubuntu:latest

RUN apt-get update && apt-get install -y build-essential git cmake
RUN apt-get install -y python3
# RUN apt-get install -y python-flask

RUN mkdir /src
WORKDIR /src
COPY server.py /src/server.py
COPY requirements.txt /src/requirements.txt

# RUN pip install -r requirements.txt
RUN pip install flask

EXPOSE 8080
CMD [ "python3", "/src/server.py" ]