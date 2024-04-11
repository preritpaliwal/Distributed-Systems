#!/bin/bash

echo "Start mysqld ..."
service mariadb start

cntr=0
until mysql -u root -e "SHOW DATABASES; ALTER USER 'root'@'localhost' IDENTIFIED BY 'Mysql@123';" ; do
    sleep 1
    read -r -p "Can't connect, retrying..."
    # echo "Retrying..."
    cntr=$((cntr+1))
    if [ $cntr -gt 5 ]; then
        echo "Failed to start MySQL server."
        exit 1
    fi
done

# sudo docker cp 00067dfe1fab:/load.json ~/Desktop/sem8/Distributed-Systems/asgn1/sha256.json
exec python3 server.py