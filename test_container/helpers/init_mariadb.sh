#!/usr/bin/env bash
set -e

if [ $(whoami) != "root" ]; then
    echo "must run as the root user"
    exit 1
fi

echo "Starting mysql server"
/etc/init.d/mariadb start

echo "Waiting for mysql to start"
while ! mysqladmin ping -h 127.0.0.1 --silent; do
    sleep 0.2
done

echo "Create the 'gis' role"
mariadb -e "CREATE USER 'gis'@'%' IDENTIFIED BY 'gis';"
mariadb -e "GRANT ALL PRIVILEGES ON *.* TO 'gis'@'%' WITH GRANT OPTION;"

echo "Create the 'gis' database"
mariadb -u gis --password=gis -e "CREATE DATABASE gis;"
