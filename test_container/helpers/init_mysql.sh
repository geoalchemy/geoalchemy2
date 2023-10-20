#!/usr/bin/env bash
set -e

if [ $(whoami) != "root" ]; then
    echo "must run as the root user"
    exit 1
fi

/etc/init.d/mysql start

echo "waiting for mysql to start"
while ! mysqladmin ping -h 127.0.0.1 --silent; do
    sleep 0.2
done

echo "Create the 'gis' role"
mysql -e "CREATE USER 'gis'@'%' IDENTIFIED BY 'gis';"
mysql -e "GRANT ALL PRIVILEGES ON *.* TO 'gis'@'%' WITH GRANT OPTION;"

echo "Create the 'gis' database"
mysql -u gis --password=gis -e "CREATE DATABASE gis;"
