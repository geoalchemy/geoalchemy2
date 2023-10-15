#!/usr/bin/env bash

echo "starting postgres"
(su postgres -c '${POSTGRES_PATH}/bin/pg_ctl start' > /dev/null 2>&1) &

while ! ${POSTGRES_PATH}/bin/pg_isready --quiet; do
    sleep 0.2
done

echo "starting mysql"
/etc/init.d/mysql start

echo "waiting for mysql to start"
while ! mysqladmin ping -h 127.0.0.1 --silent; do
    sleep 0.2
done

echo "###############################"
echo "GeoAlchemy2 Test Container"
echo ""
echo 'run tests with `pytest /geoalchemy2/tests`'
echo "###############################"

exec bash --init-file <(echo ". \"$HOME/.bashrc\"; . /venv/bin/activate;")
