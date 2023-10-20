#!/usr/bin/env bash
set -e

if [ $(whoami) != "postgres" ]; then
    echo "must run as the postgres user"
    exit 1
fi

${POSTGRES_PATH}/bin/initdb --auth=trust --username=postgres -E 'UTF-8'

# by default only listens on localhost so host cannot connect
echo "listen_addresses = '0.0.0.0'" >> "${PGDATA}/postgresql.conf"

(${POSTGRES_PATH}/bin/pg_ctl start > /dev/null 2>&1) &

while ! ${POSTGRES_PATH}/bin/pg_isready --quiet; do
    sleep 0.2
done

echo "Create the 'gis' role"
psql -c "CREATE ROLE gis PASSWORD 'gis' SUPERUSER CREATEDB CREATEROLE INHERIT LOGIN;"

echo "Create the 'gis' database"
createdb -E UTF-8 gis
psql -d gis -c 'CREATE SCHEMA gis;'
psql -c 'GRANT CREATE ON DATABASE gis TO "gis";'
psql -d gis -c 'GRANT USAGE,CREATE ON SCHEMA gis TO "gis";'


echo "Enable PostGIS for the 'gis' database"
psql -d gis -c "CREATE EXTENSION postgis;"

echo "With PostGIS 3 enable PostGIS Raster as well"
psql -d gis -c "CREATE EXTENSION postgis_raster;"


${POSTGRES_PATH}/bin/pg_ctl stop
sleep 1
