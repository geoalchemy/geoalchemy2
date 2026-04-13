#!/usr/bin/env bash
set -euo pipefail

echo "waiting for postgres at ${POSTGRES_HOST}"
until PGPASSWORD="${POSTGRES_PASSWORD}" pg_isready \
    -h "${POSTGRES_HOST}" \
    -U "${POSTGRES_USER}" \
    -d "${POSTGRES_DB}" \
    --quiet; do
    sleep 0.5
done

echo "waiting for mysql at ${MYSQL_HOST}"
until mysqladmin ping \
    -h "${MYSQL_HOST}" \
    -u root \
    --password="${MYSQL_ROOT_PASSWORD}" \
    --silent; do
    sleep 0.5
done

echo "waiting for mariadb at ${MARIADB_HOST}"
until mysqladmin ping \
    -h "${MARIADB_HOST}" \
    -u root \
    --password="${MARIADB_ROOT_PASSWORD}" \
    --silent; do
    sleep 0.5
done

echo "initializing mssql"
/init_mssql.sh

echo "###############################"
echo "GeoAlchemy2 Test Container"
echo ""
echo 'run tests with `tox --workdir /output -v run`'
echo 'run only a specific job, e.g. `py310-sqlalatest`, with `tox --workdir /output -v run -e py310-sqlalatest`'
echo "MSSQL defaults: server=${MSSQL_HOST} db=${MSSQL_TEST_DB} user=${MSSQL_TEST_LOGIN} password=${MSSQL_TEST_PASSWORD}"
echo "###############################"

###############################
# workarounds to get the tests working while mounting the code in as read-only
mkdir /geoalchemy2
find /geoalchemy2_read_only -mindepth 1 -maxdepth 1 | while read -r item; do
    ln -s "${item}" "/geoalchemy2/$(basename "${item}")"
done

cd /geoalchemy2

# remove links that would cause issues if they are present and read-only
rm -f .mypy_cache .eggs *.egg-info .git .gitignore doc reports

# copy these items instead of symlinking
cp -r /geoalchemy2_read_only/doc /geoalchemy2/doc
cp /geoalchemy2_read_only/.gitignore ./

# store reports in the output directory
mkdir -p /output/reports
ln -s /output/reports /geoalchemy2/reports

# to allow pre-commit to run
git config --global init.defaultBranch master
git config --global user.email "user@example.com"
git config --global user.name "user"
git init > /dev/null
git add --all
git commit -m "dummy commit" > /dev/null

export MYPY_CACHE_DIR=/output/.mypy_cache

###############################

exec bash
