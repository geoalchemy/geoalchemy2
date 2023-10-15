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
echo 'run tests with `tox --workdir /output -vv`'
echo "###############################"

mkdir /geoalchemy2
find /geoalchemy2_read_only -mindepth 1 -maxdepth 1 | while read -r item; do
    ln -s "${item}" "/geoalchemy2/$(basename "${item}")"
done

cd /geoalchemy2
rm -f .mypy_cache
rm .git
rm .gitignore

rm /geoalchemy2/doc
cp -r /geoalchemy2_read_only/doc /geoalchemy2/doc

export MYPY_CACHE_DIR=/output/.mypy_cache

mkdir -p /output/reports
rm -f /geoalchemy2/reports
ln -s /output/reports /geoalchemy2/reports

# to allow pre-commit to run
cp /geoalchemy2_read_only/.gitignore ./
git config --global init.defaultBranch master
git config --global user.email "user@example.com"
git config --global user.name "user"
git init > /dev/null
git add --all
git commit -m "dummy commit" > /dev/null

exec bash
