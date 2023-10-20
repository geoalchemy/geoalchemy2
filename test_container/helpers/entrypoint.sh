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
