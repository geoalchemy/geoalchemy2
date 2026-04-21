#!/usr/bin/env bash
set -euo pipefail

mysql \
    -h "${MYSQL_HOST}" \
    -u root \
    --password="${MYSQL_ROOT_PASSWORD}" <<'SQL'
CREATE DATABASE IF NOT EXISTS gis;
CREATE USER IF NOT EXISTS 'gis'@'%' IDENTIFIED BY 'gis';
ALTER USER 'gis'@'%' IDENTIFIED BY 'gis';
GRANT ALL PRIVILEGES ON *.* TO 'gis'@'%' WITH GRANT OPTION;
SQL
