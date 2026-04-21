#!/usr/bin/env bash
set -euo pipefail

mysql \
    -h "${MARIADB_HOST}" \
    -u root \
    --password="${MARIADB_ROOT_PASSWORD}" <<'SQL'
CREATE DATABASE IF NOT EXISTS gis;
CREATE USER IF NOT EXISTS 'gis'@'%' IDENTIFIED BY 'gis';
ALTER USER 'gis'@'%' IDENTIFIED BY 'gis';
GRANT ALL PRIVILEGES ON *.* TO 'gis'@'%' WITH GRANT OPTION;
SQL
