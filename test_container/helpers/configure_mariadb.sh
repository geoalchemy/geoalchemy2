#!/usr/bin/env bash
set -euo pipefail

mysql_args=(
    -h "${MARIADB_HOST}"
    -u root
    --password="${MARIADB_ROOT_PASSWORD}"
)

if [ -n "${MARIADB_PORT:-}" ]; then
    mysql_args+=(-P "${MARIADB_PORT}")
fi

mysql "${mysql_args[@]}" <<'SQL'
CREATE DATABASE IF NOT EXISTS gis;
CREATE USER IF NOT EXISTS 'gis'@'%' IDENTIFIED BY 'gis';
ALTER USER 'gis'@'%' IDENTIFIED BY 'gis';
GRANT ALL PRIVILEGES ON *.* TO 'gis'@'%' WITH GRANT OPTION;
SQL
