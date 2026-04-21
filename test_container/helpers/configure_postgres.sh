#!/usr/bin/env bash
set -euo pipefail

PGPASSWORD="${POSTGRES_PASSWORD}" psql \
    -h "${POSTGRES_HOST}" \
    -U "${POSTGRES_USER}" \
    -d "${POSTGRES_DB}" \
    -v db_name="${POSTGRES_DB}" \
    -v ON_ERROR_STOP=1 <<'SQL'
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_raster;
CREATE SCHEMA IF NOT EXISTS gis AUTHORIZATION CURRENT_USER;
ALTER DATABASE :"db_name" SET search_path = "$user", public;
SQL
