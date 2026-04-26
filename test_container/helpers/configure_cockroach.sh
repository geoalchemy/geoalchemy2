#!/usr/bin/env bash
set -euo pipefail

cockroach_port="${COCKROACH_PORT:-26257}"
root_db_url="postgresql://root@${COCKROACH_HOST}:${cockroach_port}/defaultdb?sslmode=disable"
target_root_db_url="postgresql://root@${COCKROACH_HOST}:${cockroach_port}/${COCKROACH_DATABASE}?sslmode=disable"
target_user_db_url="postgresql://${COCKROACH_USER}@${COCKROACH_HOST}:${cockroach_port}/${COCKROACH_DATABASE}?sslmode=disable"

echo "Waiting for CockroachDB server at ${COCKROACH_HOST}:${cockroach_port} to start"
until psql "${root_db_url}" -v ON_ERROR_STOP=1 -c "SELECT 1" >/dev/null 2>&1; do
    sleep 1
done

psql \
    "${root_db_url}" \
    -v db_name="${COCKROACH_DATABASE}" \
    -v user_name="${COCKROACH_USER}" \
    -v ON_ERROR_STOP=1 <<'SQL'
CREATE DATABASE IF NOT EXISTS :"db_name";
CREATE USER IF NOT EXISTS :"user_name";
GRANT ALL ON DATABASE :"db_name" TO :"user_name";
GRANT admin TO :"user_name";
SQL

psql \
    "${target_root_db_url}" \
    -v user_name="${COCKROACH_USER}" \
    -v ON_ERROR_STOP=1 <<'SQL'
GRANT ALL ON SCHEMA public TO :"user_name";
SQL

echo "Waiting for CockroachDB user '${COCKROACH_USER}' to connect to '${COCKROACH_DATABASE}'"
until psql "${target_user_db_url}" -v ON_ERROR_STOP=1 -c "SELECT 1" >/dev/null 2>&1; do
    sleep 1
done
