#!/usr/bin/env bash
set -euo pipefail

echo "Waiting for MSSQL server at ${MSSQL_HOST} to start"
until /opt/mssql-tools18/bin/sqlcmd \
    -S "${MSSQL_HOST}" \
    -U sa \
    -P "${MSSQL_SA_PASSWORD}" \
    -C \
    -b \
    -Q "SELECT 1" >/dev/null 2>&1; do
    sleep 1
done

echo "Create the '${MSSQL_TEST_DB}' database"
/opt/mssql-tools18/bin/sqlcmd \
    -S "${MSSQL_HOST}" \
    -U sa \
    -P "${MSSQL_SA_PASSWORD}" \
    -C \
    -b \
    -Q "IF DB_ID(N'${MSSQL_TEST_DB}') IS NULL CREATE DATABASE [${MSSQL_TEST_DB}];"

echo "Waiting for database '${MSSQL_TEST_DB}' to become ONLINE"
until [ "$(
    /opt/mssql-tools18/bin/sqlcmd \
        -S "${MSSQL_HOST}" \
        -U sa \
        -P "${MSSQL_SA_PASSWORD}" \
        -C \
        -b \
        -h -1 \
        -W \
        -Q "SET NOCOUNT ON; SELECT state_desc FROM sys.databases WHERE name = N'${MSSQL_TEST_DB}';" \
        | tr -d '\r'
)" = "ONLINE" ]; do
    sleep 1
done

echo "Create or update the '${MSSQL_TEST_LOGIN}' login"
/opt/mssql-tools18/bin/sqlcmd \
    -S "${MSSQL_HOST}" \
    -U sa \
    -P "${MSSQL_SA_PASSWORD}" \
    -C \
    -b \
    -Q "IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'${MSSQL_TEST_LOGIN}') ALTER LOGIN [${MSSQL_TEST_LOGIN}] WITH PASSWORD = N'${MSSQL_TEST_PASSWORD}', DEFAULT_DATABASE = [${MSSQL_TEST_DB}], CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF; ELSE CREATE LOGIN [${MSSQL_TEST_LOGIN}] WITH PASSWORD = N'${MSSQL_TEST_PASSWORD}', DEFAULT_DATABASE = [${MSSQL_TEST_DB}], CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF;"

/opt/mssql-tools18/bin/sqlcmd \
    -S "${MSSQL_HOST}" \
    -U sa \
    -P "${MSSQL_SA_PASSWORD}" \
    -C \
    -b \
    -d "${MSSQL_TEST_DB}" \
    -Q "IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'${MSSQL_TEST_LOGIN}') CREATE USER [${MSSQL_TEST_LOGIN}] FOR LOGIN [${MSSQL_TEST_LOGIN}]; ELSE ALTER USER [${MSSQL_TEST_LOGIN}] WITH LOGIN = [${MSSQL_TEST_LOGIN}];"

/opt/mssql-tools18/bin/sqlcmd \
    -S "${MSSQL_HOST}" \
    -U sa \
    -P "${MSSQL_SA_PASSWORD}" \
    -C \
    -b \
    -d "${MSSQL_TEST_DB}" \
    -Q "GRANT CONNECT TO [${MSSQL_TEST_LOGIN}];"

/opt/mssql-tools18/bin/sqlcmd \
    -S "${MSSQL_HOST}" \
    -U sa \
    -P "${MSSQL_SA_PASSWORD}" \
    -C \
    -b \
    -d "${MSSQL_TEST_DB}" \
    -Q "IF IS_ROLEMEMBER('db_owner', '${MSSQL_TEST_LOGIN}') <> 1 ALTER ROLE db_owner ADD MEMBER [${MSSQL_TEST_LOGIN}];"

echo "Waiting for the '${MSSQL_TEST_LOGIN}' login to connect to '${MSSQL_TEST_DB}'"
until /opt/mssql-tools18/bin/sqlcmd \
    -S "${MSSQL_HOST}" \
    -U "${MSSQL_TEST_LOGIN}" \
    -P "${MSSQL_TEST_PASSWORD}" \
    -C \
    -b \
    -d "${MSSQL_TEST_DB}" \
    -Q "SELECT 1" >/dev/null 2>&1; do
    sleep 1
done
