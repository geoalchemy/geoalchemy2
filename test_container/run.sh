#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

mkdir -p "${SCRIPT_DIR}/output"

cleanup() {
    docker compose down >/dev/null 2>&1 || true
}

trap cleanup EXIT

cd "${SCRIPT_DIR}"
docker compose up -d postgres mysql mariadb mssql cockroachdb
docker compose run --rm runner "$@"
