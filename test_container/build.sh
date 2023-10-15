#!/usr/bin/env bash
set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cp "${ROOT}/requirements.txt" "helpers/requirements.txt"

docker build -t geoalchemy2 .
