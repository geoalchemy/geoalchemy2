#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

mkdir -p "${SCRIPT_DIR}/output"

docker run --rm -it \
    --mount type=bind,source="${ROOT}",target=/geoalchemy2_read_only,ro \
    --mount type=bind,source="${SCRIPT_DIR}/output",target=/output \
    geoalchemy2
