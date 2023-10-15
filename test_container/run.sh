#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

docker run --rm -it --mount type=bind,source="${ROOT}",target=/geoalchemy2,ro geoalchemy2
