#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

mkdir -p "${SCRIPT_DIR}/output"

SERVICES=(postgres mysql mariadb mssql cockroachdb)

is_healthy() {
    local service="$1"
    local container_id
    local health_status

    container_id=$(docker compose ps -q "${service}" 2>/dev/null || true)
    if [ -z "${container_id}" ]; then
        return 1
    fi

    health_status=$(docker inspect --format '{{ if .State.Health }}{{ .State.Health.Status }}{{ end }}' "${container_id}" 2>/dev/null || true)
    [ "${health_status}" = "healthy" ]
}

cd "${SCRIPT_DIR}"

missing_services=()
for service in "${SERVICES[@]}"; do
    if ! is_healthy "${service}"; then
        missing_services+=("${service}")
    fi
done

if [ "${#missing_services[@]}" -gt 0 ]; then
    echo "Starting test services: ${missing_services[*]}"
    "${SCRIPT_DIR}/start.sh"
fi

docker compose run --rm --no-deps runner "$@"
