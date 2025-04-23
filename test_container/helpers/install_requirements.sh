#!/usr/bin/env bash
set -e

# based on geoalchemy2/TEST.rst
packages=(
    # for managing virtual environments
    git
    pypy3
    pypy3-dev
    pypy3-venv
    python3-pip
    python3.10
    python3.10-dev
    python3.10-venv
    python3.11
    python3.11-dev
    python3.11-venv
    python3.12
    python3.12-dev
    python3.12-venv
    python3.13
    python3.13-dev
    python3.13-venv
    tox

    # PostgreSQL and PostGIS
    postgresql-16
    postgresql-16-postgis-3
    postgresql-16-postgis-3-scripts
    libpq-dev
    libgeos-dev

    # SpatiaLite
    libsqlite3-mod-spatialite

    # MySQL
    mysql-client
    mysql-server

    # mysqlclient requirements
    # https://github.com/PyMySQL/mysqlclient#linux
    default-libmysqlclient-dev
    build-essential
    pkg-config

    # rasterio requirements with pypy
    libgdal-dev
)

export DEBIAN_FRONTEND=noninteractive

apt-get update -y
apt-get install --no-install-recommends -y software-properties-common gnupg2 wget curl ca-certificates
add-apt-repository -y ppa:deadsnakes/ppa
add-apt-repository -y ppa:pypy/ppa
mkdir -p /usr/share/postgresql-common/pgdg/
curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc
sh -c 'echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
apt-get update -y

apt-get install --no-install-recommends -y "${packages[@]}"

# clear the package list cache (populated with apt-get update)
apt-get clean
rm -rf /var/lib/apt/lists/*
