#!/usr/bin/env bash
set -e

# based on geoalchemy2/TEST.rst
packages=(
    # for managing virtual environments
    tox
    git
    pypy3
    pypy3-dev
    pypy3-venv
    python3.7
    python3.7-dev
    python3.7-venv
    python3.8
    python3.8-dev
    python3.8-venv
    python3.9
    python3.9-dev
    python3.9-venv
    python3.10
    python3.10-dev
    python3.10-venv
    python3.11
    python3.11-dev
    python3.11-venv
    python3.12
    python3.12-dev
    python3.12-venv

    # PostgreSQL and PostGIS
    postgresql
    postgresql-14-postgis-3
    postgresql-14-postgis-3-scripts
    libpq-dev
    libgeos-dev

    # SpatiaLite
    libsqlite3-mod-spatialite

    # MySQL
    mysql-client
    mysql-server
    default-libmysqlclient-dev

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
apt-get install --no-install-recommends -y software-properties-common gnupg2
add-apt-repository ppa:deadsnakes/ppa
apt-get update -y

apt-get install --no-install-recommends -y "${packages[@]}"

# clear the package list cache (populated with apt-get update)
rm -rf /var/lib/apt/lists/*
