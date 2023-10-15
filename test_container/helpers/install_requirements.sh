#!/usr/bin/env bash
set -e

# based on geoalchemy2/TEST.rst
packages=(
    # for creating a virtual environment
    python3-dev
    python3-venv

    # PostgreSQL and PostGIS
    postgresql
    postgresql-14-postgis-3
    postgresql-14-postgis-3-scripts
    python3-dev
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
    python3-dev
    default-libmysqlclient-dev
    build-essential
    pkg-config
)

export DEBIAN_FRONTEND=noninteractive
apt-get update -y

apt-get install --no-install-recommends -y "${packages[@]}"

# clear the package list cache (populated with apt-get update)
rm -rf /var/lib/apt/lists/*
