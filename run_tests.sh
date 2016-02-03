#! /bin/bash
# -----------------------------------------------------------------------------
# GeoAlchemy2: Run Unit Tests
# -----------------------------------------------------------------------------
# This SHELL script creates a database named "gis" and adds PostGIS support.
# -----------------------------------------------------------------------------

#  Set current PostGIS version when not provided.
# -----------------------------------------------------------------------------
if [ "$POSTGIS_VERSION" == "" ]; then
    export POSTGIS_VERSION=2.1
fi

# Set up the test environment in PosgreSQL.
# -----------------------------------------------------------------------------
echo "Setting up GIS test database ..."
source ./tests/setup_tests.sh

# Use pytest to run unit tests. 
# -----------------------------------------------------------------------------
py.test

