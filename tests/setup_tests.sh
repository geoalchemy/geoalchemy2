#! /bin/bash
# -----------------------------------------------------------------------------
# GeoAlchemy2 Setup Tests: Set up the test environment 
# -----------------------------------------------------------------------------
# This SHELL script creates a database named "gis" and adds PostGIS support.
# The env variable POSTGIS_VERSION selects PostGIS 1.5 or 2.x+ versions.
# Use this to prepare for the GeoAlchemy2 unit tests for Python.
# -----------------------------------------------------------------------------
# NOTE: Log-in as a DB Superuser, e.g. "postgres".
# -----------------------------------------------------------------------------

echo "Creating GIS test database ..."
psql -U postgres -f ./tests/create_database.sql

# -----------------------------------------------------------------------------
# Enable PostGIS for the "gis" database.
# -----------------------------------------------------------------------------
echo "Installing PostGIS version $POSTGIS_VERSION ..."

if [ "$POSTGIS_VERSION" == "1.5" ]; then
    # -------------------------------------------------------------------------
    # For PostGIS 1.5: Use the PSQL scripts.
    # -------------------------------------------------------------------------
    psql -U postgres -f /usr/share/postgresql/9.1/contrib/postgis-1.5/postgis.sql;
    psql -U postgres -f /usr/share/postgresql/9.1/contrib/postgis-1.5/spatial_ref_sys.sql;
    echo "OK PostGIS version $POSTGIS_VERSION"
else
    # -------------------------------------------------------------------------
    # For PostGIS 2: Create the extension in PSQL.
    # -------------------------------------------------------------------------
    psql -U postgres -c "CREATE EXTENSION postgis;";
    echo "OK PostGIS version $POSTGIS_VERSION"
fi

