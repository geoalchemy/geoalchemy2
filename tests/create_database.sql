-- GeoAlchemy2 PostGIS Tests: Set up the database
-- -----------------------------------------------------------------------------
-- This PSQL script creates a database named "gis" and adds PostGIS support.
-- Use this to run the GeoAlchemy2 unit tests for Python.
-- -----------------------------------------------------------------------------
-- NOTE: Log-in as a DB Superuser, e.g. "postgres".
-- -----------------------------------------------------------------------------

-- Create the "gis" role:
CREATE ROLE gis PASSWORD 'gis' SUPERUSER CREATEDB CREATEROLE INHERIT LOGIN;

-- Create the "gis" database:
CREATE DATABASE gis ENCODING 'UTF8';

-- Connect to "gis" database for all other objects.
\connect gis

CREATE SCHEMA gis;

-- Grant permissions:
GRANT CREATE ON DATABASE gis TO "gis";
GRANT USAGE, CREATE ON SCHEMA gis TO "gis";

-- Enable PostGIS for the "gis" database.
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- For PostGIS 1.5: Use the PSQL scripts.
--
-- OFF \i /usr/share/postgresql/9.x/contrib/postgis-1.5/postgis.sql
-- OFF \i /usr/share/postgresql/9.x/contrib/postgis-1.5/spatial_ref_sys.sql
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- For PostGIS 2: Create the extension here.
-- -----------------------------------------------------------------------------
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_topology;

