=====
Tests
=====

Install system dependencies
===========================

Install PostgreSQL and PostGIS::

    $ apt-get install postgresql-9.1 postgresql-9.1-postgis postgis

Install the Python and PostgreSQL development packages::

    $ apt-get install python2.7-dev libpq-dev

Install the Python dependencies::

    $ pip install -r requirements.txt

Set up the database
===================

Create the ``gis`` role::

    $ sudo -u postgres psql -c "CREATE ROLE gis PASSWORD 'gis' SUPERUSER CREATEDB CREATEROLE INHERIT LOGIN;"

Create the ``gis`` database::

    $ sudo -u postgres createdb -E UTF-8 gis
    $ sudo -u postgres psql -d gis -c 'CREATE SCHEMA gis;'
    $ sudo -u postgres psql -d gis -c 'GRANT CREATE ON DATABASE gis TO "gis";'
    $ sudo -u postgres psql -d gis -c 'GRANT USAGE,CREATE ON SCHEMA gis TO "gis";'

Enable PostGIS for the ``gis`` database.

For PostGIS 1.5::

    $ sudo -u postgres psql -d gis -f /usr/share/postgresql/9.1/contrib/postgis-1.5/postgis.sql
    $ sudo -u postgres psql -d gis -f /usr/share/postgresql/9.1/contrib/postgis-1.5/spatial_ref_sys.sql

For PostGIS 2::

    $sudo -u postgres psql -d gis -U postgres -c "CREATE EXTENSION postgis;"

Run Tests
=========

To run the tests::

    $ py.test
