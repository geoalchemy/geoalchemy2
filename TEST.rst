=====
Tests
=====

Install system dependencies
===========================

Install PostgreSQL and PostGIS::

    $ sudo apt-get install postgresql postgis

Install the Python and PostgreSQL development packages::

    $ sudo apt-get install python2.7-dev libpq-dev libgeos-dev

Install SpatiaLite::

    $ sudo apt-get install libsqlite3-mod-spatialite

Install the Python dependencies::

    $ pip install -r requirements.txt
    $ pip install psycopg2

Set up the PostGIS database
===========================

Create the ``gis`` role::

    $ sudo -u postgres psql -c "CREATE ROLE gis PASSWORD 'gis' SUPERUSER CREATEDB CREATEROLE INHERIT LOGIN;"

Create the ``gis`` database::

    $ sudo -u postgres createdb -E UTF-8 gis
    $ sudo -u postgres psql -d gis -c 'CREATE SCHEMA gis;'
    $ sudo -u postgres psql -c 'GRANT CREATE ON DATABASE gis TO "gis";'
    $ sudo -u postgres psql -d gis -c 'GRANT USAGE,CREATE ON SCHEMA gis TO "gis";'

Enable PostGIS for the ``gis`` database::

    $ sudo -u postgres psql -d gis -U postgres -c "CREATE EXTENSION postgis;"

Set the path to the SpatiaLite module
=====================================

By default the SpatiaLite functional tests are not run. To run them the ``SPATIALITE_LIBRARY_PATH``
environment variable must be set.

For example, on Debian Sid, and relying on the official SpatiaLite Debian package, the path to
the SpatiaLite library is ``/usr/lib/x86_64-linux-gnu/mod_spatialite.so``, so you would use this::

    $ export SPATIALITE_LIBRARY_PATH="/usr/lib/x86_64-linux-gnu/mod_spatialite.so"

Run Tests
=========

To run the tests::

    $ py.test
