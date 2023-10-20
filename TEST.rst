=====
Tests
=====

Test Container
==============

Instead of installing the necessary requirements onto your host machine, the `test_container/` directory contains
instructions for building a docker image with all the necessary requirements for running the tests across all
supported python versions. When you are finished the container and associated data can be removed.

Install and run the container::

    $ ./test_container/build.sh
    $ ./test_container/run.sh

Run the tests inside the container::

    # tox --workdir /output -vv

Remove the container and associated data::

    $ sudo rm -rf test_container/output
    $ docker image rm geoalchemy2
    $ docker system prune


Host System
===========

If you have a Linux system that you want to run the tests on instead of the test container, follow these steps:


Install system dependencies
---------------------------

(instructions for Ubuntu 22.04)

Install PostgreSQL and PostGIS::

    $ sudo apt-get install postgresql postgresql-14-postgis-3 postgresql-14-postgis-3-scripts

Install the Python and PostgreSQL development packages::

    $ sudo apt-get install python3-dev libpq-dev libgeos-dev

Install SpatiaLite::

    $ sudo apt-get install libsqlite3-mod-spatialite

Install MySQL::

    $ sudo apt-get install mysql-client mysql-server default-libmysqlclient-dev

Install the Python dependencies::

    $ pip install -r requirements.txt
    $ pip install psycopg2

Or you can use the Conda environment provided in the `GeoAlchemy2_dev.yml` file.

Set up the PostGIS database
---------------------------

Create the ``gis`` role::

    $ sudo -u postgres psql -c "CREATE ROLE gis PASSWORD 'gis' SUPERUSER CREATEDB CREATEROLE INHERIT LOGIN;"

Create the ``gis`` database::

    $ sudo -u postgres createdb -E UTF-8 gis
    $ sudo -u postgres psql -d gis -c 'CREATE SCHEMA gis;'
    $ sudo -u postgres psql -c 'GRANT CREATE ON DATABASE gis TO "gis";'
    $ sudo -u postgres psql -d gis -c 'GRANT USAGE,CREATE ON SCHEMA gis TO "gis";'

Enable PostGIS for the ``gis`` database::

    $ sudo -u postgres psql -d gis -c "CREATE EXTENSION postgis;"

With PostGIS 3 enable PostGIS Raster as well::

    $ sudo -u postgres psql -d gis -c "CREATE EXTENSION postgis_raster;"

Set the path to the SpatiaLite module
-------------------------------------

By default the SpatiaLite functional tests are not run. To run them the ``SPATIALITE_LIBRARY_PATH``
environment variable must be set.

For example, on Debian Sid, and relying on the official SpatiaLite Debian package, the path to
the SpatiaLite library is ``/usr/lib/x86_64-linux-gnu/mod_spatialite.so``, so you would use this::

    $ export SPATIALITE_LIBRARY_PATH="/usr/lib/x86_64-linux-gnu/mod_spatialite.so"

Set up the MySQL database
-------------------------

Create the ``gis`` role::

    $ sudo mysql -e "CREATE USER 'gis'@'%' IDENTIFIED BY 'gis';"
    $ sudo mysql -e "GRANT ALL PRIVILEGES ON *.* TO 'gis'@'%' WITH GRANT OPTION;"

Create the ``gis`` database::

    $ mysql -u gis --password=gis -e "CREATE DATABASE gis;"

Run Tests
---------

To run the tests::

    $ py.test
