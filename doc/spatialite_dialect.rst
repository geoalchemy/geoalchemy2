.. _spatialite_dialect:

SpatiaLite Tutorial
===================

GeoAlchemy 2's main target is PostGIS. But GeoAlchemy 2 also supports SpatiaLite, the spatial
extension to SQLite. This tutorial describes how to use GeoAlchemy 2 with SpatiaLite. It's based on
the :ref:`orm_tutorial`, which you may want to read first.

.. _spatialite_connect:

Connect to the DB
-----------------

Just like when using PostGIS connecting to a SpatiaLite database requires an ``Engine``. An engine
for the SpatiaLite dialect can be created in two ways. Using the plugin provided by
``GeoAlchemy2`` (see :ref:`plugin` for more details)::

    >>> from sqlalchemy import create_engine
    >>> engine = create_engine(
    ...     "sqlite:///gis.db",
    ...     echo=True,
    ...     plugins=["geoalchemy2"]
    ... )

Or by attaching the listeners manually::

    >>> from geoalchemy2 import load_spatialite
    >>> from sqlalchemy import create_engine
    >>> from sqlalchemy.event import listen
    >>>
    >>> engine = create_engine("sqlite:///gis.db", echo=True)
    >>> listen(engine, "connect", load_spatialite)

The call to ``create_engine`` creates an engine bound to the database file ``gis.db``. After that
a ``connect`` listener is registered on the engine. The listener is responsible for loading the
SpatiaLite extension, which is a necessary operation for using SpatiaLite through SQL. The path to
the ``mod_spatialite`` file should be stored in the ``SPATIALITE_LIBRARY_PATH`` environment
variable before using the ``load_spatialite`` function.

At this point you can test that you are able to connect to the database::

     >> conn = engine.connect()

Note that this call will internally call the ``load_spatialite`` function, which can take some time
to execute on a new database because it actually calls the ``InitSpatialMetaData`` function from
SpatiaLite (it is possible to reduce this time by loading only the required SRIDs, see
:func:`geoalchemy2.admin.dialects.sqlite.load_spatialite`).
Then you can also check that the ``gis.db`` SQLite database file was created on the file system.

Note that when ``InitSpatialMetaData`` is executed again it will report an error::

    InitSpatiaMetaData() error:"table spatial_ref_sys already exists"

You can safely ignore that error.

Before going further we can close the current connection::

    >>> conn.close()

Declare a Mapping
-----------------

Now that we have a working connection we can go ahead and create a mapping between
a Python class and a database table::

    >>> from sqlalchemy.orm import declarative_base
    >>> from sqlalchemy import Column, Integer, String
    >>> from geoalchemy2 import Geometry
    >>>
    >>> Base = declarative_base()
    >>>
    >>> class Lake(Base):
    ...     __tablename__ = "lake"
    ...     id = Column(Integer, primary_key=True)
    ...     name = Column(String)
    ...     geom = Column(Geometry(geometry_type="POLYGON"))

From the user point of view this works in the same way as with PostGIS. The difference is that
internally the ``RecoverGeometryColumn`` and ``DiscardGeometryColumn`` management functions will be
used for the creation and removal of the geometry column.

Function mapping
----------------

Several functions have different names in SpatiaLite than in PostGIS. The GeoAlchemy 2 package is
based on the PostGIS syntax but it is possible to automatically translate the queries into
SpatiaLite ones. For example, the function ``ST_GeomFromEWKT`` is automatically translated into
``GeomFromEWKT``. Unfortunately, only a few functions are automatically mapped (mainly the ones
internally used by GeoAlchemy 2). Nevertheless, it is possible to define new mappings in order to
translate the queries automatically. Here is an example to register a mapping for the ``ST_Buffer``
function::

    >>> geoalchemy2.functions.register_sqlite_mapping(
    ...     {"ST_Buffer": "Buffer"}
    ... )

After this command, all ``ST_Buffer`` calls in the queries will be translated to ``Buffer`` calls
when the query is executed on a SQLite DB.

A more complex example is provided for when the PostGIS function should be mapped depending on
the given parameters. For example, the ``ST_Buffer`` function can actually be translate into either
the ``Buffer`` function or the ``SingleSidedBuffer`` function (only when ``side=right`` or ``side=left``
is passed). See the :ref:`sphx_glr_gallery_test_specific_compilation.py` example in the gallery.

GeoPackage format
-----------------

Starting from the version ``4.2`` of Spatialite, it is possible to use GeoPackage files as DB
containers. GeoAlchemy 2 is able to handle most of the GeoPackage features automatically if the
GeoPackage dialect is used (i.e. the DB URL starts with ``gpkg:///``) and the SpatiaLite extension
is loaded. Usually, this extension should be loaded using the the ``GeoAlchemy2`` plugin (see :ref:`connect <spatialite_connect>` section) or by attaching the ``load_spatialite_gpkg`` listener to the engine::

    >>> from geoalchemy2 import load_spatialite_gpkg
    >>> from sqlalchemy import create_engine
    >>> from sqlalchemy.event import listen
    >>>
    >>> engine = create_engine("gpkg:///gis.gpkg", echo=True)
    >>> listen(engine, "connect", load_spatialite_gpkg)

When using the ``load_spatialite_gpkg`` listener on a DB recognized as a GeoPackage, specific
processes are activated:

* the base tables are created if they are missing,
* the ``Amphibious`` mode is enabled using the ``EnableGpkgAmphibiousMode`` function,
* the ``VirtualGPKG`` wrapper is activated using the ``AutoGpkgStart`` function.

After that it should be possible to use a GeoPackage the same way as a standard SpatiaLite
database. GeoAlchemy 2 should be able to handle the following features in a transparent way for the
user:

* create/drop spatial tables,
* automatically create/drop spatial indexes if required,
* reflect spatial tables,
* use spatial functions on inserted geometries.

.. Note::

    If you want to use the ``ST_Transform`` function you should call the
    :func:`geoalchemy2.admin.dialects.geopackage.create_spatial_ref_sys_view` first.

Further Reference
-----------------

* GeoAlchemy 2 ORM Tutotial: :ref:`orm_tutorial`
* GeoAlchemy 2 Spatial Functions Reference: :ref:`spatial_functions`
* GeoAlchemy 2 Spatial Operators Reference: :ref:`spatial_operators`
* GeoAlchemy 2 Elements Reference: :ref:`elements`
* `SpatiaLite 4.3.0 SQL functions reference list <http://www.gaia-gis.it/gaia-sins/spatialite-sql-4.3.0.html>`_
