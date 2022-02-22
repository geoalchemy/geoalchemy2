.. _spatialite_tutorial:

SpatiaLite Tutorial
===================

GeoAlchemy 2's main target is PostGIS. But GeoAlchemy 2 also supports SpatiaLite, the spatial
extension to SQLite. This tutorial describes how to use GeoAlchemy 2 with SpatiaLite. It's based on
the :ref:`orm_tutorial`, which you may want to read first.

Connect to the DB
-----------------

Just like when using PostGIS connecting to a SpatiaLite database requires an ``Engine``. This is how
you create one for SpatiaLite::

    >>> from sqlalchemy import create_engine
    >>> from sqlalchemy.event import listen
    >>>
    >>> def load_spatialite(dbapi_conn, connection_record):
    ...     dbapi_conn.enable_load_extension(True)
    ...     dbapi_conn.load_extension('/usr/lib/x86_64-linux-gnu/mod_spatialite.so')
    ...
    >>>
    >>> engine = create_engine('sqlite:///gis.db', echo=True)
    >>> listen(engine, 'connect', load_spatialite)

The call to ``create_engine`` creates an engine bound to the database file ``gis.db``. After that
a ``connect`` listener is registered on the engine. The listener is responsible for loading the
SpatiaLite extension, which is a necessary operation for using SpatiaLite through SQL.

At this point you can test that you are able to connect to the database::

     >> conn = engine.connect()
     2018-05-30 17:12:02,675 INFO sqlalchemy.engine.base.Engine SELECT CAST('test plain returns' AS VARCHAR(60)) AS anon_1
     2018-05-30 17:12:02,676 INFO sqlalchemy.engine.base.Engine ()
     2018-05-30 17:12:02,676 INFO sqlalchemy.engine.base.Engine SELECT CAST('test unicode returns' AS VARCHAR(60)) AS anon_1
     2018-05-30 17:12:02,676 INFO sqlalchemy.engine.base.Engine ()

You can also check that the ``gis.db`` SQLite database file was created on the file system.

One additional step is required for using SpatiaLite: create the ``geometry_columns`` and
``spatial_ref_sys`` metadata tables. This is done by calling SpatiaLite's ``InitSpatialMetaData``
function::

    >>> from sqlalchemy.sql import select, func
    >>>
    >>> conn.execute(select([func.InitSpatialMetaData()]))

Note that this operation may take some time the first time it is executed for a database. When
``InitSpatialMetaData`` is executed again it will report an error::

    InitSpatiaMetaData() error:"table spatial_ref_sys already exists"

You can safely ignore that error.

Before going further we can close the current connection::

    >>> conn.close()

Declare a Mapping
-----------------

Now that we have a working connection we can go ahead and create a mapping between
a Python class and a database table.

::

    >>> from sqlalchemy.ext.declarative import declarative_base
    >>> from sqlalchemy import Column, Integer, String
    >>> from geoalchemy2 import Geometry
    >>>
    >>> Base = declarative_base()
    >>>
    >>> class Lake(Base):
    ...     __tablename__ = 'lake'
    ...     id = Column(Integer, primary_key=True)
    ...     name = Column(String)
    ...     geom = Column(Geometry(geometry_type='POLYGON', management=True))

This basically works in the way as with PostGIS. The difference is the ``management``
argument that must be set to ``True``.

Setting ``management`` to ``True`` indicates that the ``AddGeometryColumn`` and
``DiscardGeometryColumn`` management functions will be used for the creation and removal of the
geometry column. This is required with SpatiaLite.

Create the Table in the Database
--------------------------------

We can now create the ``lake`` table in the ``gis.db`` database::

    >>> Lake.__table__.create(engine)

If we wanted to drop the table we'd use::

    >>> Lake.__table__.drop(engine)

There's nothing specific to SpatiaLite here.

Create a Session
----------------

When using the SQLAlchemy ORM the ORM interacts with the database through a ``Session``.

    >>> from sqlalchemy.orm import sessionmaker
    >>> Session = sessionmaker(bind=engine)
    >>> session = Session()

The session is associated with our SpatiaLite ``Engine``. Again, there's nothing
specific to SpatiaLite here.

Add New Objects
---------------

We can now create and insert new ``Lake`` objects into the database, the same way we'd
do it using GeoAlchemy 2 with PostGIS.

::

    >>> lake = Lake(name='Majeur', geom='POLYGON((0 0,1 0,1 1,0 1,0 0))')
    >>> session.add(lake)
    >>> session.commit()

We can now query the database for ``Majeur``::

    >>> our_lake = session.query(Lake).filter_by(name='Majeur').first()
    >>> our_lake.name
    u'Majeur'
    >>> our_lake.geom
    <WKBElement at 0x9af594c; '0103000000010000000500000000000000000000000000000000000000000000000000f03f0000000000000000000000000000f03f000000000000f03f0000000000000000000000000000f03f00000000000000000000000000000000'>
    >>> our_lake.id
    1

Let's add more lakes::

    >>> session.add_all([
    ...     Lake(name='Garde', geom='POLYGON((1 0,3 0,3 2,1 2,1 0))'),
    ...     Lake(name='Orta', geom='POLYGON((3 0,6 0,6 3,3 3,3 0))')
    ... ])
    >>> session.commit()

Query
-----

Let's make a simple, non-spatial, query::

    >>> query = session.query(Lake).order_by(Lake.name)
    >>> for lake in query:
    ...     print(lake.name)
    ...
    Garde
    Majeur
    Orta

Now a spatial query::

    >>> from geolachemy2 import WKTElement
    >>> query = session.query(Lake).filter(
    ...             func.ST_Contains(Lake.geom, WKTElement('POINT(4 1)')))
    ...
    >>> for lake in query:
    ...     print(lake.name)
    ...
    Orta

Here's another spatial query, using ``ST_Intersects`` this time::

    >>> query = session.query(Lake).filter(
    ...             Lake.geom.ST_Intersects(WKTElement('LINESTRING(2 1,4 1)')))
    ...
    >>> for lake in query:
    ...     print(lake.name)
    ...
    Garde
    Orta

We can also apply relationship functions to :class:`geoalchemy2.elements.WKBElement`. For example::

    >>> lake = session.query(Lake).filter_by(name='Garde').one()
    >>> print(session.scalar(lake.geom.ST_Intersects(WKTElement('LINESTRING(2 1,4 1)'))))
    1

``session.scalar`` allows executing a clause and returning a scalar value (an integer value in this
case).

The value ``1`` indicates that the lake "Garde" does intersects the ``LINESTRING(2 1,4 1)``
geometry. See the SpatiaLite SQL functions reference list for more information.

Function mapping
----------------

Several functions have different names in SpatiaLite than in PostGIS. The GeoAlchemy 2 package is
based on the PostGIS syntax but it is possible to automatically translate the queries into
SpatiaLite ones. For example, the function `ST_GeomFromEWKT` is automatically translated into
`GeomFromEWKT`. Unfortunately, only a few functions are automatically mapped (the ones internally
used by GeoAlchemy 2). Nevertheless, it is possible to define new mappings in order to translate
the queries automatically. Here is an example to register a mapping for the `ST_Buffer` function::

    >>> geoalchemy2.functions.register_sqlite_mapping(
    ...     {'ST_Buffer': 'Buffer'}
    ... )

After this command, all `ST_Buffer` calls in the queries will be translated to `Buffer` calls when
the query is executed on a SQLite DB.

A more complex example is provided for when the `PostGIS` function should be mapped depending on
the given parameters. For example, the `ST_Buffer` function can actually be translate into either
the `Buffer` function or the `SingleSidedBuffer` function (only when `side=right` or `side=left` is
passed). See the :ref:`sphx_glr_gallery_test_specific_compilation.py` example in the gallery.

Further Reference
-----------------

* GeoAlchemy 2 ORM Tutotial: :ref:`orm_tutorial`
* GeoAlchemy 2 Spatial Functions Reference: :ref:`spatial_functions`
* GeoAlchemy 2 Spatial Operators Reference: :ref:`spatial_operators`
* GeoAlchemy 2 Elements Reference: :ref:`elements`
* `SpatiaLite 4.3.0 SQL functions reference list <http://www.gaia-gis.it/gaia-sins/spatialite-sql-4.3.0.html>`_
