.. _migrate:

Migrate to GeoAlchemy 2
=======================

This section describes how to migrate an application from the first
series of GeoAlchemy to GeoAlchemy 2.

Defining Geometry Columns
-------------------------

The first series has specific types like ``Point``, ``LineString`` and
``Polygon``. These are gone, the :class:`geoalchemy2.types.Geometry` type
should be used instead, and a ``geometry_type`` can be passed to it.

So, for example, a ``polygon`` column that used to be defined like this::

    geom = Column(Polygon)

is now defined like this::

    geom = Column(Geometry('POLYGON'))

This change is related to GeoAlchemy 2 supporting the
`geoalchemy2.types.Geography` type.

Calling Spatial Functions
-------------------------

The first series has its own namespace/object for calling spatial
functions, namely ``geoalchemy.functions``. With GeoAlchemy 2,
SQLAlchemy's ``func`` object should be used.

For example, the expression

::

    functions.buffer(functions.centroid(box), 10, 2)

would be rewritten to this with GeoAlchemy 2::

    func.ST_Buffer(func.ST_Centroid(box), 10, 2)

Also, as the previous example hinted it, the names of spatial functions are now
all prefixed with ``ST_``. (This is to be consistent with PostGIS and the
``SQL-MM`` standard.) The ``ST_`` prefix should be used even when applying
spatial functions to columns, :class:`geoalchemy2.elements.WKTElement`,
or :class:`geoalchemy2.elements.WKTElement` objects::

    Lake.geom.ST_Buffer(10, 2)
    lake_table.c.geom.ST_Buffer(10, 2)
    lake.geom.ST_Buffer(10, 2)

WKB and WKT Elements
--------------------

The first series has classes like ``PersistentSpatialElement``,
``PGPersistentSpatialElement``, ``WKTSpatialElement``.

They're all gone, and replaced by two classes only:
:class:`geoalchemy2.elements.WKTElement` and
:class:`geoalchemy2.elements.WKBElement`.

:class:`geoalchemy2.elements.WKTElement` is to be used in expressions
where a geometry with a specific SRID should be specified. For example::

    Lake.geom.ST_Touches(WKTElement('POINT(1 1)', srid=4326))

If no SRID need be specified, a string can used directly::

    Lake.geom.ST_Touches('POINT(1 1)')

* :class:`geoalchemy2.elements.WKTElement` literally replaces the
  first series' ``WKTSpatialElement``.
* :class:`geoalchemy2.elements.WKBElement` is the type into which GeoAlchemy
  2 converts geometry values read from the database.

  For example, the ``geom``
  attributes of ``Lake`` objects loaded from the database would be references
  to :class:`geoalchemy2.elements.WKBElement` objects. This class replaces
  the first series' ``PersistentSpatialElement`` classes.
