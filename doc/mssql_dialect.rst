.. _mssql_dialect:

MSSQL Tutorial
==============

GeoAlchemy 2 supports SQL Server's ``geometry`` and ``geography`` spatial types. Most
GeoAlchemy-facing APIs use the standard spatial coordinate order: ``X Y``. For geographic
coordinates this means ``longitude latitude``.

Point coordinate order
----------------------

SQL Server's native ``geography::Point`` constructor is unusual because it expects latitude before
longitude::

    geography::Point(latitude, longitude, srid)

GeoAlchemy-facing WKT values should still use standard ``X Y`` order::

    POINT(longitude latitude)

For MSSQL computed columns, GeoAlchemy also treats ``ST_POINT(x, y)`` as standard ``X Y``
input when rewriting it to SQL Server constructor syntax. For a ``Geography`` computed column this
means::

    from sqlalchemy import Column, Computed, Float, Integer, MetaData, Table
    from geoalchemy2 import Geography

    metadata = MetaData()

    places = Table(
        "places",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("longitude", Float, nullable=False),
        Column("latitude", Float, nullable=False),
        Column(
            "geog",
            Geography(geometry_type="POINT", srid=4326),
            Computed("ST_POINT(longitude, latitude)", persisted=True),
        ),
    )

is rendered as::

    geography::Point(latitude, longitude, srid)

This rewrite is specific to computed-column DDL generation.

Constructing points in INSERT or SELECT statements
--------------------------------------------------

GeoAlchemy does not currently rewrite arbitrary ``func.ST_Point(...)`` expressions in ``INSERT`` or
``SELECT`` statements for MSSQL. In those contexts, use one of the explicit MSSQL-compatible forms.

Use a WKT value or ``WKTElement`` when inserting values through a spatial column::

    from geoalchemy2 import WKTElement

    conn.execute(
        places.insert(),
        {
            "name": "Eiffel Tower",
            "geom": WKTElement("POINT(2.2945 48.8584)", srid=4326),
        },
    )

Use SQL Server's WKT constructor for SQL expressions::

    from sqlalchemy import bindparam, func, insert

    stmt = insert(places).values(
        name=bindparam("name"),
        geom=func.ST_GeogFromText(bindparam("wkt"), bindparam("srid")),
    )

In both cases, WKT uses standard ``longitude latitude`` order for geography values. If you choose to
write native SQL Server ``geography::Point`` expressions directly, use SQL Server's native
``latitude, longitude, srid`` argument order.
