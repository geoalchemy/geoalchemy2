Spatial Functions
=================

Spatial functions are called through SQLAlchemy's `func
<http://docs.sqlalchemy.org/en/latest/core/expression_api.html?highlight=expression.func#sqlalchemy.sql.expression.func>`_ object.

Example::

    >>> from sqlalchemy.sql import select, func
    >>> s = select([
            func.ST_GeometryType(
                func.ST_GeomFromText(
                    'LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'
                    )
                )
            ])
    >>>
    >>> str(s)
    'SELECT ST_GeometryType(ST_GeomFromText(:ST_GeomFromText_1)) AS "ST_GeometryType_1"'
    >>> str(s.compile().params)
    "{u'ST_GeomFromText_1': 'LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'}"
    >>>
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('postgresql://gis:gis@localhost/gis')
    >>> conn = engine.connect()
    >>> conn.execute(s).scalar()
    u'ST_LineString'

It is to be noted that GeoAlchemy is actually not involved in the above
example. SQLAlchemy only is used.

Here's another example that involves GeoAlchemy::

    >>> from sqlalchemy.sql import select, func
    >>> s = select([
            func.ST_Buffer(
                func.ST_GeomFromText(
                    'LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'
                    ),
                2
                )
            ])
    >>>
    >>> str(s)
    'SELECT ST_Buffer(ST_GeomFromText(:ST_GeomFromText_1), :ST_Buffer_2) AS "ST_Buffer_1"'
    >>> str(s.compile().params)
    "{u'ST_GeomFromText_1': 'LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)', u'ST_Buffer_2': 2}"
    >>>
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('postgresql://gis:gis@localhost/gis')
    >>> conn = engine.connect()
    >>> conn.execute(s).scalar()
    <BinaryGisElement at 0x86e1dec; u'POLYGON((...

What's important to note here is that the result resulting from the query is an
object of type ``BinaryGisElement``, which is defined by GeoAlchemy.

.. note::
    
    For ``ST_Buffer`` to return objects of a specific type
    (``BinaryGisElement``) GeoAlchemy relies on SQLALchemy's `Generic
    Functions
    <http://docs.sqlalchemy.org/ru/latest/core/expression_api.html?highlight=genericfunction#sqlalchemy.sql.functions.GenericFunction>`_.
    In GeoAlchemy ``ST_Buffer`` is a subclass of
    ``sqlalchemy.sql.functions.GenericFunction`` with a pre-configured return
    type (``geoalchemy2.Geometry``).
