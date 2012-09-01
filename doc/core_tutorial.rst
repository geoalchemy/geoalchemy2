.. _core_tutorial:

Core Tutorial
=============

This tutorial shows how to use the SQLAlchemy Expression Language (a.k.a.
SQLAlchemy Core) with GeoAlchemy. As defined by the SQLAlchemy documentation
itself, in contrast to the ORM's domain-centric mode of usage, the SQL
Expression Language provides a schema-centric usage paradigm.

Connect to the DB
-----------------

For this tutorial we will use a PostGIS 2 database. To connect we use
SQLAlchemy's ``create_engine()`` function::

    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('postgresql://gis:gis@localhost/gis', echo=True)

In this example the name of the database, the database user, and the database
password, is ``gis``.

The ``echo`` flag is a shortcut to setting up SQLAlchemy logging, which is
accomplished via Python's standard logging module. With it is enabled, we'll
see all the generated SQL produced.

The return value of ``create_engine`` is an ``Engine`` object, which
respresents the core interface to the database.

Define a Table
--------------

The very first object that we need to create is a ``Table``. Here
we create a ``lake_table`` object, which will correspond to the
``lake`` table in the database::

    >>> from sqlalchemy import Table, Column, Integer, String, MetaData
    >>> from geoalchemy2 import Polygon
    >>>
    >>> metadata = MetaData()
    >>> lake_table = Table('lake', metadata,
    ...     Column('id', Integer, primary_key=True),
    ...     Column('name', String),
    ...     Column('geom', Polygon)
    ... )

This table is composed of three columns, ``id``, ``name`` and ``geom``. The
``geom`` column is of type ``Polygon``, which is provided by GeoAlchemy.
 
Any ``Table`` object is added to a ``MetaData`` object, which is a catalog of
``Table`` objects (and other related objects).

Create the Table
----------------

With our ``Table`` being defined we're ready (to have SQLAlchemy)
create it in the database::

    >>> lake_table.create(engine)

Calling ``create_all()`` on ``metadata`` would have worked equally well::

    >>> metadata.create_all(engine)

In that case every ``Table`` that's referenced to by ``metadata`` would be
created in the database. The ``metadata`` object includes one ``Table`` here,
our now well-known ``lake_table`` object.

Insertions
----------

We want to insert records into the ``lake`` table. For that we need to create
an ``Insert`` object. SQLAlchemy provides multiple constructs for creating an
``Insert`` object, here's one::

    >>> ins = lake_table.insert()
    >>> str(ins)
    INSERT INTO lake (id, name, geom) VALUES (:id, :name, ST_GeomFromText(:geom))

The ``geom`` column being a ``Geometry`` column, the ``:geom`` bind value is
wrapped in a ``ST_GeomFromText`` call.

To limit the columns named in the ``INSERT`` query the ``values()`` method
can be used::

    >>> ins = lake_table.insert().values(name='Majeur',
    ...                                  geom='POLYGON((0 0,1 0,1 1,0 1,0 0))')
    ...
    >>> str(ins)
    INSERT INTO lake (name, geom) VALUES (:name, ST_GeomFromText(:geom))

.. tip::

    The string representation of the SQL expression does not include the
    data placed in ``values``. We got named bind parameters instead. To
    view the data we can get a compiled form of the expression, and ask
    for its ``params``::

        >>> ins.compile.params()
        {'geom': 'POLYGON((0 0,1 0,1 1,0 1,0 0))', 'name': 'Majeur'}

Up to now we've created an ``INSERT`` query but we haven't sent this query to
the database yet. Before being able to send it to the database we need
a database ``Connection``. We can get a ``Connection`` from the ``Engine``
object we created earlier::

    >>> conn = engine.connect()

We're now ready to execute our ``INSERT`` statement::

    >>> result = conn.execute(ins)

This is what the logging system should output::

    INSERT INTO lake (name, geom) VALUES (%(name)s, ST_GeomFromText(%(geom)s)) RETURNING lake.id
    {'geom': 'POLYGON((0 0,1 0,1 1,0 1,0 0))', 'name': 'Majeur'}
    COMMIT

The value returned by ``conn.execute()``, stored in ``result``, is
a ``sqlalchemy.engine.ResultProxy`` object. In the case of an ``INSERT`` we can
get the primary key value which was generated from our statement::

    >>> result.inserted_primary_key
    [1]

Instead of using ``values()`` to specify our ``INSERT`` data, we can send
the data to the ``execute()`` method on ``Connection``. So we could rewrite
things as follows::

    >>> conn.execute(lake_table.insert(),
    ...              name='Majeur', geom='POLYGON((0 0,1 0,1 1,0 1,0 0))')

Now let's use another form, allowing to insert multiple rows at once::

    >>> conn.execute(lake_table.insert(), [
    ...     {'name': 'Garde', 'geom': 'POLYGON((1 0,3 0,3 2,1 2,1 0))'},
    ...     {'name': 'Orta', 'geom': 'POLYGON((3 0,6 0,6 3,3 3,3 0))'}
    ...     ])
    ...

Selections
----------

Inserting involved creating an ``Insert`` object, so it'd come to no surprise
that Selecting involves creating a ``Select`` object.  The primary construct to
generate ``SELECT`` statements is SQLAlchemy`s ``select()`` function::

    >>> from sqlalchemy.sql import select
    >>> s = select([lake_table])
    >>> str(s)
    SELECT lake.id, lake.name, ST_AsBinary(lake.geom) AS geom_1 FROM lake

The ``geom`` column being a ``Geometry`` it is wrapped in ``ST_AsBinary`` call
when specified as a column in a ``SELECT`` statement.

We can now execute the statement and look at the results::

    >>> result = conn.execute(s)
    >>> for row in result:
    ...     print 'name:', row[lake_table.c.name], \
    ...           '; geom:', row[lake_table.c.geom].desc
    ...
    name: Majeur ; geom: 0103...
    name: Garde ; geom: 0103...
    name: Orta ; geom: 0103...

``row[lake_table.c.geom]`` is a :class:`geoalchemy2.types.WKBElement` instance.
In this example we just get an hexadecimal representation of the geometry's WKB
value using the ``desc`` property.

Spatial Query
-------------

As spatial database users executing spatial queries is of a great interest to
us. There comes GeoAlchemy!

Spatial relationship
~~~~~~~~~~~~~~~~~~~~

Using spatial filters in SQL SELECT queries is very common. Such queries are
performed by using spatial relationship functions, or operators, in the
``WHERE`` clause of the SQL query.

For example, to find lakes that contain the point ``POINT(4 1)``,
we can use this::


    >>> from sqlalchemy import func
    >>> s = select([lake_table],
                   func.ST_Contains(lake_table.c.geom, 'POINT(4 1)'))
    >>> str(s)
    SELECT lake.id, lake.name, ST_AsBinary(lake.geom) AS geom_1 FROM lake WHERE ST_Contains(lake.geom, :param_1)
    >>> result = conn.execute(s)
    >>> for row in result:
    ...     print 'name:', row[lake_table.c.name], \
    ...           '; geom:', row[lake_table.c.geom].desc
    ...
    name: Orta ; geom: 0103...

GeoAlchemy allows rewriting this more concisely::

    >>> s = select([lake_table], lake_table.c.geom.ST_Contains('POINT(4 1)'))
    >>> str(s)
    SELECT lake.id, lake.name, ST_AsBinary(lake.geom) AS geom_1 FROM lake WHERE ST_Contains(lake.geom, :param_1)

Here the ``ST_Contains`` function is applied to ``lake.c.geom``. In that case
the column is actually passed to the function, as its first argument.

Here's another spatial query, based on ``ST_Intersects``::

    >>> s = select([lake_table],
    ...            lake_table.c.geom.ST_Intersects('LINESTRING(2 1,4 1)'))
    >>> result = conn.execute(s)
    >>> for row in result:
    ...     print 'name:', row[lake_table.c.name], \
    ...           '; geom:', row[lake_table.c.geom].desc
    ...
    name: Garde ; geom: 0103...
    name: Orta ; geom: 0103...

The GeoAlchemy functions all start with ``ST_``. Operators are also called as
functions, but the function names don't include the ``ST_`` prefix. As an
example let's use PostGIS' ``&&`` operator, which allows testing
whether the bounding boxes of geometries intersect. GeoAlchemy provides
the ``intersects`` function for that::

    >>> s = select([lake_table],
    ...            lake_table.c.geom.intersects('LINESTRING(2 1,4 1)'))
    >>> result = conn.execute(s)
    >>> for row in result:
    ...     print 'name:', row[lake_table.c.name], \
    ...           '; geom:', row[lake_table.c.geom].desc
    ...
    name: Garde ; geom: 0103...
    name: Orta ; geom: 0103...

Processing and Measurement
~~~~~~~~~~~~~~~~~~~~~~~~~~

Here's a ``Select`` that calculates the areas of buffers for our lakes::

    >>> s = select([lake_table.c.name,
                    func.ST_Area(
                        lake_table.c.geom.ST_Buffer(2)).label('bufferarea')])
    >>> str(s)
    SELECT lake.name, ST_Area(ST_Buffer(lake.geom, %(param_1)s)) AS bufferarea FROM lake
    >>> result = conn.execute(s)
    >>> for row in result:
    ...     print '%s: %f' % (row['name'], row['bufferarea'])
    Majeur: 21.485781
    Garde: 32.485781
    Orta: 45.485781

Obviously, processing and measurement functions can alo be used in ``WHERE``
clauses. For example::

    >>> s = select([lake_table.c.name],
                   lake_table.c.geom.ST_Buffer(2).ST_Area() > 33)
    >>> str(s)
    SELECT lake.name FROM lake WHERE ST_Area(ST_Buffer(lake.geom, :param_1)) > :ST_Area_1
    >>> result = conn.execute(s)
    >>> for row in result:
    ...     print row['name']
    Orta

And, like any other functions supported by GeoAlchemy, processing and
measurement functions can be applied to ``WKBElement``. For example::

    >>> s = select([lake_table], lake_table.c.name == 'Majeur')
    >>> result = conn.execute(s)
    >>> lake = result.fetchone()
    >>> bufferarea = conn.scalar(lake[lake_table.c.geom].ST_Buffer(2).ST_Area())
    >>> print '%s: %f' % (lake['name'], bufferarea)
    Majeur: 21.485781
