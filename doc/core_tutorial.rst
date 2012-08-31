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
``lake`` table in the database.

::

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
 
 Any ``Table`` object is added to a ``MetaData`` object, which is catalog of
 ``Table`` objects (and other related objects).

Create the Table in the Database
--------------------------------

With our ``Table`` being defined we're ready (to have SQLAlchemy)
create it in the database::

    >>> lake_table.create(engine)

Calling ``create_all()`` on ``metadata`` would have worked equally well::

    >>> metadata.create_all(engine)

In that case every ``Table`` that's referenced to by ``metadata`` would be
created in the database. The ``metadata`` object includes one ``Table`` here,
our now well-known ``lake_table`` object.

Add New Objects
---------------

To persist our ``Lake`` object, we ``add()`` it to the ``Session``::

    >>> session.add(lake)

At this point the ``lake`` object has been added to the ``Session``, but no SQL
has been issued to the database. The object is in a *pending* state. To persist
the object a *flush* or *commit* operation must occur (commit implies flush)::

    >>> session.commit()

We can now query the database for ``Majeur``::

    >>> our_lake = session.query(Lake).filter_by(name='Majeur').first()
    >>> our_lake.name
    u'Majeur'
    >>> our_lake.geom
    <WKBElement at 0x9af594c; '0103000000010000000500000000000000000000000000000000000000000000000000f03f0000000000000000000000000000f03f000000000000f03f0000000000000000000000000000f03f00000000000000000000000000000000'>
    >>> our_lake.id
    1

``our_lake.geom`` is a ``WKBElement``, which a type provided by GeoAlchemy.
``WKBElement`` wraps a WKB value returned by the database.

Let's add more lakes::

    >>> session.add_all([
    ...     Lake(name='Garde', geom='POLYGON((1 0,3 0,3 2,1 2,1 0))'),
    ...     Lake(name='Orta', geom='POLYGON((3 0,6 0,6 3,3 3,3 0))')
    ... ])
    >>> session.commit()

Query
-----

A ``Query`` object is created using the ``query()`` function on ``Session``.
For example here's a ``Query`` that loads ``Lake`` instances ordered by
their names::

    >>> query = session.query(Lake).order_by(Lake.name)

Any ``Query`` is iterable::

    >>> for lake in query:
    ...     print lake.name
    ...
    Garde
    Majeur
    Orta

Another way to execute the query and get a list of ``Lake`` objects involves
calling ``all()`` on the ``Query``::

    >>> lakes = session.query(Lake).order_by(Lake.name).all()

The SQLAlchemy ORM Tutorial's `Querying section
<http://docs.sqlalchemy.org/en/latest/orm/tutorial.html#querying>`_ provides
more examples of queries.

Spatial Query
-------------

As spatial database users executing spatial queries is of a great interest to
us. There comes GeoAlchemy!

Spatial relationship
~~~~~~~~~~~~~~~~~~~~

Using spatial filters in SQL SELECT queries is very common. Such queries are
performed by using spatial relationship functions, or operators, in the
``WHERE`` clause of the SQL query.

For example, to find the ``Lake`` s that contain the point ``POINT(4 1)``,
we can use this ``Query``::

    >>> from sqlalchemy import func
    >>> query = session.query(Lake).filter(
    ...             func.ST_Contains(Lake.geom, 'POINT(4 1)'))
    ...
    >>> for lake in query:
    ...     print lake.name
    ...
    Orta

GeoAlchemy allows rewriting this ``Query`` more concisely::

    >>> from sqlalchemy import func
    >>> query = session.query(Lake).filter(Lake.geom.ST_Contains('POINT(4 1)'))
    >>> for lake in query:
    ...     print lake.name
    ...
    Orta

Here the ``ST_Contains`` function is applied to the ``Lake.geom`` column
property. In that case the column property is actually passed to the function,
as its first argument.

Here's another spatial filtering query, based on ``ST_Intersects``::

    >>> query = session.query(Lake).filter(
    ...             Lake.geom.ST_Intersects('LINESTRING(2 1,4 1)'))
    ...
    >>> for lake in query:
    ...     print lake.name
    ...
    Garde
    Orta

We can also apply relationship functions to ``WKBElement``. For example::

    >>> lake = session.query(Lake).filter_by(name='Garde').one()
    >>> print session.scalar(lake.geom.ST_Intersects('LINESTRING(2 1,4 1)'))
    True

``session.scalar`` allows executing a clause and returning a scalar
value (a boolean value in this case).

The GeoAlchemy functions all start with ``ST_``. Operators are also called as
functions, but the function names don't include the ``ST_`` prefix. As an
example let's use PostGIS' ``&&`` operator, which allows testing
whether the bounding boxes of geometries intersect. GeoAlchemy provides
the ``intersects`` function for that::

    >>> query = session.queryt
    >>> query = session.query(Lake).filter(
    ...             Lake.geom.intersects('LINESTRING(2 1,4 1)'))
    ...
    >>> for lake in query:
    ...     print lake.name
    ...
    Garde
    Orta

Processing and Measurement
~~~~~~~~~~~~~~~~~~~~~~~~~~

Here's a ``Query`` that calculates the areas of buffers for our lakes::

    >>> from sqlalchemy import func
    >>> query = session.query(Lake.name,
    ...                       func.ST_Area(func.ST_Buffer(Lake.geom, 2)) \
    ...                           .label('bufferarea'))
    >>> for row in query:
    ...     print '%s: %f' % (row.name, row.bufferarea)
    ...
    Majeur: 21.485781
    Garde: 32.485781
    Orta: 45.485781

This ``Query`` applies the PostGIS ``ST_Buffer`` function to the geometry
column of every row of the ``lake`` table. The return value is a list of rows,
where each row is actually a tuple of two values: the lake name, and the area
of a buffer of the lake. Each tuple is actually an SQLAlchemy ``KeyedTuple``
object, which provides property type accessors.

Again, the ``Query`` can written more concisely::

    >>> query = session.query(Lake.name,
    ...                       Lake.geom.ST_Buffer(2).ST_Area().label('bufferarea'))
    >>> for row in query:
    ...     print '%s: %f' % (row.name, row.bufferarea)
    ...
    Majeur: 21.485781
    Garde: 32.485781
    Orta: 45.485781

Obviously, processing and measurement functions can alo be used in ``WHERE``
clauses. For example::

    >>> lake = session.query(Lake).filter(
    ...             Lake.geom.ST_Buffer(2).ST_Area() > 33).one()
    ...
    >>> print lake.name
    Orta

And, like any other functions supported by GeoAlchemy, processing and
measurement functions can be applied to ``WKBElement``. For example::

    >>> lake = session.query(Lake).filter_by(name='Majeur').one()
    >>> bufferarea = session.scalar(lake.geom.ST_Buffer(2).ST_Area())
    >>> print '%s: %f' % (lake.name, bufferarea)
    Majeur: 21.485781
