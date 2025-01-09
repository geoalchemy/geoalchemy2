.. _orm_tutorial:

ORM Tutorial
============

(This tutorial is greatly inspired by the `SQLAlchemy ORM Tutorial`_, which is
recommended reading, eventually.)

.. _SQLAlchemy ORM Tutorial:
    http://docs.sqlalchemy.org/en/latest/orm/tutorial.html

GeoAlchemy does not provide an Object Relational Mapper (ORM), but works well
with the SQLAlchemy ORM. This tutorial shows how to use the SQLAlchemy ORM with
spatial tables, using GeoAlchemy.

Connect to the DB
-----------------

For this tutorial we will use a PostGIS 2 database. To connect we use
SQLAlchemy's ``create_engine()`` function::

    >>> from sqlalchemy import create_engine
    >>> engine = create_engine(
    ...     'postgresql://gis:gis@localhost/gis',
    ...     echo=True,
    ...     plugins=["geoalchemy2"]
    ... )

In this example the name of the database, the database user, and the database
password, is ``gis``.

The ``echo`` flag is a shortcut to setting up SQLAlchemy logging, which is
accomplished via Python's standard logging module. With it is enabled, we'll
see all the generated SQL produced.

The ``plugins`` argument adds some event listeners to adapt the behavior of
``GeoAlchemy2`` to the dialect. This is not mandatory but if the plugin is not
loaded, then the listeners will have to be added to the engine manually (see an
example in :ref:`spatialite_dialect`).

The return value of ``create_engine`` is an ``Engine`` object, which
represents the core interface to the database.

Declare a Mapping
-----------------

When using the ORM, the configurational process starts by describing the
database tables we'll be dealing with, and then by defining our own classes
which will be mapped to those tables. In modern SQLAlchemy, these two tasks are
usually performed together, using a system known as ``Declarative``, which
allows us to create classes that include directives to describe the actual
database table they will be mapped to.

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
    ...     geom = Column(Geometry('POLYGON'))


The ``Lake`` class establishes details about the table being mapped, including
the name of the table denoted by ``__tablename__``, and three columns ``id``,
``name``, and ``geom``. The ``id`` column will be the primary key of the table.
The ``geom`` column is a :class:`geoalchemy2.types.Geometry` column whose
``geometry_type`` is ``POLYGON``.

Create the Table in the Database
--------------------------------

The ``Lake`` class has a corresponding ``Table`` object representing
the database table. This ``Table`` object was created automatically
by SQLAlchemy, it is referenced to by the ``Lake.__table__`` property::

    >>> Lake.__table__
    Table('lake', MetaData(bind=None), Column('id', Integer(), table=<lake>,
    primary_key=True, nullable=False), Column('name', String(), table=<lake>),
    Column('geom', Polygon(srid=4326), table=<lake>), schema=None)

To create the ``lake`` table in the database::

    >>> Lake.__table__.create(engine)

If we wanted to drop the table we'd use::

    >>> Lake.__table__.drop(engine)


Create an Instance of the Mapped Class
--------------------------------------

With the mapping declared, we can create a ``Lake`` object::

    >>> lake = Lake(name='Majeur', geom='POLYGON((0 0,1 0,1 1,0 1,0 0))')
    >>> lake.geom
    'POLYGON((0 0,1 0,1 1,0 1,0 0))'
    >>> str(lake.id)
    'None'

A WKT is passed to the ``Lake`` constructor for its geometry. This WKT
represents the shape of our lake. Since we have not yet told SQLAlchemy
to persist the ``lake`` object, its ``id`` is ``None``.

The EWKT (Extended WKT) format is also supported. So, for example, if the
spatial reference system for the geometry column were ``4326``, the string
``SRID=4326;POLYGON((0 0,1 0,1,0 1,0 0))`` could be used as the geometry
representation.


Create a Session
----------------

The ORM interacts with the database through a ``Session``. Let's
create a ``Session`` class::

    >>> from sqlalchemy.orm import sessionmaker
    >>> Session = sessionmaker(bind=engine)

This custom-made ``Session`` class will create new ``Session`` objects which
are bound to our database. Then, whenever we need to have a conversation with
the database, we instantiate a ``Session``::

    >>> session = Session()

The above ``Session`` is associated with our PostgreSQL ``Engine``, but
it hasn't opened any connection yet.

Add New Objects
---------------

To persist our ``Lake`` object, we ``add()`` it to the ``Session``::

    >>> lake = Lake(name="Majeur", geom="POLYGON((0 0,1 0,1 1,0 1,0 0))")
    >>> session.add(lake)
    >>> session.commit()

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

``our_lake.geom`` is a :class:`geoalchemy2.elements.WKBElement`, which a type
provided by GeoAlchemy.  :class:`geoalchemy2.elements.WKBElement` wraps a WKB
value returned by the database.

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

Make Spatial Queries
--------------------

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

We can also apply relationship functions to
:class:`geoalchemy2.elements.WKBElement`. For example::

    >>> lake = session.query(Lake).filter_by(name='Garde').one()
    >>> print session.scalar(lake.geom.ST_Intersects('LINESTRING(2 1,4 1)'))
    True

``session.scalar`` allows executing a clause and returning a scalar
value (a boolean value in this case).

The value ``True`` indicates that the lake "Garde" does intersects the ``LINESTRING(2 1,4 1)``
geometry. See the SpatiaLite SQL functions reference list for more information.

The GeoAlchemy functions all start with ``ST_``. Operators are also called as
functions, but the function names don't include the ``ST_`` prefix. As an
example let's use PostGIS' ``&&`` operator, which allows testing
whether the bounding boxes of geometries intersect. GeoAlchemy provides
the ``intersects`` function for that::

    >>> query = session.query
    >>> query = session.query(Lake).filter(
    ...             Lake.geom.intersects('LINESTRING(2 1,4 1)'))
    ...
    >>> for lake in query:
    ...     print lake.name
    ...
    Garde
    Orta

Set Spatial Relationships in the Model
--------------------------------------

Let's assume that in addition to ``lake``  we have another table, ``treasure``, that includes
treasure locations. And let's say that we are interested in discovering the treasures hidden at the
bottom of lakes.

The ``Treasure`` class is the following::


    >>> class Treasure(Base):
    ...      __tablename__ = 'treasure'
    ...      id = Column(Integer, primary_key=True)
    ...      geom = Column(Geometry('POINT'))

We can now add a ``relationship`` to the ``Lake`` table to automatically load the treasures
contained by each lake::

    >>> from sqlalchemy.orm import relationship, backref
    >>> class Lake(Base):
    ...     __tablename__ = 'lake'
    ...     id = Column(Integer, primary_key=True)
    ...     name = Column(String)
    ...     geom = Column(Geometry('POLYGON'))
    ...     treasures = relationship(
    ...         'Treasure',
    ...         primaryjoin='func.ST_Contains(foreign(Lake.geom), Treasure.geom).as_comparison(1, 2)',
    ...         backref=backref('lake', uselist=False),
    ...         viewonly=True,
    ...         uselist=True,
    ...     )

Note the use of the ``as_comparison`` function. It is required for using an SQL function
(``ST_Contains`` here) in a ``primaryjoin`` condition. This only works with SQLAlchemy 1.3, as the
``as_comparison`` function did not exist before that version. See the `Custom operators based on SQL function
<https://docs.sqlalchemy.org/en/latest/orm/join_conditions.html#custom-operators-based-on-sql-functions>`_
section of the SQLAlchemy documentation for more information.

Some information on the parameters used for configuring this ``relationship``:

* ``backref`` is used to provide the name of property to be placed on the class that handles this
  relationship in the other direction, namely ``Treasure``;
* ``viewonly=True`` specifies that the relationship is used only for loading objects, and not for
  persistence operations;
* ``uselist=True`` indicates that the property should be loaded as a list, as opposed to a scalar.

Also, note that the ``treasures`` property on ``lake`` objects (and the ``lake`` property on
``treasure`` objects) is loaded "lazily" when the property is first accessed. Another loading
strategy may be configured in the ``relationship``. For example you'd use ``lazy='joined'`` for
related items to be loaded "eagerly" in the same query as that of the parent, using a ``JOIN`` or
``LEFT OUTER JOIN``.

See the `Relationships API
<https://docs.sqlalchemy.org/en/latest/orm/relationship_api.html#relationships-api>`_ section of the
SQLAlchemy documentation for more detail on the ``relationship`` function, and all the parameters that
can be used to configure it.

Use Other Spatial Functions
---------------------------

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

Obviously, processing and measurement functions can also be used in ``WHERE``
clauses. For example::

    >>> lake = session.query(Lake).filter(
    ...             Lake.geom.ST_Buffer(2).ST_Area() > 33).one()
    ...
    >>> print lake.name
    Orta

And, like any other functions supported by GeoAlchemy, processing and
measurement functions can be applied to
:class:`geoalchemy2.elements.WKBElement`. For example::

    >>> lake = session.query(Lake).filter_by(name='Majeur').one()
    >>> bufferarea = session.scalar(lake.geom.ST_Buffer(2).ST_Area())
    >>> print '%s: %f' % (lake.name, bufferarea)
    Majeur: 21.485781
    Majeur: 21.485781

Use Raster functions
--------------------

A few functions (like `ST_Transform()`, `ST_Union()`, `ST_SnapToGrid()`, ...) can be
used on both :class:`geoalchemy2.types.Geometry` and :class:`geoalchemy2.types.Raster`
types. In GeoAlchemy2, these functions are only defined for
:class:`Geometry` as it can not be defined for several types at the
same time. Thus using these functions on :class:`Raster` requires
minor tweaking to enforce the type by passing the `type_=Raster` argument to the
function:

    >>> query = session.query(Lake.raster.ST_Transform(2154, type_=Raster))

Further Reference
-----------------

* Spatial Functions Reference: :ref:`spatial_functions`
* Spatial Operators Reference: :ref:`spatial_operators`
* Elements Reference: :ref:`elements`
