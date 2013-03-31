"""

This module defines the :class:`GenericFunction` class, which is the base for
the implementation of spatial functions in GeoAlchemy.  This module is also
where actual spatial functions are defined. Spatial functions supported by
GeoAlchemy are defined in this module. See :class:`GenericFunction` to know how
to create new spatial functions.

.. note::

    By convention the names of spatial functions are prefixed by ``ST_``.  This
    is to be consistent with PostGIS', which itself is based on the ``SQL-MM``
    standard.

Functions created by subclassing :class:`GenericFunction` can be called
in several ways:

* By using the ``func`` object, which is the SQLAlchemy standard way of calling
  a function. For example, without the ORM::

      select([func.ST_Area(lake_table.c.geom)])

  and with the ORM::

      Session.query(func.ST_Area(Lake.geom))

* By applying the function to a geometry column. For example, without the
  ORM::

      select([lake_table.c.geom.ST_Area()])

  and with the ORM::

      Session.query(Lake.geom.ST_Area())

* By applying the function to a :class:`geoalchemy2.elements.WKBElement`
  object (:class:`geoalchemy2.elements.WKBElement` is the type into
  which GeoAlchemy converts geometry values read from the database), or
  to a :class:`geoalchemy2.elements.WKTElement` object. For example,
  without the ORM::

      conn.scalar(lake['geom'].ST_Area())

  and with the ORM::

      session.scalar(lake.geom.ST_Area())

Reference
---------

"""

from sqlalchemy.sql import functions

from . import types


class GenericFunction(functions.GenericFunction):
    """
    The base class for GeoAlchemy functions.

    This class inherits from ``sqlalchemy.sql.functions.GenericFunction``, so
    functions defined by subclassing this class can be given a fixed return
    type. For example, functions like :class:`ST_Buffer` and
    :class:`ST_Envelope` have their ``type`` attributes set to
    :class:`geoalchemy2.types.Geometry`.

    This class allows constructs like ``Lake.geom.ST_Buffer(2)``. In that
    case the ``Function`` instance is bound to an expression (``Lake.geom``
    here), and that expression is passed to the function when the function
    is actually called.

    If you need to use a function that GeoAlchemy does not provide you will
    certainly want to subclass this class. For example, if you need the
    ``ST_TransScale`` spatial function, which isn't (currently) natively
    supported by GeoAlchemy, you will write this::

        from geoalchemy2 import Geometry
        from geoalchemy2.functions import GenericFunction

        class ST_TransScale(GenericFunction):
            name = 'ST_TransScale'
            type = Geometry
    """

    def __init__(self, *args, **kwargs):
        expr = kwargs.pop('expr', None)
        if expr is not None:
            args = (expr,) + args
        functions.GenericFunction.__init__(self, *args, **kwargs)


# Functions are classified as in the PostGIS doc.
# <http://www.postgis.org/documentation/manual-svn/reference.html>


_FUNCTIONS = [
    #
    # Geometry Accessors
    #

    ('ST_Envelope', types.Geometry,
     '''Returns a geometry representing the double precision (float8) bounding
        box of the supplied geometry.'''),

    ('ST_GeometryN', None,
     '''Return the 1-based Nth geometry if the geometry is a
        ``GEOMETRYCOLLECTION``, ``(MULTI)POINT``, ``(MULTI)LINESTRING``,
        ``MULTICURVE`` or ``(MULTI)POLYGON``, ``POLYHEDRALSURFACE`` Otherwise,
        return ``NULL``.'''),

    ('ST_GeometryType', None,
     '''Return the geometry type of the ``ST_Geometry`` value.'''),

    ('ST_IsValid', None,
     '''Returns ``true`` if the ``ST_Geometry`` is well formed.'''),

    ('ST_NPoints', None,
     '''Return the number of points (vertexes) in a geometry.'''),

    ('ST_SRID', None,
     '''Returns the spatial reference identifier for the ``ST_Geometry`` as
        defined in ``spatial_ref_sys`` table.'''),

    ('ST_X', None,
     '''Return the X coordinate of the point, or ``NULL`` if not available.
        Input must be a point.'''),

    ('ST_Y', None,
     '''Return the Y coordinate of the point, or ``NULL`` if not available.
        Input must be a point.'''),

    #
    # Geometry Editors
    #

    ('ST_Transform', types.Geometry, None),

    #
    # Geometry Outputs
    #

    ('ST_AsBinary', None, None),
    ('ST_AsGeoJSON', None, None),
    ('ST_AsGML', None, None),
    ('ST_AsKML', None, None),
    ('ST_AsSVG', None, None),
    ('ST_AsText', None, None),
    ('ST_AsText', None, None),

    #
    # Spatial Relationships and Measurements
    #

    ('ST_Area', None, None),
    ('ST_Centroid', types.Geometry, None),
    ('ST_Contains', None, None),
    ('ST_ContainsProperly', None, None),
    ('ST_Covers', None, None),
    ('ST_CoveredBy', None, None),
    ('ST_Crosses', None, None),
    ('ST_Disjoint', None, None),
    ('ST_Distance', None, None),
    ('ST_Distance_Sphere', None, None),
    ('ST_DFullyWithin', None, None),
    ('ST_DWithin', None, None),
    ('ST_Equals', None, None),
    ('ST_Intersects', None, None),
    ('ST_Length', None, None),
    ('ST_OrderingEquals', None, None),
    ('ST_Overlaps', None, None),
    ('ST_Perimeter', None, None),
    ('ST_Project', types.Geography, None),
    ('ST_Relate', None, None),
    ('ST_Touches', None, None),
    ('ST_Within', None, None),

    #
    # Geometry Processing
    #

    ('ST_Buffer', types.Geometry, None),
    ('ST_Difference', types.Geometry, None),
    ('ST_Intersection', types.Geometry, None),
    ('ST_Union', types.Geometry, None),
]

# Iterate through _FUNCTION and create GenericFunction classes dynamically
for name, type_, doc in _FUNCTIONS:
    attributes = {'name': name}
    docs = []

    if doc is not None:
        docs.append(doc)

    if type_ is not None:
        attributes['type'] = type_

        type_str = '{}.{}'.format(type_.__module__, type_.__name__)
        docs.append('Return type: :class:`{}`.'.format(type_str))

    if len(docs) != 0:
        attributes['__doc__'] = '\n\n'.join(docs)

    globals()[name] = type(name, (GenericFunction,), attributes)
