from sqlalchemy.sql import functions

from . import types


class GenericFunction(functions.GenericFunction):
    """
    The base class for GeoAlchemy functions.

    This class inherits from ``sqlalchemy.sql.functions.GenericFunction``, so
    functions defined by subclassing this class can be given a fixed return
    type. For example, functions like :class:`ST_Buffer` and
    :class:`ST_Envelope` have their ``type`` attribues set to
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


#
# Geometry Accessors
#


class ST_Envelope(GenericFunction):
    """
    Return type: :class:`geoalchemy2.types.Geometry`.
    """
    name = 'ST_Envelope'
    type = types.Geometry


class ST_GeometryN(GenericFunction):
    name = 'ST_GeometryN'


class ST_GeometryType(GenericFunction):
    name = 'ST_GeometryType'


class ST_IsValid(GenericFunction):
    name = 'ST_IsValid'


class ST_NPoints(GenericFunction):
    name = 'ST_NPoints'


class ST_X(GenericFunction):
    name = 'ST_X'


class ST_Y(GenericFunction):
    name = 'ST_Y'


#
# Geometry Outputs
#


class ST_AsBinary(GenericFunction):
    name = 'ST_AsBinary'


class ST_AsGeoJSON(GenericFunction):
    name = 'ST_AsGeoJSON'


class ST_AsGML(GenericFunction):
    name = 'ST_AsGML'


class ST_AsKML(GenericFunction):
    name = 'ST_AsKML'


class ST_AsSVG(GenericFunction):
    name = 'ST_AsSVG'


class ST_AsText(GenericFunction):
    name = 'ST_AsText'


class ST_AsText(GenericFunction):
    name = 'ST_AsText'


#
# Spatial Relationships and Measurements
#


class ST_Area(GenericFunction):
    name = 'ST_Area'


class ST_Centroid(GenericFunction):
    """
    Return type: :class:`geoalchemy2.types.Geometry`.
    """
    name = 'ST_Centroid'
    type = types.Geometry


class ST_Contains(GenericFunction):
    name = 'ST_Contains'


class ST_ContainsProperly(GenericFunction):
    name = 'ST_ContainsProperly'


class ST_Covers(GenericFunction):
    name = 'ST_Covers'


class ST_CoveredBy(GenericFunction):
    name = 'ST_CoveredBy'


class ST_Crosses(GenericFunction):
    name = 'ST_Crosses'


class ST_Disjoint(GenericFunction):
    name = 'ST_Disjoint'


class ST_Distance(GenericFunction):
    name = 'ST_Distance'


class ST_DistanceSphere(GenericFunction):
    name = 'ST_DistanceSphere'


class ST_DFullyWithin(GenericFunction):
    name = 'ST_DFullyWithin'


class ST_DWithin(GenericFunction):
    name = 'ST_DWithin'


class ST_Equals(GenericFunction):
    name = 'ST_Equals'


class ST_Intersects(GenericFunction):
    name = 'ST_Intersects'


class ST_Length(GenericFunction):
    name = 'ST_Length'


class ST_OrderingEquals(GenericFunction):
    name = 'ST_OrderingEquals'


class ST_Overlaps(GenericFunction):
    name = 'ST_Overlaps'


class ST_Perimeter(GenericFunction):
    name = 'ST_Perimeter'


class ST_Relate(GenericFunction):
    name = 'ST_Relate'


class ST_Touches(GenericFunction):
    name = 'ST_Touches'


class ST_Within(GenericFunction):
    name = 'ST_Within'


#
# Geometry Processing
#


class ST_Buffer(GenericFunction):
    """
    Return type: :class:`geoalchemy2.types.Geometry`.
    """
    name = 'ST_Buffer'
    type = types.Geometry


class ST_Difference(GenericFunction):
    """
    Return type: :class:`geoalchemy2.types.Geometry`.
    """
    name = 'ST_Difference'
    type = types.Geometry


class ST_Intersection(GenericFunction):
    """
    Return type: :class:`geoalchemy2.types.Geometry`.
    """
    name = 'ST_Intersection'
    type = types.Geometry


class ST_Union(GenericFunction):
    """
    Return type: :class:`geoalchemy2.types.Geometry`.
    """
    name = 'ST_Union'
    type = types.Geometry
