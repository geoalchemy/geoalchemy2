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


class ST_GeometryType(GenericFunction):
    name = 'ST_GeometryType'


#
# Geometry Outputs
#


class ST_AsText(GenericFunction):
    name = 'ST_AsText'

#
# Spatial Relationships and Measurements
#


class ST_Area(GenericFunction):
    name = 'ST_Area'


class ST_Contains(GenericFunction):
    name = 'ST_Contains'


class ST_Intersects(GenericFunction):
    name = 'ST_Intersects'


#
# Geometry Processing
#


class ST_Buffer(GenericFunction):
    """
    Return type: :class:`geoalchemy2.types.Geometry`.
    """
    name = 'ST_Buffer'
    type = types.Geometry
