from sqlalchemy.sql import functions

from . import types


class GenericFunction(functions.GenericFunction):

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


class GeometryType(GenericFunction):
    name = 'ST_GeometryType'


#
# Geometry Outputs
#


class AsText(GenericFunction):
    name = 'ST_AsText'

#
# Spatial Relationships and Measurements
#


class Area(GenericFunction):
    name = 'ST_Area'


class Contains(GenericFunction):
    name = 'ST_Contains'


class Intersects(GenericFunction):
    name = 'ST_Intersects'


#
# Geometry Processing
#


class Buffer(GenericFunction):
    name = 'ST_Buffer'
    type = types.Geometry
