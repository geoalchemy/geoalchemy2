from sqlalchemy.sql import functions

from . import types

__all__ = [
        'GenericFunction',
        'GeometryType',
        'AsText',
        'Buffer'
        ]


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
# Geometry Processing
#


class Buffer(GenericFunction):
    name = 'ST_Buffer'
    type = types.Geometry
