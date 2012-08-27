from sqlalchemy.sql import functions

from .. import types


class GenericFunction(functions.GenericFunction):

    def __init__(self, *args, **kwargs):
        expr = kwargs.pop('expr', None)
        if expr is not None:
            args = (expr,) + args
        functions.GenericFunction.__init__(self, *args, **kwargs)


class GeometryType(GenericFunction):
    name = 'ST_GeometryType'
    identifier = 'geometry_type'
    package = 'geo'


class Buffer(GenericFunction):
    name = 'ST_Buffer'
    identifier = 'buffer'
    type_ = types.Geometry
    package = 'geo'
