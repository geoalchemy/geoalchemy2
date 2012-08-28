from sqlalchemy.sql import functions

from . import types

__all__ = [
        'GenericFunction', 'GeometryType', 'Buffer'
        ]


class GenericFunction(functions.GenericFunction):

    def __init__(self, *args, **kwargs):
        expr = kwargs.pop('expr', None)
        if expr is not None:
            args = (expr,) + args
        functions.GenericFunction.__init__(self, *args, **kwargs)


class GeometryType(GenericFunction):
    name = 'ST_GeometryType'


class Buffer(GenericFunction):
    name = 'ST_Buffer'
    type = types.Geometry
