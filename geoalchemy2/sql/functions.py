from sqlalchemy.sql.functions import GenericFunction

from .. import types


class GeometryType(GenericFunction):
    name = 'ST_GeometryType'
    identifier = 'geometry_type'
    package = 'geo'


class Buffer(GenericFunction):
    name = 'ST_Buffer'
    identifier = 'buffer'
    type_ = types.Geometry
    package = 'geo'
