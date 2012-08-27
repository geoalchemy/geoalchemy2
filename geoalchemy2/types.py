from sqlalchemy.types import UserDefinedType
from sqlalchemy.sql import func


class Geometry(UserDefinedType):

    name = "GEOMETRY"

    def __init__(self, srid=-1):
        self.srid = srid

    def column_expression(self, col):
        return func.ST_AsBinary(col, type_=self)

    def bind_expression(self, bindvalue):
        return func.ST_GeomFromText(bindvalue, type_=self)
