import binascii

from sqlalchemy.types import UserDefinedType
from sqlalchemy.sql import func, expression


class _SpatialElement(object):

    def __str__(self):
        return self.desc

    def __repr__(self):
        return "<%s at 0x%x; %r>" % (self.__class__.__name__,
                                     id(self), self.desc)


class WKTElement(_SpatialElement):

    def __init__(self, data, srid=-1):
        self.srid = srid
        self.data = data

    @property
    def desc(self):
        return self.data


class WKBElement(_SpatialElement, expression.Function):

    def __init__(self, data):
        self.data = data
        expression.Function.__init__(self, "ST_GeomFromWKB", data)

    @property
    def desc(self):
        return binascii.hexlify(self.data)

    def __getattr__(self, name):
        # This is the scheme by which expressions like
        # Lake.geom.ST_Buffer(2) work.

        # Function names that don't start with "ST_" are rejected. This not
        # to mess up with SQLAlchemy's use of hasattr/getattr on Column
        # objects.
        if not name.startswith('ST_'):
            raise AttributeError

        # We create our own _FunctionGenerator here, and use it in place of
        # SQLAlchemy's "func" object. This is to be able to "bind" the
        # function to the SQL expression. See also
        # geoalchemy2.functions.GenericFunction.
        func_ = expression._FunctionGenerator(expr=self)

        return getattr(func_, name)


class Geometry(UserDefinedType):

    name = "GEOMETRY"

    class comparator_factory(UserDefinedType.Comparator):

        def __getattr__(self, name):
            # This is the scheme by which expressions like
            # Lake.geom.ST_Buffer(2) work.

            # Function names that don't start with "ST_" are rejected. This not
            # to mess up with SQLAlchemy's use of hasattr/getattr on Column
            # objects.
            if not name.startswith('ST_'):
                raise AttributeError

            # We create our own _FunctionGenerator here, and use it in place of
            # SQLAlchemy's "func" object. This is to be able to "bind" the
            # function to the SQL expression. See also
            # geoalchemy2.functions.GenericFunction.
            func_ = expression._FunctionGenerator(expr=self.expr)

            return getattr(func_, name)

    def __init__(self, srid=-1, dimension=2):
        self.srid = srid
        self.dimension = dimension

    def bind_expression(self, bindvalue):
        return func.ST_GeomFromText(bindvalue, type_=self)

    def column_expression(self, col):
        return func.ST_AsBinary(col, type_=self)

    def bind_processor(self, dialect):
        def process(value):
            if isinstance(value, WKTElement):
                return value.desc
            else:
                return value
        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is not None:
                return WKBElement(value)
        return process


class LineString(Geometry):
    name = "LINESTRING"
