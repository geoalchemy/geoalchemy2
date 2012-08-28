# FIXME not sure it makes sense that WKBElement uses UserDefinedType.Comparator
# FIXME add more specific geometry types
# FIXME add appropriate get_col_spec for PostGIS 2 (typmod)

import binascii

from sqlalchemy.types import UserDefinedType
from sqlalchemy.sql import func, expression
from sqlalchemy.util import memoized_property


class _Comparator(UserDefinedType.Comparator):
    """
    A custom comparator class. Used both in the Geometry type, and
    in WKBElement.
    """

    def __getattr__(self, name):

        # Function names that don't start with "ST_" are rejected. This is not
        # to mess up with SQLAlchemy's use of hasattr/getattr on Column
        # objects.

        if not name.startswith('ST_'):
            raise AttributeError

        # We create our own _FunctionGenerator here, and use it in place of
        # SQLAlchemy's "func" object. This is to be able to "bind" the function
        # to the SQL expression. See also
        # geoalchemy2.functions.GenericFunction.

        func_ = expression._FunctionGenerator(expr=self.expr)

        return getattr(func_, name)

    def intersects(self, other):
        """
        The && operator. A's BBOX intersects B's.
        """
        return self.op('&&')(other)

    def overlaps_or_left(self, other):
        """
        The &< operator. A's BBOX overlaps or is to the left of B's.
        """
        return self.op('&<')(other)

    def overlaps_or_right(self, other):
        """
        The &> operator. A's BBOX overlaps or is to the right of B's.
        """
        return self.op('&>')(other)


class _SpatialElement(object):

    def __str__(self):
        return self.desc  # pragma: no cover

    def __repr__(self):
        return "<%s at 0x%x; %r>" % \
            (self.__class__.__name__, id(self), self.desc)  # pragma: no cover


class WKTElement(_SpatialElement, expression.Function):

    def __init__(self, data, srid=-1):
        self.srid = srid
        self.data = data
        expression.Function.__init__(self, "ST_GeomFromText", data)

    @property
    def desc(self):
        return self.data


class WKBElement(_SpatialElement, expression.Function):

    comparator_factory = _Comparator

    def __init__(self, data):
        self.data = data
        expression.Function.__init__(self, "ST_GeomFromWKB", data)

    @property
    def desc(self):
        return binascii.hexlify(self.data)

    @memoized_property
    def comparator(self):
        return self.comparator_factory(self)

    def __getattr__(self, name):
        #
        # This is how things like Lake.geom.ST_Buffer(2) creates
        # SQL expressions of this form:
        #
        # ST_Buffer(ST_GeomFromWKB(:ST_GeomFromWKB_1), :param_1)
        #
        return getattr(self.comparator, name)


class Geometry(UserDefinedType):

    name = "GEOMETRY"

    comparator_factory = _Comparator

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
