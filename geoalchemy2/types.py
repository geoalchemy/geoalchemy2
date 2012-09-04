import binascii

from sqlalchemy.types import UserDefinedType
from sqlalchemy.sql import func, expression


def _generate_function(name, expr=None):
    """
    Generate a spatial function from its name, and "bind" it
    to the expression passed in ``expr``.
    """

    # We create our own _FunctionGenerator here, and use it in place of
    # SQLAlchemy's "func" object. This is to be able to "bind" the
    # function to the SQL expression. See also GenericFunction above.

    func_ = expression._FunctionGenerator(expr=expr)
    return getattr(func_, name)


class _Comparator(UserDefinedType.Comparator):
    """
    A custom comparator class. Used in :class:`geoalchemy2.types.Geometry`.

    This is where spatial operators like ``&&`` and ``&<`` are defined.
    """

    def __getattr__(self, name):

        # Function names that don't start with "ST_" are rejected.
        # This is not to mess up with SQLAlchemy's use of
        # hasattr/getattr on Column objects.

        if not name.startswith('ST_'):
            raise AttributeError

        return _generate_function(name, expr=self.expr)

    def intersects(self, other):
        """
        The ``&&`` operator. A's BBOX intersects B's.
        """
        return self.op('&&')(other)

    def overlaps_or_left(self, other):
        """
        The ``&<`` operator. A's BBOX overlaps or is to the left of B's.
        """
        return self.op('&<')(other)

    def overlaps_or_right(self, other):
        """
        The ``&>`` operator. A's BBOX overlaps or is to the right of B's.
        """
        return self.op('&>')(other)

    def distance_between_points(self, other):
        """
        The ``<->`` operator. The distance between two points.
        """
        return self.op('<->')(other)

    def distance_between_bbox(self, other):
        """
        The ``<#>`` operator. The distance between bounding box of two
        geometries.
        """
        return self.op('<#>')(other)


class _SpatialElement(object):

    def __str__(self):
        return self.desc  # pragma: no cover

    def __repr__(self):
        return "<%s at 0x%x; %r>" % \
            (self.__class__.__name__, id(self), self.desc)  # pragma: no cover


class WKTElement(_SpatialElement, expression.Function):
    """
    Instances of this class wrap a WKT value.
    """

    def __init__(self, data, srid=-1):
        self.srid = srid
        self.data = data
        expression.Function.__init__(self, "ST_GeomFromText", data)

    @property
    def desc(self):
        """
        This element's description string.
        """
        return self.data


class WKBElement(_SpatialElement, expression.Function):
    """
    Instances of this class wrap a WKB value. Geometry values read
    from the database are converted to instances of type.
    """

    def __init__(self, data):
        self.data = data
        expression.Function.__init__(self, "ST_GeomFromWKB", data)

    @property
    def desc(self):
        """
        This element's description string.
        """
        return binascii.hexlify(self.data)

    def __getattr__(self, name):
        #
        # This is how things like lake.geom.ST_Buffer(2) creates
        # SQL expressions of this form:
        #
        # ST_Buffer(ST_GeomFromWKB(:ST_GeomFromWKB_1), :param_1)
        #

        return _generate_function(name, expr=self)


class _GISType(UserDefinedType):
    """
    The base class for :class:`geoalchemy2.types.Geometry` and
    :class:`geoalchemy2.types.Geography`.

    This class defines ``bind_expression`` and ``column_expression`` methods
    that wrap column expressions in ``ST_GeomFromText``, ``ST_GeogFromText``,
    or ``ST_AsBinary`` calls.

    This class also defines the ``result_processor`` method, so that WKB values
    received from the database are converted to
    :class:`geoalchemy2.types.WKBElement` objects.

    """

    name = None
    """ Name used for defining the main geo type (geometry or geography)
        in CREATE TABLE statements. Set in subclasses. """

    from_text = None
    """ The name of ST_*FromText function for this type.
        Set in subclasses. """

    comparator_factory = _Comparator
    """ This is the way by which spatial operators are defined for
        geometry/geography columns. """

    def __init__(self, geometry_type='GEOMETRY', srid=-1, dimension=2):
        self.geometry_type = geometry_type
        self.srid = srid
        self.dimension = dimension

    def get_col_spec(self):
        return '%s(%s,%d)' % (self.name, self.geometry_type, self.srid)

    def column_expression(self, col):
        return func.ST_AsBinary(col, type_=self)

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is not None:
                return WKBElement(value)
        return process

    def bind_expression(self, bindvalue):
        return getattr(func, self.from_text)(bindvalue, type_=self)


class Geometry(_GISType):
    """
    The Geometry type.

    """

    name = 'geometry'
    """ Type name used for defining geometry columns in ``CREATE TABLE``. """

    from_text = 'ST_GeomFromText'
    """ The ``FromText`` geometry constructor. """


class Geography(_GISType):
    """
    The Geography type.
    """

    name = 'geography'
    """ Type name used for defining geography columns in ``CREATE TABLE``. """

    from_text = 'ST_GeogFromText'
    """ The ``FromText`` geography constructor. """
