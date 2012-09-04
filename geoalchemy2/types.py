from sqlalchemy.types import UserDefinedType
from sqlalchemy.sql import func

from .comparator import Comparator
from .elements import WKBElement


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

    comparator_factory = Comparator
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
