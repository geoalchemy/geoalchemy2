"""
This module includes the :class:`geoalchemy2.types.Geometry` and
:class:`geoalchemy2.types.Geography` to use when defining geometry
and geography columns, respecively.

Reference
---------
"""

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

    def __init__(self, geometry_type='GEOMETRY', srid=-1, dimension=2,
                 spatial_index=True):
        self.geometry_type = geometry_type
        self.srid = srid
        self.dimension = dimension
        self.spatial_index = spatial_index

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

    Creating a geometry column is done like this::

        Column(Geometry(geometry_type='POINT', srid=4326))

    Set ``mgmt`` to ``True`` in the arguments passed to ``Geometry`` for the
    ``AddGeometryColumn`` and ``DropGeometryColumn`` management function to
    be applied when the geometry column is created and dropped, respectively.
    Default is ``False``.

    """

    name = 'geometry'
    """ Type name used for defining geometry columns in ``CREATE TABLE``. """

    from_text = 'ST_GeomFromText'
    """ The ``FromText`` geometry constructor. Used by the parent class'
        ``bind_expression`` method. """

    def __init__(self, geometry_type='GEOMETRY', srid=-1, dimension=2,
                 spatial_index=True, mgmt=False):
        _GISType.__init__(self, geometry_type, srid, dimension, spatial_index)
        self.mgmt = mgmt


class Geography(_GISType):
    """
    The Geography type.

    Creating a geography column is done like this::

        Column(Geography(geometry_type='POINT', srid=4326))

    """

    name = 'geography'
    """ Type name used for defining geography columns in ``CREATE TABLE``. """

    from_text = 'ST_GeogFromText'
    """ The ``FromText`` geography constructor. Used by the parent class'
        ``bind_expression`` method. """
