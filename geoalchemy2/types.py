""" This module defines the :class:`geoalchemy2.types.Geometry`,
:class:`geoalchemy2.types.Geography`, and :class:`geoalchemy2.types.Raster`
classes, that are used when defining geometry, geography and raster
columns/properties in models.

Reference
---------
"""
import warnings

from sqlalchemy.types import UserDefinedType, Integer
from sqlalchemy.sql import func
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql.base import ischema_names

from .comparator import BaseComparator, Comparator
from .elements import WKBElement, WKTElement, RasterElement, CompositeElement
from .exc import ArgumentError


class _GISType(UserDefinedType):
    """
    The base class for :class:`geoalchemy2.types.Geometry` and
    :class:`geoalchemy2.types.Geography`.

    This class defines ``bind_expression`` and ``column_expression`` methods
    that wrap column expressions in ``ST_GeomFromEWKT``, ``ST_GeogFromText``,
    or ``ST_AsEWKB`` calls.

    This class also defines ``result_processor`` and ``bind_processor``
    methods. The function returned by ``result_processor`` converts WKB values
    received from the database to :class:`geoalchemy2.elements.WKBElement`
    objects. The function returned by ``bind_processor`` converts
    :class:`geoalchemy2.elements.WKTElement` objects to EWKT strings.

    Constructor arguments:

    ``geometry_type``

        The geometry type.

        Possible values are:

          * ``"GEOMETRY"``,
          * ``"POINT"``,
          * ``"LINESTRING"``,
          * ``"POLYGON"``,
          * ``"MULTIPOINT"``,
          * ``"MULTILINESTRING"``,
          * ``"MULTIPOLYGON"``,
          * ``"GEOMETRYCOLLECTION"``
          * ``"CURVE"``,
          * ``None``.

       The latter is actually not supported with
       :class:`geoalchemy2.types.Geography`.

       When set to ``None`` then no "geometry type" constraints will be
       attached to the geometry type declaration. Using ``None`` here
       is not compatible with setting ``management`` to ``True``.

       Default is ``"GEOMETRY"``.

    ``srid``

        The SRID for this column. E.g. 4326. Default is ``-1``.

    ``dimension``

        The dimension of the geometry. Default is ``2``.

    ``spatial_index``

        Indicate if a spatial index should be created. Default is ``True``.

    ``management``

        Indicate if the ``AddGeometryColumn`` and ``DropGeometryColumn``
        managements functions should be called when adding and dropping the
        geometry column. Should be set to ``True`` for PostGIS 1.x. Default is
        ``False``. Note that this option has no effect for
        :class:`geoalchemy2.types.Geography`.

    ``use_typmod``

        By default PostgreSQL type modifiers are used to create the geometry
        column. To use check constraints instead set ``use_typmod`` to
        ``False``. By default this option is not included in the call to
        ``AddGeometryColumn``. Note that this option is only taken
        into account if ``management`` is set to ``True`` and is only available
        for PostGIS 2.x.

    """

    name = None
    """ Name used for defining the main geo type (geometry or geography)
        in CREATE TABLE statements. Set in subclasses. """

    from_text = None
    """ The name of "from text" function for this type.
        Set in subclasses. """

    as_binary = None
    """ The name of the "as binary" function for this type.
        Set in subclasses. """

    comparator_factory = Comparator
    """ This is the way by which spatial operators are defined for
        geometry/geography columns. """

    def __init__(self, geometry_type='GEOMETRY', srid=-1, dimension=2,
                 spatial_index=True, management=False, use_typmod=None):
        geometry_type, srid = self.check_ctor_args(
            geometry_type, srid, management, use_typmod)
        self.geometry_type = geometry_type
        self.srid = srid
        self.dimension = dimension
        self.spatial_index = spatial_index
        self.management = management
        self.use_typmod = use_typmod
        self.extended = self.as_binary == 'ST_AsEWKB'

    def get_col_spec(self):
        if not self.geometry_type:
            return self.name
        return '%s(%s,%d)' % (self.name, self.geometry_type, self.srid)

    def column_expression(self, col):
        return getattr(func, self.as_binary)(col, type_=self)

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is not None:
                return WKBElement(value, srid=self.srid,
                                  extended=self.extended)
        return process

    def bind_expression(self, bindvalue):
        return getattr(func, self.from_text)(bindvalue, type_=self)

    def bind_processor(self, dialect):
        def process(bindvalue):
            if isinstance(bindvalue, WKTElement):
                return 'SRID=%d;%s' % (bindvalue.srid, bindvalue.data)
            else:
                return bindvalue
        return process

    @staticmethod
    def check_ctor_args(geometry_type, srid, management, use_typmod):
        try:
            srid = int(srid)
        except ValueError:
            raise ArgumentError('srid must be convertible to an integer')
        if geometry_type:
            geometry_type = geometry_type.upper()
        else:
            if management:
                raise ArgumentError('geometry_type set to None not compatible '
                                    'with management')
            if srid > 0:
                warnings.warn('srid not enforced when geometry_type is None')
        if use_typmod and not management:
            warnings.warn('use_typmod ignored when management is False')
        return geometry_type, srid


class Geometry(_GISType):
    """
    The Geometry type.

    Creating a geometry column is done like this::

        Column(Geometry(geometry_type='POINT', srid=4326))

    See :class:`geoalchemy2.types._GISType` for the list of arguments that can
    be passed to the constructor.

    """

    name = 'geometry'
    """ Type name used for defining geometry columns in ``CREATE TABLE``. """

    from_text = 'ST_GeomFromEWKT'
    """ The "from text" geometry constructor. Used by the parent class'
        ``bind_expression`` method. """

    as_binary = 'ST_AsEWKB'
    """ The "as binary" function to use. Used by the parent class'
        ``column_expression`` method. """


class Geography(_GISType):
    """
    The Geography type.

    Creating a geography column is done like this::

        Column(Geography(geometry_type='POINT', srid=4326))

    See :class:`geoalchemy2.types._GISType` for the list of arguments that can
    be passed to the constructor.

    """

    name = 'geography'
    """ Type name used for defining geography columns in ``CREATE TABLE``. """

    from_text = 'ST_GeogFromText'
    """ The ``FromText`` geography constructor. Used by the parent class'
        ``bind_expression`` method. """

    as_binary = 'ST_AsBinary'
    """ The "as binary" function to use. Used by the parent class'
        ``column_expression`` method. """


class Raster(UserDefinedType):
    """
    The Raster column type.

    Creating a raster column is done like this::

        Column(Raster)

    This class defines the ``result_processor`` method, so that raster values
    received from the database are converted to
    :class:`geoalchemy2.elements.RasterElement` objects.

    Constructor arguments:

    ``spatial_index``

        Indicate if a spatial index should be created. Default is ``True``.

    """

    comparator_factory = BaseComparator
    """
    This is the way by which spatial operators and functions are
    defined for raster columns.
    """

    def __init__(self, spatial_index=True):
        self.spatial_index = spatial_index

    def get_col_spec(self):
        return 'raster'

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is not None:
                return RasterElement(value)
        return process


class CompositeType(UserDefinedType):
    """
    A wrapper for :class:`geoalchemy2.elements.CompositeElement`, that can be
    used as the return type in PostgreSQL functions that return composite
    values.

    This is used as the base class of :class:`geoalchemy2.types.GeometryDump`.
    """

    typemap = {}
    """ Dictionary used for defining the content types and their
        corresponding keys. Set in subclasses. """

    class comparator_factory(UserDefinedType.Comparator):
        def __getattr__(self, key):
            try:
                type_ = self.type.typemap[key]
            except KeyError:
                raise KeyError("Type '%s' doesn't have an attribute: '%s'"
                               % (self.type, key))

            return CompositeElement(self.expr, key, type_)


class GeometryDump(CompositeType):
    """
    The return type for functions like ``ST_Dump``, consisting of a path and
    a geom field. You should normally never use this class directly.
    """

    typemap = {'path': postgresql.ARRAY(Integer), 'geom': Geometry}
    """ Dictionary defining the contents of a ``geometry_dump``. """


# Register Geometry, Geography and Raster to SQLAlchemy's Postgres reflection
# subsystem.
ischema_names['geometry'] = Geometry
ischema_names['geography'] = Geography
ischema_names['raster'] = Raster
