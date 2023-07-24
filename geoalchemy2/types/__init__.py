"""This module defines the Column types.

The :class:`geoalchemy2.types.Geometry`, :class:`geoalchemy2.types.Geography`, and
:class:`geoalchemy2.types.Raster` classes are used when defining geometry, geography and raster
columns/properties in models.
"""
import warnings
from typing import Any
from typing import Dict
from typing import Optional

from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql.base import ischema_names as _postgresql_ischema_names
from sqlalchemy.dialects.sqlite.base import ischema_names as _sqlite_ischema_names
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import func
from sqlalchemy.types import Float
from sqlalchemy.types import Integer
from sqlalchemy.types import UserDefinedType

try:
    # SQLAlchemy >= 2
    from sqlalchemy.sql._typing import _TypeEngineArgument
except ImportError:
    # SQLAlchemy < 2
    _TypeEngineArgument = Any  # type: ignore

from geoalchemy2.comparator import BaseComparator
from geoalchemy2.comparator import Comparator
from geoalchemy2.elements import CompositeElement
from geoalchemy2.elements import RasterElement
from geoalchemy2.elements import WKBElement
from geoalchemy2.exc import ArgumentError
from geoalchemy2.types import dialects


def select_dialect(dialect_name):
    """Select the dialect from its name."""
    known_dialects = {
        "geopackage": dialects.geopackage,
        "mysql": dialects.mysql,
        "postgresql": dialects.postgresql,
        "sqlite": dialects.sqlite,
    }
    return known_dialects.get(dialect_name, dialects.common)


class _GISType(UserDefinedType):
    """The base class for spatial types.

    This class defines ``bind_expression`` and ``column_expression`` methods
    that wrap column expressions in ``ST_GeomFromEWKT``, ``ST_GeogFromText``,
    or ``ST_AsEWKB`` calls.

    This class also defines ``result_processor`` and ``bind_processor``
    methods. The function returned by ``result_processor`` converts WKB values
    received from the database to :class:`geoalchemy2.elements.WKBElement`
    objects. The function returned by ``bind_processor`` converts
    :class:`geoalchemy2.elements.WKTElement` objects to EWKT strings.

    Args:
        geometry_type: The geometry type.

            Possible values are:

              * ``"GEOMETRY"``,
              * ``"POINT"``,
              * ``"LINESTRING"``,
              * ``"POLYGON"``,
              * ``"MULTIPOINT"``,
              * ``"MULTILINESTRING"``,
              * ``"MULTIPOLYGON"``,
              * ``"GEOMETRYCOLLECTION"``,
              * ``"CURVE"``,
              * ``None``.

           The latter is actually not supported with
           :class:`geoalchemy2.types.Geography`.

           When set to ``None`` then no "geometry type" constraints will be
           attached to the geometry type declaration.

           Default is ``"GEOMETRY"``.

        srid: The SRID for this column. E.g. 4326. Default is ``-1``.
        dimension: The dimension of the geometry. Default is ``2``.
        spatial_index: Indicate if a spatial index should be created. Default is ``True``.
        use_N_D_index: Use the N-D index instead of the standard 2-D index.
        use_typmod: By default PostgreSQL type modifiers are used to create the geometry
            column. To use check constraints instead set ``use_typmod`` to
            ``False``. By default this option is not included in the call to
            ``AddGeometryColumn``. Note that this option is only available for PostGIS 2.x.
    """

    name: Optional[str] = None
    """ Name used for defining the main geo type (geometry or geography)
        in CREATE TABLE statements. Set in subclasses. """

    from_text: Optional[str] = None
    """ The name of "from text" function for this type.
        Set in subclasses. """

    as_binary: Optional[str] = None
    """ The name of the "as binary" function for this type.
        Set in subclasses. """

    comparator_factory: Any = Comparator
    """ This is the way by which spatial operators are defined for
        geometry/geography columns. """

    cache_ok = False
    """ Disable cache for this type. """

    def __init__(
        self,
        geometry_type="GEOMETRY",
        srid=-1,
        dimension=2,
        spatial_index=True,
        use_N_D_index=False,
        use_typmod=None,
        from_text=None,
        name=None,
        nullable=True,
        _spatial_index_reflected=None,
    ) -> None:
        geometry_type, srid = self.check_ctor_args(
            geometry_type, srid, dimension, use_typmod, nullable
        )
        self.geometry_type = geometry_type
        self.srid = srid
        if name is not None:
            self.name = name
        if from_text is not None:
            self.from_text = from_text
        self.dimension = dimension
        self.spatial_index = spatial_index
        self.use_N_D_index = use_N_D_index
        self.use_typmod = use_typmod
        self.extended: Optional[bool] = self.as_binary == "ST_AsEWKB"
        self.nullable = nullable
        self._spatial_index_reflected = _spatial_index_reflected

    def get_col_spec(self):
        if not self.geometry_type:
            return self.name
        return "%s(%s,%d)" % (self.name, self.geometry_type, self.srid)

    def column_expression(self, col):
        """Specific column_expression that automatically adds a conversion function."""
        return getattr(func, self.as_binary)(col, type_=self)

    def result_processor(self, dialect, coltype):
        """Specific result_processor that automatically process spatial elements."""

        def process(value):
            if value is not None:
                kwargs = {}
                if self.srid > 0:
                    kwargs["srid"] = self.srid
                if self.extended is not None and dialect.name != "mysql":
                    kwargs["extended"] = self.extended
                return self.ElementType(value, **kwargs)

        return process

    def bind_expression(self, bindvalue):
        """Specific bind_expression that automatically adds a conversion function."""
        return getattr(func, self.from_text)(bindvalue, type_=self)

    def bind_processor(self, dialect):
        """Specific bind_processor that automatically process spatial elements."""

        def process(bindvalue):
            return select_dialect(dialect.name).bind_processor_process(self, bindvalue)

        return process

    @staticmethod
    def check_ctor_args(geometry_type, srid, dimension, use_typmod, nullable):
        try:
            srid = int(srid)
        except ValueError:
            raise ArgumentError("srid must be convertible to an integer")
        if geometry_type:
            geometry_type = geometry_type.upper()
        elif srid > 0:
            warnings.warn("srid not enforced when geometry_type is None")

        if use_typmod is not None and not nullable:
            raise ArgumentError(
                'The "nullable" and "use_typmod" arguments can not be used together'
            )

        return geometry_type, srid


@compiles(_GISType, "mysql")
def get_col_spec(self, *args, **kwargs):
    if self.geometry_type is not None:
        spec = "%s" % self.geometry_type
    else:
        spec = "GEOMETRY"

    if not self.nullable or self.spatial_index:
        spec += " NOT NULL"
    if self.srid > 0:
        spec += " SRID %d" % self.srid
    return spec


class Geometry(_GISType):
    """The Geometry type.

    Creating a geometry column is done like this::

        Column(Geometry(geometry_type='POINT', srid=4326))

    See :class:`geoalchemy2.types._GISType` for the list of arguments that can
    be passed to the constructor.

    If ``srid`` is set then the ``WKBElement`` objects resulting from queries will
    have that SRID, and, when constructing the ``WKBElement`` objects, the SRID
    won't be read from the data returned by the database. If ``srid`` is not set
    (meaning it's ``-1``) then the SRID set in ``WKBElement`` objects will be read
    from the data returned by the database.
    """

    name = "geometry"
    """ Type name used for defining geometry columns in ``CREATE TABLE``. """

    from_text = "ST_GeomFromEWKT"
    """ The "from text" geometry constructor. Used by the parent class'
        ``bind_expression`` method. """

    as_binary = "ST_AsEWKB"
    """ The "as binary" function to use. Used by the parent class'
        ``column_expression`` method. """

    ElementType = WKBElement
    """ The element class to use. Used by the parent class'
        ``result_processor`` method. """

    cache_ok = False
    """ Disable cache for this type. """


class Geography(_GISType):
    """The Geography type.

    Creating a geography column is done like this::

        Column(Geography(geometry_type='POINT', srid=4326))

    See :class:`geoalchemy2.types._GISType` for the list of arguments that can
    be passed to the constructor.
    """

    name = "geography"
    """ Type name used for defining geography columns in ``CREATE TABLE``. """

    from_text = "ST_GeogFromText"
    """ The ``FromText`` geography constructor. Used by the parent class'
        ``bind_expression`` method. """

    as_binary = "ST_AsBinary"
    """ The "as binary" function to use. Used by the parent class'
        ``column_expression`` method. """

    ElementType = WKBElement
    """ The element class to use. Used by the parent class'
        ``result_processor`` method. """

    cache_ok = False
    """ Disable cache for this type. """


class Raster(_GISType):
    """The Raster column type.

    Creating a raster column is done like this::

        Column(Raster)

    This class defines the ``result_processor`` method, so that raster values
    received from the database are converted to
    :class:`geoalchemy2.elements.RasterElement` objects.

    Args:
        spatial_index: Indicate if a spatial index should be created. Default is ``True``.
    """

    comparator_factory = BaseComparator
    """
    This is the way by which spatial operators and functions are
    defined for raster columns.
    """

    name = "raster"
    """ Type name used for defining raster columns in ``CREATE TABLE``. """

    from_text = "raster"
    """ The "from text" raster constructor. Used by the parent class'
        ``bind_expression`` method. """

    as_binary = "raster"
    """ The "as binary" function to use. Used by the parent class'
        ``column_expression`` method. """

    ElementType = RasterElement
    """ The element class to use. Used by the parent class'
        ``result_processor`` method. """

    cache_ok = False
    """ Disable cache for this type. """

    def __init__(self, spatial_index=True, from_text=None, name=None, nullable=True) -> None:
        # Enforce default values
        super(Raster, self).__init__(
            geometry_type=None,
            srid=-1,
            dimension=2,
            spatial_index=spatial_index,
            use_N_D_index=False,
            use_typmod=False,
            from_text=from_text,
            name=name,
            nullable=nullable,
        )
        self.extended = None

    @staticmethod
    def check_ctor_args(*args, **kwargs):
        return None, -1


class _DummyGeometry(Geometry):
    """A dummy type only used with SQLite."""

    def get_col_spec(self):
        return self.geometry_type or "GEOMETRY"


class CompositeType(UserDefinedType):
    """A composite type used by some spatial functions.

    A wrapper for :class:`geoalchemy2.elements.CompositeElement`, that can be
    used as the return type in PostgreSQL functions that return composite
    values.

    This is used as the base class of :class:`geoalchemy2.types.GeometryDump`.
    """

    typemap: Dict[str, _TypeEngineArgument] = {}
    """ Dictionary used for defining the content types and their
        corresponding keys. Set in subclasses. """

    class comparator_factory(UserDefinedType.Comparator):
        def __getattr__(self, key):
            try:
                type_ = self.type.typemap[key]
            except KeyError:
                raise AttributeError("Type '%s' doesn't have an attribute: '%s'" % (self.type, key))

            return CompositeElement(self.expr, key, type_)


class GeometryDump(CompositeType):
    """The return type for functions like ``ST_Dump``.

    The type consists in a path and a geom field.
    You should normally never use this class directly.
    """

    typemap = {"path": postgresql.ARRAY(Integer), "geom": Geometry}
    """ Dictionary defining the contents of a ``geometry_dump``. """

    cache_ok = True
    """ Enable cache for this type. """


# Register Geometry, Geography and Raster to SQLAlchemy's reflection subsystems.
_postgresql_ischema_names["geometry"] = Geometry
_postgresql_ischema_names["geography"] = Geography
_postgresql_ischema_names["raster"] = Raster

_sqlite_ischema_names["GEOMETRY"] = Geometry
_sqlite_ischema_names["POINT"] = Geometry
_sqlite_ischema_names["LINESTRING"] = Geometry
_sqlite_ischema_names["POLYGON"] = Geometry
_sqlite_ischema_names["MULTIPOINT"] = Geometry
_sqlite_ischema_names["MULTILINESTRING"] = Geometry
_sqlite_ischema_names["MULTIPOLYGON"] = Geometry
_sqlite_ischema_names["CURVE"] = Geometry
_sqlite_ischema_names["GEOMETRYCOLLECTION"] = Geometry
_sqlite_ischema_names["RASTER"] = Raster


class SummaryStats(CompositeType):
    """Define the composite type returned by the function ST_SummaryStatsAgg."""

    typemap = {
        "count": Integer,
        "sum": Float,
        "mean": Float,
        "stddev": Float,
        "min": Float,
        "max": Float,
    }

    cache_ok = True
    """ Enable cache for this type. """


__all__ = [
    "_GISType",
    "CompositeType",
    "Geography",
    "Geometry",
    "GeometryDump",
    "Raster",
    "SummaryStats",
    "dialects",
    "select_dialect",
]


def __dir__():
    return __all__
