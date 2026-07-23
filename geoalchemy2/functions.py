"""This module defines the internals to map the spatial functions to the spatial columns.

This module defines the :class:`GenericFunction` class, which is the base for
the implementation of spatial functions in GeoAlchemy.  This module is also
where actual spatial functions are defined. Spatial functions supported by
GeoAlchemy are defined in this module. See :class:`GenericFunction` to know how
to create new spatial functions.

.. note::

    By convention the names of spatial functions are prefixed by ``ST_``.  This
    is to be consistent with PostGIS', which itself is based on the ``SQL-MM``
    standard.

Functions created by subclassing :class:`GenericFunction` can be called
in several ways:

* By using the ``func`` object, which is the SQLAlchemy standard way of calling
  a function. For example, without the ORM::

      select(func.ST_Area(lake_table.c.geom))

  and with the ORM::

      Session.query(func.ST_Area(Lake.geom))

* By applying the function to a geometry column. For example, without the
  ORM::

      select(lake_table.c.geom.ST_Area())

  and with the ORM::

      Session.query(Lake.geom.ST_Area())

* By applying the function to a :class:`geoalchemy2.elements.WKBElement`
  object (:class:`geoalchemy2.elements.WKBElement` is the type into
  which GeoAlchemy converts geometry values read from the database), or
  to a :class:`geoalchemy2.elements.WKTElement` object. For example,
  without the ORM::

      conn.scalar(lake._mapping["geom"].ST_Area())

  and with the ORM::

      session.scalar(lake.geom.ST_Area())

.. warning::

    Some functions (e.g. `ST_Transform()`, `ST_Buffer()`, `ST_Intersection()` - see
    :data:`geoalchemy2._functions._FUNCTION_OVERLOADS` for the full list) can be used on
    several spatial types (:class:`geoalchemy2.types.Geometry`,
    :class:`geoalchemy2.types.Geography` and / or :class:`geoalchemy2.types.Raster`), and
    their return type depends on which type they were actually called with (e.g.
    ``ST_Transform`` returns a Geometry when called on a Geometry column but a Raster when
    called on a Raster column). GeoAlchemy2 detects this automatically from the arguments you
    pass, so no extra step is needed::

        s = select(
            func.ST_Transform(
                lake_table.c.raster,
                2154,
            ).label("transformed_raster")
        )

    You can still pass an explicit `type_=` argument to override the detected type, which is
    also the only option for functions not in that list that happen to support more than one
    spatial type.

Reference
---------

"""

import re

from sqlalchemy import inspect
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import annotation
from sqlalchemy.sql import functions
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.selectable import FromClause

from geoalchemy2 import elements
from geoalchemy2 import types
from geoalchemy2._functions import _FUNCTION_OVERLOADS
from geoalchemy2._functions import _FUNCTIONS
from geoalchemy2._functions_helpers import _get_docstring

_GeoFunctionBase: type[functions.GenericFunction]
_GeoFunctionParent: type[functions.GenericFunction]
try:
    # SQLAlchemy < 2

    from sqlalchemy.sql.functions import _GenericMeta  # type: ignore
    from sqlalchemy.util import with_metaclass  # type: ignore

    class _GeoGenericMeta(_GenericMeta):
        """Extend the registering mechanism of sqlalchemy.

        The spatial functions are registered in a specific registry for geoalchemy2.
        """

        _register = False

        def __init__(cls, clsname, bases, clsdict) -> None:
            # Register the function
            elements.function_registry.add(clsname.lower())

            super().__init__(clsname, bases, clsdict)

    _GeoFunctionBase = with_metaclass(_GeoGenericMeta, functions.GenericFunction)
    _GeoFunctionParent = functions.GenericFunction
except ImportError:
    # SQLAlchemy >= 2

    class GeoGenericFunction(functions.GenericFunction):
        def __init_subclass__(cls) -> None:
            if annotation.Annotated not in cls.__mro__:
                cls._register_geo_function(cls.__name__, cls.__dict__)
            super().__init_subclass__()

        @classmethod
        def _register_geo_function(cls, clsname, clsdict) -> None:
            # Check _register attribute status
            cls._register = getattr(cls, "_register", True)

            # Register the function if required
            if cls._register:
                elements.function_registry.add(clsname.lower())
            else:
                # Set _register to True to register child classes by default
                cls._register = True

    _GeoFunctionBase = GeoGenericFunction
    _GeoFunctionParent = GeoGenericFunction


class TableRowElement(ColumnElement):
    inherit_cache: bool = False
    """The cache is disabled for this class."""

    def __init__(self, selectable: FromClause) -> None:
        self.selectable = selectable

    @property
    def _from_objects(self) -> list[FromClause]:
        return [self.selectable]


class ST_AsGeoJSON(_GeoFunctionBase):  # type: ignore
    """Special process for the ST_AsGeoJSON() function.

    This is to be able to work with its feature version introduced in PostGIS 3.
    """

    name: str = "ST_AsGeoJSON"
    inherit_cache: bool = True
    """The cache is enabled for this class."""

    def __init__(self, *args, **kwargs) -> None:
        expr = kwargs.pop("expr", None)
        args_list = list(args)
        if expr is not None:
            args_list = [expr] + args_list
        for idx, element in enumerate(args_list):
            if isinstance(element, functions.Function):
                continue
            elif isinstance(element, elements._SpatialElement):
                if element.extended:
                    func_name = element.geom_from_extended_version
                    func_args = [element.data]
                else:
                    func_name = element.geom_from
                    func_args = [element.data, element.srid]
                args_list[idx] = getattr(functions.func, func_name)(*func_args)
            else:
                try:
                    insp = inspect(element)
                    if hasattr(insp, "selectable"):
                        args_list[idx] = TableRowElement(insp.selectable)
                except Exception:
                    continue

        _GeoFunctionParent.__init__(self, *args_list, **kwargs)

    __doc__ = (
        'Return the geometry as a GeoJSON "geometry" object, or the row as a '
        'GeoJSON feature" object (PostGIS 3 only). (Cf GeoJSON specifications RFC '
        "7946). 2D and 3D Geometries are both supported. GeoJSON only support SFS "
        "1.1 geometry types (no curve support for example). "
        "See https://postgis.net/docs/ST_AsGeoJSON.html"
    )


@compiles(TableRowElement)
def _compile_table_row_thing(element, compiler, **kw):
    # In order to get a name as reliably as possible, noting that some
    # SQL compilers don't say "table AS name" and might not have the "AS",
    # table and alias names can have spaces in them, etc., get it from
    # a column instead because that's what we want to be showing here anyway.

    compiled = compiler.process(list(element.selectable.columns)[0], **kw)

    # 1. check for exact name of the selectable is here, use that.
    # This way if it has dots and spaces and anything else in it, we
    # can get it w/ correct quoting
    schema = getattr(element.selectable, "schema", "")
    name = element.selectable.name
    pattern = rf"(.?{schema}.?\.)?(.?{name}.?)\."
    m = re.match(pattern, compiled)
    if m:
        return m.group(2)

    # 2. just split on the dot, assume anonymized name
    return compiled.split(".")[0]


# Exact-type lookup tables backing `_spatial_arg_type` below. Keyed by `type(...)` rather
# than checked via a chain of `isinstance()` calls, so classifying an argument is a couple of
# dict lookups regardless of how many GIS types/element classes exist. Every concrete
# subclass that can show up here has to be listed explicitly (dict lookups don't follow
# inheritance the way `isinstance` does), which is why both the plain and "Dynamic" element
# variants are present.
_GIS_COLUMN_TYPE_MARKERS: dict[type, type] = {
    types.Geometry: types.Geometry,
    types._DummyGeometry: types.Geometry,
    types.Geography: types.Geography,
    types.Raster: types.Raster,
}

_SPATIAL_ELEMENT_MARKERS: dict[type, type] = {
    elements.WKTElement: types.Geometry,
    elements.DynamicWKTElement: types.Geometry,
    elements.WKBElement: types.Geometry,
    elements.DynamicWKBElement: types.Geometry,
    elements.RasterElement: types.Raster,
    elements.DynamicRasterElement: types.Raster,
}


def _spatial_arg_type(value) -> type | None:
    """Best-effort classification of a function-call argument's GIS type.

    Returns ``types.Geometry``/``types.Geography``/``types.Raster`` when confidently
    identifiable (either a bound column/expression carrying one of those types, or a
    :class:`geoalchemy2.elements._SpatialElement` value), or ``None`` for anything else
    (plain literals like an SRID integer or an algorithm name string). ``None`` results are
    skipped when matching a signature in :data:`geoalchemy2._functions._FUNCTION_OVERLOADS` -
    only the relative order of the *spatial* arguments matters.
    """
    marker = _GIS_COLUMN_TYPE_MARKERS.get(type(getattr(value, "type", None)))
    if marker is not None:
        return marker
    return _SPATIAL_ELEMENT_MARKERS.get(type(value))


def _resolve_overload_type(name: str, args) -> type | None:
    """Return the return type override for calling function ``name`` with ``args``.

    Only returns a non-``None`` value if ``name`` is a known polymorphic function (see
    :data:`geoalchemy2._functions._FUNCTION_OVERLOADS`) and its spatial argument signature is
    recognized.
    """
    overloads = _FUNCTION_OVERLOADS.get(name.lower())
    if not overloads:
        return None
    signature = tuple(t for t in (_spatial_arg_type(a) for a in args) if t is not None)
    return overloads.get(signature)


class GenericFunction(_GeoFunctionBase):  # type: ignore
    """The base class for GeoAlchemy functions.

    This class inherits from ``sqlalchemy.sql.functions.GenericFunction``, so
    functions defined by subclassing this class can be given a fixed return
    type. For example, functions like :class:`ST_Buffer` and
    :class:`ST_Envelope` have their ``type`` attributes set to
    :class:`geoalchemy2.types.Geometry`.

    This class allows constructs like ``Lake.geom.ST_Buffer(2)``. In that
    case the ``Function`` instance is bound to an expression (``Lake.geom``
    here), and that expression is passed to the function when the function
    is actually called.

    If you need to use a function that GeoAlchemy does not provide you will
    certainly want to subclass this class. For example, if you need the
    ``ST_TransScale`` spatial function, which isn't (currently) natively
    supported by GeoAlchemy, you will write this::

        from geoalchemy2 import Geometry
        from geoalchemy2.functions import GenericFunction

        class ST_TransScale(GenericFunction):
            name = 'ST_TransScale'
            type = Geometry
    """

    # Set _register to False in order not to register this class in
    # sqlalchemy.sql.functions._registry. Only its children will be registered.
    _register = False

    def __init__(self, *args, **kwargs) -> None:
        expr = kwargs.pop("expr", None)
        args_list = list(args)
        if expr is not None:
            args_list = [expr] + args_list

        if "type_" not in kwargs:
            override = _resolve_overload_type(self.name, args_list)
            if override is not None:
                kwargs["type_"] = override

        for idx, elem in enumerate(args_list):
            if isinstance(elem, elements._SpatialElement):
                if elem.extended:
                    func_name = elem.geom_from_extended_version
                    func_args = [elem.data]
                else:
                    func_name = elem.geom_from
                    func_args = [elem.data, elem.srid]
                args_list[idx] = getattr(functions.func, func_name)(*func_args)
        _GeoFunctionParent.__init__(self, *args_list, **kwargs)


__all__ = [
    "GenericFunction",
    "ST_AsGeoJSON",
    "TableRowElement",
]


def _create_dynamic_functions() -> None:
    # Iterate through _FUNCTIONS and create GenericFunction classes dynamically
    for name, type_, doc in _FUNCTIONS:
        attributes = {
            "name": name,
            "inherit_cache": True,
            "__doc__": _get_docstring(name, doc, type_),
        }

        if type_ is not None:
            attributes["type"] = type_

        globals()[name] = type(name, (GenericFunction,), attributes)
        __all__.append(name)


_create_dynamic_functions()


def __dir__() -> list[str]:
    return __all__
