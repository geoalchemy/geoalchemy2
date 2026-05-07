"""This module defines specific functions for MySQL dialect."""

import contextlib
import hashlib
import re
import weakref
from collections.abc import Mapping
from typing import Any

from sqlalchemy import text
from sqlalchemy.dialects.mysql.base import ischema_names as _mysql_ischema_names
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import expression
from sqlalchemy.sql import visitors
from sqlalchemy.sql.elements import BindParameter
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.types import Integer
from sqlalchemy.types import LargeBinary
from sqlalchemy.types import String
from sqlalchemy.types import TypeDecorator

from geoalchemy2 import functions
from geoalchemy2.admin.dialects.common import _check_spatial_type
from geoalchemy2.admin.dialects.common import _spatial_idx_name
from geoalchemy2.admin.dialects.common import compile_bin_literal
from geoalchemy2.admin.dialects.common import setup_create_drop
from geoalchemy2.elements import WKBElement
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry

# Register Geometry, Geography and Raster to SQLAlchemy's reflection subsystems.
_mysql_ischema_names["geometry"] = Geometry
_mysql_ischema_names["point"] = Geometry
_mysql_ischema_names["linestring"] = Geometry
_mysql_ischema_names["polygon"] = Geometry
_mysql_ischema_names["multipoint"] = Geometry
_mysql_ischema_names["multilinestring"] = Geometry
_mysql_ischema_names["multipolygon"] = Geometry
_mysql_ischema_names["geometrycollection"] = Geometry


_POSSIBLE_TYPES = [
    "geometry",
    "point",
    "linestring",
    "polygon",
    "multipoint",
    "multilinestring",
    "multipolygon",
    "geometrycollection",
]

_MYSQL_DYNAMIC_EWKB_KEY_PREFIX = "_geoalchemy2_mysql_ewkb"
_MYSQL_DISABLE_DYNAMIC_EWKB_SPLIT_OPTION = "geoalchemy2_mysql_disable_dynamic_ewkb_split"
_MYSQL_WKB_HEX = re.compile(r"^[0-9A-Fa-f]+$")
_MYSQL_DYNAMIC_EWKB_SOURCE_BIND_CACHE: weakref.WeakKeyDictionary[Any, tuple[Any, ...]] = (
    weakref.WeakKeyDictionary()
)


def reflect_geometry_column(inspector, table, column_info):
    """Reflect a column of type Geometry with Postgresql dialect."""
    if not isinstance(column_info.get("type"), (Geometry, NullType)):
        return

    column_name = column_info.get("name")
    schema = table.schema or inspector.default_schema_name

    select_srid = "-1, " if inspector.dialect.name == "mariadb" else "SRS_ID, "

    # Check geometry type, SRID and if the column is nullable
    geometry_type_query = f"""SELECT DATA_TYPE, {select_srid}IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table.name}' and COLUMN_NAME = '{column_name}'"""
    if schema is not None:
        geometry_type_query += f""" and table_schema = '{schema}'"""
    geometry_type, srid, nullable_str = inspector.bind.execute(text(geometry_type_query)).one()
    is_nullable = str(nullable_str).lower() == "yes"

    if geometry_type not in _POSSIBLE_TYPES:
        return  # pragma: no cover

    # Check if the column has spatial index
    has_index_query = f"""SELECT DISTINCT
            INDEX_TYPE
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_NAME = '{table.name}' and COLUMN_NAME = '{column_name}'"""
    if schema is not None:
        has_index_query += f""" and TABLE_SCHEMA = '{schema}'"""
    spatial_index_res = inspector.bind.execute(text(has_index_query)).scalar()
    spatial_index = str(spatial_index_res).lower() == "spatial"

    # Set attributes
    column_info["type"] = Geometry(
        geometry_type=geometry_type.upper(),
        srid=srid,
        spatial_index=spatial_index,
        nullable=is_nullable,
        _spatial_index_reflected=True,
    )


def before_cursor_execute(conn, cursor, statement, parameters, context, executemany, convert=True):  # noqa: D417
    """Event handler to cast the parameters properly.

    Args:
        convert (bool): Trigger the conversion.
    """
    if convert:
        if isinstance(parameters, (tuple, list)):
            parameters = tuple(x.tobytes() if isinstance(x, memoryview) else x for x in parameters)
        elif isinstance(parameters, dict):
            for k in parameters:
                if isinstance(parameters[k], memoryview):
                    parameters[k] = parameters[k].tobytes()

    return statement, parameters


def before_create(table, bind, **kw):
    """Handle spatial indexes during the before_create event."""
    dialect, gis_cols, regular_cols = setup_create_drop(table, bind)

    # Remove the spatial indexes from the table metadata because they should not be
    # created during the table.create() step since the associated columns do not exist
    # at this time.
    table.info["_after_create_indexes"] = []
    current_indexes = set(table.indexes)
    for idx in current_indexes:
        for col in table.info["_saved_columns"]:
            if (_check_spatial_type(col.type, Geometry, dialect)) and col in idx.columns.values():
                table.indexes.remove(idx)
                if idx.name != _spatial_idx_name(table.name, col.name) or not getattr(
                    col.type, "spatial_index", False
                ):
                    table.info["_after_create_indexes"].append(idx)

    table.columns = table.info.pop("_saved_columns")


def after_create(table, bind, **kw):
    """Handle spatial indexes during the after_create event."""
    # Restore original column list including managed Geometry columns
    dialect = bind.dialect

    # table.columns = table.info.pop("_saved_columns")

    for col in table.columns:
        # Add spatial indices for the Geometry and Geography columns
        if (
            _check_spatial_type(col.type, (Geometry, Geography), dialect)
            and col.type.spatial_index is True
            and col.computed is None
            and not [i for i in table.indexes if col in i.columns.values()]
        ):
            sql = f"ALTER TABLE {table.name} ADD SPATIAL INDEX({col.name});"
            q = text(sql)
            bind.execute(q)

    for idx in table.info.pop("_after_create_indexes"):
        table.indexes.add(idx)


def before_drop(table, bind, **kw):
    return


def after_drop(table, bind, **kw):
    return


_MYSQL_FUNCTIONS = {"ST_AsEWKB": "ST_AsBinary", "ST_SetSRID": "ST_SRID"}


def _compiles_mysql(cls, fn):
    def _compile_mysql(element, compiler, **kw):
        return f"{fn}({compiler.process(element.clauses, **kw)})"

    compiles(getattr(functions, cls), "mysql")(_compile_mysql)


def register_mysql_mapping(mapping):
    """Register compilation mappings for the given functions.

    Args:
        mapping: Should have the following form::

                {
                    "function_name_1": "mysql_function_name_1",
                    "function_name_2": "mysql_function_name_2",
                    ...
                }
    """
    for cls, fn in mapping.items():
        _compiles_mysql(cls, fn)


register_mysql_mapping(_MYSQL_FUNCTIONS)


def _ewkb_to_wkb_data(value, *, as_hex=False):
    if value is None:
        return None

    if isinstance(value, bytearray):
        value = bytes(value)

    if isinstance(value, WKBElement):
        wkb_element = value.as_wkb()
    else:
        wkb_element = WKBElement(value, extended=None).as_wkb()

    wkb_data = wkb_element.desc if as_hex else wkb_element.data
    if isinstance(wkb_data, memoryview):
        return wkb_data.tobytes()
    if isinstance(wkb_data, str) and not as_hex:
        return WKBElement._data_from_desc(wkb_data)
    return wkb_data


def _ewkb_srid(value, *, default_srid=0):
    if value is None:
        return default_srid

    if isinstance(value, bytearray):
        value = bytes(value)

    if isinstance(value, WKBElement):
        if value.srid >= 0:
            return value.srid
        value = value.data

    if isinstance(value, (bytes, memoryview, str)):
        srid = WKBElement(value, extended=None).srid
        if srid >= 0:
            return srid

    return default_srid


class _MySQLEWKBBindType(TypeDecorator):
    impl = LargeBinary
    cache_ok = True

    def __init__(self, *, as_hex=False):
        super().__init__()
        self.as_hex = as_hex

    def load_dialect_impl(self, dialect):
        impl = String() if self.as_hex else LargeBinary()
        return dialect.type_descriptor(impl)

    def process_bind_param(self, value, dialect):
        return _ewkb_to_wkb_data(value, as_hex=self.as_hex)


class _MySQLDynamicEWKBSRIDBindType(TypeDecorator):
    impl = Integer
    cache_ok = True

    def __init__(self, *, default_srid=0):
        super().__init__()
        self.default_srid = default_srid

    def process_bind_param(self, value, dialect):
        return _ewkb_srid(value, default_srid=self.default_srid)


class _MySQLDynamicEWKBCallable:
    def __init__(self, source_callable):
        self.source_callable = source_callable
        self._consumer_count = 2
        self._pending = None
        self._remaining = 0

    def add_consumers(self, count):
        self._consumer_count += count

    def __call__(self):
        if self._remaining == 0:
            self._pending = self.source_callable()
            self._remaining = self._consumer_count

        self._remaining -= 1
        value = self._pending
        if self._remaining == 0:
            self._pending = None
        return value


def _is_bindparam_clause(clause):
    return isinstance(clause, BindParameter)


def _is_mysql_auto_constructor_bindparam(clause, constructor_name):
    return (
        _is_bindparam_clause(clause)
        and getattr(clause, "unique", False)
        and getattr(clause, "_orig_key", None) == constructor_name
    )


def _coerce_ewkb_bind_clause_to_wkb(wkb_clause, *, as_hex=False, literal=False):
    if not hasattr(wkb_clause, "value"):
        return wkb_clause

    if literal:
        wkb_clause, _ = _coerce_ewkb_clause_to_wkb(wkb_clause, as_hex=as_hex)
        return wkb_clause

    return expression.type_coerce(wkb_clause, _MySQLEWKBBindType(as_hex=as_hex))


def _mysql_dynamic_ewkb_bind_identifier(source_bind):
    return getattr(source_bind, "_identifying_key", source_bind.key)


def _mysql_dynamic_ewkb_bind_keys(source_bind, default_srid=0):
    source_name = getattr(source_bind, "_orig_key", None) or source_bind.key
    source_name = str(source_name)
    source_key = str(source_bind.key)
    key_token = re.sub(r"[^0-9A-Za-z_]+", "_", source_name).strip("_") or "param"
    srid_token = re.sub(r"[^0-9A-Za-z_]+", "_", str(default_srid)).strip("_") or "0"
    key_digest = hashlib.sha1(f"{source_key}:{default_srid}".encode()).hexdigest()[:8]
    key_base = f"{_MYSQL_DYNAMIC_EWKB_KEY_PREFIX}_{key_token}_srid_{srid_token}_{key_digest}"
    return f"{key_base}_wkb", f"{key_base}_srid"


def _is_wkb_hex(value):
    value = value.strip()
    return (
        len(value) >= 10
        and len(value) % 2 == 0
        and value[:2] in {"00", "01"}
        and _MYSQL_WKB_HEX.match(value) is not None
    )


def _value_may_need_dynamic_ewkb(value):
    if value is None:
        return True
    if isinstance(value, (bytes, bytearray, memoryview, WKBElement)):
        return True
    if isinstance(value, str):
        return _is_wkb_hex(value)
    return False


def _mapping_may_need_dynamic_ewkb(parameters):
    return isinstance(parameters, Mapping) and any(
        _value_may_need_dynamic_ewkb(value) for value in parameters.values()
    )


def _parameters_may_need_dynamic_ewkb(multiparams, params):
    if _mapping_may_need_dynamic_ewkb(params):
        return True

    for parameters in multiparams or ():  # noqa: SIM110
        if _mapping_may_need_dynamic_ewkb(parameters):
            return True

    return False


def _make_mysql_dynamic_ewkb_bind_clauses(
    wkb_clause,
    *,
    default_srid=0,
    as_hex=False,
    shared_callable=None,
):
    wkb_key, srid_key = _mysql_dynamic_ewkb_bind_keys(wkb_clause, default_srid=default_srid)
    bind_kwargs = {
        "required": wkb_clause.required,
    }
    if getattr(wkb_clause, "callable", None) is not None:
        if shared_callable is None:
            shared_callable = _MySQLDynamicEWKBCallable(wkb_clause.callable)
        bind_kwargs["callable_"] = shared_callable
    elif not wkb_clause.required:
        bind_kwargs["value"] = getattr(wkb_clause, "value", None)

    return (
        expression.bindparam(
            key=wkb_key,
            type_=_MySQLEWKBBindType(as_hex=as_hex),
            **bind_kwargs,
        ),
        expression.bindparam(
            key=srid_key,
            type_=_MySQLDynamicEWKBSRIDBindType(default_srid=default_srid),
            **bind_kwargs,
        ),
    )


def _get_mysql_dynamic_ewkb_bind_clauses(
    wkb_clause,
    compiler,
    *,
    default_srid=0,
    as_hex=False,
):
    cache = getattr(compiler, "_geoalchemy2_mysql_dynamic_ewkb_bind_cache", None)
    if cache is None:
        cache = {}
        compiler._geoalchemy2_mysql_dynamic_ewkb_bind_cache = cache

    cache_key = (_mysql_dynamic_ewkb_bind_identifier(wkb_clause), default_srid, as_hex)
    if cache_key not in cache:
        shared_callable = None
        if getattr(wkb_clause, "callable", None) is not None:
            callable_cache = getattr(
                compiler,
                "_geoalchemy2_mysql_dynamic_ewkb_callable_cache",
                None,
            )
            if callable_cache is None:
                callable_cache = {}
                compiler._geoalchemy2_mysql_dynamic_ewkb_callable_cache = callable_cache

            callable_key = _mysql_dynamic_ewkb_bind_identifier(wkb_clause)
            shared_callable = callable_cache.get(callable_key)
            if shared_callable is None:
                shared_callable = _MySQLDynamicEWKBCallable(wkb_clause.callable)
                callable_cache[callable_key] = shared_callable
            else:
                shared_callable.add_consumers(2)

        cache[cache_key] = _make_mysql_dynamic_ewkb_bind_clauses(
            wkb_clause,
            default_srid=default_srid,
            as_hex=as_hex,
            shared_callable=shared_callable,
        )
    return cache[cache_key]


def _default_srid(element):
    return _default_srid_from_type(element.type)


def _default_srid_from_type(spatial_type):
    srid = getattr(spatial_type, "srid", -1)
    return srid if srid >= 0 else 0


def _prepare_ewkb_wkb_clause(element, compiler, *, as_hex=False, **kw):
    clauses = list(element.clauses)
    original_wkb_clause = clauses[0]
    inferred_srid = None
    dynamic_srid_clause = None
    split_disabled = bool(
        getattr(compiler, "execution_options", {}).get(
            _MYSQL_DISABLE_DYNAMIC_EWKB_SPLIT_OPTION,
            False,
        )
    )
    user_bind = _is_bindparam_clause(
        original_wkb_clause
    ) and not _is_mysql_auto_constructor_bindparam(
        original_wkb_clause,
        "ST_GeomFromEWKB",
    )

    if (
        user_bind
        and len(clauses) == 1
        and not kw.get("literal_binds", False)
        and not split_disabled
    ):
        wkb_value_clause, dynamic_srid_clause = _get_mysql_dynamic_ewkb_bind_clauses(
            original_wkb_clause,
            compiler,
            default_srid=_default_srid(element),
            as_hex=as_hex,
        )
    elif user_bind and not kw.get("literal_binds", False):
        wkb_value_clause = _coerce_ewkb_bind_clause_to_wkb(original_wkb_clause, as_hex=as_hex)
    else:
        wkb_value_clause, inferred_srid = _coerce_ewkb_clause_to_wkb(
            original_wkb_clause,
            as_hex=as_hex,
        )

    return clauses, wkb_value_clause, inferred_srid, dynamic_srid_clause


def _compile_srid_clause(srid_clause, compiler, **kw):
    if _is_bindparam_clause(srid_clause) and not (
        _is_mysql_auto_constructor_bindparam(srid_clause, "ST_GeomFromEWKB")
        or _is_mysql_auto_constructor_bindparam(srid_clause, "ST_GeomFromWKB")
    ):
        return compiler.process(srid_clause, **kw)

    if hasattr(srid_clause, "value"):
        value = srid_clause.value
        if value is None:
            return compiler.process(srid_clause, **kw)
        try:
            srid = int(value)
        except (TypeError, ValueError):
            return compiler.process(srid_clause, **kw)
        return str(srid) if srid > 0 else None
    return compiler.process(srid_clause, **kw)


def _compile_srid_arg(element, clauses, inferred_srid, dynamic_srid_clause, compiler, **kw):
    if dynamic_srid_clause is not None:
        return compiler.process(dynamic_srid_clause, **kw)
    if len(clauses) > 1:
        return _compile_srid_clause(clauses[1], compiler, **kw)

    srid = inferred_srid if inferred_srid is not None else element.type.srid
    return str(srid) if srid > 0 else None


def _dml_ewkb_spatial_columns(clauseelement):
    table = getattr(clauseelement, "table", None)
    if table is None:
        return {}

    spatial_columns = {}
    for column in table.columns:
        from_text = getattr(column.type, "from_text", None)
        if from_text == "ST_GeomFromEWKB":
            spatial_columns[column.key] = column.type
    return spatial_columns


def _column_key(value):
    return getattr(value, "key", value)


def _iter_dml_value_pairs(value_container, table):
    if isinstance(value_container, Mapping):
        yield from value_container.items()
        return

    if isinstance(value_container, (list, tuple)):
        if value_container and all(
            isinstance(item, tuple) and len(item) == 2 for item in value_container
        ):
            yield from value_container
            return

        yield from zip(table.columns, value_container, strict=False)


def _iter_dml_source_bind_pairs(clauseelement):
    table = getattr(clauseelement, "table", None)

    values = getattr(clauseelement, "_values", None) or {}
    if values:
        yield from _iter_dml_value_pairs(values, table)

    ordered_values = getattr(clauseelement, "_ordered_values", None) or ()
    if ordered_values:
        yield from _iter_dml_value_pairs(ordered_values, table)

    for multi_values in getattr(clauseelement, "_multi_values", ()) or ():
        for values in multi_values:
            yield from _iter_dml_value_pairs(values, table)


def _wrap_mysql_ewkb_dml_value(column_key, value, spatial_columns):
    column_key = _column_key(column_key)
    spatial_type = spatial_columns.get(column_key)
    if spatial_type is None or not _is_bindparam_clause(value):
        return value, False
    if isinstance(value, functions.ST_GeomFromEWKB):
        return value, False
    return functions.ST_GeomFromEWKB(value, type_=spatial_type), True


def _wrap_mysql_ewkb_dml_value_container(value_container, table, spatial_columns):
    if isinstance(value_container, Mapping):
        changed = False
        wrapped_values = {}
        for column_key, value in value_container.items():
            wrapped_value, value_changed = _wrap_mysql_ewkb_dml_value(
                column_key,
                value,
                spatial_columns,
            )
            wrapped_values[column_key] = wrapped_value
            changed = changed or value_changed
        return wrapped_values, changed

    if isinstance(value_container, (list, tuple)):
        if value_container and all(
            isinstance(item, tuple) and len(item) == 2 for item in value_container
        ):
            changed = False
            wrapped_values = []
            for column_key, value in value_container:
                wrapped_value, value_changed = _wrap_mysql_ewkb_dml_value(
                    column_key,
                    value,
                    spatial_columns,
                )
                wrapped_values.append((column_key, wrapped_value))
                changed = changed or value_changed
            return wrapped_values, changed

        changed = False
        wrapped_values = []
        for column, value in zip(table.columns, value_container, strict=False):
            wrapped_value, value_changed = _wrap_mysql_ewkb_dml_value(
                column,
                value,
                spatial_columns,
            )
            wrapped_values.append(wrapped_value)
            changed = changed or value_changed
        return wrapped_values, changed

    return value_container, False


def _wrap_mysql_ewkb_multi_values(clauseelement):
    multi_values = getattr(clauseelement, "_multi_values", ()) or ()
    if not multi_values:
        return clauseelement

    spatial_columns = _dml_ewkb_spatial_columns(clauseelement)
    if not spatial_columns:
        return clauseelement

    table = getattr(clauseelement, "table", None)
    changed = False
    wrapped_multi_values = []
    for multi_value in multi_values:
        wrapped_multi_value = []
        for value_container in multi_value:
            wrapped_value_container, value_changed = _wrap_mysql_ewkb_dml_value_container(
                value_container,
                table,
                spatial_columns,
            )
            wrapped_multi_value.append(wrapped_value_container)
            changed = changed or value_changed
        wrapped_multi_values.append(wrapped_multi_value)

    if not changed:
        return clauseelement

    wrapped_clauseelement = clauseelement._generate()
    wrapped_clauseelement._multi_values = tuple(wrapped_multi_values)
    return wrapped_clauseelement


def _collect_mysql_dynamic_ewkb_dml_source_binds(clauseelement):
    spatial_columns = _dml_ewkb_spatial_columns(clauseelement)
    if not spatial_columns:
        return ()

    source_binds = []
    seen_bind_keys = set()
    has_value_containers = False
    for column_key, value in _iter_dml_source_bind_pairs(clauseelement):
        has_value_containers = True
        column_key = _column_key(column_key)
        if column_key not in spatial_columns:
            continue

        default_srid = _default_srid_from_type(spatial_columns[column_key])
        if _is_bindparam_clause(value):
            source_bind = value
        elif isinstance(value, functions.ST_GeomFromEWKB):
            clauses = list(value.clauses)
            if len(clauses) != 1 or not _is_bindparam_clause(clauses[0]):
                continue
            if _is_mysql_auto_constructor_bindparam(clauses[0], "ST_GeomFromEWKB"):
                continue
            source_bind = clauses[0]
            default_srid = _default_srid(value)
        else:
            continue

        bind_key = (_mysql_dynamic_ewkb_bind_identifier(source_bind), default_srid)
        if bind_key in seen_bind_keys:
            continue
        seen_bind_keys.add(bind_key)
        source_binds.append((source_bind, default_srid))

    if has_value_containers:
        return tuple(source_binds)

    for column_key, spatial_type in spatial_columns.items():
        default_srid = _default_srid_from_type(spatial_type)
        source_bind = expression.bindparam(column_key)
        bind_key = (_mysql_dynamic_ewkb_bind_identifier(source_bind), default_srid)
        if bind_key in seen_bind_keys:
            continue
        seen_bind_keys.add(bind_key)
        source_binds.append((source_bind, default_srid))

    return tuple(source_binds)


def _collect_mysql_dynamic_ewkb_source_binds_uncached(clauseelement):
    if not hasattr(clauseelement, "get_children"):
        return ()

    source_binds = []
    seen_bind_keys = set()
    for element in visitors.iterate(clauseelement):
        if not isinstance(element, functions.ST_GeomFromEWKB):
            continue

        clauses = list(element.clauses)
        if len(clauses) != 1 or not _is_bindparam_clause(clauses[0]):
            continue
        if _is_mysql_auto_constructor_bindparam(clauses[0], "ST_GeomFromEWKB"):
            continue

        default_srid = _default_srid(element)
        bind_key = (_mysql_dynamic_ewkb_bind_identifier(clauses[0]), default_srid)
        if bind_key in seen_bind_keys:
            continue
        seen_bind_keys.add(bind_key)
        source_binds.append((clauses[0], default_srid))

    for source_bind, default_srid in _collect_mysql_dynamic_ewkb_dml_source_binds(clauseelement):
        bind_key = (_mysql_dynamic_ewkb_bind_identifier(source_bind), default_srid)
        if bind_key in seen_bind_keys:
            continue
        seen_bind_keys.add(bind_key)
        source_binds.append((source_bind, default_srid))

    return tuple(source_binds)


def _collect_mysql_dynamic_ewkb_source_binds(clauseelement):
    try:
        return _MYSQL_DYNAMIC_EWKB_SOURCE_BIND_CACHE[clauseelement]
    except KeyError:
        pass
    except TypeError:
        return _collect_mysql_dynamic_ewkb_source_binds_uncached(clauseelement)

    source_binds = _collect_mysql_dynamic_ewkb_source_binds_uncached(clauseelement)
    with contextlib.suppress(TypeError):
        _MYSQL_DYNAMIC_EWKB_SOURCE_BIND_CACHE[clauseelement] = source_binds
    return source_binds


def _compile_mysql_statement_bind_name_map(clauseelement, dialect):
    if not hasattr(clauseelement, "compile"):
        return {}

    if hasattr(clauseelement, "execution_options"):
        clauseelement = clauseelement.execution_options(
            **{_MYSQL_DISABLE_DYNAMIC_EWKB_SPLIT_OPTION: True}
        )

    compiled = clauseelement.compile(dialect=dialect)
    bind_name_map = {}
    for bind, compiled_name in compiled.bind_names.items():
        bind_name_map.setdefault(
            getattr(bind, "_identifying_key", bind.key),
            compiled_name,
        )
    return bind_name_map


def _iter_parameter_mappings(multiparams, params):
    if isinstance(params, Mapping):
        yield params
    for parameters in multiparams or ():
        if isinstance(parameters, Mapping):
            yield parameters


def _parameters_include_any_key(multiparams, params, keys):
    keys = tuple(key for key in keys if key is not None)
    if not keys:
        return False

    for parameters in _iter_parameter_mappings(multiparams, params):
        if any(key in parameters for key in keys):
            return True
    return False


def _source_bind_candidate_keys(source_bind):
    candidate_keys = []
    for candidate_key in (source_bind.key, getattr(source_bind, "_orig_key", None)):
        if candidate_key is not None and candidate_key not in candidate_keys:
            candidate_keys.append(candidate_key)
    return candidate_keys


def _get_mysql_dynamic_ewkb_bind_mappings(clauseelement, dialect, multiparams=(), params=None):
    source_binds = _collect_mysql_dynamic_ewkb_source_binds(clauseelement)
    if not source_binds:
        return ()

    if params is None:
        params = {}

    statement_bind_name_map = None
    dynamic_bind_mappings = []
    for source_bind, default_srid in source_binds:
        source_identifier = _mysql_dynamic_ewkb_bind_identifier(source_bind)
        candidate_keys = _source_bind_candidate_keys(source_bind)
        if not _parameters_include_any_key(multiparams, params, candidate_keys):
            if statement_bind_name_map is None:
                statement_bind_name_map = _compile_mysql_statement_bind_name_map(
                    clauseelement,
                    dialect,
                )
            compiled_name = statement_bind_name_map.get(source_identifier)
            if compiled_name is not None and compiled_name not in candidate_keys:
                candidate_keys.append(compiled_name)

        wkb_key, srid_key = _mysql_dynamic_ewkb_bind_keys(
            source_bind,
            default_srid=default_srid,
        )
        dynamic_bind_mappings.append((tuple(candidate_keys), wkb_key, srid_key))

    return tuple(dynamic_bind_mappings)


def _expand_mysql_dynamic_ewkb_param_mapping(parameters, dynamic_bind_mappings):
    if not isinstance(parameters, Mapping):
        return parameters, False

    expanded_parameters = parameters
    changed = False
    for source_keys, wkb_key, srid_key in dynamic_bind_mappings:
        source_key = next((key for key in source_keys if key in parameters), None)
        if source_key is None:
            continue

        if wkb_key in parameters and srid_key in parameters:
            continue

        if expanded_parameters is parameters:
            expanded_parameters = dict(parameters)

        source_value = parameters[source_key]
        expanded_parameters.setdefault(wkb_key, source_value)
        expanded_parameters.setdefault(srid_key, source_value)
        changed = True

    return expanded_parameters, changed


def before_execute(conn, clauseelement, multiparams, params, execution_options):
    if isinstance(clauseelement, TextClause):
        return clauseelement, multiparams, params

    has_multi_values = bool(getattr(clauseelement, "_multi_values", ()) or ())
    if has_multi_values:
        clauseelement = _wrap_mysql_ewkb_multi_values(clauseelement)
    if not _parameters_may_need_dynamic_ewkb(multiparams, params):
        return clauseelement, multiparams, params

    dynamic_bind_mappings = _get_mysql_dynamic_ewkb_bind_mappings(
        clauseelement,
        conn.dialect,
        multiparams,
        params,
    )
    if not dynamic_bind_mappings:
        return clauseelement, multiparams, params

    multiparams_changed = False
    expanded_multiparams = multiparams
    if multiparams:
        expanded_values = []
        for value in multiparams:
            expanded_value, value_changed = _expand_mysql_dynamic_ewkb_param_mapping(
                value,
                dynamic_bind_mappings,
            )
            expanded_values.append(expanded_value)
            multiparams_changed = multiparams_changed or value_changed
        if multiparams_changed:
            expanded_multiparams = tuple(expanded_values)

    expanded_params, params_changed = _expand_mysql_dynamic_ewkb_param_mapping(
        params,
        dynamic_bind_mappings,
    )

    if multiparams_changed or params_changed:
        return clauseelement, expanded_multiparams, expanded_params
    return clauseelement, multiparams, params


def _compile_GeomFromText_MySql(element, compiler, **kw):
    identifier = "ST_GeomFromText"
    compiled = compiler.process(element.clauses, **kw)
    srid = element.type.srid

    if srid > 0:
        return f"{identifier}({compiled}, {srid})"
    else:
        return f"{identifier}({compiled})"


def _coerce_ewkb_clause_to_wkb(wkb_clause, *, as_hex=False):
    try:
        wkb_data = wkb_clause.value
    except AttributeError:
        return wkb_clause, None

    if isinstance(wkb_data, WKBElement):
        wkb_element = wkb_data.as_wkb()
    elif isinstance(wkb_data, (bytes, memoryview, str)):
        wkb_element = WKBElement(wkb_data).as_wkb()
    else:
        return wkb_clause, None

    wkb_data = wkb_element.desc if as_hex else wkb_element.data
    srid = wkb_element.srid if wkb_element.srid > 0 else None

    if isinstance(wkb_data, memoryview):
        wkb_data = wkb_data.tobytes()
    if isinstance(wkb_data, str) and not as_hex:
        wkb_data = WKBElement._data_from_desc(wkb_data)

    bindparam_args = {
        "key": wkb_clause.key,
        "value": wkb_data,
        "unique": True,
    }
    if not as_hex:
        bindparam_args["type_"] = wkb_clause.type

    return expression.bindparam(**bindparam_args), srid


def _compile_GeomFromWKB_MySql(element, compiler, *, identifier=None, coerce_ewkb=False, **kw):
    identifier = identifier or element.identifier

    # Store the SRID
    clauses = list(element.clauses)
    inferred_srid = None
    dynamic_srid_clause = None
    if coerce_ewkb:
        clauses, wkb_value_clause, inferred_srid, dynamic_srid_clause = _prepare_ewkb_wkb_clause(
            element,
            compiler,
            **kw,
        )
    else:
        wkb_value_clause = clauses[0]

    if kw.get("literal_binds", False):
        wkb_clause = compile_bin_literal(wkb_value_clause)
        prefix = "unhex("
        suffix = ")"
    else:
        wkb_clause = wkb_value_clause
        prefix = ""
        suffix = ""

    compiled = compiler.process(wkb_clause, **kw)
    compiled_srid = _compile_srid_arg(
        element,
        clauses,
        inferred_srid,
        dynamic_srid_clause,
        compiler,
        **kw,
    )

    if compiled_srid is not None:
        return f"{identifier}({prefix}{compiled}{suffix}, {compiled_srid})"
    else:
        return f"{identifier}({prefix}{compiled}{suffix})"


@compiles(functions.ST_GeomFromText, "mysql")  # type: ignore
def _MySQL_ST_GeomFromText(element, compiler, **kw):
    return _compile_GeomFromText_MySql(element, compiler, **kw)


@compiles(functions.ST_GeomFromEWKT, "mysql")  # type: ignore
def _MySQL_ST_GeomFromEWKT(element, compiler, **kw):
    return _compile_GeomFromText_MySql(element, compiler, **kw)


@compiles(functions.ST_GeomFromWKB, "mysql")  # type: ignore
def _MySQL_ST_GeomFromWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_MySql(element, compiler, **kw)


@compiles(functions.ST_GeomFromEWKB, "mysql")  # type: ignore
def _MySQL_ST_GeomFromEWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_MySql(
        element, compiler, identifier="ST_GeomFromWKB", coerce_ewkb=True, **kw
    )
