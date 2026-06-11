"""This module defines specific functions for MySQL dialect."""

import hashlib
import re
from collections.abc import Mapping

from sqlalchemy import Integer
from sqlalchemy import text
from sqlalchemy.dialects.mysql.base import ischema_names as _mysql_ischema_names
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import expression
from sqlalchemy.sql import visitors
from sqlalchemy.sql.elements import BindParameter
from sqlalchemy.sql.elements import Null
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.types import LargeBinary
from sqlalchemy.types import String
from sqlalchemy.types import TypeDecorator

from geoalchemy2 import _wkb_wkt
from geoalchemy2 import functions
from geoalchemy2._wkb_wkt import is_known_srid
from geoalchemy2.admin.dialects.common import _check_spatial_type
from geoalchemy2.admin.dialects.common import _spatial_idx_name
from geoalchemy2.admin.dialects.common import compile_bin_literal
from geoalchemy2.admin.dialects.common import setup_create_drop
from geoalchemy2.admin.dialects.common import unwrap_wkb_constructor_clauses
from geoalchemy2.elements import WKBElement
from geoalchemy2.exc import ArgumentError
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
_MYSQL_DYNAMIC_EWKB_KEY_PREFIX = "_geoalchemy2_mysql_ewkb"
_MYSQL_DISABLE_DYNAMIC_EWKB_SPLIT_OPTION = "geoalchemy2_mysql_disable_dynamic_ewkb_split"


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


def _compile_GeomFromText_MySql(element, compiler, **kw):
    identifier = "ST_GeomFromText"
    compiled = compiler.process(element.clauses, **kw)
    srid = element.type.srid

    if is_known_srid(srid):
        return f"{identifier}({compiled}, {srid})"
    else:
        return f"{identifier}({compiled})"


def _known_wkb_bindvalue_srids(bindvalue):
    if isinstance(bindvalue, bytearray):
        bindvalue = bytes(bindvalue)

    srids = []
    if isinstance(bindvalue, WKBElement):
        if is_known_srid(bindvalue.srid):
            srids.append(bindvalue.srid)
        bindvalue = bindvalue.data

    if isinstance(bindvalue, (bytes, bytearray, memoryview, str)):
        srid = _wkb_wkt.wkb_srid(bindvalue)
        if is_known_srid(srid):
            srids.append(srid)

    return srids


def _validate_wkb_bindvalue_srid(bindvalue, column_srid):
    if not is_known_srid(column_srid):
        return

    srids = _known_wkb_bindvalue_srids(bindvalue)
    for srid in srids:
        if srid != column_srid:
            raise ArgumentError(
                f"The SRID ({srid}) of the supplied value is different "
                f"from the one of the column ({column_srid})"
            )


class _MySQLEWKBProcessorContext:
    def __init__(self):
        self.fixed_srids = set()
        self.reject_known_srid_without_argument = False

    def add_context(self, *, fixed_srid=None, has_srid_argument=False):
        if is_known_srid(fixed_srid):
            self.fixed_srids.add(fixed_srid)
        elif not has_srid_argument:
            self.reject_known_srid_without_argument = True


def _validate_wkb_bindvalue_context(bindvalue, processor_context):
    if processor_context is None:
        return

    srids = _known_wkb_bindvalue_srids(bindvalue)
    if processor_context.reject_known_srid_without_argument and srids:
        raise ArgumentError(
            "Runtime ST_GeomFromEWKB values with an embedded SRID require "
            "a fixed column SRID or an explicit SRID argument for MySQL/MariaDB compilation"
        )

    for fixed_srid in sorted(processor_context.fixed_srids):
        for srid in srids:
            if srid != fixed_srid:
                raise ArgumentError(
                    f"The SRID ({srid}) of the supplied value is different "
                    f"from the one of the column ({fixed_srid})"
                )


def _ewkb_to_wkb_data(
    value,
    *,
    as_hex=False,
    fixed_srid=None,
    has_srid_argument=False,
    processor_context=None,
):
    if value is None:
        return None

    if processor_context is not None:
        _validate_wkb_bindvalue_context(value, processor_context)
    else:
        _validate_wkb_bindvalue_srid(value, fixed_srid)
    if processor_context is None and not is_known_srid(fixed_srid) and not has_srid_argument:
        srids = _known_wkb_bindvalue_srids(value)
        if srids:
            raise ArgumentError(
                "Runtime ST_GeomFromEWKB values with an embedded SRID require "
                "a fixed column SRID or an explicit SRID argument for MySQL/MariaDB compilation"
            )

    if isinstance(value, bytearray):
        value = bytes(value)

    if isinstance(value, WKBElement):
        value = value.data
    if as_hex:
        return _wkb_wkt.to_hex_wkb_no_srid(value).lower()
    return _wkb_wkt.to_wkb_no_srid(value)


def _ewkb_srid_value(value, *, default_srid=None):
    if isinstance(value, tuple) and len(value) == 2:
        value, runtime_default_srid = value
        if runtime_default_srid is not None:
            default_srid = runtime_default_srid

    srids = _known_wkb_bindvalue_srids(value)
    if srids:
        return srids[0]
    if default_srid is not None:
        return default_srid
    return None


class _MySQLEWKBBindType(TypeDecorator):
    impl = LargeBinary
    cache_ok = False

    def __init__(
        self,
        *,
        as_hex=False,
        fixed_srid=None,
        has_srid_argument=False,
        processor_context=None,
    ):
        super().__init__()
        self.as_hex = as_hex
        self.fixed_srid = fixed_srid
        self.has_srid_argument = has_srid_argument
        self._processor_context = processor_context

    def load_dialect_impl(self, dialect):
        impl = String() if self.as_hex else LargeBinary()
        return dialect.type_descriptor(impl)

    def process_bind_param(self, value, dialect):
        return _ewkb_to_wkb_data(
            value,
            as_hex=self.as_hex,
            fixed_srid=self.fixed_srid,
            has_srid_argument=self.has_srid_argument,
            processor_context=self._processor_context,
        )


class _MySQLDynamicEWKBSRIDBindType(TypeDecorator):
    impl = Integer
    cache_ok = True

    def __init__(self, *, default_srid=None):
        super().__init__()
        self.default_srid = default_srid

    def process_bind_param(self, value, dialect):
        return _ewkb_srid_value(value, default_srid=self.default_srid)


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


class _MySQLDynamicEWKBSRIDCallable:
    def __init__(self, source_callable, default_srid=None):
        self.source_callable = source_callable
        self.default_srid = default_srid

    def __call__(self):
        return self.source_callable(), self.default_srid


def _is_bindparam_clause(clause):
    return isinstance(clause, BindParameter)


def _is_mysql_auto_constructor_bindparam(clause, constructor_name):
    return (
        _is_bindparam_clause(clause)
        and getattr(clause, "unique", False)
        and getattr(clause, "_orig_key", None) == constructor_name
    )


def _is_user_bindparam_clause(clause, *constructor_names):
    return _is_bindparam_clause(clause) and not any(
        _is_mysql_auto_constructor_bindparam(clause, constructor_name)
        for constructor_name in constructor_names
    )


def _is_runtime_bindparam_clause(clause):
    return _is_bindparam_clause(clause) and (
        getattr(clause, "required", False) or getattr(clause, "callable", None) is not None
    )


def _is_effective_srid_clause(srid_clause):
    if isinstance(srid_clause, Null):
        return False

    if _is_user_bindparam_clause(
        srid_clause,
        "ST_GeomFromEWKB",
        "ST_GeomFromWKB",
    ) and _is_runtime_bindparam_clause(srid_clause):
        return True

    if hasattr(srid_clause, "value"):
        value = srid_clause.value
        if value is None:
            return False
        try:
            srid = int(value)
        except (TypeError, ValueError):
            return True
        return is_known_srid(srid)
    return True


def _has_effective_srid_argument(clauses):
    return len(clauses) > 1 and _is_effective_srid_clause(clauses[1])


def _ewkb_processor_fixed_srid(element, clauses, inferred_srid=None):
    if inferred_srid is not None:
        return inferred_srid
    if len(clauses) == 1 and is_known_srid(element.type.srid):
        return element.type.srid
    return None


def _runtime_bind_identifier(source_bind):
    return getattr(source_bind, "_identifying_key", source_bind.key)


def _mysql_dynamic_ewkb_source_token(source_bind, compiler=None):
    source_name = getattr(source_bind, "_orig_key", None) or source_bind.key
    if not getattr(source_bind, "unique", False):
        return str(source_name)

    source_identifier = _runtime_bind_identifier(source_bind)
    if compiler is None:
        return str(source_name)

    source_ordinals = getattr(compiler, "_geoalchemy2_mysql_dynamic_ewkb_source_ordinals", None)
    if source_ordinals is None:
        source_ordinals = {}
        compiler._geoalchemy2_mysql_dynamic_ewkb_source_ordinals = source_ordinals

    ordinal = source_ordinals.get(source_identifier)
    if ordinal is None:
        ordinal = len(source_ordinals) + 1
        source_ordinals[source_identifier] = ordinal
    return f"{source_name}_{ordinal}"


def _mysql_dynamic_ewkb_bind_keys(source_bind, *, source_token=None):
    source_name = source_token or getattr(source_bind, "_orig_key", None) or source_bind.key
    source_name = str(source_name)
    key_token = re.sub(r"[^0-9A-Za-z_]+", "_", source_name).strip("_") or "param"
    key_digest = hashlib.sha1(source_name.encode()).hexdigest()[:8]
    key_base = f"{_MYSQL_DYNAMIC_EWKB_KEY_PREFIX}_{key_token}_{key_digest}"
    return f"{key_base}_wkb", f"{key_base}_srid"


def _make_mysql_dynamic_ewkb_bind_clauses(
    wkb_clause,
    *,
    default_srid=None,
    as_hex=False,
    shared_callable=None,
    source_token=None,
):
    wkb_key, srid_key = _mysql_dynamic_ewkb_bind_keys(
        wkb_clause,
        source_token=source_token,
    )
    wkb_bind_kwargs = {
        "required": wkb_clause.required,
    }
    srid_bind_kwargs = dict(wkb_bind_kwargs)
    if getattr(wkb_clause, "callable", None) is not None:
        if shared_callable is None:
            shared_callable = _MySQLDynamicEWKBCallable(wkb_clause.callable)
        wkb_bind_kwargs["callable_"] = shared_callable
        srid_bind_kwargs["callable_"] = _MySQLDynamicEWKBSRIDCallable(
            shared_callable,
            default_srid=default_srid,
        )
    elif not wkb_clause.required:
        value = getattr(wkb_clause, "value", None)
        wkb_bind_kwargs["value"] = value
        srid_bind_kwargs["value"] = (value, default_srid)

    return (
        expression.bindparam(
            key=wkb_key,
            type_=_MySQLEWKBBindType(as_hex=as_hex, has_srid_argument=True),
            **wkb_bind_kwargs,
        ),
        expression.bindparam(
            key=srid_key,
            type_=_MySQLDynamicEWKBSRIDBindType(default_srid=default_srid),
            **srid_bind_kwargs,
        ),
    )


def _get_mysql_dynamic_ewkb_bind_clauses(
    wkb_clause,
    compiler,
    *,
    default_srid=None,
    as_hex=False,
):
    cache = getattr(compiler, "_geoalchemy2_mysql_dynamic_ewkb_bind_cache", None)
    if cache is None:
        cache = {}
        compiler._geoalchemy2_mysql_dynamic_ewkb_bind_cache = cache

    source_token = _mysql_dynamic_ewkb_source_token(wkb_clause, compiler)
    cache_key = (source_token, default_srid, as_hex)
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

            callable_key = _runtime_bind_identifier(wkb_clause)
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
            source_token=source_token,
        )
    return cache[cache_key]


def _runtime_ewkb_processor_context(
    compiler,
    source_bind,
    *,
    fixed_srid=None,
    has_srid_argument=False,
):
    contexts = getattr(compiler, "_geoalchemy2_mysql_ewkb_processor_contexts", None)
    if contexts is None:
        contexts = {}
        compiler._geoalchemy2_mysql_ewkb_processor_contexts = contexts

    context_key = _runtime_bind_identifier(source_bind)
    processor_context = contexts.get(context_key)
    if processor_context is None:
        processor_context = _MySQLEWKBProcessorContext()
        contexts[context_key] = processor_context

    processor_context.add_context(
        fixed_srid=fixed_srid,
        has_srid_argument=has_srid_argument,
    )
    return processor_context


def _infer_srid_from_bind_value(wkb_clause):
    if not hasattr(wkb_clause, "value"):
        return None

    srids = _known_wkb_bindvalue_srids(wkb_clause.value)
    return srids[0] if srids else None


def _dynamic_ewkb_default_srid(element, clauses):
    default_srid = _infer_srid_from_bind_value(clauses[0])
    if default_srid is not None:
        return default_srid
    if (
        getattr(clauses[0], "callable", None) is not None
        and not _has_effective_srid_argument(clauses)
        and not is_known_srid(element.type.srid)
    ):
        return 0
    return None


def _coerce_ewkb_clause_to_wkb(wkb_clause, *, as_hex=False):
    try:
        wkb_data = wkb_clause.value
    except AttributeError:
        return wkb_clause, None

    if isinstance(wkb_data, WKBElement):
        srid = wkb_data.srid if is_known_srid(wkb_data.srid) else None
        wkb_data = wkb_data.data
    elif isinstance(wkb_data, (bytes, bytearray, memoryview, str)):
        srid = _wkb_wkt.wkb_srid(wkb_data)
        srid = srid if is_known_srid(srid) else None
    else:
        return wkb_clause, None

    if as_hex:
        wkb_data = _wkb_wkt.to_hex_wkb_no_srid(wkb_data).lower()
    else:
        wkb_data = _wkb_wkt.to_wkb_no_srid(wkb_data)

    bindparam_args = {
        "key": wkb_clause.key,
        "value": wkb_data,
        "unique": True,
    }
    if not as_hex:
        bindparam_args["type_"] = wkb_clause.type

    return expression.bindparam(**bindparam_args), srid


def _coerce_ewkb_bind_clause_to_wkb(
    wkb_clause,
    *,
    as_hex=False,
    literal=False,
    fixed_srid=None,
    has_srid_argument=False,
    processor_context=None,
):
    if not hasattr(wkb_clause, "value"):
        return wkb_clause

    if literal:
        wkb_clause, _ = _coerce_ewkb_clause_to_wkb(wkb_clause, as_hex=as_hex)
        return wkb_clause

    return expression.type_coerce(
        wkb_clause,
        _MySQLEWKBBindType(
            as_hex=as_hex,
            fixed_srid=fixed_srid,
            has_srid_argument=has_srid_argument,
            processor_context=processor_context,
        ),
    )


def _dynamic_inferred_srid_bind_clauses(
    element,
    compiler,
    original_wkb_clause,
    clauses,
    *,
    inferred_srid=None,
    as_hex=False,
    split_disabled=False,
):
    if (
        split_disabled
        or inferred_srid is None
        or _has_effective_srid_argument(clauses)
        or not _is_bindparam_clause(original_wkb_clause)
    ):
        return None, None

    return _get_mysql_dynamic_ewkb_bind_clauses(
        original_wkb_clause,
        compiler,
        default_srid=inferred_srid,
        as_hex=as_hex,
    )


def _coerce_known_ewkb_clause_to_wkb(
    element,
    compiler,
    original_wkb_clause,
    clauses,
    *,
    as_hex=False,
    preserve_user_bind=True,
):
    wkb_value_clause, inferred_srid = _coerce_ewkb_clause_to_wkb(
        original_wkb_clause,
        as_hex=as_hex,
    )
    if (
        not preserve_user_bind
        or wkb_value_clause is original_wkb_clause
        or not _is_user_bindparam_clause(original_wkb_clause, "ST_GeomFromEWKB")
    ):
        return wkb_value_clause, inferred_srid

    has_srid_argument = _has_effective_srid_argument(clauses)
    fixed_srid = None
    if not has_srid_argument:
        fixed_srid = _ewkb_processor_fixed_srid(
            element,
            clauses,
            inferred_srid=inferred_srid,
        )
    processor_context = _runtime_ewkb_processor_context(
        compiler,
        original_wkb_clause,
        fixed_srid=fixed_srid,
        has_srid_argument=has_srid_argument,
    )
    wkb_value_clause = _coerce_ewkb_bind_clause_to_wkb(
        original_wkb_clause,
        as_hex=as_hex,
        fixed_srid=fixed_srid,
        has_srid_argument=has_srid_argument,
        processor_context=processor_context,
    )
    return wkb_value_clause, inferred_srid


def _compile_srid_clause(srid_clause, compiler, **kw):
    if isinstance(srid_clause, Null):
        return None

    is_user_bindparam = _is_user_bindparam_clause(
        srid_clause,
        "ST_GeomFromEWKB",
        "ST_GeomFromWKB",
    )
    if is_user_bindparam and _is_runtime_bindparam_clause(srid_clause):
        return compiler.process(srid_clause, **kw)

    if hasattr(srid_clause, "value"):
        value = srid_clause.value
        if value is None:
            return None
        try:
            srid = int(value)
        except (TypeError, ValueError):
            return compiler.process(srid_clause, **kw)
        if not is_known_srid(srid):
            return None
        return compiler.process(srid_clause, **kw) if is_user_bindparam else str(srid)
    return compiler.process(srid_clause, **kw)


def _compile_srid_arg(element, clauses, inferred_srid, compiler, *, srid_clause=None, **kw):
    if len(clauses) > 1:
        compiled_srid = _compile_srid_clause(clauses[1], compiler, **kw)
        if compiled_srid is not None:
            return compiled_srid
        if srid_clause is not None:
            return compiler.process(srid_clause, **kw)
        return str(inferred_srid) if inferred_srid is not None else None

    if srid_clause is not None:
        return compiler.process(srid_clause, **kw)

    srid = inferred_srid if inferred_srid is not None else element.type.srid
    return str(srid) if is_known_srid(srid) else None


def _dynamic_ewkb_split_disabled(compiler):
    return bool(
        getattr(compiler, "execution_options", {}).get(
            _MYSQL_DISABLE_DYNAMIC_EWKB_SPLIT_OPTION,
            False,
        )
    )


def _collect_mysql_dynamic_ewkb_source_binds(clauseelement, dialect):
    if not hasattr(clauseelement, "get_children"):
        return ()

    source_binds = []
    seen_bind_keys = set()
    for element in visitors.iterate(clauseelement):
        if not isinstance(element, functions.ST_GeomFromEWKB):
            continue

        clauses = list(element.clauses)
        if len(clauses) < 1 or not _is_bindparam_clause(clauses[0]):
            continue
        if _has_effective_srid_argument(clauses):
            continue

        default_srid = _dynamic_ewkb_default_srid(element, clauses)
        if default_srid is None:
            continue

        bind_key = (_runtime_bind_identifier(clauses[0]), default_srid)
        if bind_key in seen_bind_keys:
            continue
        seen_bind_keys.add(bind_key)
        source_binds.append((clauses[0], default_srid))

    return tuple(source_binds)


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
            _runtime_bind_identifier(bind),
            compiled_name,
        )
    return bind_name_map


def _get_mysql_dynamic_ewkb_bind_mappings(clauseelement, dialect):
    source_binds = _collect_mysql_dynamic_ewkb_source_binds(clauseelement, dialect)
    if not source_binds:
        return ()

    statement_bind_name_map = _compile_mysql_statement_bind_name_map(clauseelement, dialect)
    dynamic_bind_mappings = []
    unique_source_ordinals = {}
    for source_bind, default_srid in source_binds:
        source_identifier = _runtime_bind_identifier(source_bind)
        if getattr(source_bind, "unique", False):
            ordinal = unique_source_ordinals.get(source_identifier)
            if ordinal is None:
                ordinal = len(unique_source_ordinals) + 1
                unique_source_ordinals[source_identifier] = ordinal
            source_token = f"{getattr(source_bind, '_orig_key', None) or source_bind.key}_{ordinal}"
        else:
            source_token = _mysql_dynamic_ewkb_source_token(source_bind)

        candidate_keys = []
        for candidate_key in (
            source_bind.key,
            getattr(source_bind, "_orig_key", None),
            statement_bind_name_map.get(source_identifier),
        ):
            if candidate_key is not None and candidate_key not in candidate_keys:
                candidate_keys.append(candidate_key)

        wkb_key, srid_key = _mysql_dynamic_ewkb_bind_keys(
            source_bind,
            source_token=source_token,
        )
        dynamic_bind_mappings.append(
            (tuple(candidate_keys), wkb_key, srid_key, source_bind, default_srid)
        )

    return tuple(dynamic_bind_mappings)


def _expand_mysql_dynamic_ewkb_param_mapping(parameters, dynamic_bind_mappings):
    if not isinstance(parameters, Mapping):
        return parameters, False

    expanded_parameters = parameters
    changed = False
    for source_keys, wkb_key, srid_key, source_bind, default_srid in dynamic_bind_mappings:
        source_key = next((key for key in source_keys if key in parameters), None)
        if source_key is not None:
            source_value = parameters[source_key]
        elif getattr(source_bind, "callable", None) is not None:
            source_value = source_bind.callable()
        elif not source_bind.required:
            source_value = getattr(source_bind, "value", None)
        else:
            continue

        if wkb_key in parameters and srid_key in parameters:
            continue

        if expanded_parameters is parameters:
            expanded_parameters = dict(parameters)

        expanded_parameters.setdefault(wkb_key, source_value)
        expanded_parameters.setdefault(srid_key, (source_value, default_srid))
        changed = True

    return expanded_parameters, changed


def before_execute(conn, clauseelement, multiparams, params, execution_options):
    dynamic_bind_mappings = _get_mysql_dynamic_ewkb_bind_mappings(
        clauseelement,
        conn.dialect,
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


def _compile_GeomFromWKB_MySql(element, compiler, *, identifier=None, coerce_ewkb=False, **kw):
    identifier = identifier or element.identifier

    # Store the SRID
    clauses = list(element.clauses)
    if kw.get("literal_binds", False):
        clauses, _ = unwrap_wkb_constructor_clauses(clauses)

    inferred_srid = None
    dynamic_srid_clause = None
    if coerce_ewkb:
        original_wkb_clause = clauses[0]
        split_disabled = _dynamic_ewkb_split_disabled(compiler) or kw.get("literal_binds", False)
        should_process_at_runtime = (
            _is_bindparam_clause(original_wkb_clause)
            and not kw.get("literal_binds", False)
            and not split_disabled
            and (
                getattr(original_wkb_clause, "value", None) is None
                or getattr(original_wkb_clause, "callable", None) is not None
            )
        )
        if should_process_at_runtime:
            has_srid_argument = _has_effective_srid_argument(clauses)
            dynamic_default_srid = _dynamic_ewkb_default_srid(element, clauses)
            if dynamic_default_srid is not None:
                wkb_value_clause, dynamic_srid_clause = _get_mysql_dynamic_ewkb_bind_clauses(
                    original_wkb_clause,
                    compiler,
                    default_srid=dynamic_default_srid,
                )
            else:
                fixed_srid = None
                if not has_srid_argument:
                    fixed_srid = _ewkb_processor_fixed_srid(element, clauses)
                processor_context = _runtime_ewkb_processor_context(
                    compiler,
                    original_wkb_clause,
                    fixed_srid=fixed_srid,
                    has_srid_argument=has_srid_argument,
                )
                wkb_value_clause = _coerce_ewkb_bind_clause_to_wkb(
                    original_wkb_clause,
                    fixed_srid=fixed_srid,
                    has_srid_argument=has_srid_argument,
                    processor_context=processor_context,
                )
        else:
            _, inferred_srid = _coerce_ewkb_clause_to_wkb(original_wkb_clause)
            dynamic_wkb_clause, dynamic_srid_clause = _dynamic_inferred_srid_bind_clauses(
                element,
                compiler,
                original_wkb_clause,
                clauses,
                inferred_srid=inferred_srid,
                split_disabled=split_disabled,
            )
            if dynamic_wkb_clause is not None:
                wkb_value_clause = dynamic_wkb_clause
            else:
                wkb_value_clause, inferred_srid = _coerce_known_ewkb_clause_to_wkb(
                    element,
                    compiler,
                    original_wkb_clause,
                    clauses,
                    preserve_user_bind=not kw.get("literal_binds", False),
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
        compiler,
        srid_clause=dynamic_srid_clause,
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
