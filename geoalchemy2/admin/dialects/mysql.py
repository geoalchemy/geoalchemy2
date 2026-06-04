"""This module defines specific functions for MySQL dialect."""

from sqlalchemy import text
from sqlalchemy.dialects.mysql.base import ischema_names as _mysql_ischema_names
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import expression
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


def _compile_srid_arg(element, clauses, inferred_srid, compiler, **kw):
    if len(clauses) > 1:
        compiled_srid = _compile_srid_clause(clauses[1], compiler, **kw)
        if compiled_srid is not None:
            return compiled_srid
        return str(inferred_srid) if inferred_srid is not None else None

    srid = inferred_srid if inferred_srid is not None else element.type.srid
    return str(srid) if is_known_srid(srid) else None


def _compile_GeomFromWKB_MySql(element, compiler, *, identifier=None, coerce_ewkb=False, **kw):
    identifier = identifier or element.identifier

    # Store the SRID
    clauses = list(element.clauses)
    if kw.get("literal_binds", False):
        clauses, _ = unwrap_wkb_constructor_clauses(clauses)

    inferred_srid = None
    if coerce_ewkb:
        original_wkb_clause = clauses[0]
        should_process_at_runtime = (
            _is_bindparam_clause(original_wkb_clause)
            and not kw.get("literal_binds", False)
            and (
                getattr(original_wkb_clause, "value", None) is None
                or getattr(original_wkb_clause, "callable", None) is not None
            )
        )
        if should_process_at_runtime:
            has_srid_argument = _has_effective_srid_argument(clauses)
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
    compiled_srid = _compile_srid_arg(element, clauses, inferred_srid, compiler, **kw)

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
