"""This module defines specific functions for MSSQL dialect."""

import math
import re

from sqlalchemy import Index
from sqlalchemy import text
from sqlalchemy.dialects.mssql.base import MSSQLCompiler as _MSSQLCompiler
from sqlalchemy.dialects.mssql.base import ischema_names as _mssql_ischema_names
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import expression
from sqlalchemy.sql import operators
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.types import LargeBinary
from sqlalchemy.types import TypeDecorator
from sqlalchemy.types import UnicodeText

from geoalchemy2 import functions
from geoalchemy2.admin.dialects.common import _check_spatial_type
from geoalchemy2.admin.dialects.common import _spatial_idx_name
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.exc import ArgumentError
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry
from geoalchemy2.types.dialects.mssql import _normalize_wkt_for_mssql
from geoalchemy2.types.dialects.mssql import _to_mssql_wkt

_mssql_ischema_names["geometry"] = Geometry
_mssql_ischema_names["geography"] = Geography

# Register GeoAlchemy's spatial index kwargs so SQLAlchemy accepts them on Index(...).
for _dialect_kwarg in ("bounding_box", "cells_per_object", "grids", "using", "with"):
    Index.argument_for("mssql", _dialect_kwarg, None)

_MSSQL_WORLD_BOUNDING_BOX = (-180.0, -90.0, 180.0, 90.0)
_MSSQL_DEFAULT_BOUNDING_BOX = (-1000000000.0, -1000000000.0, 1000000000.0, 1000000000.0)
_MSSQL_GEOMETRY_TYPE_NAMES = {
    "POINT": "Point",
    "LINESTRING": "LineString",
    "POLYGON": "Polygon",
    "MULTIPOINT": "MultiPoint",
    "MULTILINESTRING": "MultiLineString",
    "MULTIPOLYGON": "MultiPolygon",
    "GEOMETRYCOLLECTION": "GeometryCollection",
}
_MSSQL_GEOMETRY_TYPE_LOOKUP = {
    value.upper(): key for key, value in _MSSQL_GEOMETRY_TYPE_NAMES.items()
}
_MSSQL_BOUNDING_BOX_ERROR = (
    "mssql_bounding_box must be a 4-value tuple/list or comma-separated string "
    "formatted as finite numeric coordinates: xmin, ymin, xmax, ymax"
)


def _quote_mssql_identifier(name):
    return f"[{name.replace(']', ']]')}]"


def _quote_mssql_string(value):
    escaped_value = value.replace("'", "''")
    return f"N'{escaped_value}'"


def _quote_mssql_table_name(table_name, schema=None):
    if schema:
        return f"{_quote_mssql_identifier(schema)}.{_quote_mssql_identifier(table_name)}"
    return _quote_mssql_identifier(table_name)


def _get_mssql_full_table_name(table_name, schema=None):
    if schema:
        return f"{schema}.{table_name}"
    return table_name


def _format_mssql_number(value):
    try:
        return format(float(value), "g")
    except (TypeError, ValueError):  # pragma: no cover
        return str(value)


def _format_mssql_bounding_box(bounding_box):
    if isinstance(bounding_box, str):
        bounding_box = [value.strip() for value in bounding_box.split(",")]

    if not isinstance(bounding_box, (tuple, list)):
        raise ArgumentError(_MSSQL_BOUNDING_BOX_ERROR)
    try:
        xmin, ymin, xmax, ymax = bounding_box
    except ValueError as exc:
        raise ArgumentError(_MSSQL_BOUNDING_BOX_ERROR) from exc
    try:
        values = [float(value) for value in (xmin, ymin, xmax, ymax)]
    except (TypeError, ValueError) as exc:
        raise ArgumentError(_MSSQL_BOUNDING_BOX_ERROR) from exc
    if not all(math.isfinite(value) for value in values):
        raise ArgumentError(_MSSQL_BOUNDING_BOX_ERROR)
    return ", ".join(_format_mssql_number(value) for value in values)


def _base_mssql_geometry_type(geometry_type):
    if geometry_type is None:
        return None
    geometry_type = geometry_type.upper()
    if geometry_type.endswith("ZM"):
        return geometry_type[:-2]
    if geometry_type.endswith(("Z", "M")):
        return geometry_type[:-1]
    return geometry_type


def _mssql_geometry_type_constraint_value(geometry_type):
    base_geometry_type = _base_mssql_geometry_type(geometry_type)
    if base_geometry_type in (None, "GEOMETRY"):
        return None
    return _MSSQL_GEOMETRY_TYPE_NAMES.get(base_geometry_type)


def _mssql_geometry_type_constraint_prefix(geometry_type):
    base_geometry_type = _base_mssql_geometry_type(geometry_type)
    if base_geometry_type in (None, "GEOMETRY"):
        return None
    return base_geometry_type if base_geometry_type in _MSSQL_GEOMETRY_TYPE_NAMES else None


def _mssql_spatial_constraint_name(table_name, column_name, constraint_type):
    return f"ck_{table_name}_{column_name}_{constraint_type}"


def _column_regex(column_name):
    quoted_column = re.escape(column_name.replace("]", "]]"))
    unquoted_column = re.escape(column_name)
    return rf"(?:\[{quoted_column}\]|{unquoted_column})"


def _default_mssql_bounding_box(col_type, is_geography=False):
    if is_geography:
        return None
    if getattr(col_type, "srid", None) == 4326:
        return _MSSQL_WORLD_BOUNDING_BOX
    return _MSSQL_DEFAULT_BOUNDING_BOX


def _get_mssql_column_type_name(bind, table_name, column_name, schema=None):
    full_table_name = _get_mssql_full_table_name(table_name, schema=schema)
    type_query = text(
        """SELECT t.name
        FROM sys.columns AS c
        JOIN sys.types AS t
            ON c.user_type_id = t.user_type_id
        WHERE c.object_id = OBJECT_ID(:full_table_name) AND c.name = :column_name"""
    )
    return bind.execute(
        type_query,
        {"full_table_name": full_table_name, "column_name": column_name},
    ).scalar()


def _get_mssql_spatial_column_constraints(bind, table_name, column_name, schema=None):
    full_table_name = _get_mssql_full_table_name(table_name, schema=schema)
    constraints_query = text(
        """SELECT definition
        FROM sys.check_constraints
        WHERE parent_object_id = OBJECT_ID(:full_table_name)"""
    )
    column_pattern = _column_regex(column_name)
    srid_pattern = re.compile(
        rf"{column_pattern}\s*\.\s*(?:\[STSrid\]|STSrid)\s*=\s*\(?\s*(-?\d+)\s*\)?",
        re.IGNORECASE,
    )
    geometry_type_pattern = re.compile(
        rf"{column_pattern}\s*\.\s*(?:\[STGeometryType\]|STGeometryType)\s*"
        rf"\(\s*\)\s*=\s*\(?\s*N?'([^']+)'",
        re.IGNORECASE,
    )
    geometry_type_prefix_pattern = re.compile(
        rf"{column_pattern}\s*\.\s*(?:\[AsTextZM\]|AsTextZM)\s*\(\s*\)\s*\)*\s+"
        rf"LIKE\s+\(?\s*N?'([^'%]+)%",
        re.IGNORECASE,
    )

    srid = -1
    geometry_type = "GEOMETRY"
    for definition in bind.execute(
        constraints_query,
        {"full_table_name": full_table_name},
    ).scalars():
        srid_match = srid_pattern.search(definition)
        if srid_match:
            srid = int(srid_match.group(1))

        geometry_type_match = geometry_type_pattern.search(definition)
        if geometry_type_match:
            geometry_type = _MSSQL_GEOMETRY_TYPE_LOOKUP.get(
                geometry_type_match.group(1).upper(),
                geometry_type,
            )
            continue

        geometry_type_prefix_match = geometry_type_prefix_pattern.search(definition)
        if geometry_type_prefix_match:
            geometry_type = _MSSQL_GEOMETRY_TYPE_LOOKUP.get(
                geometry_type_prefix_match.group(1).upper(),
                geometry_type,
            )

    return geometry_type, srid


def _get_mssql_spatial_indexes(bind, table_name, schema=None, column_name=None):
    full_table_name = _get_mssql_full_table_name(table_name, schema=schema)
    where_clauses = ["i.object_id = OBJECT_ID(:full_table_name)", "i.type_desc = 'SPATIAL'"]
    params = {"full_table_name": full_table_name}

    if column_name is not None:
        where_clauses.append("c.name = :column_name")
        params["column_name"] = column_name

    spatial_index_query = text(
        f"""SELECT
            i.name AS index_name,
            c.name AS column_name,
            si.tessellation_scheme,
            sit.cells_per_object,
            sit.bounding_box_xmin,
            sit.bounding_box_ymin,
            sit.bounding_box_xmax,
            sit.bounding_box_ymax,
            sit.level_1_grid_desc,
            sit.level_2_grid_desc,
            sit.level_3_grid_desc,
            sit.level_4_grid_desc
        FROM sys.indexes AS i
        JOIN sys.index_columns AS ic
            ON i.object_id = ic.object_id
            AND i.index_id = ic.index_id
        JOIN sys.columns AS c
            ON ic.object_id = c.object_id
            AND ic.column_id = c.column_id
        LEFT JOIN sys.spatial_indexes AS si
            ON i.object_id = si.object_id
            AND i.index_id = si.index_id
        LEFT JOIN sys.spatial_index_tessellations AS sit
            ON i.object_id = sit.object_id
            AND i.index_id = sit.index_id
        WHERE {" AND ".join(where_clauses)}
        ORDER BY i.name"""
    )

    spatial_indexes = []
    for row in bind.execute(spatial_index_query, params).mappings():
        dialect_options = {}

        if row["tessellation_scheme"] is not None:
            dialect_options["mssql_using"] = row["tessellation_scheme"]
        if row["cells_per_object"] is not None:
            dialect_options["mssql_cells_per_object"] = int(row["cells_per_object"])
        if row["bounding_box_xmin"] is not None:
            dialect_options["mssql_bounding_box"] = (
                row["bounding_box_xmin"],
                row["bounding_box_ymin"],
                row["bounding_box_xmax"],
                row["bounding_box_ymax"],
            )

        grids = tuple(
            level
            for level in (
                row["level_1_grid_desc"],
                row["level_2_grid_desc"],
                row["level_3_grid_desc"],
                row["level_4_grid_desc"],
            )
            if level is not None
        )
        if len(grids) == 4:
            dialect_options["mssql_grids"] = grids

        spatial_indexes.append(
            {
                "name": row["index_name"],
                "column_name": row["column_name"],
                "dialect_options": dialect_options,
            }
        )

    return spatial_indexes


def _get_mssql_spatial_index_with_clauses(col_type, idx_kwargs, is_geography=False):
    with_clauses = []

    raw_with = idx_kwargs.get("mssql_with")
    if raw_with:
        if isinstance(raw_with, str):
            with_clauses.append(raw_with)
        else:
            with_clauses.extend(raw_with)

    grids = idx_kwargs.get("mssql_grids")
    if grids:
        if isinstance(grids, str):
            if grids.lstrip().upper().startswith("GRIDS"):
                grids_clause = grids
            else:
                grids_clause = f"GRIDS = ({grids})"
        else:
            grids_clause = f"GRIDS = ({', '.join(str(level) for level in grids)})"
        with_clauses.append(grids_clause)

    cells_per_object = idx_kwargs.get("mssql_cells_per_object")
    if cells_per_object is not None:
        with_clauses.append(f"CELLS_PER_OBJECT = {int(cells_per_object)}")

    if not is_geography:
        has_bounding_box = any(
            clause.lstrip().upper().startswith("BOUNDING_BOX") for clause in with_clauses
        )
        if not has_bounding_box:
            bounding_box = idx_kwargs.get("mssql_bounding_box") or _default_mssql_bounding_box(
                col_type,
                is_geography=is_geography,
            )
            if bounding_box is not None:
                with_clauses.insert(
                    0,
                    f"BOUNDING_BOX = ({_format_mssql_bounding_box(bounding_box)})",
                )

    return with_clauses


def create_spatial_index(
    bind, table_name, column_name, col_type, schema=None, index_name=None, **idx_kwargs
):
    index_name = index_name or _spatial_idx_name(table_name, column_name)
    table_ref = _quote_mssql_table_name(table_name, schema=schema)

    is_geography = _check_spatial_type(col_type, Geography, bind.dialect)
    if not _check_spatial_type(col_type, (Geometry, Geography), bind.dialect):
        type_name = _get_mssql_column_type_name(bind, table_name, column_name, schema=schema)
        is_geography = str(type_name).lower() == "geography"

    if is_geography:
        using = idx_kwargs.get("mssql_using", "GEOGRAPHY_AUTO_GRID")
    else:
        using = idx_kwargs.get("mssql_using", "GEOMETRY_AUTO_GRID")

    ddl = [
        f"CREATE SPATIAL INDEX {_quote_mssql_identifier(index_name)}",
        f"ON {table_ref} ({_quote_mssql_identifier(column_name)})",
    ]
    if using:
        ddl.append(f"USING {using}")

    with_clauses = _get_mssql_spatial_index_with_clauses(
        col_type, idx_kwargs, is_geography=is_geography
    )
    if with_clauses:
        ddl.append(f"WITH ({', '.join(with_clauses)})")

    bind.execute(text(" ".join(ddl)))


def create_spatial_constraints(bind, table_name, column_name, col_type, schema=None):
    table_ref = _quote_mssql_table_name(table_name, schema=schema)
    column_ref = _quote_mssql_identifier(column_name)

    if getattr(col_type, "srid", -1) >= 0:
        constraint_name = _mssql_spatial_constraint_name(table_name, column_name, "srid")
        bind.execute(
            text(
                f"ALTER TABLE {table_ref} ADD CONSTRAINT "
                f"{_quote_mssql_identifier(constraint_name)} CHECK "
                f"({column_ref} IS NULL OR {column_ref}.STSrid = {int(col_type.srid)})"
            )
        )

    geometry_type_prefix = _mssql_geometry_type_constraint_prefix(
        getattr(col_type, "geometry_type", None)
    )
    if geometry_type_prefix is not None:
        constraint_name = _mssql_spatial_constraint_name(table_name, column_name, "geometry_type")
        bind.execute(
            text(
                f"ALTER TABLE {table_ref} ADD CONSTRAINT "
                f"{_quote_mssql_identifier(constraint_name)} CHECK "
                f"({column_ref} IS NULL OR UPPER({column_ref}.AsTextZM()) LIKE "
                f"{_quote_mssql_string(f'{geometry_type_prefix}%')})"
            )
        )


def drop_spatial_constraints(bind, table_name, column_name, schema=None):
    full_table_name = _get_mssql_full_table_name(table_name, schema=schema)
    table_ref = _quote_mssql_table_name(table_name, schema=schema)
    srid_constraint_name = _mssql_spatial_constraint_name(table_name, column_name, "srid")
    geometry_type_constraint_name = _mssql_spatial_constraint_name(
        table_name,
        column_name,
        "geometry_type",
    )
    quoted_column_token = _quote_mssql_identifier(column_name)
    srid_method_token = f"{quoted_column_token}.[STSrid]"
    srid_method_token_plain = f"{quoted_column_token}.STSrid"
    geometry_type_method_token = f"{quoted_column_token}.[STGeometryType]"
    geometry_type_method_token_plain = f"{quoted_column_token}.STGeometryType"
    astextzm_method_token = f"{quoted_column_token}.[AsTextZM]"
    astextzm_method_token_plain = f"{quoted_column_token}.AsTextZM"

    constraints_query = text(
        """SELECT DISTINCT cc.name
        FROM sys.check_constraints AS cc
        WHERE cc.parent_object_id = OBJECT_ID(:full_table_name)
            AND (
                cc.name IN (:srid_constraint_name, :geometry_type_constraint_name)
                OR (
                    CHARINDEX(:srid_method_token, cc.definition) > 0
                    OR CHARINDEX(:srid_method_token_plain, cc.definition) > 0
                    OR CHARINDEX(:geometry_type_method_token, cc.definition) > 0
                    OR CHARINDEX(:geometry_type_method_token_plain, cc.definition) > 0
                    OR CHARINDEX(:astextzm_method_token, cc.definition) > 0
                    OR CHARINDEX(:astextzm_method_token_plain, cc.definition) > 0
                )
            )"""
    )
    constraint_names = list(
        bind.execute(
            constraints_query,
            {
                "full_table_name": full_table_name,
                "srid_constraint_name": srid_constraint_name,
                "geometry_type_constraint_name": geometry_type_constraint_name,
                "srid_method_token": srid_method_token,
                "srid_method_token_plain": srid_method_token_plain,
                "geometry_type_method_token": geometry_type_method_token,
                "geometry_type_method_token_plain": geometry_type_method_token_plain,
                "astextzm_method_token": astextzm_method_token,
                "astextzm_method_token_plain": astextzm_method_token_plain,
            },
        ).scalars()
    )

    for constraint_name in constraint_names:
        bind.execute(
            text(
                f"ALTER TABLE {table_ref} DROP CONSTRAINT "
                f"{_quote_mssql_identifier(constraint_name)}"
            )
        )


def drop_spatial_index(bind, table_name, index_name, schema=None):
    table_ref = _quote_mssql_table_name(table_name, schema=schema)
    bind.execute(text(f"DROP INDEX {_quote_mssql_identifier(index_name)} ON {table_ref}"))


def reflect_geometry_column(inspector, table, column_info):
    """Reflect a geometry or geography column with the MSSQL dialect."""
    if not isinstance(column_info.get("type"), Geometry | Geography | NullType):
        return

    column_name = column_info["name"]
    schema = table.schema or inspector.default_schema_name
    full_table_name = _get_mssql_full_table_name(table.name, schema=schema)

    type_query = text(
        """SELECT t.name, c.is_nullable
        FROM sys.columns AS c
        JOIN sys.types AS t ON c.user_type_id = t.user_type_id
        WHERE c.object_id = OBJECT_ID(:full_table_name) AND c.name = :column_name"""
    )
    type_name, is_nullable = inspector.bind.execute(
        type_query,
        {"full_table_name": full_table_name, "column_name": column_name},
    ).one()
    type_name = type_name.lower()
    if type_name not in ("geometry", "geography"):
        return

    spatial_index = bool(
        _get_mssql_spatial_indexes(
            inspector.bind,
            table.name,
            schema=schema,
            column_name=column_name,
        )
    )

    geometry_type, srid = _get_mssql_spatial_column_constraints(
        inspector.bind,
        table.name,
        column_name,
        schema=schema,
    )

    spatial_type = Geography if type_name == "geography" else Geometry
    column_info["type"] = spatial_type(
        geometry_type=geometry_type,
        srid=srid,
        spatial_index=spatial_index,
        nullable=bool(is_nullable),
        _spatial_index_reflected=True,
    )


def before_create(table, bind, **kw):
    """Remove spatial indexes from CREATE TABLE so they can be emitted separately."""
    schema = table.schema
    if schema and schema != bind.dialect.default_schema_name:
        quoted_schema = _quote_mssql_identifier(schema)
        schema_literal = schema.replace("'", "''")
        quoted_schema_literal = quoted_schema.replace("'", "''")
        bind.exec_driver_sql(
            f"IF SCHEMA_ID(N'{schema_literal}') IS NULL "
            f"EXEC(N'CREATE SCHEMA {quoted_schema_literal}')"
        )

    table.info["_after_create_indexes"] = []
    current_indexes = set(table.indexes)

    for idx in current_indexes:
        for col in table.columns:
            if (
                _check_spatial_type(col.type, (Geometry, Geography), bind.dialect)
                and col in idx.columns.values()
            ):
                table.indexes.remove(idx)
                if idx.name != _spatial_idx_name(table.name, col.name) or not getattr(
                    col.type, "spatial_index", False
                ):
                    table.info["_after_create_indexes"].append(idx)
                break


def after_create(table, bind, **kw):
    dialect = bind.dialect
    after_create_indexes = table.info.pop("_after_create_indexes", [])
    delayed_spatial_index_cols = set()
    for idx in after_create_indexes:
        columns = list(idx.columns.values())
        if len(columns) == 1 and _check_spatial_type(
            columns[0].type,
            (Geometry, Geography),
            dialect,
        ):
            delayed_spatial_index_cols.add(columns[0].name)

    for col in table.columns:
        if _check_spatial_type(col.type, (Geometry, Geography), dialect):
            create_spatial_constraints(bind, table.name, col.name, col.type, schema=table.schema)

        if (
            _check_spatial_type(col.type, (Geometry, Geography), dialect)
            and getattr(col.type, "spatial_index", False)
            and col.name not in delayed_spatial_index_cols
        ):
            create_spatial_index(bind, table.name, col.name, col.type, schema=table.schema)

    for idx in after_create_indexes:
        table.indexes.add(idx)
        columns = list(idx.columns.values())
        if len(columns) == 1 and _check_spatial_type(
            columns[0].type,
            (Geometry, Geography),
            dialect,
        ):
            create_spatial_index(
                bind,
                table.name,
                columns[0].name,
                columns[0].type,
                schema=table.schema,
                index_name=idx.name,
                **idx.kwargs,
            )


def before_drop(table, bind, **kw):
    return


def after_drop(table, bind, **kw):
    return


def _process_wkt_value(value, strip_srid=False):
    if isinstance(value, WKTElement):
        value = value.data
    elif isinstance(value, (WKBElement, bytes, bytearray, memoryview)):
        value = _to_mssql_wkt(value)
    if isinstance(value, str) and strip_srid:
        wkt_match = WKTElement._REMOVE_SRID.match(value)
        value = wkt_match.group(3)
    if isinstance(value, str):
        value = _normalize_wkt_for_mssql(value)

    return value


def _process_wkb_value(value, extended=False):
    if isinstance(value, WKBElement):
        value = value.data
    if extended:
        value = WKBElement(value, extended=True).as_wkb().data
    if isinstance(value, memoryview):
        value = value.tobytes()

    return value


class _MSSQLWKTBindType(TypeDecorator):
    impl = UnicodeText
    cache_ok = True

    def __init__(self, strip_srid=False):
        super().__init__()
        self.strip_srid = strip_srid

    def process_bind_param(self, value, dialect):
        return _process_wkt_value(value, strip_srid=self.strip_srid)


class _MSSQLWKBBindType(TypeDecorator):
    impl = LargeBinary
    cache_ok = True

    def __init__(self, extended=False):
        super().__init__()
        self.extended = extended

    def process_bind_param(self, value, dialect):
        return _process_wkb_value(value, extended=self.extended)


def _coerce_wkt_bind_clause(wkt_clause, strip_srid=False, literal=False):
    if not hasattr(wkt_clause, "value"):
        return wkt_clause

    if literal:
        return expression.bindparam(
            key=wkt_clause.key,
            value=_process_wkt_value(wkt_clause.value, strip_srid=strip_srid),
            type_=UnicodeText(),
            unique=True,
        )

    return expression.type_coerce(wkt_clause, _MSSQLWKTBindType(strip_srid=strip_srid))


def _coerce_wkb_bind_clause(wkb_clause, extended=False, literal=False):
    if not hasattr(wkb_clause, "value"):
        return wkb_clause

    if literal:
        return expression.bindparam(
            key=wkb_clause.key,
            value=_process_wkb_value(wkb_clause.value, extended=extended),
            type_=LargeBinary(),
            unique=True,
        )

    return expression.type_coerce(wkb_clause, _MSSQLWKBBindType(extended=extended))


def _should_coerce_wkt_bind_clause(wkt_clause):
    if not hasattr(wkt_clause, "value"):
        return False

    value = wkt_clause.value
    if isinstance(value, (WKTElement, WKBElement, bytes, bytearray, memoryview)):
        return True
    if not isinstance(value, str):
        return False

    return _normalize_wkt_for_mssql(value) != value


def _should_coerce_wkt_bind_clause_for_text(wkt_clause, strip_srid=False):
    if not _should_coerce_wkt_bind_clause(wkt_clause):
        if not strip_srid or not hasattr(wkt_clause, "value"):
            return False
        value = wkt_clause.value
        if isinstance(value, WKTElement):
            value = value.data
        return isinstance(value, str) and value.startswith("SRID=")
    return True


def _should_coerce_wkb_bind_clause(wkb_clause):
    return hasattr(wkb_clause, "value") and isinstance(
        wkb_clause.value, (WKBElement, bytes, bytearray, memoryview)
    )


def _infer_srid_from_wkb_clause(wkb_clause, default_srid, extended=False):
    if not hasattr(wkb_clause, "value"):
        return default_srid

    value = wkb_clause.value
    if isinstance(value, WKBElement):
        return value.srid if value.srid >= 0 else default_srid

    if extended and isinstance(value, (bytes, bytearray, memoryview)):
        srid = WKBElement(value, extended=True).srid
        return srid if srid >= 0 else default_srid

    return default_srid


def _is_spatial_clause(clause, dialect=None):
    return _check_spatial_type(getattr(clause, "type", None), (Geometry, Geography), dialect)


def _is_spatial_function_target(clause, dialect=None):
    return _is_spatial_clause(clause, dialect) or isinstance(
        getattr(clause, "value", None),
        WKTElement | WKBElement,
    )


def _compile_mssql_function_fallback(element, compiler, **kw):
    return compiler.visit_function(element, **kw)


def _compile_mssql_method(element, compiler, method_name, property_=False, **kw):
    clauses = list(element.clauses)
    if not clauses or not _is_spatial_function_target(clauses[0], compiler.dialect):
        return _compile_mssql_function_fallback(element, compiler, **kw)

    target = compiler.process(clauses[0], **kw)
    if property_:
        return f"{target}.{method_name}"

    compiled_args = ", ".join(compiler.process(arg, **kw) for arg in clauses[1:])
    return f"{target}.{method_name}({compiled_args})"


def _compile_mssql_binary_method(element, compiler, method_name, **kw):
    clauses = list(element.clauses)
    if len(clauses) < 2 or not _is_spatial_function_target(clauses[0], compiler.dialect):
        return _compile_mssql_function_fallback(element, compiler, **kw)

    target = compiler.process(clauses[0], **kw)
    other = compiler.process(clauses[1], **kw)
    return f"{target}.{method_name}({other})"


def _compile_mssql_srid_clause(clause, compiler, default_srid, **kw):
    if hasattr(clause, "value"):
        value = clause.value
        try:
            if value is not None and int(value) < 0:
                return "0"
        except (TypeError, ValueError):  # pragma: no cover
            pass
    return compiler.process(clause, **kw) if clause is not None else str(default_srid)


def _compile_mssql_geom_from_text(element, compiler, strip_srid=False, **kw):
    clauses = list(element.clauses)
    original_wkt_clause = clauses[0]
    wkt_clause = original_wkt_clause
    if kw.get("literal_binds", False) or _should_coerce_wkt_bind_clause_for_text(
        original_wkt_clause, strip_srid=strip_srid
    ):
        wkt_clause = _coerce_wkt_bind_clause(
            original_wkt_clause,
            strip_srid=strip_srid,
            literal=kw.get("literal_binds", False),
        )
    compiled_wkt = compiler.process(wkt_clause, **kw)

    if len(clauses) > 1:
        compiled_srid = _compile_mssql_srid_clause(clauses[1], compiler, 0, **kw)
    else:
        srid = element.type.srid if element.type.srid >= 0 else 0
        if strip_srid and hasattr(original_wkt_clause, "value"):
            value = original_wkt_clause.value
            if isinstance(value, WKTElement):
                value = value.data
            if isinstance(value, str):
                wkt_match = WKTElement._REMOVE_SRID.match(value)
                matched_srid = wkt_match.group(2)
                if matched_srid is not None:
                    srid = int(matched_srid)
        compiled_srid = str(srid)

    return f"{element.type.name}::STGeomFromText({compiled_wkt}, {compiled_srid})"


def _compile_mssql_geom_from_wkb(element, compiler, extended=False, **kw):
    clauses = list(element.clauses)
    original_wkb_clause = clauses[0]
    wkb_clause = original_wkb_clause
    if kw.get("literal_binds", False) or _should_coerce_wkb_bind_clause(clauses[0]):
        wkb_clause = _coerce_wkb_bind_clause(
            clauses[0], extended=extended, literal=kw.get("literal_binds", False)
        )

    if kw.get("literal_binds", False) and hasattr(wkb_clause, "value"):
        compiled_wkb = f"0x{WKBElement._wkb_to_hex(wkb_clause.value)}"
    else:
        compiled_wkb = compiler.process(wkb_clause, **kw)

    if len(clauses) > 1:
        compiled_srid = _compile_mssql_srid_clause(clauses[1], compiler, 0, **kw)
    else:
        default_srid = element.type.srid if element.type.srid >= 0 else 0
        compiled_srid = str(
            _infer_srid_from_wkb_clause(original_wkb_clause, default_srid, extended=extended)
        )

    return f"{element.type.name}::STGeomFromWKB({compiled_wkb}, {compiled_srid})"


@compiles(functions.ST_GeomFromText, "mssql")  # type: ignore
def _MSSQL_ST_GeomFromText(element, compiler, **kw):
    return _compile_mssql_geom_from_text(element, compiler, **kw)


@compiles(functions.ST_GeogFromText, "mssql")  # type: ignore
def _MSSQL_ST_GeogFromText(element, compiler, **kw):
    return _compile_mssql_geom_from_text(element, compiler, **kw)


@compiles(functions.ST_GeomFromEWKT, "mssql")  # type: ignore
def _MSSQL_ST_GeomFromEWKT(element, compiler, **kw):
    return _compile_mssql_geom_from_text(element, compiler, strip_srid=True, **kw)


@compiles(functions.ST_GeomFromWKB, "mssql")  # type: ignore
def _MSSQL_ST_GeomFromWKB(element, compiler, **kw):
    return _compile_mssql_geom_from_wkb(element, compiler, **kw)


@compiles(functions.ST_GeogFromWKB, "mssql")  # type: ignore
def _MSSQL_ST_GeogFromWKB(element, compiler, **kw):
    return _compile_mssql_geom_from_wkb(element, compiler, **kw)


@compiles(functions.ST_GeomFromEWKB, "mssql")  # type: ignore
def _MSSQL_ST_GeomFromEWKB(element, compiler, **kw):
    return _compile_mssql_geom_from_wkb(element, compiler, extended=True, **kw)


@compiles(functions.ST_AsBinary, "mssql")  # type: ignore
def _MSSQL_ST_AsBinary(element, compiler, **kw):
    return _compile_mssql_method(element, compiler, "STAsBinary", **kw)


@compiles(functions.ST_AsEWKB, "mssql")  # type: ignore
def _MSSQL_ST_AsEWKB(element, compiler, **kw):
    return _compile_mssql_method(element, compiler, "AsBinaryZM", **kw)


@compiles(functions.ST_AsText, "mssql")  # type: ignore
def _MSSQL_ST_AsText(element, compiler, **kw):
    return _compile_mssql_method(element, compiler, "AsTextZM", **kw)


@compiles(functions.ST_AsEWKT, "mssql")  # type: ignore
def _MSSQL_ST_AsEWKT(element, compiler, **kw):
    return _compile_mssql_method(element, compiler, "AsTextZM", **kw)


@compiles(functions.ST_GeometryType, "mssql")  # type: ignore
def _MSSQL_ST_GeometryType(element, compiler, **kw):
    return _compile_mssql_method(element, compiler, "STGeometryType", **kw)


@compiles(functions.ST_SRID, "mssql")  # type: ignore
def _MSSQL_ST_SRID(element, compiler, **kw):
    return _compile_mssql_method(element, compiler, "STSrid", property_=True, **kw)


@compiles(functions.ST_Buffer, "mssql")  # type: ignore
def _MSSQL_ST_Buffer(element, compiler, **kw):
    return _compile_mssql_method(element, compiler, "STBuffer", **kw)


@compiles(functions.ST_Area, "mssql")  # type: ignore
def _MSSQL_ST_Area(element, compiler, **kw):
    return _compile_mssql_method(element, compiler, "STArea", **kw)


@compiles(functions.ST_Length, "mssql")  # type: ignore
def _MSSQL_ST_Length(element, compiler, **kw):
    return _compile_mssql_method(element, compiler, "STLength", **kw)


@compiles(functions.ST_Within, "mssql")  # type: ignore
def _MSSQL_ST_Within(element, compiler, **kw):
    return _compile_mssql_binary_method(element, compiler, "STWithin", **kw)


@compiles(functions.ST_Equals, "mssql")  # type: ignore
def _MSSQL_ST_Equals(element, compiler, **kw):
    return _compile_mssql_binary_method(element, compiler, "STEquals", **kw)


@compiles(functions.ST_Contains, "mssql")  # type: ignore
def _MSSQL_ST_Contains(element, compiler, **kw):
    return _compile_mssql_binary_method(element, compiler, "STContains", **kw)


@compiles(functions.ST_Intersects, "mssql")  # type: ignore
def _MSSQL_ST_Intersects(element, compiler, **kw):
    return _compile_mssql_binary_method(element, compiler, "STIntersects", **kw)


@compiles(functions.ST_Disjoint, "mssql")  # type: ignore
def _MSSQL_ST_Disjoint(element, compiler, **kw):
    return _compile_mssql_binary_method(element, compiler, "STDisjoint", **kw)


@compiles(functions.ST_Touches, "mssql")  # type: ignore
def _MSSQL_ST_Touches(element, compiler, **kw):
    return _compile_mssql_binary_method(element, compiler, "STTouches", **kw)


@compiles(functions.ST_Overlaps, "mssql")  # type: ignore
def _MSSQL_ST_Overlaps(element, compiler, **kw):
    return _compile_mssql_binary_method(element, compiler, "STOverlaps", **kw)


@compiles(functions.ST_Crosses, "mssql")  # type: ignore
def _MSSQL_ST_Crosses(element, compiler, **kw):
    return _compile_mssql_binary_method(element, compiler, "STCrosses", **kw)


_ORIGINAL_MSSQL_VISIT_BINARY = _MSSQLCompiler.visit_binary


def _mssql_visit_binary(self, binary, override_operator=None, **kw):
    operator = override_operator or binary.operator
    if operator in (operators.eq, operators.ne):
        if _is_spatial_clause(binary.left, self.dialect):
            target_clause = binary.left
            other_clause = binary.right
        elif _is_spatial_clause(binary.right, self.dialect):
            target_clause = binary.right
            other_clause = binary.left
        else:
            target_clause = None

        if target_clause is not None:
            if not _is_spatial_clause(other_clause, self.dialect):
                other_clause = expression.type_coerce(other_clause, target_clause.type)

            target = self.process(target_clause, **kw)
            other = self.process(other_clause, **kw)
            equals = f"{target}.STEquals({other})"
            return f"{equals} = {1 if operator is operators.eq else 0}"

    return _ORIGINAL_MSSQL_VISIT_BINARY(self, binary, override_operator=override_operator, **kw)


_MSSQLCompiler.visit_binary = _mssql_visit_binary  # type: ignore[method-assign]
