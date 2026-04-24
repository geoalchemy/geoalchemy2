"""This module defines specific functions for MSSQL dialect."""

import hashlib
import math
import re
from collections.abc import Mapping

from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import text
from sqlalchemy.dialects.mssql.base import ischema_names as _mssql_ischema_names
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import expression
from sqlalchemy.sql import operators
from sqlalchemy.sql import visitors
from sqlalchemy.sql.elements import BinaryExpression
from sqlalchemy.sql.elements import BindParameter
from sqlalchemy.sql.elements import Null
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
from geoalchemy2.types.dialects.mssql import bind_processor_process as _type_bind_processor_process

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
_MSSQL_DYNAMIC_EWKT_KEY_PREFIX = "_geoalchemy2_mssql_ewkt"
_MSSQL_DYNAMIC_EWKB_KEY_PREFIX = "_geoalchemy2_mssql_ewkb"
_MSSQL_DISABLE_DYNAMIC_EWKT_SPLIT_OPTION = "geoalchemy2_mssql_disable_dynamic_ewkt_split"


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
    col_type = _resolve_mssql_spatial_type(col_type, bind.dialect)
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
    constraints_query = text(
        """SELECT DISTINCT cc.name
        FROM sys.check_constraints AS cc
        WHERE cc.parent_object_id = OBJECT_ID(:full_table_name)
            AND cc.name IN (:srid_constraint_name, :geometry_type_constraint_name)"""
    )
    constraint_names = list(
        bind.execute(
            constraints_query,
            {
                "full_table_name": full_table_name,
                "srid_constraint_name": srid_constraint_name,
                "geometry_type_constraint_name": geometry_type_constraint_name,
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
    if not isinstance(column_info.get("type"), (Geometry, Geography, NullType)):
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


def _is_mssql_generated_spatial_index(idx, table, col):
    columns = list(idx.columns.values())
    return (
        getattr(idx, "_column_flag", False)
        and len(columns) == 1
        and columns[0] is col
        and idx.name == _spatial_idx_name(table.name, col.name)
        and getattr(col.type, "spatial_index", False)
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
                if not _is_mssql_generated_spatial_index(idx, table, col):
                    table.info["_after_create_indexes"].append(idx)
                break


def after_create(table, bind, **kw):
    dialect = bind.dialect
    after_create_indexes = table.info.pop("_after_create_indexes", [])
    delayed_spatial_index_cols = set()
    for idx in after_create_indexes:
        columns = list(idx.columns.values())
        for col in columns:
            if not _check_spatial_type(col.type, (Geometry, Geography), dialect):
                continue
            if len(columns) == 1 or idx.name == _spatial_idx_name(table.name, col.name):
                delayed_spatial_index_cols.add(col.name)

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
        else:
            idx.create(bind=bind)


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


def _process_ewkt_srid_value(value, default_srid=0):
    if value is None:
        return default_srid

    if isinstance(value, WKTElement):
        if value.srid >= 0:
            return value.srid
        value = value.data
    elif isinstance(value, WKBElement):
        return value.srid if value.srid >= 0 else default_srid

    if isinstance(value, str):
        wkt_match = WKTElement._REMOVE_SRID.match(value)
        srid = wkt_match.group(2)
        try:
            if srid is not None:
                return int(srid)
        except (ValueError, TypeError):  # pragma: no cover
            raise ArgumentError(
                f"The SRID ({srid}) of the supplied value can not be casted to integer"
            ) from None
    return default_srid


def _process_wkb_value(value, extended=False):
    if value is None:
        return None
    if isinstance(value, WKBElement):
        value = value.as_wkb().data if extended else value.data
    elif extended:
        value = WKBElement(value, extended=True).as_wkb().data
    if isinstance(value, memoryview):
        value = value.tobytes()

    return value


def _process_ewkb_srid_value(value, default_srid=0):
    if value is None:
        return default_srid

    if isinstance(value, WKBElement):
        return value.srid if value.srid >= 0 else default_srid

    if isinstance(value, (bytes, bytearray, memoryview, str)):
        srid = WKBElement(value, extended=True).srid
        return srid if srid >= 0 else default_srid

    return default_srid


class _MSSQLWKTBindType(TypeDecorator):
    impl = UnicodeText
    cache_ok = True

    def __init__(self, strip_srid=False, spatial_type=None):
        super().__init__()
        self.strip_srid = strip_srid
        self.spatial_type = spatial_type

    def process_bind_param(self, value, dialect):
        if self.spatial_type is not None:
            return _type_bind_processor_process(self.spatial_type, value, dialect)
        return _process_wkt_value(value, strip_srid=self.strip_srid)


class _MSSQLWKBBindType(TypeDecorator):
    impl = LargeBinary
    cache_ok = True

    def __init__(self, extended=False):
        super().__init__()
        self.extended = extended

    def process_bind_param(self, value, dialect):
        return _process_wkb_value(value, extended=self.extended)


class _MSSQLDynamicEWKTTextBindType(TypeDecorator):
    impl = UnicodeText
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return _process_wkt_value(value, strip_srid=True)


class _MSSQLDynamicEWKTSRIDBindType(TypeDecorator):
    impl = Integer
    cache_ok = True

    def __init__(self, default_srid=0):
        super().__init__()
        self.default_srid = default_srid

    def process_bind_param(self, value, dialect):
        return _process_ewkt_srid_value(value, default_srid=self.default_srid)


class _MSSQLDynamicEWKBSRIDBindType(TypeDecorator):
    impl = Integer
    cache_ok = True

    def __init__(self, default_srid=0):
        super().__init__()
        self.default_srid = default_srid

    def process_bind_param(self, value, dialect):
        return _process_ewkb_srid_value(value, default_srid=self.default_srid)


class _MSSQLDynamicEWKTCallable:
    def __init__(self, source_callable):
        self.source_callable = source_callable
        self._pending = None
        self._remaining = 0

    def __call__(self):
        if self._remaining == 0:
            self._pending = self.source_callable()
            self._remaining = 2

        self._remaining -= 1
        value = self._pending
        if self._remaining == 0:
            self._pending = None
        return value


def _coerce_wkt_bind_clause(wkt_clause, strip_srid=False, literal=False, spatial_type=None):
    if not hasattr(wkt_clause, "value"):
        return wkt_clause

    if literal:
        return expression.bindparam(
            key=wkt_clause.key,
            value=_process_wkt_value(wkt_clause.value, strip_srid=strip_srid),
            type_=UnicodeText(),
            unique=True,
        )

    return expression.type_coerce(
        wkt_clause,
        _MSSQLWKTBindType(strip_srid=strip_srid, spatial_type=spatial_type),
    )


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


def _is_bindparam_clause(clause):
    return isinstance(clause, BindParameter)


def _is_mssql_auto_constructor_bindparam(clause, constructor_name):
    return (
        _is_bindparam_clause(clause)
        and getattr(clause, "unique", False)
        and getattr(clause, "_orig_key", None) == constructor_name
    )


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
        (WKTElement, WKBElement),
    )


def _resolve_mssql_spatial_type(spatial_type, dialect):
    if isinstance(spatial_type, TypeDecorator):
        return spatial_type.load_dialect_impl(dialect)
    return spatial_type


def _is_mssql_spatial_constructor(clause):
    return isinstance(
        clause,
        (
            functions.ST_GeomFromText,
            functions.ST_GeogFromText,
            functions.ST_GeomFromEWKT,
            functions.ST_GeomFromWKB,
            functions.ST_GeogFromWKB,
            functions.ST_GeomFromEWKB,
        ),
    )


def _is_mssql_text_constructor(clause):
    return isinstance(
        clause,
        (
            functions.ST_GeomFromText,
            functions.ST_GeogFromText,
            functions.ST_GeomFromEWKT,
        ),
    )


def _is_mssql_wkb_constructor(clause):
    return isinstance(
        clause,
        (
            functions.ST_GeomFromWKB,
            functions.ST_GeogFromWKB,
            functions.ST_GeomFromEWKB,
        ),
    )


def _unwrap_mssql_constructor_clauses(clauses, predicate):
    if len(clauses) != 1:
        return clauses, None

    inner_constructor = clauses[0]
    if not predicate(inner_constructor):
        return clauses, None

    return list(inner_constructor.clauses), inner_constructor


def _spatial_constructor_matches_target(constructor_type, target_type, dialect):
    constructor_type = _resolve_mssql_spatial_type(constructor_type, dialect)
    target_type = _resolve_mssql_spatial_type(target_type, dialect)

    return (
        _check_spatial_type(constructor_type, Geometry, dialect)
        and _check_spatial_type(target_type, Geometry, dialect)
    ) or (
        _check_spatial_type(constructor_type, Geography, dialect)
        and _check_spatial_type(target_type, Geography, dialect)
    )


def _coerce_mssql_spatial_method_argument(target_clause, other_clause, dialect):
    target_type = _resolve_mssql_spatial_type(getattr(target_clause, "type", None), dialect)

    if _is_mssql_spatial_constructor(other_clause) and not _spatial_constructor_matches_target(
        getattr(other_clause, "type", None),
        target_type,
        dialect,
    ):
        other_clause = other_clause._clone()
        other_clause.type = target_type
        return other_clause

    if not _is_spatial_clause(other_clause, dialect):
        return expression.type_coerce(other_clause, target_type)

    return other_clause


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

    target_clause = clauses[0]
    other_clause = clauses[1]
    other_clause = _coerce_mssql_spatial_method_argument(
        target_clause,
        other_clause,
        compiler.dialect,
    )

    target = compiler.process(target_clause, **kw)
    other = compiler.process(other_clause, **kw)
    return f"{target}.{method_name}({other})"


def _compile_mssql_dwithin(element, compiler, **kw):
    clauses = list(element.clauses)
    if len(clauses) < 3 or not _is_spatial_function_target(clauses[0], compiler.dialect):
        return _compile_mssql_function_fallback(element, compiler, **kw)

    target_clause = clauses[0]
    other_clause = _coerce_mssql_spatial_method_argument(
        target_clause,
        clauses[1],
        compiler.dialect,
    )
    distance_clause = clauses[2]

    target = compiler.process(target_clause, **kw)
    other = compiler.process(other_clause, **kw)
    distance = compiler.process(distance_clause, **kw)
    return f"CASE WHEN {target}.STDistance({other}) <= {distance} THEN 1 ELSE 0 END"


def _mssql_little_endian_binary_from_big_endian(binary_expr):
    return " + ".join(f"SUBSTRING({binary_expr}, {position}, 1)" for position in (4, 3, 2, 1))


def _mssql_ewkb_type_from_iso_type(wkb_type):
    dimension_type = f"({wkb_type} / 1000)"
    return (
        f"CONVERT(bigint, {wkb_type} % 1000) + 536870912 + "
        f"CASE WHEN {dimension_type} IN (1, 3) THEN 2147483648 ELSE 0 END + "
        f"CASE WHEN {dimension_type} IN (2, 3) THEN 1073741824 ELSE 0 END"
    )


def _mssql_binary4_from_unsigned_int(unsigned_int_expr):
    return f"SUBSTRING(CONVERT(binary(8), CONVERT(bigint, ({unsigned_int_expr}))), 5, 4)"


def _compile_mssql_as_ewkb(element, compiler, **kw):
    clauses = list(element.clauses)
    if not clauses or not _is_spatial_function_target(clauses[0], compiler.dialect):
        return _compile_mssql_function_fallback(element, compiler, **kw)

    target = compiler.process(clauses[0], **kw)
    wkb = f"{target}.AsBinaryZM()"
    little_endian_wkb_type = (
        f"CONVERT(int, SUBSTRING({wkb}, 5, 1) + SUBSTRING({wkb}, 4, 1) + "
        f"SUBSTRING({wkb}, 3, 1) + SUBSTRING({wkb}, 2, 1))"
    )
    big_endian_wkb_type = f"CONVERT(int, SUBSTRING({wkb}, 2, 4))"
    little_endian_ewkb_type_word = _mssql_binary4_from_unsigned_int(
        _mssql_ewkb_type_from_iso_type(little_endian_wkb_type)
    )
    big_endian_ewkb_type_word = _mssql_binary4_from_unsigned_int(
        _mssql_ewkb_type_from_iso_type(big_endian_wkb_type)
    )
    little_endian_ewkb_type = _mssql_little_endian_binary_from_big_endian(
        little_endian_ewkb_type_word
    )
    big_endian_ewkb_type = big_endian_ewkb_type_word
    little_endian_srid = _mssql_little_endian_binary_from_big_endian(
        f"CONVERT(binary(4), {target}.STSrid)"
    )
    big_endian_srid = f"CONVERT(binary(4), {target}.STSrid)"
    payload = f"SUBSTRING({wkb}, 6, DATALENGTH({wkb}) - 5)"

    return (
        f"CASE WHEN {target} IS NULL THEN NULL "
        f"WHEN SUBSTRING({wkb}, 1, 1) = 0x01 THEN "
        f"CAST(0x01 AS varbinary(max)) + {little_endian_ewkb_type} + "
        f"{little_endian_srid} + {payload} "
        f"ELSE CAST(0x00 AS varbinary(max)) + {big_endian_ewkb_type} + "
        f"{big_endian_srid} + {payload} END"
    )


def _compile_mssql_as_ewkt(element, compiler, **kw):
    clauses = list(element.clauses)
    if not clauses or not _is_spatial_function_target(clauses[0], compiler.dialect):
        return _compile_mssql_function_fallback(element, compiler, **kw)

    target = compiler.process(clauses[0], **kw)
    return (
        f"CASE WHEN {target} IS NULL THEN NULL "
        f"ELSE CONCAT('SRID=', {target}.STSrid, ';', {target}.AsTextZM()) END"
    )


def _compile_mssql_srid_clause(clause, compiler, default_srid, **kw):
    if hasattr(clause, "value"):
        value = clause.value
        try:
            if value is not None and int(value) < 0:
                return "0"
        except (TypeError, ValueError):  # pragma: no cover
            pass
    return compiler.process(clause, **kw) if clause is not None else str(default_srid)


def _mssql_dynamic_ewkt_bind_keys(source_bind):
    source_name = getattr(source_bind, "_orig_key", None) or source_bind.key
    source_name = str(source_name)
    source_key = str(source_bind.key)
    key_token = re.sub(r"[^0-9A-Za-z_]+", "_", source_name).strip("_") or "param"
    key_digest = hashlib.sha1(source_key.encode("utf-8")).hexdigest()[:8]
    key_base = f"{_MSSQL_DYNAMIC_EWKT_KEY_PREFIX}_{key_token}_{key_digest}"
    return f"{key_base}_text", f"{key_base}_srid"


def _mssql_dynamic_ewkt_bind_identifier(source_bind):
    return getattr(source_bind, "_identifying_key", source_bind.key)


def _make_mssql_dynamic_ewkt_bind_clauses(wkt_clause, default_srid=0):
    text_key, srid_key = _mssql_dynamic_ewkt_bind_keys(wkt_clause)
    bind_kwargs = {
        "required": wkt_clause.required,
    }
    if getattr(wkt_clause, "callable", None) is not None:
        shared_callable = _MSSQLDynamicEWKTCallable(wkt_clause.callable)
        bind_kwargs["callable_"] = shared_callable
    elif not wkt_clause.required:
        bind_kwargs["value"] = getattr(wkt_clause, "value", None)

    return (
        expression.bindparam(
            key=text_key,
            type_=_MSSQLDynamicEWKTTextBindType(),
            **bind_kwargs,
        ),
        expression.bindparam(
            key=srid_key,
            type_=_MSSQLDynamicEWKTSRIDBindType(default_srid=default_srid),
            **bind_kwargs,
        ),
    )


def _get_mssql_dynamic_ewkt_bind_clauses(wkt_clause, compiler, default_srid=0):
    cache = getattr(compiler, "_geoalchemy2_mssql_dynamic_ewkt_bind_cache", None)
    if cache is None:
        cache = {}
        compiler._geoalchemy2_mssql_dynamic_ewkt_bind_cache = cache

    source_identifier = _mssql_dynamic_ewkt_bind_identifier(wkt_clause)
    if source_identifier not in cache:
        cache[source_identifier] = _make_mssql_dynamic_ewkt_bind_clauses(
            wkt_clause,
            default_srid=default_srid,
        )
    return cache[source_identifier]


def _mssql_dynamic_ewkb_bind_keys(source_bind):
    source_name = getattr(source_bind, "_orig_key", None) or source_bind.key
    source_name = str(source_name)
    source_key = str(source_bind.key)
    key_token = re.sub(r"[^0-9A-Za-z_]+", "_", source_name).strip("_") or "param"
    key_digest = hashlib.sha1(source_key.encode("utf-8")).hexdigest()[:8]
    key_base = f"{_MSSQL_DYNAMIC_EWKB_KEY_PREFIX}_{key_token}_{key_digest}"
    return f"{key_base}_wkb", f"{key_base}_srid"


def _make_mssql_dynamic_ewkb_bind_clauses(wkb_clause, default_srid=0):
    wkb_key, srid_key = _mssql_dynamic_ewkb_bind_keys(wkb_clause)
    bind_kwargs = {
        "required": wkb_clause.required,
    }
    if getattr(wkb_clause, "callable", None) is not None:
        shared_callable = _MSSQLDynamicEWKTCallable(wkb_clause.callable)
        bind_kwargs["callable_"] = shared_callable
    elif not wkb_clause.required:
        bind_kwargs["value"] = getattr(wkb_clause, "value", None)

    return (
        expression.bindparam(
            key=wkb_key,
            type_=_MSSQLWKBBindType(extended=True),
            **bind_kwargs,
        ),
        expression.bindparam(
            key=srid_key,
            type_=_MSSQLDynamicEWKBSRIDBindType(default_srid=default_srid),
            **bind_kwargs,
        ),
    )


def _get_mssql_dynamic_ewkb_bind_clauses(wkb_clause, compiler, default_srid=0):
    cache = getattr(compiler, "_geoalchemy2_mssql_dynamic_ewkb_bind_cache", None)
    if cache is None:
        cache = {}
        compiler._geoalchemy2_mssql_dynamic_ewkb_bind_cache = cache

    source_identifier = _mssql_dynamic_ewkt_bind_identifier(wkb_clause)
    if source_identifier not in cache:
        cache[source_identifier] = _make_mssql_dynamic_ewkb_bind_clauses(
            wkb_clause,
            default_srid=default_srid,
        )
    return cache[source_identifier]


def _collect_mssql_dynamic_ewkt_source_binds(clauseelement, dialect):
    if not hasattr(clauseelement, "get_children"):
        return ()

    source_binds = []
    seen_source_identifiers = set()
    for element in visitors.iterate(clauseelement):
        if not isinstance(element, functions.ST_GeomFromEWKT):
            continue

        clauses = list(element.clauses)
        if len(clauses) != 1 or not _is_bindparam_clause(clauses[0]):
            continue

        candidate_spatial_type = _resolve_mssql_spatial_type(element.type, dialect)
        if (
            _check_spatial_type(candidate_spatial_type, (Geometry, Geography), dialect)
            and getattr(candidate_spatial_type, "srid", -1) >= 0
        ):
            continue

        source_identifier = _mssql_dynamic_ewkt_bind_identifier(clauses[0])
        if source_identifier in seen_source_identifiers:
            continue
        seen_source_identifiers.add(source_identifier)
        source_binds.append(clauses[0])

    return tuple(source_binds)


def _collect_mssql_dynamic_ewkb_source_binds(clauseelement, dialect):
    if not hasattr(clauseelement, "get_children"):
        return ()

    source_binds = []
    seen_source_identifiers = set()
    for element in visitors.iterate(clauseelement):
        if not isinstance(element, functions.ST_GeomFromEWKB):
            continue

        clauses = list(element.clauses)
        if len(clauses) != 1 or not _is_bindparam_clause(clauses[0]):
            continue
        if _is_mssql_auto_constructor_bindparam(clauses[0], "ST_GeomFromEWKB"):
            continue

        candidate_spatial_type = _resolve_mssql_spatial_type(element.type, dialect)
        if (
            _check_spatial_type(candidate_spatial_type, (Geometry, Geography), dialect)
            and getattr(candidate_spatial_type, "srid", -1) >= 0
        ):
            continue

        source_identifier = _mssql_dynamic_ewkt_bind_identifier(clauses[0])
        if source_identifier in seen_source_identifiers:
            continue
        seen_source_identifiers.add(source_identifier)
        source_binds.append(clauses[0])

    return tuple(source_binds)


def _compile_mssql_statement_bind_name_map(clauseelement, dialect):
    if not hasattr(clauseelement, "compile"):
        return {}

    if hasattr(clauseelement, "execution_options"):
        clauseelement = clauseelement.execution_options(
            **{_MSSQL_DISABLE_DYNAMIC_EWKT_SPLIT_OPTION: True}
        )

    compiled = clauseelement.compile(dialect=dialect)
    bind_name_map = {}
    for bind, compiled_name in compiled.bind_names.items():
        bind_name_map.setdefault(
            getattr(bind, "_identifying_key", bind.key),
            compiled_name,
        )
    return bind_name_map


def _get_mssql_dynamic_ewkt_bind_mappings(clauseelement, dialect):
    source_binds = _collect_mssql_dynamic_ewkt_source_binds(clauseelement, dialect)
    if not source_binds:
        return ()

    statement_bind_name_map = _compile_mssql_statement_bind_name_map(clauseelement, dialect)
    dynamic_bind_mappings = []
    for source_bind in source_binds:
        source_identifier = _mssql_dynamic_ewkt_bind_identifier(source_bind)
        candidate_keys = []
        for candidate_key in (
            source_bind.key,
            getattr(source_bind, "_orig_key", None),
            statement_bind_name_map.get(source_identifier),
        ):
            if candidate_key is not None and candidate_key not in candidate_keys:
                candidate_keys.append(candidate_key)

        text_key, srid_key = _mssql_dynamic_ewkt_bind_keys(source_bind)
        dynamic_bind_mappings.append((tuple(candidate_keys), text_key, srid_key))

    return tuple(dynamic_bind_mappings)


def _get_mssql_dynamic_ewkb_bind_mappings(clauseelement, dialect):
    source_binds = _collect_mssql_dynamic_ewkb_source_binds(clauseelement, dialect)
    if not source_binds:
        return ()

    statement_bind_name_map = _compile_mssql_statement_bind_name_map(clauseelement, dialect)
    dynamic_bind_mappings = []
    for source_bind in source_binds:
        source_identifier = _mssql_dynamic_ewkt_bind_identifier(source_bind)
        candidate_keys = []
        for candidate_key in (
            source_bind.key,
            getattr(source_bind, "_orig_key", None),
            statement_bind_name_map.get(source_identifier),
        ):
            if candidate_key is not None and candidate_key not in candidate_keys:
                candidate_keys.append(candidate_key)

        wkb_key, srid_key = _mssql_dynamic_ewkb_bind_keys(source_bind)
        dynamic_bind_mappings.append((tuple(candidate_keys), wkb_key, srid_key))

    return tuple(dynamic_bind_mappings)


def _expand_mssql_dynamic_ewkt_param_mapping(parameters, dynamic_bind_mappings):
    if not isinstance(parameters, Mapping):
        return parameters, False

    expanded_parameters = parameters
    changed = False
    for source_keys, text_key, srid_key in dynamic_bind_mappings:
        source_key = next((key for key in source_keys if key in parameters), None)
        if source_key is None:
            continue

        if text_key in parameters and srid_key in parameters:
            continue

        if expanded_parameters is parameters:
            expanded_parameters = dict(parameters)

        source_value = parameters[source_key]
        expanded_parameters.setdefault(text_key, source_value)
        expanded_parameters.setdefault(srid_key, source_value)
        changed = True

    return expanded_parameters, changed


def before_execute(conn, clauseelement, multiparams, params, execution_options):
    dynamic_bind_mappings = _get_mssql_dynamic_ewkt_bind_mappings(
        clauseelement,
        conn.dialect,
    ) + _get_mssql_dynamic_ewkb_bind_mappings(clauseelement, conn.dialect)
    if not dynamic_bind_mappings:
        return clauseelement, multiparams, params

    multiparams_changed = False
    expanded_multiparams = multiparams
    if multiparams:
        expanded_values = []
        for value in multiparams:
            expanded_value, value_changed = _expand_mssql_dynamic_ewkt_param_mapping(
                value,
                dynamic_bind_mappings,
            )
            expanded_values.append(expanded_value)
            multiparams_changed = multiparams_changed or value_changed
        if multiparams_changed:
            expanded_multiparams = tuple(expanded_values)

    expanded_params, params_changed = _expand_mssql_dynamic_ewkt_param_mapping(
        params,
        dynamic_bind_mappings,
    )

    if multiparams_changed or params_changed:
        return clauseelement, expanded_multiparams, expanded_params
    return clauseelement, multiparams, params


def _compile_mssql_geom_from_text(element, compiler, strip_srid=False, **kw):
    clauses = list(element.clauses)
    clauses, inner_constructor = _unwrap_mssql_constructor_clauses(
        clauses,
        _is_mssql_text_constructor,
    )
    strip_srid = strip_srid or isinstance(inner_constructor, functions.ST_GeomFromEWKT)
    original_wkt_clause = clauses[0]
    wkt_clause = original_wkt_clause
    spatial_type = None
    split_disabled = bool(
        getattr(compiler, "execution_options", {}).get(
            _MSSQL_DISABLE_DYNAMIC_EWKT_SPLIT_OPTION,
            False,
        )
    )
    if strip_srid:
        candidate_spatial_type = _resolve_mssql_spatial_type(element.type, compiler.dialect)
        if (
            _check_spatial_type(candidate_spatial_type, (Geometry, Geography), compiler.dialect)
            and getattr(candidate_spatial_type, "srid", -1) >= 0
        ):
            spatial_type = candidate_spatial_type
    if (
        strip_srid
        and spatial_type is None
        and len(clauses) == 1
        and _is_bindparam_clause(original_wkt_clause)
        and not kw.get("literal_binds", False)
        and not split_disabled
    ):
        dynamic_text_clause, dynamic_srid_clause = _get_mssql_dynamic_ewkt_bind_clauses(
            original_wkt_clause,
            compiler,
        )
        compiled_wkt = compiler.process(dynamic_text_clause, **kw)
        compiled_srid = compiler.process(dynamic_srid_clause, **kw)
        return f"{element.type.name}::STGeomFromText({compiled_wkt}, {compiled_srid})"
    if (
        kw.get("literal_binds", False)
        or _is_bindparam_clause(original_wkt_clause)
        or _should_coerce_wkt_bind_clause_for_text(
            original_wkt_clause,
            strip_srid=strip_srid,
        )
    ):
        wkt_clause = _coerce_wkt_bind_clause(
            original_wkt_clause,
            strip_srid=strip_srid,
            literal=kw.get("literal_binds", False),
            spatial_type=spatial_type,
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
    clauses, inner_constructor = _unwrap_mssql_constructor_clauses(
        clauses,
        _is_mssql_wkb_constructor,
    )
    extended = extended or isinstance(inner_constructor, functions.ST_GeomFromEWKB)
    original_wkb_clause = clauses[0]
    wkb_clause = original_wkb_clause
    spatial_type = None
    split_disabled = bool(
        getattr(compiler, "execution_options", {}).get(
            _MSSQL_DISABLE_DYNAMIC_EWKT_SPLIT_OPTION,
            False,
        )
    )
    if extended:
        candidate_spatial_type = _resolve_mssql_spatial_type(element.type, compiler.dialect)
        if (
            _check_spatial_type(candidate_spatial_type, (Geometry, Geography), compiler.dialect)
            and getattr(candidate_spatial_type, "srid", -1) >= 0
        ):
            spatial_type = candidate_spatial_type
    if (
        extended
        and spatial_type is None
        and len(clauses) == 1
        and _is_bindparam_clause(original_wkb_clause)
        and not _is_mssql_auto_constructor_bindparam(original_wkb_clause, "ST_GeomFromEWKB")
        and not kw.get("literal_binds", False)
        and not split_disabled
    ):
        default_srid = element.type.srid if element.type.srid >= 0 else 0
        dynamic_wkb_clause, dynamic_srid_clause = _get_mssql_dynamic_ewkb_bind_clauses(
            original_wkb_clause,
            compiler,
            default_srid=default_srid,
        )
        compiled_wkb = compiler.process(dynamic_wkb_clause, **kw)
        compiled_srid = compiler.process(dynamic_srid_clause, **kw)
        return f"{element.type.name}::STGeomFromWKB({compiled_wkb}, {compiled_srid})"

    if (
        kw.get("literal_binds", False)
        or (extended and _is_bindparam_clause(original_wkb_clause))
        or _should_coerce_wkb_bind_clause(clauses[0])
    ):
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
    return _compile_mssql_as_ewkb(element, compiler, **kw)


@compiles(functions.ST_AsText, "mssql")  # type: ignore
def _MSSQL_ST_AsText(element, compiler, **kw):
    return _compile_mssql_method(element, compiler, "AsTextZM", **kw)


@compiles(functions.ST_AsEWKT, "mssql")  # type: ignore
def _MSSQL_ST_AsEWKT(element, compiler, **kw):
    return _compile_mssql_as_ewkt(element, compiler, **kw)


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


@compiles(functions.ST_Distance, "mssql")  # type: ignore
def _MSSQL_ST_Distance(element, compiler, **kw):
    return _compile_mssql_binary_method(element, compiler, "STDistance", **kw)


@compiles(functions.ST_DWithin, "mssql")  # type: ignore
def _MSSQL_ST_DWithin(element, compiler, **kw):
    return _compile_mssql_dwithin(element, compiler, **kw)


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


@compiles(BinaryExpression, "mssql")  # type: ignore
def _MSSQL_binary_expression(binary, compiler, override_operator=None, **kw):
    operator = override_operator or binary.operator
    if operator in (operators.eq, operators.ne):
        if _is_spatial_clause(binary.left, compiler.dialect):
            target_clause = binary.left
            other_clause = binary.right
        elif _is_spatial_clause(binary.right, compiler.dialect):
            target_clause = binary.right
            other_clause = binary.left
        else:
            target_clause = None

        if target_clause is not None and isinstance(other_clause, Null):
            target = compiler.process(target_clause, **kw)
            return f"{target} IS {'NOT ' if operator is operators.ne else ''}NULL"

        if target_clause is not None:
            other_clause = _coerce_mssql_spatial_method_argument(
                target_clause,
                other_clause,
                compiler.dialect,
            )

            target = compiler.process(target_clause, **kw)
            other = compiler.process(other_clause, **kw)
            equals = f"{target}.STEquals({other})"
            return f"{equals} = {1 if operator is operators.eq else 0}"

    return compiler.visit_binary(binary, override_operator=override_operator, **kw)
