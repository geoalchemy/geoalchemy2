"""This module defines specific functions for MSSQL dialect."""

from sqlalchemy import text
from sqlalchemy.dialects.mssql.base import ischema_names as _mssql_ischema_names
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import expression
from sqlalchemy.sql.sqltypes import NullType

from geoalchemy2 import functions
from geoalchemy2.admin.dialects.common import _check_spatial_type
from geoalchemy2.admin.dialects.common import _spatial_idx_name
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry

_mssql_ischema_names["geometry"] = Geometry
_mssql_ischema_names["geography"] = Geography


def reflect_geometry_column(inspector, table, column_info):
    """Reflect a geometry or geography column with the MSSQL dialect."""
    if not isinstance(column_info.get("type"), Geometry | Geography | NullType):
        return

    column_name = column_info["name"]
    schema = table.schema or inspector.default_schema_name
    full_table_name = f"{schema}.{table.name}" if schema else table.name

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

    index_query = text(
        """SELECT COUNT(1)
        FROM sys.indexes AS i
        JOIN sys.index_columns AS ic
            ON i.object_id = ic.object_id
            AND i.index_id = ic.index_id
        JOIN sys.columns AS c
            ON ic.object_id = c.object_id
            AND ic.column_id = c.column_id
        WHERE
            i.object_id = OBJECT_ID(:full_table_name)
            AND c.name = :column_name
            AND i.type_desc = 'SPATIAL'"""
    )
    spatial_index = bool(
        inspector.bind.execute(
            index_query,
            {"full_table_name": full_table_name, "column_name": column_name},
        ).scalar()
    )

    spatial_type = Geography if type_name.lower() == "geography" else Geometry
    column_info["type"] = spatial_type(
        geometry_type="GEOMETRY",
        srid=-1,
        spatial_index=spatial_index,
        nullable=bool(is_nullable),
        _spatial_index_reflected=True,
    )


def before_create(table, bind, **kw):
    """Remove auto-generated spatial indexes before CREATE TABLE.

    MSSQL spatial indexes need dedicated DDL. For now we skip automatic index
    creation rather than emitting an invalid regular CREATE INDEX statement.
    """
    table.info["_after_create_indexes"] = []
    current_indexes = set(table.indexes)

    for idx in current_indexes:
        for col in table.columns:
            if _check_spatial_type(
                col.type, (Geometry, Geography), bind.dialect
            ) and col in idx.columns.values():
                table.indexes.remove(idx)
                if idx.name != _spatial_idx_name(table.name, col.name) or not getattr(
                    col.type, "spatial_index", False
                ):
                    table.info["_after_create_indexes"].append(idx)
                break


def after_create(table, bind, **kw):
    for idx in table.info.pop("_after_create_indexes", []):
        table.indexes.add(idx)


def before_drop(table, bind, **kw):
    return


def after_drop(table, bind, **kw):
    return


def _coerce_wkt_bind_clause(wkt_clause, strip_srid=False):
    if not hasattr(wkt_clause, "value"):
        return wkt_clause

    value = wkt_clause.value
    if isinstance(value, WKTElement):
        value = value.data
    if isinstance(value, str) and strip_srid:
        wkt_match = WKTElement._REMOVE_SRID.match(value)
        value = wkt_match.group(3)

    return expression.bindparam(
        key=wkt_clause.key,
        value=value,
        type_=wkt_clause.type,
        unique=True,
    )


def _coerce_wkb_bind_clause(wkb_clause, extended=False):
    if not hasattr(wkb_clause, "value"):
        return wkb_clause

    value = wkb_clause.value
    if isinstance(value, WKBElement):
        value = value.data
    if extended:
        value = WKBElement(value, extended=True).as_wkb().data
    if isinstance(value, memoryview):
        value = value.tobytes()

    return expression.bindparam(
        key=wkb_clause.key,
        value=value,
        type_=wkb_clause.type,
        unique=True,
    )


def _compile_mssql_method(element, compiler, method_name, property_=False, **kw):
    clauses = list(element.clauses)
    target = compiler.process(clauses[0], **kw)
    if property_:
        return f"{target}.{method_name}"

    compiled_args = ", ".join(compiler.process(arg, **kw) for arg in clauses[1:])
    return f"{target}.{method_name}({compiled_args})"


def _compile_mssql_geom_from_text(element, compiler, strip_srid=False, **kw):
    clauses = list(element.clauses)
    original_wkt_clause = clauses[0]
    wkt_clause = original_wkt_clause
    if kw.get("literal_binds", False):
        wkt_clause = _coerce_wkt_bind_clause(original_wkt_clause, strip_srid=strip_srid)
    compiled_wkt = compiler.process(wkt_clause, **kw)

    if len(clauses) > 1:
        compiled_srid = compiler.process(clauses[1], **kw)
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
    wkb_clause = clauses[0]
    if kw.get("literal_binds", False):
        wkb_clause = _coerce_wkb_bind_clause(clauses[0], extended=extended)

    if kw.get("literal_binds", False) and hasattr(wkb_clause, "value"):
        compiled_wkb = f"0x{WKBElement._wkb_to_hex(wkb_clause.value)}"
    else:
        compiled_wkb = compiler.process(wkb_clause, **kw)

    if len(clauses) > 1:
        compiled_srid = compiler.process(clauses[1], **kw)
    else:
        compiled_srid = str(element.type.srid if element.type.srid >= 0 else 0)

    return f"{element.type.name}::STGeomFromWKB({compiled_wkb}, {compiled_srid})"


@compiles(functions.ST_GeomFromText, "mssql")  # type: ignore
def _MSSQL_ST_GeomFromText(element, compiler, **kw):
    return _compile_mssql_geom_from_text(element, compiler, **kw)


@compiles(functions.ST_GeomFromEWKT, "mssql")  # type: ignore
def _MSSQL_ST_GeomFromEWKT(element, compiler, **kw):
    return _compile_mssql_geom_from_text(element, compiler, strip_srid=True, **kw)


@compiles(functions.ST_GeomFromWKB, "mssql")  # type: ignore
def _MSSQL_ST_GeomFromWKB(element, compiler, **kw):
    return _compile_mssql_geom_from_wkb(element, compiler, **kw)


@compiles(functions.ST_GeomFromEWKB, "mssql")  # type: ignore
def _MSSQL_ST_GeomFromEWKB(element, compiler, **kw):
    return _compile_mssql_geom_from_wkb(element, compiler, extended=True, **kw)


@compiles(functions.ST_AsBinary, "mssql")  # type: ignore
def _MSSQL_ST_AsBinary(element, compiler, **kw):
    return _compile_mssql_method(element, compiler, "STAsBinary", **kw)


@compiles(functions.ST_AsEWKB, "mssql")  # type: ignore
def _MSSQL_ST_AsEWKB(element, compiler, **kw):
    return _compile_mssql_method(element, compiler, "STAsBinary", **kw)


@compiles(functions.ST_AsText, "mssql")  # type: ignore
def _MSSQL_ST_AsText(element, compiler, **kw):
    return _compile_mssql_method(element, compiler, "STAsText", **kw)


@compiles(functions.ST_GeometryType, "mssql")  # type: ignore
def _MSSQL_ST_GeometryType(element, compiler, **kw):
    return _compile_mssql_method(element, compiler, "STGeometryType", **kw)


@compiles(functions.ST_SRID, "mssql")  # type: ignore
def _MSSQL_ST_SRID(element, compiler, **kw):
    return _compile_mssql_method(element, compiler, "STSrid", property_=True, **kw)


@compiles(functions.ST_Buffer, "mssql")  # type: ignore
def _MSSQL_ST_Buffer(element, compiler, **kw):
    return _compile_mssql_method(element, compiler, "STBuffer", **kw)
