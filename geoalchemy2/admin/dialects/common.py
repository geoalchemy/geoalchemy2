"""This module defines functions used by several dialects."""

import sqlalchemy
from packaging import version
from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy.sql import expression
from sqlalchemy.sql.elements import BindParameter
from sqlalchemy.types import TypeDecorator

from geoalchemy2 import _wkb_wkt
from geoalchemy2.elements import WKBElement
from geoalchemy2.types import Geometry

_SQLALCHEMY_VERSION_BEFORE_14 = version.parse(sqlalchemy.__version__) < version.parse("1.4")
_COLUMN_COLLECTION_CLASS = getattr(
    expression, "WriteableColumnCollection", expression.ColumnCollection
)


def _spatial_idx_name(table_name, column_name):
    return f"idx_{table_name}_{column_name}"


def _format_select_args(*args):
    if _SQLALCHEMY_VERSION_BEFORE_14:
        return [args]
    else:
        return args


def check_management(*args):
    """Default function to check management (always True by default)."""
    return True


def _get_gis_cols(table, spatial_types, dialect, check_col_management=None):
    func = check_col_management if check_col_management is not None else check_management
    return [
        col
        for col in table.columns
        if (
            isinstance(col, Column)
            and _check_spatial_type(col.type, spatial_types, dialect)
            and func(col)
        )
    ]


def _check_spatial_type(tested_type, spatial_types, dialect=None):
    return isinstance(tested_type, spatial_types) or (
        isinstance(tested_type, TypeDecorator)
        and isinstance(tested_type.load_dialect_impl(dialect), spatial_types)
    )


def _get_dispatch_info(table, bind, check_col_management=None):
    """Get info required for dispatch events."""
    dialect = bind.dialect

    # Filter Geometry columns from the table
    # Note: Geography and PostGIS >= 2.0 don't need this
    gis_cols = _get_gis_cols(table, Geometry, dialect, check_col_management=check_col_management)

    # Find all other columns that are not managed Geometries
    regular_cols = [x for x in table.columns if x not in gis_cols]

    return dialect, gis_cols, regular_cols


def _update_table_for_dispatch(table, regular_cols):
    """Update the table before dispatch events."""
    # Save original table column list for later
    table.info["_saved_columns"] = table.columns

    # Temporarily patch a set of columns not including the
    # managed Geometry columns
    column_collection = _COLUMN_COLLECTION_CLASS()
    for col in regular_cols:
        column_collection.add(col)
    table.columns = column_collection


def setup_create_drop(table, bind, check_col_management=None):
    """Prepare the table for before_create and before_drop events."""
    dialect, gis_cols, regular_cols = _get_dispatch_info(table, bind, check_col_management)
    _update_table_for_dispatch(table, regular_cols)
    return dialect, gis_cols, regular_cols


def reflect_geometry_column(inspector, table, column_info):
    return  # pragma: no cover


def before_create(table, bind, **kw):
    return  # pragma: no cover


def after_create(table, bind, **kw):
    return  # pragma: no cover


def before_drop(table, bind, **kw):
    return  # pragma: no cover


def after_drop(table, bind, **kw):
    return  # pragma: no cover


def compile_bin_literal(wkb_clause):
    """Compile a binary literal for WKBElement."""
    if not hasattr(wkb_clause, "value"):
        return wkb_clause

    wkb_data = wkb_clause.value
    if isinstance(wkb_data, (bytes, bytearray, memoryview, WKBElement)):
        if isinstance(wkb_data, bytearray):
            wkb_data = bytes(wkb_data)
        if isinstance(wkb_data, memoryview):
            wkb_data = wkb_data.tobytes()
        if isinstance(wkb_data, bytes):
            wkb_data = WKBElement._wkb_to_hex(wkb_data)
        elif isinstance(wkb_data, WKBElement):
            wkb_data = wkb_data.desc

        wkb_clause = expression.bindparam(
            key=wkb_clause.key,
            value=wkb_data,
            type_=String(),
            unique=True,
        )
    return wkb_clause


def _is_auto_constructor_bindparam(clause, constructor_name):
    return (
        isinstance(clause, BindParameter)
        and getattr(clause, "unique", False)
        and getattr(clause, "_orig_key", None) == constructor_name
    )


def _has_known_literal_srid(clause):
    if not hasattr(clause, "value"):
        return True

    try:
        return _wkb_wkt.is_known_srid(int(clause.value))
    except (TypeError, ValueError):
        return True


def unwrap_wkb_constructor_clauses(clauses):
    if len(clauses) != 1:
        return clauses, None

    from geoalchemy2 import functions

    inner_constructor = clauses[0]
    constructor_names = (
        ("ST_GeomFromWKB", functions.ST_GeomFromWKB),
        ("ST_GeomFromEWKB", functions.ST_GeomFromEWKB),
    )
    for constructor_name, constructor_class in constructor_names:
        if not isinstance(inner_constructor, constructor_class):
            continue
        inner_clauses = list(inner_constructor.clauses)
        if inner_clauses and _is_auto_constructor_bindparam(inner_clauses[0], constructor_name):
            if len(inner_clauses) > 1 and not _has_known_literal_srid(inner_clauses[1]):
                inner_clauses = inner_clauses[:1]
            return inner_clauses, inner_constructor

    return clauses, None
