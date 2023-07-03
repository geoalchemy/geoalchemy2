"""This module defines functions used by several dialects."""
import sqlalchemy
from packaging import version
from sqlalchemy import Column
from sqlalchemy.sql import expression
from sqlalchemy.types import TypeDecorator

from geoalchemy2.types import Geometry

_SQLALCHEMY_VERSION_BEFORE_14 = version.parse(sqlalchemy.__version__) < version.parse("1.4")


def _spatial_idx_name(table_name, column_name):
    return "idx_{}_{}".format(table_name, column_name)


def _format_select_args(*args):
    if _SQLALCHEMY_VERSION_BEFORE_14:
        return [args]
    else:
        return args


def check_management(*args):
    """Default function to check management (always True by default)."""
    return True


def _get_gis_cols(table, spatial_types, dialect, check_col_management=None):
    if check_col_management is not None:
        func = check_col_management
    else:
        func = check_management
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
    column_collection = expression.ColumnCollection()
    for col in regular_cols:
        column_collection.add(col)
    table.columns = column_collection


def setup_create_drop(table, bind, check_col_management=None):
    """Prepare the table for before_create and before_drop events."""
    dialect, gis_cols, regular_cols = _get_dispatch_info(table, bind, check_col_management)
    _update_table_for_dispatch(table, regular_cols)
    return dialect, gis_cols, regular_cols


def reflect_geometry_column(inspector, table, column_info):
    return


def before_create(table, bind, **kw):
    return


def after_create(table, bind, **kw):
    return


def before_drop(table, bind, **kw):
    return


def after_drop(table, bind, **kw):
    return
