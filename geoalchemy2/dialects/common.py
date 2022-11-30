"""This module defines functions used by several dialects."""
import sqlalchemy
from packaging import version
from sqlalchemy import Column
from sqlalchemy.types import TypeDecorator

_SQLALCHEMY_VERSION_BEFORE_14 = version.parse(sqlalchemy.__version__) < version.parse("1.4")


def _spatial_idx_name(table_name, column_name):
    return 'idx_{}_{}'.format(table_name, column_name)


def _format_select_args(*args):
    if _SQLALCHEMY_VERSION_BEFORE_14:
        return [args]
    else:
        return args


def _get_gis_cols(table, spatial_types, dialect, check_col_management=False):
    return [
        col
        for col in table.columns
        if (
            isinstance(col, Column)
            and _check_spatial_type(col.type, spatial_types, dialect)
            and (
                not check_col_management
                or check_management(col, dialect.name)
            )
        )
    ]


def _check_spatial_type(tested_type, spatial_types, dialect=None):
    return (
        isinstance(tested_type, spatial_types)
        or (
            isinstance(tested_type, TypeDecorator)
            and isinstance(tested_type.load_dialect_impl(dialect), spatial_types)
        )
    )


def check_management(column, dialect_name):
    return getattr(column.type, "management", False) is True or dialect_name == 'sqlite'
