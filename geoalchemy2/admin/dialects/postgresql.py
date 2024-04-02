"""This module defines specific functions for Postgresql dialect."""

from sqlalchemy import Index
from sqlalchemy import text
from sqlalchemy.sql import func
from sqlalchemy.sql import select

from geoalchemy2.admin.dialects.common import _check_spatial_type
from geoalchemy2.admin.dialects.common import _format_select_args
from geoalchemy2.admin.dialects.common import _spatial_idx_name
from geoalchemy2.admin.dialects.common import setup_create_drop
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry


def check_management(column):
    """Check if the column should be managed."""
    return getattr(column.type, "use_typmod", None) is False


def create_spatial_index(bind, table, col):
    """Create spatial index on the given column."""
    if col.type.use_N_D_index:
        postgresql_ops = {col.name: "gist_geometry_ops_nd"}
    else:
        postgresql_ops = {}
    idx = Index(
        _spatial_idx_name(table.name, col.name),
        col,
        postgresql_using="gist",
        postgresql_ops=postgresql_ops,
        _column_flag=True,
    )
    idx.create(bind=bind)


def reflect_geometry_column(inspector, table, column_info):
    """Reflect a column of type Geometry with Postgresql dialect."""
    if not isinstance(column_info.get("type"), Geometry):
        return
    geo_type = column_info["type"]
    geometry_type = geo_type.geometry_type
    coord_dimension = geo_type.dimension
    if geometry_type.endswith("ZM"):
        coord_dimension = 4
    elif geometry_type[-1] in ["Z", "M"]:
        coord_dimension = 3

    # Query to check a given column has spatial index
    if table.schema is not None:
        schema_part = " AND nspname = '{}'".format(table.schema)
    else:
        schema_part = ""

    has_index_query = """SELECT (indexrelid IS NOT NULL) AS has_index
        FROM (
            SELECT
                    n.nspname,
                    c.relname,
                    c.oid AS relid,
                    a.attname,
                    a.attnum
            FROM pg_attribute a
            INNER JOIN pg_class c ON (a.attrelid=c.oid)
            INNER JOIN pg_type t ON (a.atttypid=t.oid)
            INNER JOIN pg_namespace n ON (c.relnamespace=n.oid)
            WHERE t.typname='geometry'
                    AND c.relkind='r'
        ) g
        LEFT JOIN pg_index i ON (g.relid = i.indrelid AND g.attnum = ANY(i.indkey))
        WHERE relname = '{}' AND attname = '{}'{};
    """.format(
        table.name, column_info["name"], schema_part
    )
    spatial_index = inspector.bind.execute(text(has_index_query)).scalar()

    # Set attributes
    column_info["type"].geometry_type = geometry_type
    column_info["type"].dimension = coord_dimension
    column_info["type"].spatial_index = bool(spatial_index)

    # Spatial indexes are automatically reflected with PostgreSQL dialect
    column_info["type"]._spatial_index_reflected = True


def before_create(table, bind, **kw):
    """Handle spatial indexes during the before_create event."""
    dialect, gis_cols, regular_cols = setup_create_drop(table, bind, check_management)

    # Remove the spatial indexes from the table metadata because they should not be
    # created during the table.create() step since the associated columns do not exist
    # at this time.
    table.info["_after_create_indexes"] = []
    current_indexes = set(table.indexes)
    for idx in current_indexes:
        for col in table.info["_saved_columns"]:
            if (
                _check_spatial_type(col.type, Geometry, dialect) and check_management(col)
            ) and col in idx.columns.values():
                table.indexes.remove(idx)
                if idx.name != _spatial_idx_name(table.name, col.name) or not getattr(
                    col.type, "spatial_index", False
                ):
                    table.info["_after_create_indexes"].append(idx)


def after_create(table, bind, **kw):
    """Handle spatial indexes during the after_create event."""
    # Restore original column list including managed Geometry columns
    dialect = bind.dialect

    table.columns = table.info.pop("_saved_columns")

    for col in table.columns:
        # Add the managed Geometry columns with AddGeometryColumn()
        if _check_spatial_type(col.type, Geometry, dialect) and check_management(col):
            dimension = col.type.dimension
            args = [table.schema] if table.schema else []
            args.extend([table.name, col.name, col.type.srid, col.type.geometry_type, dimension])
            if col.type.use_typmod is not None:
                args.append(col.type.use_typmod)

            stmt = select(*_format_select_args(func.AddGeometryColumn(*args)))
            stmt = stmt.execution_options(autocommit=True)
            bind.execute(stmt)

        # Add spatial indices for the Geometry and Geography columns
        if (
            _check_spatial_type(col.type, (Geometry, Geography), dialect)
            and col.type.spatial_index is True
        ):
            # If the index does not exist, define it and create it
            if not [i for i in table.indexes if col in i.columns.values()] and check_management(
                col
            ):
                create_spatial_index(bind, table, col)

    for idx in table.info.pop("_after_create_indexes"):
        table.indexes.add(idx)
        idx.create(bind=bind)


def before_drop(table, bind, **kw):
    """Handle spatial indexes during the before_drop event."""
    dialect, gis_cols, regular_cols = setup_create_drop(table, bind, check_management)

    # Drop the managed Geometry columns
    for col in gis_cols:
        args = [table.schema] if table.schema else []
        args.extend([table.name, col.name])

        stmt = select(*_format_select_args(func.DropGeometryColumn(*args)))
        stmt = stmt.execution_options(autocommit=True)
        bind.execute(stmt)


def after_drop(table, bind, **kw):
    """Handle spatial indexes during the after_drop event."""
    # Restore original column list including managed Geometry columns
    saved_cols = table.info.pop("_saved_columns", None)
    if saved_cols is not None:
        table.columns = saved_cols
