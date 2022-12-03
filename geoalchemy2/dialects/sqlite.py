"""This module defines specific functions for SQLite dialect."""
import os

from sqlalchemy import text
from sqlalchemy.sql import func
from sqlalchemy.sql import select

from geoalchemy2.dialects.common import _check_spatial_type
from geoalchemy2.dialects.common import _format_select_args
from geoalchemy2.dialects.common import _spatial_idx_name
from geoalchemy2.dialects.common import check_management
from geoalchemy2.dialects.common import setup_create_drop
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry
from geoalchemy2.types import _DummyGeometry


def load_spatialite(dbapi_conn, connection_record):
    """Load SpatiaLite extension in SQLite DB.

    The path to the SpatiaLite module should be set in the `SPATIALITE_LIBRARY_PATH` environment
    variable.
    """
    if "SPATIALITE_LIBRARY_PATH" not in os.environ:
        raise RuntimeError("The SPATIALITE_LIBRARY_PATH environment variable is not set.")
    dbapi_conn.enable_load_extension(True)
    dbapi_conn.load_extension(os.environ["SPATIALITE_LIBRARY_PATH"])
    dbapi_conn.enable_load_extension(False)
    dbapi_conn.execute("SELECT InitSpatialMetaData();")


def _get_spatialite_attrs(bind, table_name, col_name):
    col_attributes = bind.execute(
        text(
            """SELECT * FROM "geometry_columns"
           WHERE f_table_name = '{}' and f_geometry_column = '{}'
        """.format(
                table_name, col_name
            )
        )
    ).fetchone()
    return col_attributes


def get_spatialite_version(bind):
    """Get the version of the currently loaded Spatialite extension."""
    return bind.execute(text("""SELECT spatialite_version();""")).fetchone()[0]


def _setup_dummy_type(table, gis_cols):
    """Setup dummy type for new Geometry columns so they can be updated later into."""
    for col in gis_cols:
        # Add dummy columns with GEOMETRY type
        col._actual_type = col.type
        col.type = _DummyGeometry()
        col.nullable = col._actual_type.nullable
    table.columns = table.info["_saved_columns"]


def get_col_dim(col):
    """Get dimension of the column type."""
    if col.type.dimension == 4:
        dimension = "XYZM"
    elif col.type.dimension == 2:
        dimension = "XY"
    else:
        if col.type.geometry_type.endswith("M"):
            dimension = "XYM"
        else:
            dimension = "XYZ"
    return dimension


def create_spatial_index(bind, table, col):
    """Create spatial index on the given column."""
    stmt = select(*_format_select_args(func.CreateSpatialIndex(table.name, col.name)))
    stmt = stmt.execution_options(autocommit=True)
    bind.execute(stmt)


def disable_spatial_index(bind, table, col):
    """Disable spatial indexes if present."""
    stmt = select(*_format_select_args(func.CheckSpatialIndex(table.name, col.name)))
    if bind.execute(stmt).fetchone()[0] is not None:
        stmt = select(*_format_select_args(func.DisableSpatialIndex(table.name, col.name)))
        stmt = stmt.execution_options(autocommit=True)
        bind.execute(stmt)
        bind.execute(
            text(
                """DROP TABLE IF EXISTS {};""".format(
                    _spatial_idx_name(
                        table.name,
                        col.name,
                    )
                )
            )
        )


def reflect_geometry_column(inspector, table, column_info):
    """Reflect a column of type Geometry with SQLite dialect."""
    # Get geometry type, SRID and spatial index from the SpatiaLite metadata
    col_attributes = _get_spatialite_attrs(inspector.bind, table.name, column_info["name"])
    if col_attributes is not None:
        _, _, geometry_type, coord_dimension, srid, spatial_index = col_attributes

        if isinstance(geometry_type, int):
            geometry_type_str = str(geometry_type)
            if geometry_type >= 1000:
                first_digit = geometry_type_str[0]
                has_z = first_digit in ["1", "3"]
                has_m = first_digit in ["2", "3"]
            else:
                has_z = has_m = False
            geometry_type = {
                "0": "GEOMETRY",
                "1": "POINT",
                "2": "LINESTRING",
                "3": "POLYGON",
                "4": "MULTIPOINT",
                "5": "MULTILINESTRING",
                "6": "MULTIPOLYGON",
                "7": "GEOMETRYCOLLECTION",
            }[geometry_type_str[-1]]
            if has_z:
                geometry_type += "Z"
            if has_m:
                geometry_type += "M"
        else:
            if "Z" in coord_dimension:
                geometry_type += "Z"
            if "M" in coord_dimension:
                geometry_type += "M"
            coord_dimension = {
                "XY": 2,
                "XYZ": 3,
                "XYM": 3,
                "XYZM": 4,
            }.get(coord_dimension, coord_dimension)

        # Set attributes
        column_info["type"].geometry_type = geometry_type
        column_info["type"].dimension = coord_dimension
        column_info["type"].srid = srid
        column_info["type"].spatial_index = bool(spatial_index)

        # Spatial indexes are not automatically reflected with SQLite dialect
        column_info["type"]._spatial_index_reflected = False


def before_create(table, bind, **kw):
    """Handle spatial indexes during the before_create event."""
    dialect, gis_cols, regular_cols = setup_create_drop(table, bind)
    dialect_name = dialect.name

    # Remove the spatial indexes from the table metadata because they should not be
    # created during the table.create() step since the associated columns do not exist
    # at this time.
    table.info["_after_create_indexes"] = []
    current_indexes = set(table.indexes)
    for idx in current_indexes:
        for col in table.info["_saved_columns"]:
            if (
                _check_spatial_type(col.type, Geometry, dialect)
                and check_management(col, dialect_name)
            ) and col in idx.columns.values():
                table.indexes.remove(idx)
                if idx.name != _spatial_idx_name(table.name, col.name) or not getattr(
                    col.type, "spatial_index", False
                ):
                    table.info["_after_create_indexes"].append(idx)
    _setup_dummy_type(table, gis_cols)


def after_create(table, bind, **kw):
    """Handle spatial indexes during the after_create event."""
    dialect = bind.dialect
    dialect_name = dialect.name

    table.columns = table.info.pop("_saved_columns")

    for col in table.columns:
        # Add the managed Geometry columns with AddGeometryColumn()
        if _check_spatial_type(col.type, Geometry, dialect) and check_management(col, dialect_name):
            col.type = col._actual_type
            del col._actual_type
            dimension = get_col_dim(col)
            args = [table.schema] if table.schema else []
            args.extend([table.name, col.name, col.type.srid, col.type.geometry_type, dimension])

            stmt = select(*_format_select_args(func.RecoverGeometryColumn(*args)))
            stmt = stmt.execution_options(autocommit=True)
            bind.execute(stmt)

        # Add spatial indices for the Geometry and Geography columns
        if (
            _check_spatial_type(col.type, (Geometry, Geography), dialect)
            and col.type.spatial_index is True
        ):
            create_spatial_index(bind, table, col)

    for idx in table.info.pop("_after_create_indexes"):
        table.indexes.add(idx)
        idx.create(bind=bind)


def before_drop(table, bind, **kw):
    """Handle spatial indexes during the before_drop event."""
    dialect, gis_cols, regular_cols = setup_create_drop(table, bind)
    for col in gis_cols:
        # Disable spatial indexes if present
        disable_spatial_index(bind, table, col)

        args = [table.schema] if table.schema else []
        args.extend([table.name, col.name])

        stmt = select(*_format_select_args(func.DiscardGeometryColumn(*args)))
        stmt = stmt.execution_options(autocommit=True)
        bind.execute(stmt)


def after_drop(table, bind, **kw):
    """Handle spatial indexes during the after_drop event."""
    table.columns = table.info.pop("_saved_columns")
