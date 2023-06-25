"""This module defines specific functions for SQLite dialect."""
import os
import re

from sqlalchemy import text
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import func
from sqlalchemy.sql import select

from geoalchemy2 import functions
from geoalchemy2.admin.dialects.common import _check_spatial_type
from geoalchemy2.admin.dialects.common import _format_select_args
from geoalchemy2.admin.dialects.common import _spatial_idx_name
from geoalchemy2.admin.dialects.common import check_management
from geoalchemy2.admin.dialects.common import setup_create_drop
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry
from geoalchemy2.types import _DummyGeometry


def is_gpkg(bind):
    """Check if a connection is linked to a GeoPackage."""
    try:
        return bind.info["_is_gpkg"]
    except KeyError:
        if bool(bind.execute(text("""SELECT HasGeopackage();""")).scalar()) and bool(
            bind.execute(text("""SELECT CheckGeoPackageMetaData();""")).scalar()
        ):
            db_is_gpkg = True
        else:
            db_is_gpkg = False
        bind.info["_is_gpkg"] = db_is_gpkg
        return db_is_gpkg


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
    try:
        databases = [
            i[-1].endswith(".gpkg") for i in dbapi_conn.execute("PRAGMA database_list;").fetchall()
        ]
        is_GeoPkg = any(databases)
    except Exception:
        is_GeoPkg = False
    if is_GeoPkg:
        if not dbapi_conn.execute("SELECT CheckGeoPackageMetaData();").fetchone()[0]:
            # This only works on the main database
            dbapi_conn.execute("SELECT gpkgCreateBaseTables();")
        dbapi_conn.execute("SELECT AutoGpkgStart();")
        dbapi_conn.execute("SELECT EnableGpkgAmphibiousMode();")
    else:
        dbapi_conn.execute("SELECT InitSpatialMetaData();")


def _get_spatialite_attrs(bind, table_name, col_name):
    if is_gpkg(bind):
        attrs = bind.execute(
            text(
                """SELECT
                    A.geometry_type_name,
                    A.srs_id,
                    A.z,
                    A.m,
                    IFNULL(B.has_index, 0) AS has_index
                FROM gpkg_geometry_columns
                AS A
                LEFT JOIN (
                    SELECT table_name, column_name, COUNT(*) AS has_index
                    FROM gpkg_extensions
                    WHERE table_name = '{table_name}'
                        AND column_name = '{column_name}'
                        AND extension_name = 'gpkg_rtree_index'
                ) AS B
                ON A.table_name = B.table_name AND A.column_name = B.column_name
                WHERE A.table_name = '{table_name}' AND A.column_name = '{column_name}';
            """.format(
                    table_name=table_name, column_name=col_name
                )
            )
        ).fetchone()
        if attrs is None:
            # If the column is not registered as a spatial column we ignore it
            return None
        geometry_type, srid, has_z, has_m, has_index = attrs
        coord_dimension = "XY"
        if has_z:
            coord_dimension += "Z"
        if has_m:
            coord_dimension += "M"
        col_attributes = geometry_type, coord_dimension, srid, has_index
    else:
        attrs = bind.execute(
            text(
                """SELECT * FROM "geometry_columns"
               WHERE f_table_name = '{}' and f_geometry_column = '{}'
            """.format(
                    table_name, col_name
                )
            )
        ).fetchone()
        if attrs is None:
            # If the column is not registered as a spatial column we ignore it
            return None
        col_attributes = attrs[2:]
    return col_attributes


def get_spatialite_version(bind):
    """Get the version of the currently loaded Spatialite extension."""
    return bind.execute(text("""SELECT spatialite_version();""")).fetchone()[0]


def _setup_dummy_type(table, gis_cols, gpkg=False):
    """Setup dummy type for new Geometry columns so they can be updated later into."""
    for col in gis_cols:
        # Add dummy columns with GEOMETRY type
        if gpkg:
            type_str = re.fullmatch("(.+?)[ZMzm]*", col.type.geometry_type).group(1)
        else:
            type_str = "GEOMETRY"
        col._actual_type = col.type
        col.type = _DummyGeometry(geometry_type=type_str)
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
    if is_gpkg(bind):
        index_func = func.gpkgAddSpatialIndex
    else:
        index_func = func.CreateSpatialIndex
    stmt = select(*_format_select_args(index_func(table.name, col.name)))
    stmt = stmt.execution_options(autocommit=True)
    bind.execute(stmt)


def disable_spatial_index(bind, table, col):
    """Disable spatial indexes if present."""
    if is_gpkg(bind):
        # return
        for i in ["", "_node", "_parent", "_rowid"]:
            bind.execute(
                text(
                    """DROP TABLE IF EXISTS rtree_{}_{}{};""".format(
                        table.name,
                        col.name,
                        i,
                    )
                )
            )
        bind.execute(
            text(
                """DELETE FROM gpkg_extensions
                WHERE table_name = '{}'
                    AND column_name = '{}'
                    AND extension_name = 'gpkg_rtree_index';""".format(
                    table.name,
                    col.name,
                )
            )
        )
    else:
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
    if not isinstance(column_info.get("type"), Geometry):
        return
    col_attributes = _get_spatialite_attrs(inspector.bind, table.name, column_info["name"])
    if col_attributes is not None:
        geometry_type, coord_dimension, srid, spatial_index = col_attributes

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
            if "Z" in coord_dimension and "Z" not in geometry_type[-2:]:
                geometry_type += "Z"
            if "M" in coord_dimension and "M" not in geometry_type[-2:]:
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
    db_is_gpkg = is_gpkg(bind)

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

    if db_is_gpkg:
        if len(gis_cols) > 1:
            raise ValueError(
                "Only one geometry column is allowed for a table stored in a GeoPackage."
            )
        elif len(gis_cols) == 1:
            col = gis_cols[0]
            srid = col.type.srid

            if col.type.geometry_type is None:
                col.type.geometry_type = "GEOMETRY"

            # Add the SRID of the table in 'gpkg_spatial_ref_sys' if this table exists
            if (
                bind.execute(text("""PRAGMA main.table_info("gpkg_spatial_ref_sys");""")).fetchall()
                and not bind.execute(
                    text(
                        """SELECT COUNT(*) FROM gpkg_spatial_ref_sys WHERE srs_id = {};""".format(
                            srid
                        )
                    )
                ).scalar()
            ):
                bind.execute(text("SELECT gpkgInsertEpsgSRID({})".format(srid)))
            # table.columns = table.info.pop("_saved_columns")
        _setup_dummy_type(table, gis_cols, gpkg=True)
    else:
        _setup_dummy_type(table, gis_cols, gpkg=False)


def after_create(table, bind, **kw):
    """Handle spatial indexes during the after_create event."""
    dialect = bind.dialect
    dialect_name = dialect.name
    db_is_gpkg = is_gpkg(bind)

    if db_is_gpkg:
        for col in table.columns:
            # Add the managed Geometry columns with gpkgAddGeometryColumn()
            if _check_spatial_type(col.type, Geometry, dialect) and check_management(
                col, dialect_name
            ):
                col.type = col._actual_type
                del col._actual_type
                dimension = get_col_dim(col)
                has_z = "Z" in dimension
                has_m = "M" in dimension

                bind.execute(
                    text(
                        """INSERT INTO gpkg_contents
                        VALUES (
                            '{}',
                            'features',
                            NULL,
                            NULL,
                            strftime('%Y-%m-%dT%H:%M:%fZ', CURRENT_TIMESTAMP),
                            NULL,
                            NULL,
                            NULL,
                            NULL,
                            {}
                        );""".format(
                            table.name,
                            col.type.srid,
                        )
                    )
                )
                bind.execute(
                    text(
                        """INSERT INTO gpkg_geometry_columns
                        VALUES ('{}', '{}', '{}', {}, {}, {});""".format(
                            table.name,
                            col.name,
                            col.type.geometry_type,
                            col.type.srid,
                            has_z,
                            has_m,
                        )
                    )
                )
                stmt = select(
                    *_format_select_args(func.gpkgAddGeometryTriggers(table.name, col.name))
                )
                stmt = stmt.execution_options(autocommit=True)
                bind.execute(stmt)
    else:
        table.columns = table.info.pop("_saved_columns")
        for col in table.columns:
            # Add the managed Geometry columns with RecoverGeometryColumn()
            if _check_spatial_type(col.type, Geometry, dialect) and check_management(
                col, dialect_name
            ):
                col.type = col._actual_type
                del col._actual_type
                dimension = get_col_dim(col)
                args = [table.name, col.name, col.type.srid, col.type.geometry_type, dimension]

                stmt = select(*_format_select_args(func.RecoverGeometryColumn(*args)))
                stmt = stmt.execution_options(autocommit=True)
                bind.execute(stmt)

    for col in table.columns:
        # Add spatial indexes for the Geometry and Geography columns
        # TODO: Check that the Geography type makes sense here
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
    db_is_gpkg = is_gpkg(bind)

    if not db_is_gpkg:
        for col in gis_cols:
            # Disable spatial indexes if present
            disable_spatial_index(bind, table, col)

            args = [table.name, col.name]

            stmt = select(*_format_select_args(func.DiscardGeometryColumn(*args)))
            stmt = stmt.execution_options(autocommit=True)
            bind.execute(stmt)
    else:
        for col in gis_cols:
            # Disable spatial indexes if present
            # TODO: This is useless but if we remove it then the disable_spatial_index should be
            # tested separately
            disable_spatial_index(bind, table, col)

            # Remove metadata from internal tables
            # (this is equivalent to DiscardGeometryColumn but for GeoPackage)
            bind.execute(
                text(
                    """DELETE FROM gpkg_extensions
                    WHERE table_name = '{}'
                        AND column_name = '{}';""".format(
                        table.name,
                        col.name,
                    )
                )
            )
            bind.execute(
                text(
                    """DELETE FROM gpkg_geometry_columns
                    WHERE table_name = '{}'
                        AND column_name = '{}';""".format(
                        table.name,
                        col.name,
                    )
                )
            )
            bind.execute(
                text(
                    """DELETE FROM gpkg_contents
                    WHERE table_name = '{}';""".format(
                        table.name
                    )
                )
            )


def after_drop(table, bind, **kw):
    """Handle spatial indexes during the after_drop event."""
    table.columns = table.info.pop("_saved_columns")


# Define compiled versions for functions in SpatiaLite whose names don't have
# the ST_ prefix.
_SQLITE_FUNCTIONS = {
    "ST_GeomFromEWKT": "GeomFromEWKT",
    "ST_GeomFromEWKB": "GeomFromEWKB",
    "ST_AsBinary": "AsBinary",
    "ST_AsEWKB": "AsEWKB",
    "ST_AsGeoJSON": "AsGeoJSON",
}


def _compiles_sqlite(cls, fn):
    def _compile_sqlite(element, compiler, **kw):
        return "{}({})".format(fn, compiler.process(element.clauses, **kw))

    compiles(getattr(functions, cls), "sqlite")(_compile_sqlite)


def register_sqlite_mapping(mapping):
    """Register compilation mappings for the given functions.

    Args:
        mapping: Should have the following form::

                {
                    "function_name_1": "sqlite_function_name_1",
                    "function_name_2": "sqlite_function_name_2",
                    ...
                }
    """
    for cls, fn in mapping.items():
        _compiles_sqlite(cls, fn)


register_sqlite_mapping(_SQLITE_FUNCTIONS)
