"""GeoAlchemy2 package."""
import os

import sqlalchemy
from packaging import version
from sqlalchemy import Column
from sqlalchemy import Index
from sqlalchemy import Table
from sqlalchemy import event
from sqlalchemy import text
from sqlalchemy.sql import expression
from sqlalchemy.sql import func
from sqlalchemy.sql import select
from sqlalchemy.types import TypeDecorator

from . import functions  # noqa
from . import types  # noqa
from .elements import RasterElement  # noqa
from .elements import WKBElement  # noqa
from .elements import WKTElement  # noqa
from .exc import ArgumentError
from .types import Geography
from .types import Geometry
from .types import Raster
from .types import _DummyGeometry

_SQLALCHEMY_VERSION_BEFORE_14 = version.parse(sqlalchemy.__version__) < version.parse("1.4")


def _format_select_args(*args):
    if _SQLALCHEMY_VERSION_BEFORE_14:
        return [args]
    else:
        return args


def _check_spatial_type(tested_type, spatial_types, dialect=None):
    return (
        isinstance(tested_type, spatial_types)
        or (
            isinstance(tested_type, TypeDecorator)
            and isinstance(tested_type.load_dialect_impl(dialect), spatial_types)
        )
    )


def _spatial_idx_name(table_name, column_name):
    return 'idx_{}_{}'.format(table_name, column_name)


def check_management(column, dialect):
    return getattr(column.type, "management", False) is True or dialect.name == 'sqlite'


def _get_gis_cols(table, spatial_types, dialect, check_col_management=False):
    return [
        col
        for col in table.columns
        if (
            isinstance(col, Column)
            and _check_spatial_type(col.type, spatial_types, dialect)
            and (
                not check_col_management
                or check_management(col, dialect)
            )
        )
    ]


def _get_spatialite_attrs(bind, table_name, col_name):
    col_attributes = bind.execute(
        text("""SELECT * FROM "geometry_columns"
           WHERE f_table_name = '{}' and f_geometry_column = '{}'
        """.format(
            table_name, col_name
        ))
    ).fetchone()
    return col_attributes


def _get_spatialite_version(bind):
    return bind.execute(text("""SELECT spatialite_version();""")).fetchone()[0]


def _setup_ddl_event_listeners():
    @event.listens_for(Table, "before_create")
    def before_create(target, connection, **kw):
        dispatch("before-create", target, connection)

    @event.listens_for(Table, "after_create")
    def after_create(target, connection, **kw):
        dispatch("after-create", target, connection)

    @event.listens_for(Table, "before_drop")
    def before_drop(target, connection, **kw):
        dispatch("before-drop", target, connection)

    @event.listens_for(Table, "after_drop")
    def after_drop(target, connection, **kw):
        dispatch("after-drop", target, connection)

    @event.listens_for(Column, 'after_parent_attach')
    def after_parent_attach(column, table):
        if not isinstance(table, Table):
            # For old versions of SQLAlchemy, subqueries might trigger the after_parent_attach event
            # with a selectable as table, so we want to skip this case.
            return

        if (
            not getattr(column.type, "spatial_index", False)
            and getattr(column.type, "use_N_D_index", False)
        ):
            raise ArgumentError('Arg Error(use_N_D_index): spatial_index must be True')

        if (
            getattr(column.type, "management", True)
            or not getattr(column.type, "spatial_index", False)
        ):
            # If the column is managed, the indexes are created after the table
            return

        try:
            if column.type._spatial_index_reflected:
                return
        except AttributeError:
            pass

        kwargs = {
            'postgresql_using': 'gist',
            '_column_flag': True,
        }
        col = column
        if _check_spatial_type(column.type, (Geometry, Geography)):
            if column.type.use_N_D_index:
                kwargs['postgresql_ops'] = {column.name: "gist_geometry_ops_nd"}
        elif _check_spatial_type(column.type, Raster):
            col = func.ST_ConvexHull(column)

        table.append_constraint(
            Index(
                _spatial_idx_name(table.name, column.name),
                col,
                **kwargs,
            )
        )

    def dispatch(current_event, table, bind):
        if current_event in ('before-create', 'before-drop'):
            dialect = bind.dialect

            # Filter Geometry columns from the table with management=True
            # Note: Geography and PostGIS >= 2.0 don't need this
            gis_cols = _get_gis_cols(table, Geometry, dialect, check_col_management=True)

            # Find all other columns that are not managed Geometries
            regular_cols = [x for x in table.columns if x not in gis_cols]

            # Save original table column list for later
            table.info["_saved_columns"] = table.columns

            # Temporarily patch a set of columns not including the
            # managed Geometry columns
            column_collection = expression.ColumnCollection()
            for col in regular_cols:
                column_collection.add(col)
            table.columns = column_collection

            if current_event == 'before-drop':
                # Drop the managed Geometry columns
                for col in gis_cols:
                    if dialect.name == 'sqlite':
                        drop_func = 'DiscardGeometryColumn'

                        # Disable spatial indexes if present
                        stmt = select(
                            *_format_select_args(
                                getattr(func, 'CheckSpatialIndex')(table.name, col.name)
                            )
                        )
                        if bind.execute(stmt).fetchone()[0] is not None:
                            stmt = select(
                                *_format_select_args(
                                    getattr(func, 'DisableSpatialIndex')(table.name, col.name)
                                )
                            )
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
                    elif dialect.name == 'postgresql':
                        drop_func = 'DropGeometryColumn'
                    else:
                        raise ArgumentError('dialect {} is not supported'.format(dialect.name))
                    args = [table.schema] if table.schema else []
                    args.extend([table.name, col.name])

                    stmt = select(*_format_select_args(getattr(func, drop_func)(*args)))
                    stmt = stmt.execution_options(autocommit=True)
                    bind.execute(stmt)
            elif current_event == 'before-create':
                # Remove the spatial indexes from the table metadata because they should not be
                # created during the table.create() step since the associated columns do not exist
                # at this time.
                table.info["_after_create_indexes"] = []
                current_indexes = set(table.indexes)
                for idx in current_indexes:
                    for col in table.info["_saved_columns"]:
                        if (
                            _check_spatial_type(col.type, Geometry, dialect)
                            and check_management(col, dialect)
                        ) and col in idx.columns.values():
                            table.indexes.remove(idx)
                            if (
                                idx.name != _spatial_idx_name(table.name, col.name)
                                or not getattr(col.type, "spatial_index", False)
                            ):
                                table.info["_after_create_indexes"].append(idx)
                if dialect.name == 'sqlite':
                    for col in gis_cols:
                        # Add dummy columns with GEOMETRY type
                        col._actual_type = col.type
                        col.type = _DummyGeometry()
                        col.nullable = col._actual_type.nullable
                    table.columns = table.info["_saved_columns"]

        elif current_event == 'after-create':
            # Restore original column list including managed Geometry columns
            dialect = bind.dialect

            table.columns = table.info.pop('_saved_columns')

            for col in table.columns:
                # Add the managed Geometry columns with AddGeometryColumn()
                if (
                    _check_spatial_type(col.type, Geometry, dialect)
                    and check_management(col, dialect)
                ):
                    dimension = col.type.dimension
                    if dialect.name == 'sqlite':
                        col.type = col._actual_type
                        del col._actual_type
                        create_func = func.RecoverGeometryColumn
                        if col.type.dimension == 4:
                            dimension = 'XYZM'
                        elif col.type.dimension == 2:
                            dimension = 'XY'
                        else:
                            if col.type.geometry_type.endswith('M'):
                                dimension = 'XYM'
                            else:
                                dimension = 'XYZ'
                    else:
                        create_func = func.AddGeometryColumn
                    args = [table.schema] if table.schema else []
                    args.extend([
                        table.name,
                        col.name,
                        col.type.srid,
                        col.type.geometry_type,
                        dimension
                    ])
                    if col.type.use_typmod is not None and dialect.name != 'sqlite':
                        args.append(col.type.use_typmod)

                    stmt = select(*_format_select_args(create_func(*args)))
                    stmt = stmt.execution_options(autocommit=True)
                    bind.execute(stmt)

                # Add spatial indices for the Geometry and Geography columns
                if (
                    _check_spatial_type(col.type, (Geometry, Geography), dialect)
                    and col.type.spatial_index is True
                ):
                    if dialect.name == 'sqlite':
                        stmt = select(*_format_select_args(func.CreateSpatialIndex(table.name,
                                                                                   col.name)))
                        stmt = stmt.execution_options(autocommit=True)
                        bind.execute(stmt)
                    elif dialect.name == 'postgresql':
                        # If the index does not exist (which might be the case when
                        # management=False), define it and create it
                        if (
                            not [i for i in table.indexes if col in i.columns.values()]
                            and check_management(col, dialect)
                        ):
                            if col.type.use_N_D_index:
                                postgresql_ops = {col.name: "gist_geometry_ops_nd"}
                            else:
                                postgresql_ops = {}
                            idx = Index(
                                _spatial_idx_name(table.name, col.name),
                                col,
                                postgresql_using='gist',
                                postgresql_ops=postgresql_ops,
                                _column_flag=True,
                            )
                            idx.create(bind=bind)

                    else:
                        raise ArgumentError('dialect {} is not supported'.format(dialect.name))

            for idx in table.info.pop("_after_create_indexes"):
                table.indexes.add(idx)
                idx.create(bind=bind)

        elif current_event == 'after-drop':
            # Restore original column list including managed Geometry columns
            table.columns = table.info.pop('_saved_columns')

    @event.listens_for(Table, "column_reflect")
    def _reflect_geometry_column(inspector, table, column_info):
        if not isinstance(column_info.get("type"), Geometry):
            return

        if inspector.bind.dialect.name == "postgresql":
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
        elif inspector.bind.dialect.name == "sqlite":
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


_setup_ddl_event_listeners()


def load_spatialite(dbapi_conn, connection_record):
    """Load SpatiaLite extension in SQLite DB.

    The path to the SpatiaLite module should be set in the `SPATIALITE_LIBRARY_PATH` environment
    variable.
    """
    if "SPATIALITE_LIBRARY_PATH" not in os.environ:
        raise RuntimeError(
            "The SPATIALITE_LIBRARY_PATH environment variable is not set."
        )
    dbapi_conn.enable_load_extension(True)
    dbapi_conn.load_extension(os.environ["SPATIALITE_LIBRARY_PATH"])
    dbapi_conn.enable_load_extension(False)
    dbapi_conn.execute("SELECT InitSpatialMetaData();")


# Get version number
__version__ = "UNKNOWN VERSION"

# Attempt to use importlib.metadata first because it's much faster
# though it's only available in Python 3.8+ so we'll need to fall
# back to pkg_resources for Python 3.7 support
try:
    import importlib.metadata
except ImportError:
    try:
        from pkg_resources import DistributionNotFound
        from pkg_resources import get_distribution
    except ImportError:  # pragma: no cover
        pass
    else:
        try:
            __version__ = get_distribution('GeoAlchemy2').version
        except DistributionNotFound:  # pragma: no cover
            pass
else:
    try:
        __version__ = importlib.metadata.version('GeoAlchemy2')
    except importlib.metadata.PackageNotFoundError:  # pragma: no cover
        pass
