"""GeoAlchemy2 package."""
from sqlalchemy import Column
from sqlalchemy import Index
from sqlalchemy import Table
from sqlalchemy import event
from sqlalchemy import text
from sqlalchemy.sql import expression
from sqlalchemy.sql import func
from sqlalchemy.sql import select
from sqlalchemy.types import TypeDecorator

from geoalchemy2 import functions  # noqa
from geoalchemy2 import types  # noqa
from geoalchemy2.dialects import postgresql
from geoalchemy2.dialects import sqlite
from geoalchemy2.dialects.common import _check_spatial_type
from geoalchemy2.dialects.common import _format_select_args
from geoalchemy2.dialects.common import _get_gis_cols
from geoalchemy2.dialects.common import _spatial_idx_name
from geoalchemy2.dialects.common import check_management
from geoalchemy2.dialects.sqlite import get_col_dim
from geoalchemy2.dialects.sqlite import load_spatialite  # noqa
from geoalchemy2.elements import RasterElement  # noqa
from geoalchemy2.elements import WKBElement  # noqa
from geoalchemy2.elements import WKTElement  # noqa
from geoalchemy2.exc import ArgumentError
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry
from geoalchemy2.types import Raster


def _setup_ddl_event_listeners():

    def _get_dispatch_info(table, bind):
        """Get info required for dispatch events."""
        dialect = bind.dialect

        # Filter Geometry columns from the table with management=True
        # Note: Geography and PostGIS >= 2.0 don't need this
        gis_cols = _get_gis_cols(table, Geometry, dialect, check_col_management=True)

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

    def _setup_create_drop(table, bind):
        """Prepare the table for before_create and before_drop events."""
        dialect, gis_cols, regular_cols = _get_dispatch_info(table, bind)
        _update_table_for_dispatch(table, regular_cols)
        return dialect, gis_cols, regular_cols

    @event.listens_for(Table, "before_create")
    def before_create(table, bind, **kw):
        """Handle spatial indexes."""
        dialect, gis_cols, regular_cols = _setup_create_drop(table, bind)
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
                    if (
                        idx.name != _spatial_idx_name(table.name, col.name)
                        or not getattr(col.type, "spatial_index", False)
                    ):
                        table.info["_after_create_indexes"].append(idx)
        if dialect_name == 'sqlite':
            sqlite._setup_dummy_type(table, gis_cols)

    @event.listens_for(Table, "after_create")
    def after_create(table, bind, **kw):
        """Restore original column list including managed Geometry columns."""
        dialect = bind.dialect
        dialect_name = dialect.name

        table.columns = table.info.pop('_saved_columns')

        for col in table.columns:
            # Add the managed Geometry columns with AddGeometryColumn()
            if (
                _check_spatial_type(col.type, Geometry, dialect)
                and check_management(col, dialect_name)
            ):
                dimension = col.type.dimension
                if dialect_name == 'sqlite':
                    create_func = func.RecoverGeometryColumn
                    col.type = col._actual_type
                    del col._actual_type
                    dimension = get_col_dim(col)
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
                if col.type.use_typmod is not None and dialect_name != 'sqlite':
                    args.append(col.type.use_typmod)

                stmt = select(*_format_select_args(create_func(*args)))
                stmt = stmt.execution_options(autocommit=True)
                bind.execute(stmt)

            # Add spatial indices for the Geometry and Geography columns
            if (
                _check_spatial_type(col.type, (Geometry, Geography), dialect)
                and col.type.spatial_index is True
            ):
                if dialect_name == 'sqlite':
                    sqlite.create_spatial_index(bind, table, col)
                elif dialect_name == 'postgresql':
                    # If the index does not exist (which might be the case when
                    # management=False), define it and create it
                    postgresql.create_spatial_index(bind, table, col)
                else:
                    raise ArgumentError('dialect {} is not supported'.format(dialect_name))

        for idx in table.info.pop("_after_create_indexes"):
            table.indexes.add(idx)
            idx.create(bind=bind)

    @event.listens_for(Table, "before_drop")
    def before_drop(table, bind, **kw):
        """Drop the managed Geometry columns."""
        dialect, gis_cols, regular_cols = _setup_create_drop(table, bind)
        dialect_name = dialect.name
        for col in gis_cols:
            if dialect_name == 'sqlite':
                drop_func = 'DiscardGeometryColumn'

                # Disable spatial indexes if present
                sqlite.disable_spatial_index(bind, table, col)
            elif dialect_name == 'postgresql':
                drop_func = 'DropGeometryColumn'
            else:
                raise ArgumentError('dialect {} is not supported'.format(dialect_name))
            args = [table.schema] if table.schema else []
            args.extend([table.name, col.name])

            stmt = select(*_format_select_args(getattr(func, drop_func)(*args)))
            stmt = stmt.execution_options(autocommit=True)
            bind.execute(stmt)

    @event.listens_for(Table, "after_drop")
    def after_drop(table, bind, **kw):
        """Restore original column list including managed Geometry columns."""
        table.columns = table.info.pop('_saved_columns')

    @event.listens_for(Column, 'after_parent_attach')
    def after_parent_attach(column, table):
        """Automatically add spatial indexes."""
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

    @event.listens_for(Table, "column_reflect")
    def _reflect_geometry_column(inspector, table, column_info):
        if not isinstance(column_info.get("type"), Geometry):
            return

        if inspector.bind.dialect.name == "postgresql":
            postgresql.reflect_geometry_column(inspector, table, column_info)
        elif inspector.bind.dialect.name == "sqlite":
            sqlite.reflect_geometry_column(inspector, table, column_info)


_setup_ddl_event_listeners()


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
