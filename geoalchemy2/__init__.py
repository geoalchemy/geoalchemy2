from .types import (  # NOQA
    Geometry,
    Geography,
    Raster
)

from .elements import (  # NOQA
    WKTElement,
    WKBElement,
    RasterElement
)

from .exc import ArgumentError

from . import functions  # NOQA
from . import types  # NOQA

import sqlalchemy
from sqlalchemy import Column, Index, Table, event
from sqlalchemy.sql import select, func, expression
from sqlalchemy.types import TypeDecorator

from packaging import version

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


def _spatial_idx_name(table, column):
    return 'idx_{}_{}'.format(table.name, column.name)


def check_management(column, dialect):
    return getattr(column.type, "management", False) is True or dialect == 'sqlite'


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

        if _check_spatial_type(column.type, (Geometry, Geography)):
            if column.type.use_N_D_index:
                postgresql_ops = {column.name: "gist_geometry_ops_nd"}
            else:
                postgresql_ops = {}
            Index(
                _spatial_idx_name(table, column),
                column,
                postgresql_using='gist',
                postgresql_ops=postgresql_ops,
            )
        elif _check_spatial_type(column.type, Raster):
            Index(
                _spatial_idx_name(table, column),
                func.ST_ConvexHull(column),
                postgresql_using='gist',
            )

    def dispatch(event, table, bind):
        if event in ('before-create', 'before-drop'):
            # Filter Geometry columns from the table with management=True
            # Note: Geography and PostGIS >= 2.0 don't need this
            gis_cols = [c for c in table.c if
                        _check_spatial_type(c.type, Geometry, bind.dialect)
                        and check_management(c, bind.dialect.name)]

            # Find all other columns that are not managed Geometries
            regular_cols = [x for x in table.c if x not in gis_cols]

            # Save original table column list for later
            table.info["_saved_columns"] = table.c

            # Temporarily patch a set of columns not including the
            # managed Geometry columns
            column_collection = expression.ColumnCollection()
            for col in regular_cols:
                column_collection.add(col)
            table.columns = column_collection

            if event == 'before-drop':
                # Drop the managed Geometry columns
                for c in gis_cols:
                    if bind.dialect.name == 'sqlite':
                        drop_func = 'DiscardGeometryColumn'
                    elif bind.dialect.name == 'postgresql':
                        drop_func = 'DropGeometryColumn'
                    else:
                        raise ArgumentError('dialect {} is not supported'.format(bind.dialect.name))
                    args = [table.schema] if table.schema else []
                    args.extend([table.name, c.name])

                    stmt = select(*_format_select_args(getattr(func, drop_func)(*args)))
                    stmt = stmt.execution_options(autocommit=True)
                    bind.execute(stmt)
            elif event == 'before-create':
                # Remove the spatial indexes from the table metadata because they should not be
                # created during the table.create() step since the associated columns do not exist
                # at this time.
                table.info["_after_create_indexes"] = []
                current_indexes = set(table.indexes)
                for idx in current_indexes:
                    for c in table.info["_saved_columns"]:
                        if (
                            _check_spatial_type(c.type, Geometry, bind.dialect)
                            and check_management(c, bind.dialect.name)
                        ) and c in idx.columns.values():
                            table.indexes.remove(idx)
                            if (
                                idx.name != _spatial_idx_name(table, c)
                                or not getattr(c.type, "spatial_index", False)
                            ):
                                table.info["_after_create_indexes"].append(idx)

        elif event == 'after-create':
            # Restore original column list including managed Geometry columns
            table.columns = table.info.pop('_saved_columns')

            for c in table.c:
                # Add the managed Geometry columns with AddGeometryColumn()
                if (
                    _check_spatial_type(c.type, Geometry, bind.dialect)
                    and check_management(c, bind.dialect.name)
                ):
                    args = [table.schema] if table.schema else []
                    args.extend([
                        table.name,
                        c.name,
                        c.type.srid,
                        c.type.geometry_type,
                        c.type.dimension
                    ])
                    if c.type.use_typmod is not None:
                        args.append(c.type.use_typmod)
                    if bind.dialect.name == 'sqlite':
                        args.append(not c.type.nullable)

                    stmt = select(*_format_select_args(func.AddGeometryColumn(*args)))
                    stmt = stmt.execution_options(autocommit=True)
                    bind.execute(stmt)

                # Add spatial indices for the Geometry and Geography columns
                if (
                    _check_spatial_type(c.type, (Geometry, Geography), bind.dialect)
                    and c.type.spatial_index is True
                ):
                    if bind.dialect.name == 'sqlite':
                        stmt = select(*_format_select_args(func.CreateSpatialIndex(table.name,
                                                                                   c.name)))
                        stmt = stmt.execution_options(autocommit=True)
                        bind.execute(stmt)
                    elif bind.dialect.name == 'postgresql':
                        # If the index does not exist (which might be the case when
                        # management=False), define it and create it
                        if (
                            not [i for i in table.indexes if c in i.columns.values()]
                            and check_management(c, bind.dialect.name)
                        ):
                            if c.type.use_N_D_index:
                                postgresql_ops = {c.name: "gist_geometry_ops_nd"}
                            else:
                                postgresql_ops = {}
                            idx = Index(
                                _spatial_idx_name(table, c),
                                c,
                                postgresql_using='gist',
                                postgresql_ops=postgresql_ops,
                            )
                            idx.create(bind=bind)

                    else:
                        raise ArgumentError('dialect {} is not supported'.format(bind.dialect.name))

                if isinstance(c.type, (Geometry, Geography)) and c.type.spatial_index is False and \
                        c.type.use_N_D_index is True:
                    raise ArgumentError('Arg Error(use_N_D_index): spatial_index must be True')

            for idx in table.info.pop("_after_create_indexes"):
                table.indexes.add(idx)
                idx.create(bind=bind)

        elif event == 'after-drop':
            # Restore original column list including managed Geometry columns
            table.columns = table.info.pop('_saved_columns')


_setup_ddl_event_listeners()

# Get version number
__version__ = "UNKNOWN VERSION"
try:
    from pkg_resources import get_distribution, DistributionNotFound
    try:
        __version__ = get_distribution('GeoAlchemy2').version
    except DistributionNotFound:  # pragma: no cover
        pass  # pragma: no cover
except ImportError:  # pragma: no cover
    pass  # pragma: no cover
