"""GeoAlchemy2 package."""
from sqlalchemy import Column
from sqlalchemy import Index
from sqlalchemy import Table
from sqlalchemy import event
from sqlalchemy.sql import func

from geoalchemy2 import functions  # noqa
from geoalchemy2 import types  # noqa
from geoalchemy2.dialects import common
from geoalchemy2.dialects import postgresql
from geoalchemy2.dialects import sqlite
from geoalchemy2.dialects.common import _check_spatial_type
from geoalchemy2.dialects.common import _spatial_idx_name
from geoalchemy2.dialects.sqlite import load_spatialite  # noqa
from geoalchemy2.elements import RasterElement  # noqa
from geoalchemy2.elements import WKBElement  # noqa
from geoalchemy2.elements import WKTElement  # noqa
from geoalchemy2.exc import ArgumentError
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry
from geoalchemy2.types import Raster


def _select_dialect(dialect_name):
    """Select the dialect from its name."""
    known_dialects = {
        "postgresql": postgresql,
        "sqlite": sqlite,
    }
    return known_dialects.get(dialect_name, common)


def _setup_ddl_event_listeners():
    @event.listens_for(Table, "before_create")
    def before_create(table, bind, **kw):
        """Handle spatial indexes."""
        _select_dialect(bind.dialect.name).before_create(table, bind, **kw)

    @event.listens_for(Table, "after_create")
    def after_create(table, bind, **kw):
        """Restore original column list including managed Geometry columns."""
        _select_dialect(bind.dialect.name).after_create(table, bind, **kw)

    @event.listens_for(Table, "before_drop")
    def before_drop(table, bind, **kw):
        """Drop the managed Geometry columns."""
        _select_dialect(bind.dialect.name).before_drop(table, bind, **kw)

    @event.listens_for(Table, "after_drop")
    def after_drop(table, bind, **kw):
        """Restore original column list including managed Geometry columns."""
        _select_dialect(bind.dialect.name).after_drop(table, bind, **kw)

    @event.listens_for(Column, "after_parent_attach")
    def after_parent_attach(column, table):
        """Automatically add spatial indexes."""
        if not isinstance(table, Table):
            # For old versions of SQLAlchemy, subqueries might trigger the after_parent_attach event
            # with a selectable as table, so we want to skip this case.
            return

        if not getattr(column.type, "spatial_index", False) and getattr(
            column.type, "use_N_D_index", False
        ):
            raise ArgumentError("Arg Error(use_N_D_index): spatial_index must be True")

        if getattr(column.type, "management", True) or not getattr(
            column.type, "spatial_index", False
        ):
            # If the column is managed, the indexes are created after the table
            return

        try:
            if column.type._spatial_index_reflected:
                return
        except AttributeError:
            pass

        kwargs = {
            "postgresql_using": "gist",
            "_column_flag": True,
        }
        col = column
        if _check_spatial_type(column.type, (Geometry, Geography)):
            if column.type.use_N_D_index:
                kwargs["postgresql_ops"] = {column.name: "gist_geometry_ops_nd"}
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
            __version__ = get_distribution("GeoAlchemy2").version
        except DistributionNotFound:  # pragma: no cover
            pass
else:
    try:
        __version__ = importlib.metadata.version("GeoAlchemy2")
    except importlib.metadata.PackageNotFoundError:  # pragma: no cover
        pass
