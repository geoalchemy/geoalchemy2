"""Some helpers to use with Alembic migration tool."""
from alembic.autogenerate import renderers
from alembic.autogenerate.render import _add_column
from alembic.autogenerate.render import _drop_column
from alembic.operations import Operations
from alembic.operations import ops
from packaging.version import parse as parse_version
from sqlalchemy import text
from sqlalchemy.types import TypeDecorator

from geoalchemy2 import Column
from geoalchemy2 import Geography
from geoalchemy2 import Geometry
from geoalchemy2 import Raster
from geoalchemy2 import _check_spatial_type
from geoalchemy2 import func


def render_item(obj_type, obj, autogen_context):
    """Apply custom rendering for selected items."""
    if obj_type == 'type' and isinstance(obj, (Geometry, Geography, Raster)):
        import_name = obj.__class__.__name__
        autogen_context.imports.add(f"from geoalchemy2 import {import_name}")
        return "%r" % obj

    # default rendering for other objects
    return False


def include_object(obj, name, obj_type, reflected, compare_to):
    """Do not include spatial indexes if they are automatically created by GeoAlchemy2."""
    if obj_type == "index":
        if len(obj.expressions) == 1:
            try:
                col = obj.expressions[0]
                if (
                    _check_spatial_type(col.type, (Geometry, Geography, Raster))
                    and col.type.spatial_index
                ):
                    return False
            except AttributeError:
                pass
    # Never include the spatial_ref_sys table
    if (obj_type == "table" and name == "spatial_ref_sys"):
        return False
    return True


@Operations.register_operation("add_geospatial_column")
class AddGeospatialColumn(ops.AddColumnOp):
    """
    Add a Geospatial Column in an Alembic migration context. This methodology originates from:
    https://alembic.sqlalchemy.org/en/latest/api/operations.html#operation-plugins
    """

    @classmethod
    def add_geospatial_column(cls, operations, table_name, column, schema=None):
        """Handle the different situations arising from adding geospatial column to a DB."""
        op = cls(table_name, column, schema=schema)
        return operations.invoke(op)

    def reverse(self):
        """Used to autogenerate the downgrade function."""
        return DropGeospatialColumn.from_column_and_tablename(
            self.schema, self.table_name, self.column.name
        )


@Operations.register_operation("drop_geospatial_column")
class DropGeospatialColumn(ops.DropColumnOp):
    """Drop a Geospatial Column in an Alembic migration context."""

    @classmethod
    def drop_geospatial_column(cls, operations, table_name, column_name, schema=None, **kw):
        """Handle the different situations arising from dropping geospatial column from a DB."""

        op = cls(table_name, column_name, schema=schema, **kw)
        return operations.invoke(op)

    def reverse(self):
        """Used to autogenerate the downgrade function."""
        return AddGeospatialColumn.from_column_and_tablename(
            self.schema, self.table_name, self.column
        )


@Operations.implementation_for(AddGeospatialColumn)
def add_geospatial_column(operations, operation):
    """Handle the actual column addition according to the dialect backend.

    Args:
        operations: Operations object from alembic base, defining high level migration operations.
        operation: AddGeospatialColumn call, with attributes for table_name, column_name,
            column_type, and optional keywords.
    """

    table_name = operation.table_name
    column_name = operation.column.name

    dialect = operations.get_bind().dialect

    if isinstance(operation.column, TypeDecorator):
        # Will be either geoalchemy2.types.Geography or geoalchemy2.types.Geometry, if using a
        # custom type
        geospatial_core_type = operation.column.type.load_dialect_impl(dialect)
    else:
        geospatial_core_type = operation.column.type

    if "sqlite" in dialect.name:
        operations.execute(func.AddGeometryColumn(
            table_name,
            column_name,
            geospatial_core_type.srid,
            geospatial_core_type.geometry_type
        ))
    elif "postgresql" in dialect.name:
        operations.add_column(
            table_name,
            Column(
                column_name,
                operation.column
            )
        )


@Operations.implementation_for(DropGeospatialColumn)
def drop_geospatial_column(operations, operation):
    """Handle the actual column removal according to the dialect backend.

    Args:
        operations: Operations object from alembic base, defining high level migration operations.
        operation: AddGeospatialColumn call, with attributes for table_name, column_name,
            column_type, and optional keywords.
    """

    table_name = operation.table_name
    column_name = operation.column_name

    dialect = operations.get_bind().dialect

    if "sqlite" in dialect.name:
        operations.execute(func.DiscardGeometryColumn(table_name, column_name))
        # This second drop column call is necessary; SpatiaLite was designed for a SQLite that did
        # not support dropping columns from tables at all. DiscardGeometryColumn removes associated
        # metadata and triggers from the DB associated with a geospatial column, without removing
        # the column itself. The next call actually removes the geospatial column, IF the underlying
        # SQLite package version >= 3.35
        conn = operations.get_bind()
        sqlite_version = conn.execute(text("SELECT sqlite_version();")).scalar()
        if parse_version(sqlite_version) >= parse_version("3.35"):
            operations.drop_column(table_name, column_name)
    elif "postgresql" in dialect.name:
        operations.drop_column(table_name, column_name)


@renderers.dispatch_for(AddGeospatialColumn)
def render_add_geo_column(autogen_context, op):
    """Render the add_geospatial_column operation in migration script."""
    col_render = _add_column(autogen_context, op)
    return col_render.replace(".add_column(", ".add_geospatial_column(")


@renderers.dispatch_for(DropGeospatialColumn)
def render_drop_geo_column(autogen_context, op):
    """Render the drop_geospatial_column operation in migration script."""
    col_render = _drop_column(autogen_context, op)
    return col_render.replace(".drop_column(", ".drop_geospatial_column(")
