"""Some helpers to use with Alembic migration tool."""
import os

from alembic.autogenerate import renderers
from alembic.autogenerate import rewriter
from alembic.autogenerate.render import _add_column
from alembic.autogenerate.render import _add_index
from alembic.autogenerate.render import _add_table
from alembic.autogenerate.render import _drop_column
from alembic.autogenerate.render import _drop_index
from alembic.autogenerate.render import _drop_table
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
from geoalchemy2 import _get_gis_cols
from geoalchemy2 import _get_spatialite_version
from geoalchemy2 import check_management
from geoalchemy2 import func


writer = rewriter.Rewriter()


def render_item(obj_type, obj, autogen_context):
    """Apply custom rendering for selected items."""
    if obj_type == 'type' and isinstance(obj, (Geometry, Geography, Raster)):
        import_name = obj.__class__.__name__
        autogen_context.imports.add(f"from geoalchemy2 import {import_name}")
        return "%r" % obj

    # Default rendering for other objects
    return False


@Operations.register_operation("add_geospatial_column")
class AddGeospatialColumnOp(ops.AddColumnOp):
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
        return DropGeospatialColumnOp.from_column_and_tablename(
            self.schema, self.table_name, self.column.name
        )


@Operations.register_operation("drop_geospatial_column")
class DropGeospatialColumnOp(ops.DropColumnOp):
    """Drop a Geospatial Column in an Alembic migration context."""

    @classmethod
    def drop_geospatial_column(cls, operations, table_name, column_name, schema=None, **kw):
        """Handle the different situations arising from dropping geospatial column from a DB."""

        op = cls(table_name, column_name, schema=schema, **kw)
        return operations.invoke(op)

    def reverse(self):
        """Used to autogenerate the downgrade function."""
        return AddGeospatialColumnOp.from_column_and_tablename(
            self.schema, self.table_name, self.column
        )


@Operations.implementation_for(AddGeospatialColumnOp)
def add_geospatial_column(operations, operation):
    """Handle the actual column addition according to the dialect backend.

    Args:
        operations: Operations object from alembic base, defining high level migration operations.
        operation: AddGeospatialColumnOp call, with attributes for table_name, column_name,
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
            geospatial_core_type.geometry_type,
            geospatial_core_type.dimension,
            not geospatial_core_type.nullable,
        ))
    elif "postgresql" in dialect.name:
        operations.add_column(
            table_name,
            operation.column,
            schema=operation.schema,
        )


@Operations.implementation_for(DropGeospatialColumnOp)
def drop_geospatial_column(operations, operation):
    """Handle the actual column removal according to the dialect backend.

    Args:
        operations: Operations object from alembic base, defining high level migration operations.
        operation: AddGeospatialColumnOp call, with attributes for table_name, column_name,
            column_type, and optional keywords.
    """

    table_name = operation.table_name
    column_name = operation.column_name

    dialect = operations.get_bind().dialect

    if "sqlite" in dialect.name:
        # Discard the column and remove associated index
        # (the column is not actually dropped here)
        operations.execute(func.DiscardGeometryColumn(table_name, column_name))

        # This second drop column call is necessary: SpatiaLite was designed for a SQLite that did
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


@renderers.dispatch_for(AddGeospatialColumnOp)
def render_add_geo_column(autogen_context, op):
    """Render the add_geospatial_column operation in migration script."""
    col_render = _add_column(autogen_context, op)
    return col_render.replace(".add_column(", ".add_geospatial_column(")


@renderers.dispatch_for(DropGeospatialColumnOp)
def render_drop_geo_column(autogen_context, op):
    """Render the drop_geospatial_column operation in migration script."""
    col_render = _drop_column(autogen_context, op)
    return col_render.replace(".drop_column(", ".drop_geospatial_column(")


@writer.rewrites(ops.AddColumnOp)
def add_geo_column(context, revision, op):
    """This function replaces the default AddColumnOp by a geospatial-specific one."""
    col_type = op.column.type
    if isinstance(col_type, TypeDecorator):
        dialect = context.bind.dialect
        col_type = col_type.load_dialect_impl(dialect)
    if isinstance(col_type, (Geometry, Geography, Raster)):
        op.column.type.spatial_index = False
        new_op = AddGeospatialColumnOp(op.table_name, op.column, op.schema)
    else:
        new_op = op
    return new_op


@writer.rewrites(ops.DropColumnOp)
def drop_geo_column(context, revision, op):
    """This function replaces the default DropColumnOp by a geospatial-specific one."""
    col_type = op.to_column().type
    if isinstance(col_type, TypeDecorator):
        dialect = context.bind.dialect
        col_type = col_type.load_dialect_impl(dialect)
    if isinstance(col_type, (Geometry, Geography, Raster)):
        new_op = DropGeospatialColumnOp(op.table_name, op.column_name, op.schema)
    else:
        new_op = op
    return new_op


@Operations.register_operation("create_geospatial_table")
class CreateGeospatialTableOp(ops.CreateTableOp):
    """
    Create a Geospatial Table in an Alembic migration context. This methodology originates from:
    https://alembic.sqlalchemy.org/en/latest/api/operations.html#operation-plugins
    """

    @classmethod
    def create_geospatial_table(cls, operations, table_name, *columns, **kw):
        """Handle the different situations arising from creating geospatial table to a DB."""
        op = cls(table_name, columns, **kw)
        return operations.invoke(op)

    def reverse(self):
        """Used to autogenerate the downgrade function."""
        return DropGeospatialColumnOp.from_table(
            self.to_table(),
            _namespace_metadata=self._namespace_metadata,
        )

    @classmethod
    def from_table(
        cls, table: "Table", _namespace_metadata=None
    ) -> "CreateGeospatialTableOp":
        obj = super().from_table(table, _namespace_metadata)
        return obj

    def to_table(
        self, migration_context=None
    ) -> "Table":
        table = super().to_table(migration_context)
        for col in table.columns:
            try:
                if col.type.spatial_index:
                    col.type.spatial_index = False
            except AttributeError:
                pass
        return table


@Operations.register_operation("drop_geospatial_table")
class DropGeospatialTableOp(ops.DropTableOp):
    @classmethod
    def drop_geospatial_table(cls, operations, table_name, schema=None, **kw):
        """Handle the different situations arising from dropping geospatial table from a DB."""

        op = cls(table_name, schema=schema, table_kw=kw)
        return operations.invoke(op)

    def reverse(self):
        """Used to autogenerate the downgrade function."""
        return CreateGeospatialTableOp.from_table(
            self.to_table(),
            _namespace_metadata=self._namespace_metadata,
        )

    @classmethod
    def from_table(
        cls, table: "Table", _namespace_metadata=None
    ) -> "DropGeospatialTableOp":
        obj = super().from_table(table, _namespace_metadata)
        return obj

    def to_table(
        self, migration_context=None
    ) -> "Table":
        table = super().to_table(migration_context)

        # Set spatial_index attribute to False so the indexes are created explicitely
        for col in table.columns:
            try:
                if col.type.spatial_index:
                    col.type.spatial_index = False
            except AttributeError:
                pass
        return table


@Operations.implementation_for(CreateGeospatialTableOp)
def create_geospatial_table(operations, operation):
    """Handle the actual table creation according to the dialect backend.

    Args:
        operations: Operations object from alembic base, defining high level migration operations.
        operation: CreateGeospatialTableOp call, with attributes for table_name, column_name,
            column_type, and optional keywords.
    """
    table_name = operation.table_name

    # For now the default events defined in geoalchemy2 are enought to handle table creation
    operations.create_table(table_name, *operation.columns, **operation.kw)


@Operations.implementation_for(DropGeospatialTableOp)
def drop_geospatial_table(operations, operation):
    """Handle the actual table removal according to the dialect backend.

    Args:
        operations: Operations object from alembic base, defining high level migration operations.
        operation: DropGeospatialTableOp call, with attributes for table_name, column_name,
            column_type, and optional keywords.
    """
    table_name = operation.table_name
    bind = operations.get_bind()
    dialect = bind.dialect

    if "sqlite" in dialect.name:
        spatialite_version = _get_spatialite_version(bind)
        if parse_version(spatialite_version) >= parse_version("5"):
            drop_func = func.DropTable
        else:
            drop_func = func.DropGeoTable
        operations.execute(drop_func(table_name))
    else:
        operations.drop_table(table_name, operation.schema, **operation.table_kw)


@renderers.dispatch_for(CreateGeospatialTableOp)
def render_create_geo_table(autogen_context, op):
    """Render the create_geospatial_table operation in migration script."""
    table_render = _add_table(autogen_context, op)
    return table_render.replace(".create_table(", ".create_geospatial_table(")


@renderers.dispatch_for(DropGeospatialTableOp)
def render_drop_geo_table(autogen_context, op):
    """Render the drop_geospatial_table operation in migration script."""
    table_render = _drop_table(autogen_context, op)
    return table_render.replace(".drop_table(", ".drop_geospatial_table(")


@writer.rewrites(ops.CreateTableOp)
def create_geo_table(context, revision, op):
    """This function replaces the default CreateTableOp by a geospatial-specific one."""
    dialect = context.bind.dialect
    gis_cols = _get_gis_cols(op, (Geometry, Geography, Raster), dialect, check_col_management=False)

    if gis_cols:
        new_op = CreateGeospatialTableOp(op.table_name, op.columns, op.schema)
    else:
        new_op = op

    return new_op


@writer.rewrites(ops.DropTableOp)
def drop_geo_table(context, revision, op):
    """This function replaces the default DropTableOp by a geospatial-specific one."""
    dialect = context.bind.dialect
    table = op.to_table()
    gis_cols = _get_gis_cols(table, (Geometry, Geography, Raster), dialect, check_col_management=False)

    if gis_cols:
        new_op = DropGeospatialTableOp(op.table_name, op.schema)
    else:
        new_op = op

    return new_op


@Operations.register_operation("create_geospatial_index")
class CreateGeospatialIndexOp(ops.CreateIndexOp):
    @classmethod
    def create_geospatial_index(cls, operations, index_name, table_name, columns, schema=None, unique=False, **kw):
        """Handle the different situations arising from dropping geospatial table from a DB."""
        op = cls(index_name, table_name, columns, schema=schema, unique=unique, **kw)
        return operations.invoke(op)

    def reverse(self):
        """Used to autogenerate the downgrade function."""
        return DropGeospatialIndexOp(
            self.index_name,
            self.table_name,
            column_name=self.columns[0].name,
            schema=self.schema,
        )


@Operations.register_operation("drop_geospatial_index")
class DropGeospatialIndexOp(ops.DropIndexOp):

    def __init__(self, *args, column_name, **kwargs):
        super().__init__(*args, **kwargs)
        self.column_name = column_name

    @classmethod
    def drop_geospatial_index(cls, operations, index_name, table_name, column_name, schema=None, unique=False, **kw):
        """Handle the different situations arising from dropping geospatial table from a DB."""
        op = cls(index_name, table_name, column_name=column_name, schema=schema, unique=unique, **kw)
        return operations.invoke(op)

    def reverse(self):
        """Used to autogenerate the downgrade function."""
        return CreateGeospatialIndexOp(
            self.index_name,
            self.table_name,
            column_name=self.column_name,
            schema=self.schema,
            _reverse=self,
            **self.kw
        )

    @classmethod
    def from_index(cls, index: "Index") -> "DropGeospatialIndexOp":
        assert index.table is not None
        assert len(index.columns) == 1, "A spatial index must be set on one column only"
        return cls(
            index.name,
            index.table.name,
            column_name=index.columns[0].name,
            schema=index.table.schema,
            _reverse=CreateGeospatialIndexOp.from_index(index),
            **index.kwargs,
        )


@Operations.implementation_for(CreateGeospatialIndexOp)
def create_geospatial_index(operations, operation):
    """Handle the actual index creation according to the dialect backend.

    Args:
        operations: Operations object from alembic base, defining high level migration operations.
        operation: CreateGeospatialIndexOp call, with attributes for table_name, column_name,
            column_type, and optional keywords.
    """
    # return  # Do nothing and rely on the
    bind = operations.get_bind()
    dialect = bind.dialect

    if "sqlite" in dialect.name:
        assert len(operation.columns) == 1, "A spatial index must be set on one column only"
        operations.execute(func.CreateSpatialIndex(operation.table_name, operation.columns[0]))
    else:
        operations.create_index(
            operation.index_name,
            operation.table_name,
            operation.columns,
            operation.schema,
            operation.unique,
            **operation.kw
        )


@Operations.implementation_for(DropGeospatialIndexOp)
def drop_geospatial_index(operations, operation):
    """Handle the actual index drop according to the dialect backend.

    Args:
        operations: Operations object from alembic base, defining high level migration operations.
        operation: DropGeospatialIndexOp call, with attributes for table_name, column_name,
            column_type, and optional keywords.
    """
    bind = operations.get_bind()
    dialect = bind.dialect

    if "sqlite" in dialect.name:
        operations.execute(func.DisableSpatialIndex(operation.table_name, operation.column_name))
    else:
        operations.drop_index(
            operation.index_name,
            operation.table_name,
            operation.schema,
            **operation.kw
        )


@renderers.dispatch_for(CreateGeospatialIndexOp)
def render_create_geo_index(autogen_context, op):
    """Render the create_geospatial_index operation in migration script."""
    idx_render = _add_index(autogen_context, op)
    return idx_render.replace(".create_index(", ".create_geospatial_index(")


@renderers.dispatch_for(DropGeospatialIndexOp)
def render_drop_geo_index(autogen_context, op):
    """Render the drop_geospatial_index operation in migration script."""
    idx_render = _drop_index(autogen_context, op)

    # Replace function name
    text = idx_render.replace(".drop_index(", ".drop_geospatial_index(")

    # Add column name as keyword argument
    text = text[:-1] + ", column_name='%s')" % (op.column_name,)

    return text


@writer.rewrites(ops.CreateIndexOp)
def create_geo_index(context, revision, op):
    """This function replaces the default CreateIndexOp by a geospatial-specific one."""
    dialect = context.bind.dialect

    if len(op.columns) == 1:
        col = op.columns[0]
        if (
            isinstance(col, Column)
            and _check_spatial_type(col.type, (Geometry, Geography, Raster), dialect)
        ):
            # Fix index properties
            op.kw["postgresql_using"] = op.kw.get("postgresql_using", "gist")
            if col.type.use_N_D_index:
                postgresql_ops = {col.name: "gist_geometry_ops_nd"}
            else:
                postgresql_ops = {}
            op.kw["postgresql_ops"] = op.kw.get("postgresql_ops", postgresql_ops)

            return CreateGeospatialIndexOp(
                op.index_name,
                op.table_name,
                op.columns,
                op.schema,
                op.unique,
                **op.kw
            )

    return op


@writer.rewrites(ops.DropIndexOp)
def drop_geo_index(context, revision, op):
    """This function replaces the default DropIndexOp by a geospatial-specific one."""
    dialect = context.bind.dialect
    idx = op.to_index()

    if len(idx.columns) == 1:
        col = idx.columns[0]
        if (
            isinstance(col, Column)
            and _check_spatial_type(col.type, (Geometry, Geography, Raster), dialect)
        ):
            return DropGeospatialIndexOp(
                op.index_name,
                op.table_name,
                column_name=col.name,
                schema=op.schema,
                **op.kw
            )

    return op


def load_spatialite(dbapi_conn, connection_record):
    """Load SpatiaLite extension in SQLite DB."""
    if "SPATIALITE_LIBRARY_PATH" not in os.environ:
        raise RuntimeError("The SPATIALITE_LIBRARY_PATH environment variable is not set.")
    dbapi_conn.enable_load_extension(True)
    dbapi_conn.load_extension(os.environ['SPATIALITE_LIBRARY_PATH'])
    dbapi_conn.enable_load_extension(False)
    dbapi_conn.execute('SELECT InitSpatialMetaData()')
