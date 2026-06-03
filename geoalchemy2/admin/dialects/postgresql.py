"""This module defines specific functions for Postgresql dialect."""

import sqlalchemy
from packaging import version
from sqlalchemy import Index
from sqlalchemy import text
from sqlalchemy.dialects.postgresql.base import ischema_names as _postgresql_ischema_names
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import expression
from sqlalchemy.sql import func
from sqlalchemy.sql import select
from sqlalchemy.types import LargeBinary
from sqlalchemy.types import TypeDecorator

from geoalchemy2 import _wkb_wkt
from geoalchemy2 import functions
from geoalchemy2.admin.dialects.common import _check_spatial_type
from geoalchemy2.admin.dialects.common import _format_select_args
from geoalchemy2.admin.dialects.common import _spatial_idx_name
from geoalchemy2.admin.dialects.common import compile_bin_literal
from geoalchemy2.admin.dialects.common import setup_create_drop
from geoalchemy2.admin.dialects.common import unwrap_wkb_constructor_clauses
from geoalchemy2.elements import WKBElement
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry
from geoalchemy2.types import Raster
from geoalchemy2.types.dialects.common import as_binary_ewkb

_SQLALCHEMY_VERSION_BEFORE_2 = version.parse(sqlalchemy.__version__) < version.parse("2")

# Register Geometry, Geography and Raster to SQLAlchemy's reflection subsystems.
_postgresql_ischema_names["geometry"] = Geometry
_postgresql_ischema_names["geography"] = Geography
_postgresql_ischema_names["raster"] = Raster


class _PostgreSQLEWKBBindType(TypeDecorator):
    """Bind runtime ST_GeomFromEWKB values as EWKB bytes."""

    impl = LargeBinary
    cache_ok = True

    def __init__(self, column_srid=None):
        super().__init__()
        self.column_srid = column_srid

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        embedded_srid = None
        try:
            embedded_source = value.data if isinstance(value, WKBElement) else value
            embedded_srid = _wkb_wkt.wkb_srid(embedded_source)
        except (TypeError, ValueError):
            pass
        column_srid = None if _wkb_wkt.is_known_srid(embedded_srid) else self.column_srid
        return as_binary_ewkb(value, column_srid=column_srid)


def _uses_ewkb_geometry_bind_processor(clause):
    spatial_type = getattr(clause, "type", None)
    from_text = getattr(spatial_type, "from_text", "") or ""
    return isinstance(spatial_type, Geometry) and "ewkb" in from_text.lower()


def check_management(column):
    """Check if the column should be managed."""
    if _check_spatial_type(column.type, Raster):
        # Raster columns are not managed
        return _SQLALCHEMY_VERSION_BEFORE_2
    return getattr(column.type, "use_typmod", None) is False


def create_spatial_index(bind, table, col):
    """Create spatial index on the given column."""
    postgresql_ops = {col.name: "gist_geometry_ops_nd"} if col.type.use_N_D_index else {}
    col_func = func.ST_ConvexHull(col) if _check_spatial_type(col.type, Raster) else col
    idx = Index(
        _spatial_idx_name(table.name, col.name),
        col_func,
        postgresql_using="gist",
        postgresql_ops=postgresql_ops,
        _column_flag=True,
    )
    if bind is not None:
        idx.create(bind=bind)
    return idx


def reflect_geometry_column(inspector, table, column_info):
    """Reflect a column of type Geometry with Postgresql dialect."""
    if not _check_spatial_type(column_info.get("type"), (Geometry, Geography, Raster)):
        return
    geo_type = column_info["type"]
    geometry_type = geo_type.geometry_type
    coord_dimension = geo_type.dimension
    if geometry_type is not None:
        if geometry_type.endswith("ZM"):
            coord_dimension = 4
        elif geometry_type[-1] in ["Z", "M"]:
            coord_dimension = 3

    # Query to check a given column has spatial index
    schema_part = f" AND nspname = '{table.schema}'" if table.schema is not None else ""

    # Check if the column has a spatial index (the regular expression checks for the column name
    # in the index definition, which is required for functional indexes)
    has_index_query = """SELECT EXISTS (
        SELECT 1
        FROM pg_class t
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am ON i.relam = am.oid
        WHERE
            t.relname = '{table_name}'{schema_part}
            AND am.amname = 'gist'
            AND (
                EXISTS (
                    SELECT 1
                    FROM pg_attribute a
                    WHERE a.attrelid = t.oid
                    AND a.attnum = ANY(ix.indkey)
                    AND a.attname = '{col_name}'
                )
                OR pg_get_indexdef(
                    ix.indexrelid
                ) ~ '(^|[^a-zA-Z0-9_])("?{col_name}"?)($|[^a-zA-Z0-9_])'
            )
    );""".format(table_name=table.name, col_name=column_info["name"], schema_part=schema_part)
    spatial_index = inspector.bind.execute(text(has_index_query)).scalar()

    # Set attributes
    if not _check_spatial_type(column_info["type"], Raster):
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
                _check_spatial_type(col.type, (Geometry, Raster), dialect) and check_management(col)
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

        # Add spatial indices for the Geometry, Geography and Raster columns
        if (
            _check_spatial_type(col.type, (Geometry, Geography, Raster), dialect)
            and col.type.spatial_index is True
            and not [i for i in table.indexes if col in i.columns.values()]
            and check_management(col)
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
        if _check_spatial_type(col.type, Raster):
            # Raster columns are dropped with the table, no need to drop them separately
            continue
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


def _compile_GeomFromWKB_Postgresql(element, compiler, *, include_srid=True, **kw):
    # Store the SRID
    clauses = list(element.clauses)
    if kw.get("literal_binds", False):
        clauses, _ = unwrap_wkb_constructor_clauses(clauses)
    try:
        srid = clauses[1].value
    except (IndexError, TypeError, ValueError):
        srid = element.type.srid

    if kw.get("literal_binds", False):
        if not include_srid and hasattr(clauses[0], "value") and clauses[0].value is not None:
            value = clauses[0].value
            embedded_srid = None
            try:
                embedded_source = value.data if isinstance(value, WKBElement) else value
                embedded_srid = _wkb_wkt.wkb_srid(embedded_source)
            except (TypeError, ValueError):
                pass
            column_srid = None if _wkb_wkt.is_known_srid(embedded_srid) else srid
            clauses[0] = expression.bindparam(
                key=clauses[0].key,
                value=as_binary_ewkb(value, column_srid=column_srid),
                unique=True,
            )
        wkb_clause = compile_bin_literal(clauses[0])
        prefix = "decode("
        suffix = ", 'hex')"
    else:
        wkb_clause = clauses[0]
        skip_bind_expression = False
        if (
            not include_srid
            and _wkb_wkt.is_known_srid(srid)
            and not _uses_ewkb_geometry_bind_processor(wkb_clause)
        ):
            wkb_clause = expression.type_coerce(
                wkb_clause,
                _PostgreSQLEWKBBindType(column_srid=srid),
            )
        elif not include_srid and _uses_ewkb_geometry_bind_processor(wkb_clause):
            skip_bind_expression = True
        prefix = ""
        suffix = ""

    process_kw = dict(kw)
    if not kw.get("literal_binds", False) and skip_bind_expression:
        process_kw["skip_bind_expression"] = True
    compiled = compiler.process(wkb_clause, **process_kw)

    if include_srid and srid > 0:
        return f"{element.identifier}({prefix}{compiled}{suffix}, {srid})"
    else:
        return f"{element.identifier}({prefix}{compiled}{suffix})"


@compiles(functions.ST_GeomFromWKB, "postgresql")  # type: ignore
def _PostgreSQL_ST_GeomFromWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_Postgresql(element, compiler, **kw)


@compiles(functions.ST_GeomFromEWKB, "postgresql")  # type: ignore
def _PostgreSQL_ST_GeomFromEWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_Postgresql(element, compiler, include_srid=False, **kw)
