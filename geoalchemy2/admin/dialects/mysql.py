"""This module defines specific functions for MySQL dialect."""

from sqlalchemy import text
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.sqltypes import NullType

from geoalchemy2 import functions
from geoalchemy2.admin.dialects.common import _check_spatial_type
from geoalchemy2.admin.dialects.common import _spatial_idx_name
from geoalchemy2.admin.dialects.common import setup_create_drop
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry

_POSSIBLE_TYPES = [
    "geometry",
    "point",
    "linestring",
    "polygon",
    "multipoint",
    "multilinestring",
    "multipolygon",
    "geometrycollection",
]


def reflect_geometry_column(inspector, table, column_info):
    """Reflect a column of type Geometry with Postgresql dialect."""
    if not isinstance(column_info.get("type"), (Geometry, NullType)):
        return

    column_name = column_info.get("name")
    schema = table.schema or inspector.default_schema_name

    # Check geometry type, SRID and if the column is nullable
    geometry_type_query = """SELECT DATA_TYPE, SRS_ID, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{}' and COLUMN_NAME = '{}'""".format(
        table.name, column_name
    )
    if schema is not None:
        geometry_type_query += """ and table_schema = '{}'""".format(schema)
    geometry_type, srid, nullable_str = inspector.bind.execute(text(geometry_type_query)).one()
    is_nullable = str(nullable_str).lower() == "yes"

    if geometry_type not in _POSSIBLE_TYPES:
        return

    # Check if the column has spatial index
    has_index_query = """SELECT DISTINCT
            INDEX_TYPE
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_NAME = '{}' and COLUMN_NAME = '{}'""".format(
        table.name, column_name
    )
    if schema is not None:
        has_index_query += """ and TABLE_SCHEMA = '{}'""".format(schema)
    spatial_index_res = inspector.bind.execute(text(has_index_query)).scalar()
    spatial_index = str(spatial_index_res).lower() == "spatial"

    # Set attributes
    column_info["type"] = Geometry(
        geometry_type=geometry_type.upper(),
        srid=srid,
        spatial_index=spatial_index,
        nullable=is_nullable,
        _spatial_index_reflected=True,
    )


def before_create(table, bind, **kw):
    """Handle spatial indexes during the before_create event."""
    dialect, gis_cols, regular_cols = setup_create_drop(table, bind)

    # Remove the spatial indexes from the table metadata because they should not be
    # created during the table.create() step since the associated columns do not exist
    # at this time.
    table.info["_after_create_indexes"] = []
    current_indexes = set(table.indexes)
    for idx in current_indexes:
        for col in table.info["_saved_columns"]:
            if (_check_spatial_type(col.type, Geometry, dialect)) and col in idx.columns.values():
                table.indexes.remove(idx)
                if idx.name != _spatial_idx_name(table.name, col.name) or not getattr(
                    col.type, "spatial_index", False
                ):
                    table.info["_after_create_indexes"].append(idx)

    table.columns = table.info.pop("_saved_columns")


def after_create(table, bind, **kw):
    """Handle spatial indexes during the after_create event."""
    # Restore original column list including managed Geometry columns
    dialect = bind.dialect

    # table.columns = table.info.pop("_saved_columns")

    for col in table.columns:
        # Add spatial indices for the Geometry and Geography columns
        if (
            _check_spatial_type(col.type, (Geometry, Geography), dialect)
            and col.type.spatial_index is True
        ):
            # If the index does not exist, define it and create it
            if not [i for i in table.indexes if col in i.columns.values()]:
                sql = "ALTER TABLE {} ADD SPATIAL INDEX({});".format(table.name, col.name)
                q = text(sql)
                bind.execute(q)

    for idx in table.info.pop("_after_create_indexes"):
        table.indexes.add(idx)


def before_drop(table, bind, **kw):
    return


def after_drop(table, bind, **kw):
    return


_MYSQL_FUNCTIONS = {
    "ST_AsEWKB": "ST_AsBinary",
}


def _compiles_mysql(cls, fn):
    def _compile_mysql(element, compiler, **kw):
        return "{}({})".format(fn, compiler.process(element.clauses, **kw))

    compiles(getattr(functions, cls), "mysql")(_compile_mysql)
    compiles(getattr(functions, cls), "mariadb")(_compile_mysql)


def register_mysql_mapping(mapping):
    """Register compilation mappings for the given functions.

    Args:
        mapping: Should have the following form::

                {
                    "function_name_1": "mysql_function_name_1",
                    "function_name_2": "mysql_function_name_2",
                    ...
                }
    """
    for cls, fn in mapping.items():
        _compiles_mysql(cls, fn)


register_mysql_mapping(_MYSQL_FUNCTIONS)


def _compile_GeomFromText_MySql(element, compiler, **kw):
    element.identifier = "ST_GeomFromText"
    compiled = compiler.process(element.clauses, **kw)
    srid = element.type.srid

    if srid > 0:
        return "{}({}, {})".format(element.identifier, compiled, srid)
    else:
        return "{}({})".format(element.identifier, compiled)


def _compile_GeomFromWKB_MySql(element, compiler, **kw):
    element.identifier = "ST_GeomFromWKB"
    wkb_data = list(element.clauses)[0].value
    if isinstance(wkb_data, memoryview):
        list(element.clauses)[0].value = wkb_data.tobytes()
    compiled = compiler.process(element.clauses, **kw)
    srid = element.type.srid

    if srid > 0:
        return "{}({}, {})".format(element.identifier, compiled, srid)
    else:
        return "{}({})".format(element.identifier, compiled)


@compiles(functions.ST_GeomFromText, "mysql")  # type: ignore
@compiles(functions.ST_GeomFromText, "mariadb")  # type: ignore
def _MySQL_ST_GeomFromText(element, compiler, **kw):
    return _compile_GeomFromText_MySql(element, compiler, **kw)


@compiles(functions.ST_GeomFromEWKT, "mysql")  # type: ignore
@compiles(functions.ST_GeomFromEWKT, "mariadb")  # type: ignore
def _MySQL_ST_GeomFromEWKT(element, compiler, **kw):
    return _compile_GeomFromText_MySql(element, compiler, **kw)


@compiles(functions.ST_GeomFromWKB, "mysql")  # type: ignore
@compiles(functions.ST_GeomFromWKB, "mariadb")  # type: ignore
def _MySQL_ST_GeomFromWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_MySql(element, compiler, **kw)


@compiles(functions.ST_GeomFromEWKB, "mysql")  # type: ignore
@compiles(functions.ST_GeomFromEWKB, "mariadb")  # type: ignore
def _MySQL_ST_GeomFromEWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_MySql(element, compiler, **kw)
