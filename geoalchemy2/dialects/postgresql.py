"""This module defines specific functions for Postgresql dialect."""
from sqlalchemy import Index
from sqlalchemy import text

from geoalchemy2.dialects.common import _spatial_idx_name
from geoalchemy2.dialects.common import check_management


def create_spatial_index(bind, table, col):
    """Create spatial index on the given column."""
    # If the index does not exist (which might be the case when
    # management=False), define it and create it
    if (
        not [i for i in table.indexes if col in i.columns.values()]
        and check_management(col, "postgresql")
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


def reflect_geometry_column(inspector, table, column_info):
    """Reflect a column of type Geometry with Postgresql dialect."""
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
