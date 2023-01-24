"""This module defines specific functions for MySQL dialect."""
from sqlalchemy import Index
from sqlalchemy import text

# from geoalchemy2.dialects.common import before_create
# from geoalchemy2.dialects.common import after_create
from geoalchemy2.dialects.common import _check_spatial_type
from geoalchemy2.dialects.common import _spatial_idx_name
from geoalchemy2.dialects.common import after_drop
from geoalchemy2.dialects.common import before_drop
from geoalchemy2.dialects.common import check_management
from geoalchemy2.dialects.common import setup_create_drop
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry


def before_create(table, bind, **kw):
    """Handle spatial indexes during the before_create event."""
    dialect, gis_cols, regular_cols = setup_create_drop(table, bind)
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
                if idx.name != _spatial_idx_name(table.name, col.name) or not getattr(
                    col.type, "spatial_index", False
                ):
                    table.info["_after_create_indexes"].append(idx)

    table.columns = table.info.pop("_saved_columns")


def after_create(table, bind, **kw):
    """Handle spatial indexes during the after_create event."""
    # Restore original column list including managed Geometry columns
    dialect = bind.dialect
    dialect_name = dialect.name

    # table.columns = table.info.pop("_saved_columns")

    for col in table.columns:
        # Add the managed Geometry columns with AddGeometryColumn()
        # if _check_spatial_type(col.type, Geometry, dialect) and check_management(col, dialect_name):
        #     dimension = col.type.dimension

        #     # Add geometry columns for MySQL
        #     spec = '%s' % col.type.geometry_type
        #     if not col.type.nullable:
        #         spec += ' NOT NULL'
        #     if col.type.srid > 0:
        #         spec += ' SRID %d' % col.type.srid
        #     sql = "ALTER TABLE {} ADD COLUMN {} {}".format(table.name, col.name, spec)
        #     stmt = text(sql)
        #     stmt = stmt.execution_options(autocommit=True)
        #     bind.execute(stmt)
        #     create_func = None

        # Add spatial indices for the Geometry and Geography columns
        if (
            _check_spatial_type(col.type, (Geometry, Geography), dialect)
            and col.type.spatial_index is True
        ):
            # If the index does not exist, define it and create it
            if not [i for i in table.indexes if col in i.columns.values()] and check_management(
                col, dialect_name
            ):
                # if col.type.use_N_D_index:
                #     postgresql_ops = {col.name: "gist_geometry_ops_nd"}
                # else:
                #     postgresql_ops = {}
                # idx = Index(
                #     _spatial_idx_name(table.name, col.name),
                #     col,
                #     postgresql_using="gist",
                #     postgresql_ops=postgresql_ops,
                #     _column_flag=True,
                # )
                sql = "ALTER TABLE {} ADD SPATIAL INDEX({});".format(table.name, col.name)
                q = text(sql)
                bind.execute(q)

    for idx in table.info.pop("_after_create_indexes"):
        table.indexes.add(idx)


# def before_drop(table, bind, **kw):
#     return


# def after_drop(table, bind, **kw):
#     return
