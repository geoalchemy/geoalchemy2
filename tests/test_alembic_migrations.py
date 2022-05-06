"""Test alembic migrations of spatial columns."""
import re

import sqlalchemy as sa  # noqa (This import is only used in the migration scripts)
from alembic.autogenerate import compare_metadata
from alembic.autogenerate import produce_migrations
from alembic.autogenerate import render_python_code
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import text

from geoalchemy2 import Geometry
from geoalchemy2 import alembic_helpers


def filter_tables(name, type_, parent_names):
    """Filter tables that we don't care about."""
    return type_ != "table" or name in ["lake", "alembic_table"]


class TestAutogenerate:
    def test_no_diff(self, conn, Lake, setup_tables):
        """Check that the autogeneration detects spatial types properly."""
        metadata = MetaData()

        Table(
            "lake",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "geom",
                Geometry(
                    geometry_type="LINESTRING",
                    srid=4326,
                    management=Lake.__table__.c.geom.type.management,
                ),
            ),
            schema=Lake.__table__.schema,
        )

        mc = MigrationContext.configure(
            conn,
            opts={
                "include_object": alembic_helpers.include_object,
                "include_name": filter_tables,
            },
        )

        diff = compare_metadata(mc, metadata)

        assert diff == []

    def test_diff(self, conn, Lake, setup_tables):
        """Check that the autogeneration detects spatial types properly."""
        metadata = MetaData()

        Table(
            "lake",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("new_col", Integer, primary_key=True),
            Column(
                "geom",
                Geometry(
                    geometry_type="LINESTRING",
                    srid=4326,
                    management=Lake.__table__.c.geom.type.management,
                ),
            ),
            Column(
                "new_geom_col",
                Geometry(
                    geometry_type="LINESTRING",
                    srid=4326,
                    management=Lake.__table__.c.geom.type.management,
                ),
            ),
            schema=Lake.__table__.schema,
        )

        mc = MigrationContext.configure(
            conn,
            opts={
                "include_object": alembic_helpers.include_object,
                "include_name": filter_tables,
            },
        )

        diff = compare_metadata(mc, metadata)

        # Check column of type Integer
        add_new_col = diff[0]
        assert add_new_col[0] == "add_column"
        assert add_new_col[1] is None
        assert add_new_col[2] == "lake"
        assert add_new_col[3].name == "new_col"
        assert isinstance(add_new_col[3].type, Integer)
        assert add_new_col[3].primary_key is True
        assert add_new_col[3].nullable is False

        # Check column of type Geometry
        add_new_geom_col = diff[1]
        assert add_new_geom_col[0] == "add_column"
        assert add_new_geom_col[1] is None
        assert add_new_geom_col[2] == "lake"
        assert add_new_geom_col[3].name == "new_geom_col"
        assert isinstance(add_new_geom_col[3].type, Geometry)
        assert add_new_geom_col[3].primary_key is False
        assert add_new_geom_col[3].nullable is True
        assert add_new_geom_col[3].type.srid == 4326
        assert add_new_geom_col[3].type.geometry_type == "LINESTRING"
        assert add_new_geom_col[3].type.name == "geometry"
        assert add_new_geom_col[3].type.dimension == 2


def test_migration(conn, metadata):
    """Test the actual migration of spatial types."""
    Table(
        "alembic_table",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("int_col", Integer, index=True),
        Column(
            "geom",
            Geometry(
                geometry_type="POINT",
                srid=4326,
                management=False,
            ),
        ),
        # The managed column does not work for now
        # Column(
        #     "managed_geom",
        #     Geometry(
        #         geometry_type="POINT",
        #         srid=4326,
        #         management=True,
        #     ),
        # ),
        Column(
            "geom_no_idx",
            Geometry(
                geometry_type="POINT",
                srid=4326,
                spatial_index=False,
            ),
        ),
    )

    mc = MigrationContext.configure(
        conn,
        opts={
            "include_object": alembic_helpers.include_object,
            "include_name": filter_tables,
            "user_module_prefix": "geoalchemy2.types.",
        },
    )

    migration_script = produce_migrations(mc, metadata)
    upgrade_script = render_python_code(
        migration_script.upgrade_ops, render_item=alembic_helpers.render_item
    )
    downgrade_script = render_python_code(
        migration_script.downgrade_ops, render_item=alembic_helpers.render_item
    )

    op = Operations(mc)  # noqa (This variable is only used in the migration scripts)

    # Compile and execute the upgrade part of the migration script
    eval(compile(upgrade_script.replace("    ", ""), "upgrade_script.py", "exec"))

    if conn.dialect.name == "postgresql":
        # Postgresql dialect

        # Query to check the indexes
        index_query = text(
            """SELECT indexname, indexdef
            FROM pg_indexes
            WHERE
                tablename = 'alembic_table'
            ORDER BY indexname;"""
        )
        indexes = conn.execute(index_query).fetchall()

        expected_indices = [
            (
                "alembic_table_pkey",
                """CREATE UNIQUE INDEX alembic_table_pkey
                ON gis.alembic_table
                USING btree (id)""",
            ),
            (
                "idx_alembic_table_geom",
                """CREATE INDEX idx_alembic_table_geom
                ON gis.alembic_table
                USING gist (geom)""",
            ),
            (
                "ix_alembic_table_int_col",
                """CREATE INDEX ix_alembic_table_int_col
                ON gis.alembic_table
                USING btree (int_col)""",
            ),
        ]

        assert len(indexes) == 3

        for idx, expected_idx in zip(indexes, expected_indices):
            assert idx[0] == expected_idx[0]
            assert idx[1] == re.sub("\n *", " ", expected_idx[1])

    elif conn.dialect.name == "sqlite":
        # SQLite dialect

        # Query to check the indexes
        query_indexes = text(
            """SELECT *
            FROM geometry_columns
            WHERE f_table_name = 'alembic_table'
            ORDER BY f_table_name, f_geometry_column;"""
        )

        # Check the actual properties
        geom_cols = conn.execute(query_indexes).fetchall()
        assert geom_cols == [
            ("alembic_table", "geom", 1, 2, 4326, 1),
            ("alembic_table", "geom_no_idx", 1, 2, 4326, 0),
        ]

    # Compile and execute the downgrade part of the migration script
    eval(compile(downgrade_script.replace("    ", ""), "downgrade_script.py", "exec"))

    if conn.dialect.name == "postgresql":
        # Postgresql dialect
        # Here the indexes are attached to the table so if the DROP TABLE works it's ok
        pass
    elif conn.dialect.name == "sqlite":
        # SQLite dialect

        # Query to check the indexes
        query_indexes = text(
            """SELECT *
            FROM geometry_columns
            WHERE f_table_name = 'alembic_table'
            ORDER BY f_table_name, f_geometry_column;"""
        )

        # Now the indexes should have been dropped
        geom_cols = conn.execute(query_indexes).fetchall()
        assert geom_cols == []
