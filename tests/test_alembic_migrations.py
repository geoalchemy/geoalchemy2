"""Test alembic migrations of spatial columns."""

import pytest
import sqlalchemy as sa  # noqa (This import is only used in the migration scripts)
from alembic import command
from alembic import script
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import text

from geoalchemy2 import Geometry
from geoalchemy2 import alembic_helpers

from . import check_indexes
from . import test_only_with_dialects


def filter_tables(name, type_, parent_names):
    """Filter tables that we don't care about."""
    return type_ != "table" or name in ["lake", "alembic_table"]


class TestAutogenerate:
    def test_no_diff(self, conn, Lake, setup_tables, use_alembic_monkeypatch, dialect_name):
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
                    nullable=dialect_name not in ["mysql", "mariadb"],
                ),
            ),
            schema=Lake.__table__.schema,
        )

        mc = MigrationContext.configure(
            conn,
            opts={
                "include_name": filter_tables,
                "process_revision_directives": alembic_helpers.writer,
            },
        )

        diff = compare_metadata(mc, metadata)

        assert diff == []

    def test_diff(self, conn, Lake, setup_tables, use_alembic_monkeypatch):
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
                ),
            ),
            Column(
                "new_geom_col",
                Geometry(
                    geometry_type="LINESTRING",
                    srid=4326,
                ),
            ),
            schema=Lake.__table__.schema,
        )

        mc = MigrationContext.configure(
            conn,
            opts={
                "include_name": filter_tables,
                "process_revision_directives": alembic_helpers.writer,
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


@pytest.fixture
def alembic_dir(tmpdir):
    return tmpdir / "alembic_files"


@pytest.fixture
def alembic_config_path(alembic_dir):
    return alembic_dir / "test_alembic.ini"


@pytest.fixture
def alembic_env_path(alembic_dir):
    return alembic_dir / "env.py"


@pytest.fixture
def test_script_path(alembic_dir):
    return alembic_dir / "test_script.py"


@pytest.fixture
def alembic_env(engine, alembic_dir, alembic_config_path, alembic_env_path, test_script_path):
    cfg_tmp = Config(alembic_config_path)
    with engine.begin() as connection:
        connection.execute(text("DROP TABLE IF EXISTS alembic_version;"))
    command.init(cfg_tmp, str(alembic_dir), template="generic")
    with alembic_env_path.open(mode="w", encoding="utf8") as f:
        f.write(
            """
import importlib

from alembic import context
from sqlalchemy import MetaData, engine_from_config
from sqlalchemy.event import listen
from geoalchemy2 import alembic_helpers
from geoalchemy2 import load_spatialite

config = context.config

engine = engine_from_config(
    config.get_section(config.config_ini_section),
    prefix='sqlalchemy.',
    echo=True,
)

if engine.dialect.name == "sqlite":
    listen(engine, 'connect', load_spatialite)

spec = importlib.util.spec_from_file_location("test_script", "{}")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

target_metadata = module.metadata

connection = engine.connect()

context.configure(
    connection=connection,
    target_metadata=target_metadata,
    version_table_pk=True,
    process_revision_directives=alembic_helpers.writer,
    render_item=alembic_helpers.render_item,
    include_object=alembic_helpers.include_object,
    render_as_batch={}
)

try:
    with context.begin_transaction():
        context.run_migrations()
finally:
    connection.close()
    engine.dispose()

""".format(
                str(test_script_path),
                True if engine.dialect.name == "sqlite" else False,
            )
        )
    with test_script_path.open(mode="w", encoding="utf8") as f:
        f.write(
            """
from sqlalchemy import MetaData

metadata = MetaData()

"""
        )
    sc = script.ScriptDirectory.from_config(cfg_tmp)
    return sc


@pytest.fixture
def alembic_config(engine, alembic_dir, alembic_config_path, alembic_env):
    cfg = Config(str(alembic_config_path))
    with alembic_config_path.open(mode="w", encoding="utf8") as f:
        f.write(
            """
[alembic]
script_location = {}
sqlalchemy.url = {}

[loggers]
keys = root

[handlers]
keys = console

[logger_root]
level = WARN
handlers = console
qualname =

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatters]
keys = generic

[formatter_generic]
format = %%(levelname)-5.5s [%%(name)s] %%(message)s
datefmt = %%H:%%M:%%S

""".format(
                alembic_dir, str(engine.url).replace("***", engine.url.password or "")
            )
        )
    return cfg


@test_only_with_dialects("postgresql", "sqlite-spatialite4")
def test_migration_revision(
    conn,
    metadata,
    alembic_config,
    alembic_env_path,
    test_script_path,
    use_alembic_monkeypatch,
    dialect_name,
):
    initial_rev = command.revision(
        alembic_config,
        "Initial state",
        autogenerate=True,
        rev_id="initial",
    )
    command.upgrade(alembic_config, initial_rev.revision)

    # Add a new table in metadata
    with test_script_path.open(mode="w", encoding="utf8") as f:
        f.write(
            """
from geoalchemy2 import Geometry
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table

metadata = MetaData()

group_table = Table(
    "new_groups",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String),
)

new_table = Table(
    "new_spatial_table",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("group_id", Integer, ForeignKey(group_table.c.id)),
    Column(
        "geom_with_idx",
        Geometry(
            geometry_type="LINESTRING",
            srid=4326,
        ),
    ),
    Column(
        "geom_without_idx",
        Geometry(
            geometry_type="LINESTRING",
            srid=4326,
            spatial_index=False,
        ),
    ),
    Column(
        "geom_without_idx_2",
        Geometry(
            geometry_type="LINESTRING",
            srid=4326,
            spatial_index=False,
        ),
    ),
)

"""
        )

    # Auto-generate a new migration script
    rev_table = command.revision(
        alembic_config,
        "Add a new table",
        autogenerate=True,
        rev_id="table",
    )

    # Apply the upgrade script
    command.upgrade(alembic_config, rev_table.revision)

    check_indexes(
        conn,
        dialect_name,
        {
            "postgresql": [
                (
                    "idx_new_spatial_table_geom_with_idx",
                    """CREATE INDEX idx_new_spatial_table_geom_with_idx
                    ON gis.new_spatial_table
                    USING gist (geom_with_idx)""",
                ),
                (
                    "new_spatial_table_pkey",
                    """CREATE UNIQUE INDEX new_spatial_table_pkey
                    ON gis.new_spatial_table
                    USING btree (id)""",
                ),
            ],
            "sqlite": [
                ("new_spatial_table", "geom_with_idx", 2, 2, 4326, 1),
                ("new_spatial_table", "geom_without_idx", 2, 2, 4326, 0),
                ("new_spatial_table", "geom_without_idx_2", 2, 2, 4326, 0),
            ],
        },
        table_name="new_spatial_table",
    )

    # Insert data in new table to check that everything works when Alembic copies the tables
    from_text = "GeomFromEWKT" if conn.dialect.name == "sqlite" else "ST_GeomFromEWKT"
    conn.execute(
        text(
            """INSERT INTO new_spatial_table (
            geom_with_idx,
            geom_without_idx,
            geom_without_idx_2
        ) VALUES (
            {from_text}('SRID=4326;LINESTRING(0 0, 1 1)'),
            {from_text}('SRID=4326;LINESTRING(0 0, 1 1)'),
            {from_text}('SRID=4326;LINESTRING(0 0, 1 1)')
        )
        """.format(
                from_text=from_text
            )
        )
    )
    conn.execute(text("COMMIT"))

    # Remove spatial columns and add new ones
    with test_script_path.open(mode="w", encoding="utf8") as f:
        f.write(
            """
from geoalchemy2 import Geometry
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table

metadata = MetaData()

group_table = Table(
    "new_groups",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String),
)


new_table = Table(
    "new_spatial_table",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("group_id", Integer, ForeignKey(group_table.c.id)),
    Column(
        "geom_with_idx",
        Geometry(
            geometry_type="LINESTRING",
            srid=4326,
        ),
        nullable=False,
    ),
    Column(
        "geom_without_idx",
        Geometry(
            geometry_type="LINESTRING",
            srid=4326,
            spatial_index=False,
        ),
        nullable=False,
    ),
    Column(
        "new_geom_with_idx",
        Geometry(
            geometry_type="LINESTRING",
            srid=4326,
        ),
    ),
    Column(
        "new_geom_without_idx",
        Geometry(
            geometry_type="LINESTRING",
            srid=4326,
            spatial_index=False,
        ),
    ),
)

"""
        )

    # Auto-generate a new migration script
    rev_cols = command.revision(
        alembic_config,
        "Add, alter and remove spatial columns",
        autogenerate=True,
        rev_id="columns",
    )

    # Apply the upgrade script
    command.upgrade(alembic_config, rev_cols.revision)

    check_indexes(
        conn,
        dialect_name,
        {
            "postgresql": [
                (
                    "idx_new_spatial_table_geom_with_idx",
                    """CREATE INDEX idx_new_spatial_table_geom_with_idx ON gis.new_spatial_table
                    USING gist (geom_with_idx)""",
                ),
                (
                    "idx_new_spatial_table_new_geom_with_idx",
                    """CREATE INDEX idx_new_spatial_table_new_geom_with_idx ON gis.new_spatial_table
                    USING gist (new_geom_with_idx)""",
                ),
                (
                    "new_spatial_table_pkey",
                    """CREATE UNIQUE INDEX new_spatial_table_pkey ON gis.new_spatial_table
                    USING btree (id)""",
                ),
            ],
            "sqlite": [
                ("new_spatial_table", "geom_with_idx", 2, 2, 4326, 1),
                ("new_spatial_table", "geom_without_idx", 2, 2, 4326, 0),
                ("new_spatial_table", "new_geom_with_idx", 2, 2, 4326, 1),
                ("new_spatial_table", "new_geom_without_idx", 2, 2, 4326, 0),
            ],
        },
        table_name="new_spatial_table",
    )

    # Apply the downgrade script for columns
    command.downgrade(alembic_config, rev_table.revision)

    check_indexes(
        conn,
        dialect_name,
        {
            "postgresql": [
                (
                    "idx_new_spatial_table_geom_with_idx",
                    """CREATE INDEX idx_new_spatial_table_geom_with_idx
                    ON gis.new_spatial_table
                    USING gist (geom_with_idx)""",
                ),
                (
                    "new_spatial_table_pkey",
                    """CREATE UNIQUE INDEX new_spatial_table_pkey
                    ON gis.new_spatial_table
                    USING btree (id)""",
                ),
            ],
            "sqlite": [
                ("new_spatial_table", "geom_with_idx", 2, 2, 4326, 1),
                ("new_spatial_table", "geom_without_idx", 2, 2, 4326, 0),
                ("new_spatial_table", "geom_without_idx_2", 2, 2, 4326, 0),
            ],
        },
        table_name="new_spatial_table",
    )

    # Apply the downgrade script for tables
    command.downgrade(alembic_config, initial_rev.revision)

    check_indexes(
        conn,
        dialect_name,
        {
            "postgresql": [],
            "sqlite": [],
        },
        table_name="new_spatial_table",
    )
