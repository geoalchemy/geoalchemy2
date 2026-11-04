import io
import os

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import BigInteger
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import bindparam
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.event import listen

from geoalchemy2 import Geometry
from geoalchemy2 import load_spatialite_gpkg
from geoalchemy2.admin.dialects.geopackage import GeoPackageDialect

from .schema_fixtures import TransformedGeometry


class TestAdmin:
    def test_create_gpkg(self, tmpdir, _engine_echo, check_spatialite):
        """Test GeoPackage creation."""
        # Create empty GeoPackage
        tmp_db = tmpdir / "test_spatial_db.gpkg"
        db_url = f"gpkg:///{tmp_db}"
        engine = create_engine(
            db_url, echo=_engine_echo, execution_options={"schema_translate_map": {"gis": None}}
        )
        try:
            # Check that the DB is empty
            with engine.connect() as raw_conn:
                assert not raw_conn.execute(
                    text("PRAGMA main.table_info('gpkg_geometry_columns');")
                ).fetchall()
                raw_conn.connection.dbapi_connection.enable_load_extension(True)
                raw_conn.connection.dbapi_connection.load_extension(
                    os.getenv("SPATIALITE_LIBRARY_PATH")
                )
                raw_conn.connection.dbapi_connection.enable_load_extension(False)
                assert not raw_conn.execute(
                    text("PRAGMA main.table_info('gpkg_geometry_columns');")
                ).fetchall()
                assert raw_conn.execute(text("SELECT HasGeoPackage();")).scalar()
                assert not raw_conn.execute(text("SELECT CheckGeoPackageMetaData();")).scalar()

            engine.dispose()

            # Check that the DB is properly initialized using load_spatialite_gpkg
            listen(engine, "connect", load_spatialite_gpkg)
            with engine.connect() as conn:
                assert conn.execute(text("SELECT HasGeoPackage();")).scalar()
                assert conn.execute(text("SELECT CheckGeoPackageMetaData();")).scalar()

                # Create a new table in this manually created GeoPackage
                t = Table(
                    "a_table",
                    MetaData(),
                    Column("id", Integer, primary_key=True),
                    Column("geom", Geometry(srid=2154)),
                )
                t.create(conn)
                t.drop(conn)
        finally:
            engine.dispose()

    def test_manual_initialization(self, tmpdir, _engine_echo, check_spatialite):
        """Test GeoPackage creation."""
        # Create empty GeoPackage
        tmp_db = tmpdir / "test_spatial_db.gpkg"
        db_url = f"gpkg:///{tmp_db}"
        engine = create_engine(
            db_url, echo=_engine_echo, execution_options={"schema_translate_map": {"gis": None}}
        )
        try:
            with engine.connect() as raw_conn:
                # Check that the DB is empty
                assert not raw_conn.execute(
                    text("PRAGMA main.table_info('gpkg_geometry_columns');")
                ).fetchall()
                raw_conn.connection.dbapi_connection.enable_load_extension(True)
                raw_conn.connection.dbapi_connection.load_extension(
                    os.environ["SPATIALITE_LIBRARY_PATH"]
                )
                raw_conn.connection.dbapi_connection.enable_load_extension(False)
                assert not raw_conn.execute(
                    text("PRAGMA main.table_info('gpkg_geometry_columns');")
                ).fetchall()
                assert raw_conn.execute(text("SELECT HasGeoPackage();")).scalar()
                assert not raw_conn.execute(text("SELECT CheckGeoPackageMetaData();")).scalar()

                # Create a new table in this manually created GeoPackage
                t = Table(
                    "a_table",
                    MetaData(),
                    Column("id", Integer, primary_key=True),
                )
                t.create(raw_conn)

                # Check that the table was properly created
                res = raw_conn.execute(text("PRAGMA main.table_info(a_table)")).fetchall()
                assert res == [
                    (0, "id", "INTEGER", 1, None, 1),
                ]

                t.drop(raw_conn)

                # Manual initialization
                raw_conn.execute(text("SELECT gpkgCreateBaseTables();"))

                # Create again a new table in this manually created GeoPackage
                t = Table(
                    "a_table",
                    MetaData(),
                    Column("id", Integer, primary_key=True),
                    Column("geom", Geometry(srid=2154)),
                )
                t.create(raw_conn)

                # Check that the table and the spatial indexes were properly created
                res = raw_conn.execute(text("PRAGMA main.table_info(a_table)")).fetchall()
                assert res == [
                    (0, "id", "INTEGER", 1, None, 1),
                    (1, "geom", "GEOMETRY", 0, None, 0),
                ]
                res = raw_conn.execute(
                    text("PRAGMA main.table_info(rtree_a_table_geom)")
                ).fetchall()
                assert len(res) > 0

                t.drop(raw_conn)

                # Check that the table and the spatial indexes were properly removed
                res = raw_conn.execute(text("PRAGMA main.table_info(a_table)")).fetchall()
                assert res == []
                res = raw_conn.execute(
                    text("PRAGMA main.table_info(rtree_a_table_geom)")
                ).fetchall()
                assert res == []

                # Check that the DB is properly initialized
                assert raw_conn.execute(text("SELECT HasGeoPackage();")).scalar()
                assert raw_conn.execute(text("SELECT CheckGeoPackageMetaData();")).scalar()
        finally:
            engine.dispose()

    def test_multi_geom_cols_fail(self, conn):
        t = Table(
            "a_table",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("geom", Geometry()),
            Column("geom_2", Geometry()),
        )
        with pytest.raises(
            ValueError,
            match=r"Only one geometry column is allowed for a table stored in a GeoPackage\.",
        ):
            t.create(conn)

    def test_add_srid(self, conn):
        t = Table(
            "a_table",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("geom", Geometry(srid=3857)),
        )
        check_srid_query = text("""SELECT srs_id FROM gpkg_spatial_ref_sys WHERE srs_id = 3857;""")
        assert not conn.execute(check_srid_query).fetchall()
        t.create(conn)
        assert conn.execute(check_srid_query).fetchall()


def _get_gpkg_contents(conn, *table_names):
    return conn.execute(
        text(
            """SELECT table_name, data_type, identifier, srs_id
            FROM gpkg_contents
            WHERE table_name IN :table_names
            ORDER BY table_name;"""
        ).bindparams(bindparam("table_names", list(table_names), expanding=True))
    ).fetchall()


class TestGpkgContents:
    def test_create_drop_tables(self, conn):
        metadata = MetaData()
        Table(
            "attributes_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String),
        )
        Table(
            "features_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("geom", Geometry("POINT", srid=4326)),
        )

        metadata.create_all(conn)
        assert _get_gpkg_contents(conn, "attributes_table", "features_table") == [
            ("attributes_table", "attributes", "attributes_table", None),
            ("features_table", "features", "features_table", 4326),
        ]
        assert conn.execute(text("SELECT CheckGeoPackageMetaData();")).scalar()

        metadata.drop_all(conn)
        assert _get_gpkg_contents(conn, "attributes_table", "features_table") == []

    @pytest.mark.parametrize(
        "table_name,columns,kwargs",
        [
            pytest.param("a_table", [Column("id", Integer)], {}, id="no_primary_key"),
            pytest.param(
                "alembic_version",
                [Column("version_num", String(32), primary_key=True)],
                {},
                id="string_primary_key",
            ),
            pytest.param(
                "a_table",
                [Column("id", BigInteger, primary_key=True)],
                {},
                id="bigint_primary_key",
            ),
            pytest.param(
                "a_table",
                [
                    Column("id_1", Integer, primary_key=True),
                    Column("id_2", Integer, primary_key=True),
                ],
                {},
                id="composite_primary_key",
            ),
            pytest.param(
                "a_table",
                [Column("id", Integer, primary_key=True)],
                {"sqlite_with_rowid": False},
                id="without_rowid",
            ),
            pytest.param(
                "a_table",
                [Column("id", Integer, primary_key=True)],
                {"prefixes": ["TEMPORARY"]},
                id="temporary_table",
            ),
        ],
    )
    def test_non_attributes_tables_are_not_registered(self, conn, table_name, columns, kwargs):
        t = Table(table_name, MetaData(), *columns, **kwargs)

        t.create(conn)
        assert _get_gpkg_contents(conn, table_name) == []

        t.drop(conn)
        assert _get_gpkg_contents(conn, table_name) == []


class TestAlembic:
    @pytest.fixture
    def op(self, conn):
        import geoalchemy2.alembic_helpers  # noqa: F401 (register the GeoPackage implementation)

        # The 'gpkg_metadata_reference_column_name_update' trigger created by SpatiaLite can not be
        # parsed by recent SQLite versions, which breaks 'ALTER TABLE ... RENAME TO ...' statements
        # unless the legacy mode is enabled
        conn.execute(text("PRAGMA legacy_alter_table = ON;"))

        Table("a_table", MetaData(), Column("id", Integer, primary_key=True)).create(conn)
        assert _get_gpkg_contents(conn, "a_table") == [("a_table", "attributes", "a_table", None)]

        return Operations(MigrationContext.configure(conn))

    def test_rename_table(self, conn, op):
        op.rename_table("a_table", "new_table")

        assert _get_gpkg_contents(conn, "a_table", "new_table") == [
            ("new_table", "attributes", "new_table", None)
        ]

    def test_batch_alter_table(self, conn, op):
        with op.batch_alter_table("a_table", recreate="always") as batch_op:
            batch_op.add_column(Column("name", String))

        assert conn.execute(text("PRAGMA main.table_info(a_table)")).fetchall() == [
            (0, "id", "INTEGER", 1, None, 1),
            (1, "name", "VARCHAR", 0, None, 0),
        ]
        assert _get_gpkg_contents(conn, "a_table", "_alembic_tmp_a_table") == [
            ("a_table", "attributes", "a_table", None)
        ]

    def test_offline_mode(self):
        """Check that the SQL script can be generated (e.g. using 'alembic upgrade --sql')."""
        import geoalchemy2.alembic_helpers  # noqa: F401 (register the GeoPackage implementation)

        buffer = io.StringIO()
        op = Operations(
            MigrationContext.configure(
                dialect=GeoPackageDialect(),
                opts={"as_sql": True, "output_buffer": buffer, "literal_binds": True},
            )
        )

        op.create_table("a_table", Column("id", Integer, primary_key=True))
        op.rename_table("a_table", "new_table")
        op.drop_table("new_table")

        sql_script = " ".join(buffer.getvalue().split())
        assert (
            "INSERT INTO gpkg_contents VALUES ( 'a_table', 'attributes', 'a_table'," in sql_script
        )
        assert "UPDATE gpkg_contents SET table_name = 'new_table'," in sql_script
        assert (
            "DELETE FROM gpkg_contents WHERE LOWER(table_name) = LOWER('new_table')" in sql_script
        )
        assert ";;" not in sql_script


class TestIndex:
    def test_index_gpkg(self, conn, Lake, setup_tables):
        assert (
            conn.execute(
                text(
                    """SELECT COUNT(*) FROM gpkg_extensions
                WHERE table_name = 'lake'
                    AND column_name = 'geom'
                    AND extension_name = 'gpkg_rtree_index';"""
                )
            ).scalar()
            == 1
        )

    def test_type_decorator_index_gpkg(self, conn, base, metadata):
        class LocalPoint(base):
            __tablename__ = "local_point"
            id = Column(Integer, primary_key=True)
            geom = Column(TransformedGeometry(db_srid=2154, app_srid=4326, geometry_type="POINT"))

        metadata.drop_all(conn, checkfirst=True)
        metadata.create_all(conn)
        assert (
            conn.execute(
                text(
                    """SELECT COUNT(*) FROM gpkg_extensions
                WHERE table_name = 'local_point'
                    AND column_name = 'geom'
                    AND extension_name = 'gpkg_rtree_index';"""
                )
            ).scalar()
            == 1
        )


class TestMiscellaneous:
    def test_load_spatialite_gpkg(self, tmpdir, _engine_echo, check_spatialite):
        # Create empty DB
        tmp_db = tmpdir / "test_spatial_db.sqlite"
        db_url = f"sqlite:///{tmp_db}"
        engine = create_engine(
            db_url, echo=_engine_echo, execution_options={"schema_translate_map": {"gis": None}}
        )
        try:
            with engine.connect() as conn:
                assert not conn.execute(
                    text("PRAGMA main.table_info('gpkg_geometry_columns')")
                ).fetchall()
                assert not conn.execute(
                    text("PRAGMA main.table_info('gpkg_spatial_ref_sys')")
                ).fetchall()

                load_spatialite_gpkg(conn.connection.dbapi_connection, None)

                assert conn.execute(text("SELECT CheckGeoPackageMetaData();")).scalar() == 1
                assert conn.execute(
                    text("PRAGMA main.table_info('gpkg_geometry_columns')")
                ).fetchall()
                assert conn.execute(
                    text("PRAGMA main.table_info('gpkg_spatial_ref_sys')")
                ).fetchall()

                # Check that spatial_ref_sys table was properly populated
                nb_srid = conn.execute(text("SELECT COUNT(*) FROM gpkg_spatial_ref_sys;")).scalar()
                assert nb_srid == 3
        finally:
            engine.dispose()

    def test_load_spatialite_no_env_variable(self, monkeypatch, conn):
        monkeypatch.delenv("SPATIALITE_LIBRARY_PATH")
        with pytest.raises(RuntimeError):
            load_spatialite_gpkg(conn.connection.dbapi_connection, None)
