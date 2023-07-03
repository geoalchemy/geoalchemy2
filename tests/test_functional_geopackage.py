import os

import pytest
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.event import listen

from geoalchemy2 import Geometry
from geoalchemy2 import load_spatialite_gpkg

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

        # Check that the DB is empty
        raw_conn = engine.connect()
        assert not raw_conn.execute(
            text("PRAGMA main.table_info('gpkg_geometry_columns');")
        ).fetchall()
        raw_conn.connection.dbapi_connection.enable_load_extension(True)
        raw_conn.connection.dbapi_connection.load_extension(os.getenv("SPATIALITE_LIBRARY_PATH"))
        raw_conn.connection.dbapi_connection.enable_load_extension(False)
        assert not raw_conn.execute(
            text("PRAGMA main.table_info('gpkg_geometry_columns');")
        ).fetchall()
        assert raw_conn.execute(text("SELECT HasGeoPackage();")).scalar()
        assert not raw_conn.execute(text("SELECT CheckGeoPackageMetaData();")).scalar()

        # Check that the DB is properly initialized using load_spatialite_gpkg
        listen(engine, "connect", load_spatialite_gpkg)
        conn = engine.connect()
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

    def test_manual_initialization(self, tmpdir, _engine_echo, check_spatialite):
        """Test GeoPackage creation."""
        # Create empty GeoPackage
        tmp_db = tmpdir / "test_spatial_db.gpkg"
        db_url = f"gpkg:///{tmp_db}"
        engine = create_engine(
            db_url, echo=_engine_echo, execution_options={"schema_translate_map": {"gis": None}}
        )

        # Check that the DB is empty
        raw_conn = engine.connect()
        assert not raw_conn.execute(
            text("PRAGMA main.table_info('gpkg_geometry_columns');")
        ).fetchall()
        raw_conn.connection.dbapi_connection.enable_load_extension(True)
        raw_conn.connection.dbapi_connection.load_extension(os.environ["SPATIALITE_LIBRARY_PATH"])
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
        res = raw_conn.execute(text("PRAGMA main.table_info(rtree_a_table_geom)")).fetchall()
        assert len(res) > 0

        t.drop(raw_conn)

        # Check that the table and the spatial indexes were properly removed
        res = raw_conn.execute(text("PRAGMA main.table_info(a_table)")).fetchall()
        assert res == []
        res = raw_conn.execute(text("PRAGMA main.table_info(rtree_a_table_geom)")).fetchall()
        assert res == []

        # Check that the DB is properly initialized
        assert raw_conn.execute(text("SELECT HasGeoPackage();")).scalar()
        assert raw_conn.execute(text("SELECT CheckGeoPackageMetaData();")).scalar()

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
        conn = engine.connect()

        assert not conn.execute(text("PRAGMA main.table_info('gpkg_geometry_columns')")).fetchall()
        assert not conn.execute(text("PRAGMA main.table_info('gpkg_spatial_ref_sys')")).fetchall()

        load_spatialite_gpkg(conn.connection.dbapi_connection, None)

        assert conn.execute(text("SELECT CheckGeoPackageMetaData();")).scalar() == 1
        assert conn.execute(text("PRAGMA main.table_info('gpkg_geometry_columns')")).fetchall()
        assert conn.execute(text("PRAGMA main.table_info('gpkg_spatial_ref_sys')")).fetchall()

        # Check that spatial_ref_sys table was properly populated
        nb_srid = conn.execute(text("SELECT COUNT(*) FROM gpkg_spatial_ref_sys;")).scalar()
        assert nb_srid == 3

    def test_load_spatialite_no_env_variable(self, monkeypatch, conn):
        monkeypatch.delenv("SPATIALITE_LIBRARY_PATH")
        with pytest.raises(RuntimeError):
            load_spatialite_gpkg(conn.connection.dbapi_connection, None)
