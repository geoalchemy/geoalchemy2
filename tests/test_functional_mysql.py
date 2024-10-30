from json import loads

import pytest
from packaging.version import parse as parse_version
from shapely.geometry import LineString
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import __version__ as SA_VERSION
from sqlalchemy import bindparam
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import StatementError
from sqlalchemy.sql import func
from sqlalchemy.sql import select

from geoalchemy2 import Geometry
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import from_shape
from geoalchemy2.shape import to_shape

from . import test_only_with_dialects


class TestAdmin:
    def test_create_drop_tables(
        self,
        conn,
        metadata,
        Lake,
        Poi,
        Summit,
        LocalPoint,
        IndexTestWithSchema,
        IndexTestWithoutSchema,
    ):
        metadata.drop_all(conn, checkfirst=True)
        metadata.create_all(conn)
        metadata.drop_all(conn, checkfirst=True)


class TestInsertionCore:
    @pytest.mark.parametrize("use_executemany", [True, False])
    def test_insert_mysql(self, conn, Lake, setup_tables, use_executemany):
        # Issue several inserts using DBAPI's executemany() method or single inserts. This tests
        # the Geometry type's bind_processor and bind_expression functions.
        elements = [
            {"geom": "SRID=4326;LINESTRING(0 0,1 1)"},
            {"geom": "LINESTRING(0 0,1 1)"},
            {"geom": WKTElement("LINESTRING(0 0,2 2)")},
            {"geom": from_shape(LineString([[0, 0], [3, 3]]), srid=4326)},
        ]

        if use_executemany:
            conn.execute(Lake.__table__.insert(), elements)
        else:
            for element in elements:
                query = Lake.__table__.insert().values(**element)
                conn.execute(query)

        results = conn.execute(Lake.__table__.select().order_by("id"))
        rows = results.fetchall()

        row = rows[0]
        assert isinstance(row[1], WKBElement)
        wkt = conn.execute(row[1].ST_AsText()).scalar()
        assert wkt == "LINESTRING(0 0,1 1)"
        srid = conn.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

        row = rows[1]
        assert isinstance(row[1], WKBElement)
        wkt = conn.execute(row[1].ST_AsText()).scalar()
        assert wkt == "LINESTRING(0 0,1 1)"
        srid = conn.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

        row = rows[2]
        assert isinstance(row[1], WKBElement)
        wkt = conn.execute(row[1].ST_AsText()).scalar()
        assert wkt == "LINESTRING(0 0,2 2)"
        srid = conn.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

        row = rows[3]
        assert isinstance(row[1], WKBElement)
        wkt = conn.execute(row[1].ST_AsText()).scalar()
        assert wkt == "LINESTRING(0 0,3 3)"
        srid = conn.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

        # Check that selected elements can be inserted again
        for row in rows:
            conn.execute(Lake.__table__.insert().values(geom=row[1]))
        conn.execute(
            Lake.__table__.insert(),
            [{"geom": row[1]} for row in rows],
        )


class TestInsertionORM:
    def test_WKT(self, session, Lake, setup_tables):
        lake = Lake("LINESTRING(0 0,1 1)")
        session.add(lake)
        session.flush()

    def test_WKTElement(self, session, Lake, setup_tables):
        lake = Lake(WKTElement("LINESTRING(0 0,1 1)", srid=4326))
        session.add(lake)
        session.flush()
        session.expire(lake)
        assert isinstance(lake.geom, WKBElement)
        assert (
            str(lake.geom) == "0102000000020000000000000000000000000000000000000000000"
            "0000000f03f000000000000f03f"
        )
        wkt = session.execute(lake.geom.ST_AsText()).scalar()
        assert wkt == "LINESTRING(0 0,1 1)"
        srid = session.execute(lake.geom.ST_SRID()).scalar()
        assert srid == 4326

    def test_WKBElement(self, session, Lake, setup_tables):
        shape = LineString([[0, 0], [1, 1]])
        lake = Lake(from_shape(shape, srid=4326))
        session.add(lake)
        session.flush()
        session.expire(lake)
        assert isinstance(lake.geom, WKBElement)
        assert (
            str(lake.geom) == "0102000000020000000000000000000000000000000000000000000"
            "0000000f03f000000000000f03f"
        )
        wkt = session.execute(lake.geom.ST_AsText()).scalar()
        assert wkt == "LINESTRING(0 0,1 1)"
        srid = session.execute(lake.geom.ST_SRID()).scalar()
        assert srid == 4326

    def test_raise_wrong_srid_str(self, session, Lake, setup_tables):
        lake = Lake("SRID=2154;LINESTRING(0 0,1 1)")
        session.add(lake)
        with pytest.raises(StatementError):
            session.flush()

    def test_raise_wrong_srid_WKTElement(self, session, Lake, setup_tables):
        lake = Lake(WKTElement("LINESTRING(0 0,1 1)", srid=2154))
        session.add(lake)
        with pytest.raises(StatementError):
            session.flush()


class TestShapely:
    def test_to_shape(self, conn, session, Lake, setup_tables):
        element = WKTElement("LINESTRING(0 0,1 1)", srid=4326)
        lake = Lake(geom=element)
        session.add(lake)
        session.flush()
        session.expire(lake)
        lake = session.query(Lake).one()
        assert isinstance(lake.geom, WKBElement)
        assert isinstance(lake.geom.data, bytes)
        assert lake.geom.srid == 4326
        s = to_shape(lake.geom)
        assert isinstance(s, LineString)
        assert s.wkt == "LINESTRING (0 0, 1 1)"

        conn.execute(Lake.__table__.insert().values(geom="LINESTRING(0 0,1 1)"))

        conn.execute(
            Lake.__table__.insert(),
            [
                {"geom": "SRID=4326;LINESTRING(0 0,1 1)"},
                {"geom": WKTElement("LINESTRING(0 0,2 2)")},
                {"geom": from_shape(LineString([[0, 0], [3, 3]]), srid=4326)},
            ],
        )

        lake = Lake(lake.geom)
        session.add(lake)
        session.flush()
        session.expire(lake)
        assert isinstance(lake.geom, WKBElement)
        assert isinstance(lake.geom.data, bytes)
        assert lake.geom.srid == 4326


class TestCallFunction:
    @pytest.fixture
    def setup_one_lake(self, session, Lake, setup_tables):
        lake = Lake(WKTElement("LINESTRING(0 0,1 1)", srid=4326))
        session.add(lake)
        session.flush()
        session.expire(lake)
        return lake.id

    def test_ST_GeometryType(self, session, Lake, setup_one_lake):
        lake_id = setup_one_lake

        s = select(func.ST_GeometryType(Lake.__table__.c.geom))
        r1 = session.execute(s).scalar()
        assert r1 == "LINESTRING"

        lake = session.query(Lake).get(lake_id)
        r2 = session.execute(lake.geom.ST_GeometryType()).scalar()
        assert r2 == "LINESTRING"

        r3 = session.query(Lake.geom.ST_GeometryType()).scalar()
        assert r3 == "LINESTRING"

        r4 = session.query(Lake).filter(Lake.geom.ST_GeometryType() == "LINESTRING").one()
        assert isinstance(r4, Lake)
        assert r4.id == lake_id

    @test_only_with_dialects("mysql")
    def test_ST_Transform(self, session, Lake, setup_one_lake):
        lake_id = setup_one_lake

        s = select(func.ST_Transform(Lake.__table__.c.geom, 2154))
        r1 = session.execute(s).scalar()
        assert isinstance(r1, WKBElement)

        lake = session.query(Lake).get(lake_id)
        r2 = session.execute(lake.geom.ST_Transform(2154)).scalar()
        assert isinstance(r2, WKBElement)

        r3 = session.query(Lake.geom.ST_Transform(2154)).scalar()
        assert isinstance(r3, WKBElement)

        assert r1.data == r2.data == r3.data

        r4 = (
            session.query(Lake)
            .filter(
                func.ST_Distance(
                    WKTElement("POINT(253531 908605)", srid=2154),
                    Lake.geom.ST_Transform(2154),
                )
                <= 1
            )
            .one()
        )
        assert isinstance(r4, Lake)
        assert r4.id == lake_id

    def test_ST_GeoJSON(self, session, Lake, setup_one_lake):
        lake_id = setup_one_lake

        def _test(r):
            r = loads(r)
            assert r["type"] == "LineString"
            assert r["coordinates"] == [[0, 0], [1, 1]]

        s = select(func.ST_AsGeoJSON(Lake.__table__.c.geom))
        r = session.execute(s).scalar()
        _test(r)

        lake = session.query(Lake).get(lake_id)
        r = session.execute(lake.geom.ST_AsGeoJSON()).scalar()
        _test(r)

        r = session.query(Lake.geom.ST_AsGeoJSON()).scalar()
        _test(r)

    @pytest.mark.skipif(
        True, reason="MySQL does not support the feature version of AsGeoJson() yet"
    )
    def test_ST_GeoJSON_feature(self, session, Lake, setup_tables):
        ss3 = select(Lake, bindparam("dummy_val", 10).label("dummy_attr")).alias()
        s3 = select(func.ST_AsGeoJSON(ss3, "geom"))
        r3 = session.execute(s3).scalar()
        assert loads(r3) == {
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": [[0, 0], [1, 1]]},
            "properties": {"dummy_attr": 10, "id": 1},
        }

    @pytest.mark.skipif(
        parse_version(SA_VERSION) < parse_version("1.3.4"),
        reason="Case-insensitivity is only available for sqlalchemy>=1.3.4",
    )
    @test_only_with_dialects("mysql")
    def test_comparator_case_insensitivity(self, session, Lake, setup_one_lake):
        lake_id = setup_one_lake

        s = select(func.ST_Transform(Lake.__table__.c.geom, 2154))
        r1 = session.execute(s).scalar()
        assert isinstance(r1, WKBElement)

        lake = session.query(Lake).get(lake_id)

        r2 = session.execute(lake.geom.ST_Transform(2154)).scalar()
        assert isinstance(r2, WKBElement)

        r3 = session.execute(lake.geom.st_transform(2154)).scalar()
        assert isinstance(r3, WKBElement)

        r4 = session.execute(lake.geom.St_TrAnSfOrM(2154)).scalar()
        assert isinstance(r4, WKBElement)

        r5 = session.query(Lake.geom.ST_Transform(2154)).scalar()
        assert isinstance(r5, WKBElement)

        r6 = session.query(Lake.geom.st_transform(2154)).scalar()
        assert isinstance(r6, WKBElement)

        r7 = session.query(Lake.geom.St_TrAnSfOrM(2154)).scalar()
        assert isinstance(r7, WKBElement)

        assert r1.data == r2.data == r3.data == r4.data == r5.data == r6.data == r7.data


class TestNullable:
    @test_only_with_dialects("mysql")
    def test_insert(self, conn, Lake, setup_tables):
        # Insert geometries
        conn.execute(
            Lake.__table__.insert(),
            [
                {"geom": "SRID=4326;LINESTRING(0 0,1 1)"},
                {"geom": WKTElement("LINESTRING(0 0,2 2)", srid=4326)},
                {"geom": from_shape(LineString([[0, 0], [3, 3]]), srid=4326)},
            ],
        )

        # Fail when trying to insert null geometry
        with pytest.raises(OperationalError):
            conn.execute(Lake.__table__.insert(), [{"geom": None}])


class TestReflection:
    @pytest.fixture
    def create_temp_db(self, _engine_echo, engine, conn, reflection_tables_metadata):
        """Temporary database, that is dropped on fixture teardown.
        Used to make sure reflection methods always uses the correct schema.
        """
        temp_db_name = "geoalchemy_test_reflection"
        temp_db_url = URL.create(
            engine.url.drivername,
            engine.url.username,
            engine.url.password,
            engine.url.host,
            engine.url.port,
            temp_db_name,
        )
        engine = create_engine(
            temp_db_url,
            echo=_engine_echo,
        )
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {temp_db_name};"))
        with engine.connect() as connection:
            with connection.begin():
                reflection_tables_metadata.drop_all(connection, checkfirst=True)
                reflection_tables_metadata.create_all(connection)
                yield connection
        conn.execute(text(f"DROP DATABASE IF EXISTS {temp_db_name};"))

    @pytest.fixture
    def setup_reflection_tables(self, reflection_tables_metadata, conn):
        reflection_tables_metadata.drop_all(conn, checkfirst=True)
        reflection_tables_metadata.create_all(conn)

    def test_reflection_mysql(self, conn, setup_reflection_tables, create_temp_db, dialect_name):
        t = Table("lake", MetaData(), autoload_with=conn)

        type_ = t.c.geom.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == "LINESTRING"
        assert type_.srid == 4326 if dialect_name == "mysql" else -1
        assert type_.dimension == 2

        type_ = t.c.geom_no_idx.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == "LINESTRING"
        assert type_.srid == 4326 if dialect_name == "mysql" else -1
        assert type_.dimension == 2

        # Drop the table
        t.drop(bind=conn)

        # Query to check the tables
        query_tables = text(
            """SELECT DISTINCT
                TABLE_NAME
            FROM
                INFORMATION_SCHEMA.COLUMNS
            WHERE
                TABLE_SCHEMA = 'gis'
            ORDER BY TABLE_NAME;"""
        )

        # Query to check the indices
        query_indexes = text(
            """SELECT
                A.TABLE_NAME,
                A.COLUMN_NAME,
                A.DATA_TYPE,
                B.INDEX_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS AS A
            LEFT JOIN INFORMATION_SCHEMA.STATISTICS AS B
            ON A.TABLE_NAME = B.TABLE_NAME
                AND A.COLUMN_NAME = B.COLUMN_NAME
                AND A.TABLE_SCHEMA = B.TABLE_SCHEMA
            WHERE A.TABLE_SCHEMA = 'gis'
            AND A.TABLE_NAME = 'lake'
            ORDER BY TABLE_NAME, COLUMN_NAME;"""
        )

        # Check the indices
        geom_cols = conn.execute(query_indexes).fetchall()
        assert geom_cols == []

        # Check the tables
        all_tables = [i[0] for i in conn.execute(query_tables).fetchall()]
        assert "lake" not in all_tables

        # Recreate the table to check that the reflected properties are correct
        t.create(bind=conn)

        # Check the actual properties
        geom_cols = conn.execute(query_indexes).fetchall()
        assert geom_cols == [
            ("lake", "geom", "linestring", "SPATIAL"),
            ("lake", "geom_no_idx", "linestring", None),
            ("lake", "id", "int", "BTREE"),
        ]

        all_tables = [i[0] for i in conn.execute(query_tables).fetchall()]
        assert "lake" in all_tables
