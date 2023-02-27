from json import loads

import pytest
from pkg_resources import parse_version
from shapely.geometry import LineString
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import __version__ as SA_VERSION
from sqlalchemy import bindparam
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import func
from sqlalchemy.sql import select

from geoalchemy2 import Geometry
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import from_shape
from geoalchemy2.shape import to_shape

from . import test_only_with_dialects


@pytest.fixture
def NotNullableLake(base):
    class NotNullableLake(base):
        __tablename__ = "NotNullablelake"
        id = Column(Integer, primary_key=True)
        geom = Column(
            Geometry(
                geometry_type="LINESTRING",
                srid=4326,
                nullable=False,
            )
        )

        def __init__(self, geom):
            self.geom = geom

    return NotNullableLake


class TestInsertionCore:
    def test_insert(self, conn, NotNullableLake, setup_tables):
        # Issue two inserts using DBAPI's executemany() method. This tests
        # the Geometry type's bind_processor and bind_expression functions.
        conn.execute(
            NotNullableLake.__table__.insert(),
            [
                {"geom": "SRID=4326;LINESTRING(0 0,1 1)"},
                {"geom": "LINESTRING(0 0,1 1)"},
                {"geom": WKTElement("LINESTRING(0 0,2 2)")},
                {"geom": from_shape(LineString([[0, 0], [3, 3]]), srid=4326)},
            ],
        )

        results = conn.execute(NotNullableLake.__table__.select())
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


class TestInsertionORM:
    def test_WKT(self, session, NotNullableLake, setup_tables):
        lake = NotNullableLake("LINESTRING(0 0,1 1)")
        session.add(lake)
        session.flush()

    def test_WKTElement(self, session, NotNullableLake, setup_tables):
        lake = NotNullableLake(WKTElement("LINESTRING(0 0,1 1)", srid=4326))
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

    def test_WKBElement(self, session, NotNullableLake, setup_tables):
        shape = LineString([[0, 0], [1, 1]])
        lake = NotNullableLake(from_shape(shape, srid=4326))
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


class TestShapely:
    def test_to_shape(self, conn, session, NotNullableLake, setup_tables):
        element = WKTElement("LINESTRING(0 0,1 1)", srid=4326)
        lake = NotNullableLake(geom=element)
        session.add(lake)
        session.flush()
        session.expire(lake)
        lake = session.query(NotNullableLake).one()
        assert isinstance(lake.geom, WKBElement)
        assert isinstance(lake.geom.data, bytes)
        assert lake.geom.srid == 4326
        s = to_shape(lake.geom)
        assert isinstance(s, LineString)
        assert s.wkt == "LINESTRING (0 0, 1 1)"

        conn.execute(NotNullableLake.__table__.insert().values(geom="LINESTRING(0 0,1 1)"))

        conn.execute(
            NotNullableLake.__table__.insert(),
            [
                {"geom": "SRID=4326;LINESTRING(0 0,1 1)"},
                {"geom": WKTElement("LINESTRING(0 0,2 2)")},
                {"geom": from_shape(LineString([[0, 0], [3, 3]]), srid=4326)},
            ],
        )

        lake = NotNullableLake(lake.geom)
        session.add(lake)
        session.flush()
        session.expire(lake)
        assert isinstance(lake.geom, WKBElement)
        assert isinstance(lake.geom.data, bytes)
        assert lake.geom.srid == 4326


class TestCallFunction:
    @pytest.fixture
    def setup_one_lake(self, session, NotNullableLake, setup_tables):
        lake = NotNullableLake(WKTElement("LINESTRING(0 0,1 1)", srid=4326))
        session.add(lake)
        session.flush()
        session.expire(lake)
        return lake.id

    def test_ST_GeometryType(self, session, NotNullableLake, setup_one_lake):
        lake_id = setup_one_lake

        s = select(func.ST_GeometryType(NotNullableLake.__table__.c.geom))
        r1 = session.execute(s).scalar()
        assert r1 == "LINESTRING"

        lake = session.query(NotNullableLake).get(lake_id)
        r2 = session.execute(lake.geom.ST_GeometryType()).scalar()
        assert r2 == "LINESTRING"

        r3 = session.query(NotNullableLake.geom.ST_GeometryType()).scalar()
        assert r3 == "LINESTRING"

        r4 = (
            session.query(NotNullableLake)
            .filter(NotNullableLake.geom.ST_GeometryType() == "LINESTRING")
            .one()
        )
        assert isinstance(r4, NotNullableLake)
        assert r4.id == lake_id

    def test_ST_Transform(self, session, NotNullableLake, setup_one_lake):
        lake_id = setup_one_lake

        s = select(func.ST_Transform(NotNullableLake.__table__.c.geom, 2154))
        r1 = session.execute(s).scalar()
        assert isinstance(r1, WKBElement)

        lake = session.query(NotNullableLake).get(lake_id)
        r2 = session.execute(lake.geom.ST_Transform(2154)).scalar()
        assert isinstance(r2, WKBElement)

        r3 = session.query(NotNullableLake.geom.ST_Transform(2154)).scalar()
        assert isinstance(r3, WKBElement)

        assert r1.data == r2.data == r3.data

        r4 = (
            session.query(NotNullableLake)
            .filter(
                func.ST_Distance(
                    WKTElement("POINT(253531 908605)", srid=2154),
                    NotNullableLake.geom.ST_Transform(2154),
                )
                <= 1
            )
            .one()
        )
        assert isinstance(r4, NotNullableLake)
        assert r4.id == lake_id

    def test_ST_GeoJSON(self, session, NotNullableLake, setup_one_lake):
        lake_id = setup_one_lake

        def _test(r):
            r = loads(r)
            assert r["type"] == "LineString"
            assert r["coordinates"] == [[0, 0], [1, 1]]

        s = select(func.ST_AsGeoJSON(NotNullableLake.__table__.c.geom))
        r = session.execute(s).scalar()
        _test(r)

        lake = session.query(NotNullableLake).get(lake_id)
        r = session.execute(lake.geom.ST_AsGeoJSON()).scalar()
        _test(r)

        r = session.query(NotNullableLake.geom.ST_AsGeoJSON()).scalar()
        _test(r)

    @pytest.mark.skipif(
        True, reason="MySQL does not support the feature version of AsGeoJson() yet"
    )
    def test_ST_GeoJSON_feature(self, session, NotNullableLake, setup_tables):
        ss3 = select(NotNullableLake, bindparam("dummy_val", 10).label("dummy_attr")).alias()
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
    def test_comparator_case_insensitivity(self, session, NotNullableLake, setup_one_lake):
        lake_id = setup_one_lake

        s = select(func.ST_Transform(NotNullableLake.__table__.c.geom, 2154))
        r1 = session.execute(s).scalar()
        assert isinstance(r1, WKBElement)

        lake = session.query(NotNullableLake).get(lake_id)

        r2 = session.execute(lake.geom.ST_Transform(2154)).scalar()
        assert isinstance(r2, WKBElement)

        r3 = session.execute(lake.geom.st_transform(2154)).scalar()
        assert isinstance(r3, WKBElement)

        r4 = session.execute(lake.geom.St_TrAnSfOrM(2154)).scalar()
        assert isinstance(r4, WKBElement)

        r5 = session.query(NotNullableLake.geom.ST_Transform(2154)).scalar()
        assert isinstance(r5, WKBElement)

        r6 = session.query(NotNullableLake.geom.st_transform(2154)).scalar()
        assert isinstance(r6, WKBElement)

        r7 = session.query(NotNullableLake.geom.St_TrAnSfOrM(2154)).scalar()
        assert isinstance(r7, WKBElement)

        assert r1.data == r2.data == r3.data == r4.data == r5.data == r6.data == r7.data


class TestNullable:
    @test_only_with_dialects("mysql")
    def test_insert(self, conn, NotNullableLake, setup_tables):
        # Insert geometries
        conn.execute(
            NotNullableLake.__table__.insert(),
            [
                {"geom": "SRID=4326;LINESTRING(0 0,1 1)"},
                {"geom": WKTElement("LINESTRING(0 0,2 2)", srid=4326)},
                {"geom": from_shape(LineString([[0, 0], [3, 3]]), srid=4326)},
            ],
        )

        # Fail when trying to insert null geometry
        with pytest.raises(OperationalError):
            conn.execute(NotNullableLake.__table__.insert(), [{"geom": None}])
