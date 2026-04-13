import pytest
from shapely.geometry import LineString
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.exc import StatementError
from sqlalchemy.sql import func
from sqlalchemy.sql import select

from geoalchemy2 import Geometry
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import from_shape
from geoalchemy2.shape import to_shape

from . import test_only_with_dialects


def normalize_wkt(value):
    return value.replace(" (", "(").replace(", ", ",")


@pytest.fixture
def Lake(base):
    class Lake(base):
        __tablename__ = "lake"
        id = Column(Integer, primary_key=True)
        geom = Column(Geometry(geometry_type="LINESTRING", srid=4326))

        def __init__(self, geom):
            self.geom = geom

    return Lake


def _assert_linestring(conn, element, expected_wkt):
    assert isinstance(element, WKBElement)
    assert element.extended is False
    assert to_shape(element).wkt == expected_wkt

    wkt = conn.execute(element.ST_AsText()).scalar()
    assert normalize_wkt(wkt).upper() == normalize_wkt(expected_wkt).upper()

    srid = conn.execute(element.ST_SRID()).scalar()
    assert srid == 4326


@test_only_with_dialects("mssql")
class TestAdmin:
    def test_create_drop_tables(self, conn, metadata, Lake):
        metadata.drop_all(conn, checkfirst=True)
        metadata.create_all(conn)
        metadata.drop_all(conn, checkfirst=True)


@test_only_with_dialects("mssql")
class TestInsertionCore:
    @pytest.mark.parametrize("use_executemany", [True, False])
    def test_insert_mssql(self, conn, Lake, setup_tables, use_executemany):
        elements = [
            {"geom": "SRID=4326;LINESTRING(0 0,1 1)"},
            {"geom": "LINESTRING(0 0,1 1)"},
            {"geom": WKTElement("LINESTRING(0 0,2 2)", srid=4326)},
            {"geom": from_shape(LineString([[0, 0], [3, 3]]), srid=4326)},
        ]

        if use_executemany:
            conn.execute(Lake.__table__.insert(), elements)
        else:
            for element in elements:
                conn.execute(Lake.__table__.insert().values(**element))

        rows = conn.execute(Lake.__table__.select().order_by("id")).fetchall()

        _assert_linestring(conn, rows[0][1], "LINESTRING (0 0, 1 1)")
        _assert_linestring(conn, rows[1][1], "LINESTRING (0 0, 1 1)")
        _assert_linestring(conn, rows[2][1], "LINESTRING (0 0, 2 2)")
        _assert_linestring(conn, rows[3][1], "LINESTRING (0 0, 3 3)")

        for row in rows:
            conn.execute(Lake.__table__.insert().values(geom=row[1]))

        conn.execute(Lake.__table__.insert(), [{"geom": row[1]} for row in rows])


@test_only_with_dialects("mssql")
class TestInsertionORM:
    def test_wkt_element(self, session, Lake, setup_tables):
        lake = Lake(WKTElement("LINESTRING(0 0,1 1)", srid=4326))
        session.add(lake)
        session.flush()
        session.expire(lake)

        _assert_linestring(session, lake.geom, "LINESTRING (0 0, 1 1)")

    def test_wkb_element(self, session, Lake, setup_tables):
        lake = Lake(from_shape(LineString([[0, 0], [1, 1]]), srid=4326))
        session.add(lake)
        session.flush()
        session.expire(lake)

        _assert_linestring(session, lake.geom, "LINESTRING (0 0, 1 1)")

    def test_raise_wrong_srid_str(self, session, Lake, setup_tables):
        lake = Lake("SRID=2154;LINESTRING(0 0,1 1)")
        session.add(lake)
        with pytest.raises(StatementError):
            session.flush()

    def test_raise_wrong_srid_wkt_element(self, session, Lake, setup_tables):
        lake = Lake(WKTElement("LINESTRING(0 0,1 1)", srid=2154))
        session.add(lake)
        with pytest.raises(StatementError):
            session.flush()


@test_only_with_dialects("mssql")
class TestCallFunction:
    @pytest.fixture
    def setup_one_lake(self, session, Lake, setup_tables):
        lake = Lake(WKTElement("LINESTRING(0 0,1 1)", srid=4326))
        session.add(lake)
        session.flush()
        session.expire(lake)
        return lake.id

    def test_st_geometrytype(self, session, Lake, setup_one_lake):
        lake_id = setup_one_lake

        stmt = select(func.ST_GeometryType(Lake.__table__.c.geom))
        assert session.execute(stmt).scalar() == "LineString"

        lake = session.query(Lake).get(lake_id)
        assert session.execute(lake.geom.ST_GeometryType()).scalar() == "LineString"
        assert session.query(Lake.geom.ST_GeometryType()).scalar() == "LineString"

        row = session.query(Lake).filter(Lake.geom.ST_GeometryType() == "LineString").one()
        assert row.id == lake_id


@test_only_with_dialects("mssql")
class TestReflection:
    @pytest.fixture
    def setup_reflection_table(self, conn):
        metadata = MetaData()
        Table(
            "reflection_lake",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "geom",
                Geometry(geometry_type="LINESTRING", srid=4326, spatial_index=False),
            ),
        )
        metadata.drop_all(conn, checkfirst=True)
        metadata.create_all(conn)
        yield
        metadata.drop_all(conn, checkfirst=True)

    def test_reflection(self, conn, setup_reflection_table):
        table = Table("reflection_lake", MetaData(), autoload_with=conn)
        type_ = table.c.geom.type

        assert isinstance(type_, Geometry)
        assert type_.geometry_type == "GEOMETRY"
        assert type_.srid == -1
        assert type_.dimension == 2
        assert type_.spatial_index is False
