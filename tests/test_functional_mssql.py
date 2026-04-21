import pytest
from shapely.geometry import LineString
from shapely.geometry import Point
from sqlalchemy import Column
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import inspect
from sqlalchemy.exc import StatementError
from sqlalchemy.sql import func
from sqlalchemy.sql import select
from sqlalchemy.sql import text

from geoalchemy2 import Geography
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

    def test_auto_spatial_index_created(self, conn):
        metadata = MetaData()
        Table(
            "auto_indexed_lake",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("geom", Geometry(geometry_type="LINESTRING", srid=4326)),
        )
        metadata.drop_all(conn, checkfirst=True)
        metadata.create_all(conn)

        indexes = conn.execute(
            text(
                """SELECT i.name, c.name
                FROM sys.indexes AS i
                JOIN sys.index_columns AS ic
                    ON i.object_id = ic.object_id
                    AND i.index_id = ic.index_id
                JOIN sys.columns AS c
                    ON ic.object_id = c.object_id
                    AND ic.column_id = c.column_id
                WHERE
                    i.object_id = OBJECT_ID('auto_indexed_lake')
                    AND i.type_desc = 'SPATIAL'
                ORDER BY i.name"""
            )
        ).fetchall()
        assert indexes == [("idx_auto_indexed_lake_geom", "geom")]

        reflected = Table("auto_indexed_lake", MetaData(), autoload_with=conn)
        assert reflected.c.geom.type.spatial_index is True

        metadata.drop_all(conn, checkfirst=True)

    def test_custom_spatial_index_options_are_reflected_without_duplicates(self, conn):
        metadata = MetaData()
        table = Table(
            "custom_indexed_lake",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "geom",
                Geometry(geometry_type="LINESTRING", srid=4326, spatial_index=False),
            ),
        )
        Index(
            "custom_spatial_idx",
            table.c.geom,
            mssql_using="GEOMETRY_GRID",
            mssql_grids=("HIGH", "HIGH", "HIGH", "HIGH"),
            mssql_cells_per_object=16,
            mssql_bounding_box=(0, 0, 10, 10),
        )
        metadata.drop_all(conn, checkfirst=True)
        metadata.create_all(conn)

        indexes = inspect(conn).get_indexes("custom_indexed_lake")
        spatial_indexes = [idx for idx in indexes if idx["column_names"] == ["geom"]]

        assert [idx["name"] for idx in spatial_indexes] == ["custom_spatial_idx"]
        assert spatial_indexes[0]["dialect_options"]["mssql_using"] == "GEOMETRY_GRID"
        assert spatial_indexes[0]["dialect_options"]["mssql_grids"] == (
            "HIGH",
            "HIGH",
            "HIGH",
            "HIGH",
        )
        assert spatial_indexes[0]["dialect_options"]["mssql_cells_per_object"] == 16
        assert spatial_indexes[0]["dialect_options"]["mssql_bounding_box"] == (
            0.0,
            0.0,
            10.0,
            10.0,
        )

        metadata.drop_all(conn, checkfirst=True)


@test_only_with_dialects("mssql")
class TestInsertionCore:
    @pytest.mark.parametrize("use_executemany", [True, False])
    def test_insert_mssql(self, conn, Lake, setup_tables, use_executemany):
        elements = [
            {"geom": "SRID=4326;LINESTRING(0 0,1 1)"},
            {"geom": "LINESTRING(0 0,1 1)"},
            {"geom": WKTElement("LINESTRING(0 0,2 2)", srid=4326)},
            {"geom": WKTElement("SRID=4326;LINESTRING(0 0,3 3)", extended=True)},
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
        _assert_linestring(conn, rows[4][1], "LINESTRING (0 0, 3 3)")

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

    def test_st_buffer(self, session, Lake, setup_one_lake):
        lake_id = setup_one_lake

        stmt = select(func.ST_Buffer(Lake.__table__.c.geom, 2))
        r1 = session.execute(stmt).scalar()
        assert isinstance(r1, WKBElement)
        assert to_shape(r1).geom_type == "Polygon"

        lake = session.query(Lake).get(lake_id)
        r2 = session.execute(lake.geom.ST_Buffer(2)).scalar()
        assert isinstance(r2, WKBElement)
        assert to_shape(r2).geom_type == "Polygon"

        r3 = session.query(Lake.geom.ST_Buffer(2)).scalar()
        assert isinstance(r3, WKBElement)
        assert to_shape(r3).geom_type == "Polygon"

        assert r1.data == r2.data == r3.data


@test_only_with_dialects("mssql")
class TestSpatialElementExecution:
    def test_execute_wkt_and_wkb_elements(self, conn):
        wkt_elem = WKTElement("POINT(1 2)", srid=4326)
        wkb_elem = from_shape(Point(1, 2), srid=4326)
        ewkb_elem = from_shape(Point(1, 2), srid=4326, extended=True)

        assert conn.execute(select(func.ST_AsText(wkt_elem))).scalar() == "POINT (1 2)"
        assert conn.execute(select(func.ST_SRID(wkt_elem))).scalar() == 4326

        assert conn.execute(select(func.ST_AsText(wkb_elem))).scalar() == "POINT (1 2)"
        assert conn.execute(select(func.ST_SRID(wkb_elem))).scalar() == 4326

        assert conn.execute(select(func.ST_AsText(ewkb_elem))).scalar() == "POINT (1 2)"
        assert conn.execute(select(func.ST_SRID(ewkb_elem))).scalar() == 4326


@test_only_with_dialects("mssql")
class TestGeography:
    def test_insert_and_select_geography(self, conn):
        metadata = MetaData()
        place = Table(
            "place",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("geog", Geography(geometry_type="POINT", srid=4326)),
        )
        metadata.drop_all(conn, checkfirst=True)
        metadata.create_all(conn)

        conn.execute(place.insert().values(geog="POINT(1 2)"))

        result = conn.execute(select(place.c.geog)).scalar_one()
        assert isinstance(result, WKBElement)
        assert result.extended is False
        assert result.srid == 4326
        assert to_shape(result).wkt == "POINT (1 2)"

        assert conn.execute(select(func.ST_AsText(place.c.geog))).scalar_one() == "POINT (1 2)"
        assert conn.execute(select(func.ST_SRID(place.c.geog))).scalar_one() == 4326

        metadata.drop_all(conn, checkfirst=True)


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
        assert type_.geometry_type == "LINESTRING"
        assert type_.srid == 4326
        assert type_.dimension == 2
        assert type_.spatial_index is False

    def test_geography_reflection_uses_schema_constraints(self, conn):
        metadata = MetaData()
        Table(
            "reflection_place",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "geog",
                Geography(geometry_type="POINT", srid=4326, spatial_index=False),
            ),
        )
        metadata.drop_all(conn, checkfirst=True)
        metadata.create_all(conn)

        reflected = Table("reflection_place", MetaData(), autoload_with=conn)
        type_ = reflected.c.geog.type

        assert isinstance(type_, Geography)
        assert type_.geometry_type == "POINT"
        assert type_.srid == 4326
        assert type_.dimension == 2
        assert type_.spatial_index is False

        metadata.drop_all(conn, checkfirst=True)

    def test_bare_spatial_columns_reflect_as_generic(self, conn):
        conn.execute(text("DROP TABLE IF EXISTS bare_spatial_lake"))
        conn.execute(
            text(
                """CREATE TABLE bare_spatial_lake (
                id INT PRIMARY KEY,
                geom geometry NULL,
                geog geography NULL
            )"""
            )
        )

        try:
            reflected = Table("bare_spatial_lake", MetaData(), autoload_with=conn)

            assert isinstance(reflected.c.geom.type, Geometry)
            assert reflected.c.geom.type.geometry_type == "GEOMETRY"
            assert reflected.c.geom.type.srid == -1
            assert isinstance(reflected.c.geog.type, Geography)
            assert reflected.c.geog.type.geometry_type == "GEOMETRY"
            assert reflected.c.geog.type.srid == -1
        finally:
            conn.execute(text("DROP TABLE bare_spatial_lake"))

    def test_reflection_with_manual_spatial_index(self, conn):
        metadata = MetaData()
        Table(
            "reflection_indexed_lake",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "geom",
                Geometry(geometry_type="LINESTRING", srid=4326, spatial_index=False),
            ),
        )
        metadata.drop_all(conn, checkfirst=True)
        metadata.create_all(conn)

        conn.execute(
            text(
                """CREATE SPATIAL INDEX idx_reflection_indexed_lake_geom
                ON reflection_indexed_lake(geom)
                WITH (BOUNDING_BOX = (0, 0, 500, 200))"""
            )
        )

        reflected = Table("reflection_indexed_lake", MetaData(), autoload_with=conn)
        type_ = reflected.c.geom.type

        assert isinstance(type_, Geometry)
        assert type_.spatial_index is True

        metadata.drop_all(conn, checkfirst=True)


@test_only_with_dialects("mssql")
class TestCompileQuery:
    def test_compile_query(self, conn):
        wkb = b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@"
        elem = WKBElement(wkb)
        query = select(func.ST_AsText(elem))
        compiled_with_literal = str(query.compile(conn, compile_kwargs={"literal_binds": True}))
        res_text = conn.execute(text(compiled_with_literal)).scalar()
        assert res_text == "POINT (1 2)"

        compiled_without_literal = str(query.compile(conn, compile_kwargs={"literal_binds": False}))
        res_query = conn.execute(query).scalar()
        assert res_query == "POINT (1 2)"

        assert (
            "geometry::STGeomFromWKB(0x0101000000000000000000f03f0000000000000040"
            in compiled_with_literal
        )
        assert ".AsTextZM()" in compiled_with_literal
        assert "geometry::STGeomFromWKB(" in compiled_without_literal
        assert ".AsTextZM()" in compiled_without_literal
        assert "0101000000000000000000f03f0000000000000040" not in compiled_without_literal
