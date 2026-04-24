import math
import re
import struct

import pytest
from shapely.geometry import LineString
from shapely.geometry import Point
from sqlalchemy import Column
from sqlalchemy import Computed
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import bindparam
from sqlalchemy import null
from sqlalchemy.dialects import mssql
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import func
from sqlalchemy.sql import insert
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.types import TypeDecorator

from geoalchemy2.admin import select_dialect as select_admin_dialect
from geoalchemy2.admin.dialects import mssql as mssql_admin
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.exc import ArgumentError
from geoalchemy2.shape import from_shape
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry
from geoalchemy2.types import select_dialect as select_type_dialect
from geoalchemy2.types.dialects import mssql as mssql_type

from . import select


def normalize_sql(sql):
    return re.sub(r"\s+", " ", str(sql)).strip()


def _pack_iso_wkb(type_code, payload, *, has_z=False, has_m=False):
    dimension_code = 3000 if has_z and has_m else 2000 if has_m else 1000 if has_z else 0
    return b"\x01" + struct.pack("<I", type_code + dimension_code) + payload


def _pack_coords(*coords):
    return struct.pack(f"<{'d' * len(coords)}", *coords)


def _pack_polygon(rings):
    payload = bytearray(struct.pack("<I", len(rings)))
    for ring in rings:
        payload.extend(struct.pack("<I", len(ring)))
        for point in ring:
            payload.extend(_pack_coords(*point))
    return bytes(payload)


def _pack_collection(*geometries):
    payload = bytearray(struct.pack("<I", len(geometries)))
    for geometry in geometries:
        payload.extend(geometry)
    return bytes(payload)


def _pack_multipolygon(polygons, *, has_z=False, has_m=False):
    payload = bytearray(struct.pack("<I", len(polygons)))
    for polygon in polygons:
        payload.extend(_pack_iso_wkb(3, _pack_polygon(polygon), has_z=has_z, has_m=has_m))
    return _pack_iso_wkb(6, bytes(payload), has_z=has_z, has_m=has_m)


@pytest.fixture
def geometry_table():
    return Table(
        "lake",
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("geom", Geometry(geometry_type="LINESTRING", srid=4326)),
    )


@pytest.fixture
def geography_table():
    return Table(
        "place",
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("geog", Geography(geometry_type="POINT", srid=4326)),
    )


class WrappedGeometry(TypeDecorator):
    impl = Geometry
    cache_ok = True


class DynamicDefaultSRIDGeometry(TypeDecorator):
    impl = Geometry
    cache_ok = True
    name = "geometry"

    def __init__(self, srid):
        super().__init__()
        self.srid = srid


class WrappedGeography(TypeDecorator):
    impl = Geography
    cache_ok = True

    def load_dialect_impl(self, dialect):
        return Geography(geometry_type="POINT", srid=4326)


class WrappedInteger(TypeDecorator):
    impl = Integer
    cache_ok = True


class TestMSSQLDialectRegistration:
    def test_type_select_dialect(self):
        assert select_type_dialect("mssql").__name__ == "geoalchemy2.types.dialects.mssql"

    def test_admin_select_dialect(self):
        assert select_admin_dialect("mssql").__name__ == "geoalchemy2.admin.dialects.mssql"


class TestMSSQLCompilation:
    dialect = mssql.dialect()
    qmark_dialect = mssql.pyodbc.dialect(paramstyle="qmark")

    def test_create_table_uses_bare_geometry_type(self, geometry_table):
        compiled = normalize_sql(CreateTable(geometry_table).compile(dialect=self.dialect))
        compiled_lower = compiled.lower()
        assert "geometry(linestring,4326)" not in compiled_lower
        assert re.search(r"geom\]?\s+geometry", compiled_lower)

    def test_column_expression_uses_stasbinary(self, geometry_table):
        stmt = select([geometry_table.c.geom])
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))
        assert ".AsBinaryZM()" in compiled
        assert "ST_AsEWKB" not in compiled

    def test_insert_bind_expression_uses_stgeomfromtext(self, geometry_table):
        stmt = insert(geometry_table).values(geom="POINT(1 2)")
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))
        assert "geometry::STGeomFromText" in compiled
        assert "ST_GeomFromEWKT" not in compiled

    def test_geography_insert_bind_expression_uses_stgeomfromtext(self, geography_table):
        stmt = insert(geography_table).values(geog="POINT(1 2)")
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))
        assert "geography::STGeomFromText" in compiled
        assert "ST_GeogFromText" not in compiled

    def test_functions_compile_to_mssql_methods(self, geometry_table):
        stmt = select(
            [
                geometry_table.c.geom.ST_Buffer(2),
                geometry_table.c.geom.ST_Area(),
                geometry_table.c.geom.ST_Length(),
                geometry_table.c.geom.ST_Intersects(WKTElement("LINESTRING(0 0,1 1)", srid=4326)),
                geometry_table.c.geom.ST_GeometryType(),
                geometry_table.c.geom.ST_SRID(),
                geometry_table.c.geom.ST_AsText(),
            ]
        )
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))
        assert ".STBuffer(" in compiled
        assert ".STArea()" in compiled
        assert ".STLength()" in compiled
        assert ".STIntersects(" in compiled
        assert ".STGeometryType()" in compiled
        assert ".STSrid" in compiled
        assert ".AsTextZM()" in compiled

    def test_func_area_and_intersects_compile_to_mssql_methods(self, geometry_table):
        stmt = select(
            [
                func.ST_Area(geometry_table.c.geom),
                func.ST_Length(geometry_table.c.geom),
                func.ST_Intersects(
                    geometry_table.c.geom,
                    WKTElement("LINESTRING(0 0,1 1)", srid=4326),
                ),
            ]
        )
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))

        assert ".STArea()" in compiled
        assert ".STLength()" in compiled
        assert ".STIntersects(" in compiled
        assert "ST_Area(" not in compiled
        assert "ST_Length(" not in compiled
        assert "ST_Intersects(" not in compiled

    def test_geography_binary_predicate_coerces_wkt_argument_to_geography(self, geography_table):
        stmt = select(
            [
                geography_table.c.geog.ST_Intersects(
                    WKTElement("POINT(1 2)", srid=4326),
                ),
            ]
        )
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))

        assert ".STIntersects(geography::STGeomFromText" in compiled
        assert "geometry::STGeomFromText" not in compiled

    def test_typedecorator_geometry_functions_compile_to_mssql_methods(self):
        table = Table(
            "lake",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("geom", WrappedGeometry(geometry_type="LINESTRING", srid=4326)),
        )
        stmt = select([func.ST_Area(table.c.geom)])
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))

        assert ".STArea()" in compiled
        assert "ST_Area(" not in compiled

    def test_non_spatial_function_arguments_keep_function_syntax(self):
        table = Table(
            "numbers",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("value", Integer),
        )
        stmt = select(
            [
                func.ST_Area(table.c.value),
                func.ST_Length(table.c.value),
                func.ST_Intersects(table.c.value, table.c.id),
            ]
        )
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))

        assert "ST_Area(" in compiled
        assert "ST_Length(" in compiled
        assert "ST_Intersects(" in compiled
        assert ".STArea(" not in compiled
        assert ".STLength(" not in compiled
        assert ".STIntersects(" not in compiled

    def test_non_spatial_typedecorator_function_arguments_keep_function_syntax(self):
        table = Table(
            "numbers",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("value", WrappedInteger()),
        )
        stmt = select([func.ST_Area(table.c.value)])
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))

        assert "ST_Area(" in compiled
        assert ".STArea(" not in compiled

    def test_extended_wkt_element_method_call_strips_srid_prefix(self):
        stmt = select([WKTElement("SRID=4326;POINT(1 2)", extended=True).ST_AsText()])
        compiled = normalize_sql(
            stmt.compile(dialect=self.dialect, compile_kwargs={"literal_binds": True})
        )
        assert re.search(r"geometry::STGeomFromText\(N?'POINT\(1 2\)', 4326\)", compiled)
        assert "SRID=" not in compiled

    def test_wkb_element_method_call_uses_stgeomfromwkb(self):
        wkb = bytes.fromhex("0101000000000000000000f03f0000000000000040")
        stmt = select([WKBElement(wkb, srid=4326).ST_AsText()])
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))
        assert "geometry::STGeomFromWKB" in compiled
        assert ".AsTextZM()" in compiled

    def test_constructor_wkt_element_argument_does_not_nest_mssql_constructor(self):
        expr = func.ST_GeomFromEWKT(WKTElement("POINT(1 2)", srid=4326))
        compiled = normalize_sql(
            expr.compile(dialect=self.dialect, compile_kwargs={"literal_binds": True})
        )

        assert compiled == "geometry::STGeomFromText(N'POINT(1 2)', 4326)"
        assert "STGeomFromText(geometry::STGeomFromText" not in compiled

    def test_constructor_extended_wkt_element_argument_does_not_nest_mssql_constructor(self):
        expr = func.ST_GeomFromEWKT(WKTElement("SRID=3857;POINT(1 2)", extended=True))
        compiled = normalize_sql(
            expr.compile(dialect=self.dialect, compile_kwargs={"literal_binds": True})
        )

        assert compiled == "geometry::STGeomFromText(N'POINT(1 2)', 3857)"
        assert "SRID=" not in compiled
        assert "STGeomFromText(geometry::STGeomFromText" not in compiled

    def test_constructor_geography_wkt_element_argument_uses_geography_constructor(self):
        expr = func.ST_GeogFromText(WKTElement("POINT(1 2)", srid=4326))
        compiled = normalize_sql(
            expr.compile(dialect=self.dialect, compile_kwargs={"literal_binds": True})
        )

        assert compiled == "geography::STGeomFromText(N'POINT(1 2)', 4326)"
        assert "geometry::STGeomFromText" not in compiled
        assert "STGeomFromText(geometry::STGeomFromText" not in compiled

    def test_constructor_wkb_element_argument_does_not_nest_mssql_constructor(self):
        wkb = bytes.fromhex("0101000000000000000000f03f0000000000000040")
        expected = "geometry::STGeomFromWKB(0x0101000000000000000000f03f0000000000000040, 4326)"
        expr = func.ST_GeomFromWKB(WKBElement(wkb, srid=4326))
        compiled = normalize_sql(
            expr.compile(dialect=self.dialect, compile_kwargs={"literal_binds": True})
        )

        assert compiled == expected
        assert "STGeomFromWKB(geometry::STGeomFromWKB" not in compiled

    def test_constructor_ewkb_element_argument_does_not_nest_mssql_constructor(self):
        wkb = bytes.fromhex("0101000000000000000000f03f0000000000000040")
        expected = "geometry::STGeomFromWKB(0x0101000000000000000000f03f0000000000000040, 4326)"
        expr = func.ST_GeomFromEWKB(WKBElement(wkb, srid=4326).as_ewkb())
        compiled = normalize_sql(
            expr.compile(dialect=self.dialect, compile_kwargs={"literal_binds": True})
        )

        assert compiled == expected
        assert "STGeomFromWKB(geometry::STGeomFromWKB" not in compiled

    def test_as_ewkb_compiles_to_ewkb_expression(self, geometry_table):
        stmt = select([geometry_table.c.geom.ST_AsEWKB()])
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))
        assert ".AsBinaryZM()" in compiled
        assert ".STSrid" in compiled
        assert "536870912" in compiled
        assert "ST_AsEWKB(" not in compiled

    def test_as_ewkt_compiles_to_srid_prefixed_astextzm(self, geometry_table):
        stmt = select([geometry_table.c.geom.ST_AsEWKT()])
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))
        assert "CONCAT('SRID='" in compiled
        assert ".AsTextZM()" in compiled
        assert ".STSrid" in compiled
        assert "ST_AsEWKT(" not in compiled

    def test_geometry_returning_buffer_ewkb_qmark_bind_positions_match_markers(
        self, geometry_table
    ):
        stmt = select([geometry_table.c.geom.ST_Buffer(2)])
        compiled = stmt.compile(dialect=self.qmark_dialect)
        sql = str(compiled)

        assert sql.count("?") == len(compiled.positiontup or [])
        assert sql.count(".STBuffer(?)") == 1

    def test_explicit_buffer_ewkt_qmark_bind_positions_match_markers(self, geometry_table):
        stmt = select([func.ST_AsEWKT(geometry_table.c.geom.ST_Buffer(2))])
        compiled = stmt.compile(dialect=self.qmark_dialect)
        sql = str(compiled)

        assert sql.count("?") == len(compiled.positiontup or [])
        assert sql.count(".STBuffer(?)") == 1

    def test_binary_predicates_compile_to_mssql_methods(self, geometry_table):
        stmt = select(
            [
                geometry_table.c.geom.ST_Equals(WKTElement("LINESTRING(0 0,1 1)", srid=4326)),
                geometry_table.c.geom.ST_Within(
                    WKTElement("POLYGON((0 0,2 0,2 2,0 0))", srid=4326)
                ),
            ]
        )
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))
        assert ".STEquals(" in compiled
        assert ".STWithin(" in compiled
        assert "ST_Equals(" not in compiled
        assert "ST_Within(" not in compiled

    def test_common_binary_predicates_compile_to_mssql_methods(self, geometry_table):
        other = WKTElement("LINESTRING(0 0,1 1)", srid=4326)
        stmt = select(
            [
                geometry_table.c.geom.ST_Contains(other),
                geometry_table.c.geom.ST_Disjoint(other),
                geometry_table.c.geom.ST_Touches(other),
                geometry_table.c.geom.ST_Overlaps(other),
                geometry_table.c.geom.ST_Crosses(other),
            ]
        )
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))

        assert ".STContains(" in compiled
        assert ".STDisjoint(" in compiled
        assert ".STTouches(" in compiled
        assert ".STOverlaps(" in compiled
        assert ".STCrosses(" in compiled
        assert "ST_Contains(" not in compiled
        assert "ST_Disjoint(" not in compiled
        assert "ST_Touches(" not in compiled
        assert "ST_Overlaps(" not in compiled
        assert "ST_Crosses(" not in compiled

    def test_distance_functions_compile_to_mssql_methods(self, geometry_table):
        other = WKTElement("LINESTRING(0 0,1 1)", srid=4326)
        stmt = select(
            [
                geometry_table.c.geom.ST_Distance(other),
                geometry_table.c.geom.ST_DWithin(other, 2),
            ]
        )
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))

        assert ".STDistance(" in compiled
        assert "CASE WHEN" in compiled
        assert "<=" in compiled
        assert "ST_Distance(" not in compiled
        assert "ST_DWithin(" not in compiled

    def test_geography_from_wkb_compiles_to_stgeomfromwkb(self):
        wkb = bytes.fromhex("0101000000000000000000f03f0000000000000040")
        expected = "geography::STGeomFromWKB(0x0101000000000000000000f03f0000000000000040, 4326)"
        expr = func.ST_GeogFromWKB(WKBElement(wkb, srid=4326))
        compiled = normalize_sql(
            expr.compile(dialect=self.dialect, compile_kwargs={"literal_binds": True})
        )

        assert compiled == expected
        assert "geometry::STGeomFromWKB" not in compiled
        assert "STGeomFromWKB(geometry::STGeomFromWKB" not in compiled

    def test_negative_srid_literal_compiles_as_zero(self):
        stmt = select([func.ST_GeomFromText("POINT(1 2)", -1)])
        compiled = normalize_sql(
            stmt.compile(dialect=self.dialect, compile_kwargs={"literal_binds": True})
        )

        assert "geometry::STGeomFromText(N'POINT(1 2)', 0)" in compiled

    def test_column_srid_argument_is_kept_when_compiling_from_text(self):
        table = Table(
            "raw_wkt",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("wkt", Integer),
            Column("srid", Integer),
        )
        stmt = select([func.ST_GeomFromText(table.c.wkt, table.c.srid)])
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))

        assert "geometry::STGeomFromText(raw_wkt.wkt, raw_wkt.srid)" in compiled

    def test_geometry_equality_compiles_to_stequals_predicate(self, geometry_table):
        stmt = select([geometry_table.c.id]).where(geometry_table.c.geom == bindparam("geom"))
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))
        assert ".STEquals(" in compiled
        assert "= geometry::STGeomFromText" not in compiled

    def test_geography_equality_compiles_to_stequals_predicate(self, geography_table):
        stmt = select([geography_table.c.id]).where(geography_table.c.geog == bindparam("geog"))
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))
        assert ".STEquals(" in compiled
        assert "= geography::STGeomFromText" not in compiled

    def test_typedecorator_geometry_equality_compiles_to_stequals_predicate(self):
        table = Table(
            "lake",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("geom", WrappedGeometry(geometry_type="LINESTRING", srid=4326)),
        )
        stmt = select([table.c.id]).where(table.c.geom == bindparam("geom"))
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))

        assert ".STEquals(" in compiled
        assert "= geometry::STGeomFromText" not in compiled

    def test_typedecorator_geometry_inequality_compiles_to_stequals_predicate(self):
        table = Table(
            "lake",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("geom", WrappedGeometry(geometry_type="LINESTRING", srid=4326)),
        )
        stmt = select([table.c.id]).where(table.c.geom != bindparam("geom"))
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))

        assert ".STEquals(" in compiled
        assert "= 0" in compiled

    def test_geometry_equality_handles_spatial_expression_on_right(self, geometry_table):
        stmt = select([geometry_table.c.id]).where(bindparam("geom") == geometry_table.c.geom)
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))

        assert "lake.geom.STEquals(" in compiled
        assert "= 1" in compiled

    def test_geometry_null_equality_compiles_to_is_null(self, geometry_table):
        stmt = select([geometry_table.c.id]).where(geometry_table.c.geom == null())
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))

        assert "lake.geom IS NULL" in compiled
        assert ".STEquals(NULL)" not in compiled

    def test_geometry_null_inequality_compiles_to_is_not_null(self, geometry_table):
        stmt = select([geometry_table.c.id]).where(geometry_table.c.geom != null())
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))

        assert "lake.geom IS NOT NULL" in compiled
        assert ".STEquals(NULL)" not in compiled

    def test_geometry_reversed_null_equality_compiles_to_is_null(self, geometry_table):
        stmt = select([geometry_table.c.id]).where(null() == geometry_table.c.geom)
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))

        assert "lake.geom IS NULL" in compiled
        assert ".STEquals(NULL)" not in compiled

    def test_geometry_reversed_null_inequality_compiles_to_is_not_null(self, geometry_table):
        stmt = select([geometry_table.c.id]).where(null() != geometry_table.c.geom)
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))

        assert "lake.geom IS NOT NULL" in compiled
        assert ".STEquals(NULL)" not in compiled

    def test_geometry_equality_between_two_spatial_columns_does_not_type_coerce(self):
        table = Table(
            "lake",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("geom1", Geometry(geometry_type="LINESTRING", srid=4326)),
            Column("geom2", Geometry(geometry_type="LINESTRING", srid=4326)),
        )
        stmt = select([table.c.id]).where(table.c.geom1 == table.c.geom2)
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))

        assert "lake.geom1.STEquals(lake.geom2) = 1" in compiled

    def test_extended_wkb_literal_uses_plain_wkb_hex(self):
        elem = from_shape(Point(1, 2), srid=4326, extended=True)
        stmt = select([elem.ST_AsText()])
        compiled = normalize_sql(
            stmt.compile(dialect=self.dialect, compile_kwargs={"literal_binds": True})
        )
        assert elem.desc not in compiled
        assert elem.as_wkb().desc in compiled
        assert "geometry::STGeomFromWKB" in compiled
        assert ".AsTextZM()" in compiled

    def test_extended_wkb_method_call_keeps_srid_without_literal_binds(self):
        elem = from_shape(Point(1, 2), srid=4326, extended=True)
        stmt = select([elem.ST_SRID()])
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))
        assert "geometry::STGeomFromWKB(" in compiled
        assert ", 4326).STSrid" in compiled

    def test_insert_coerces_spatial_elements_to_dbapi_friendly_values(self, geometry_table):
        stmt_wkt = insert(geometry_table).values(geom=WKTElement("LINESTRING(0 0,2 2)", srid=4326))
        compiled_wkt = stmt_wkt.compile(dialect=self.dialect)
        processor_wkt = next(iter(compiled_wkt._bind_processors.values()))
        assert processor_wkt(next(iter(compiled_wkt.params.values()))) == "LINESTRING(0 0,2 2)"

        stmt_wkb = insert(geometry_table).values(
            geom=from_shape(LineString([[0, 0], [3, 3]]), srid=4326)
        )
        compiled_wkb = stmt_wkb.compile(dialect=self.dialect)
        processor_wkb = next(iter(compiled_wkb._bind_processors.values()))
        assert processor_wkb(next(iter(compiled_wkb.params.values()))) == "LINESTRING (0 0, 3 3)"

    def test_insert_bind_processor_validates_wrong_srid_string(self, geometry_table):
        stmt = insert(geometry_table).values(geom="SRID=2154;LINESTRING(0 0,1 1)")
        compiled = stmt.compile(dialect=self.dialect)
        processor = next(iter(compiled._bind_processors.values()))

        with pytest.raises(ArgumentError, match=r"column \(4326\)"):
            processor(next(iter(compiled.params.values())))

    def test_insert_bind_processor_validates_wrong_srid_wkt_element(self, geometry_table):
        stmt = insert(geometry_table).values(
            geom=WKTElement("LINESTRING(0 0,1 1)", srid=2154),
        )
        compiled = stmt.compile(dialect=self.dialect)
        processor = next(iter(compiled._bind_processors.values()))

        with pytest.raises(ArgumentError, match=r"column \(4326\)"):
            processor(next(iter(compiled.params.values())))

    def test_computed_geography_point_rewrites_argument_order(self):
        table = Table(
            "computed_place",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("longitude", Integer),
            Column("latitude", Integer),
            Column(
                "geog",
                Geography(geometry_type="POINT", srid=4326),
                Computed("ST_POINT(longitude, latitude)", persisted=True),
            ),
        )
        compiled = normalize_sql(CreateTable(table).compile(dialect=self.dialect))
        assert "geography::Point(latitude, longitude, 4326)" in compiled

    def test_computed_geography_point_rewrites_nested_comma_argument(self):
        table = Table(
            "computed_place",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("longitude", Integer),
            Column("latitude", Integer),
            Column(
                "geog",
                Geography(geometry_type="POINT", srid=4326),
                Computed("ST_POINT(longitude, COALESCE(latitude, 0))", persisted=True),
            ),
        )
        compiled = normalize_sql(CreateTable(table).compile(dialect=self.dialect))
        assert "geography::Point(COALESCE(latitude, 0), longitude, 4326)" in compiled

    def test_computed_point_rewrites_typedecorator_wrapped_geography(self):
        table = Table(
            "computed_place",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("longitude", Integer),
            Column("latitude", Integer),
            Column(
                "geog",
                WrappedGeography(),
                Computed("ST_POINT(longitude, latitude)", persisted=True),
            ),
        )
        compiled = normalize_sql(CreateTable(table).compile(dialect=self.dialect))
        assert "geography::Point(latitude, longitude, 4326)" in compiled

    def test_mssql_spatial_index_kwargs_are_accepted(self, geometry_table):
        idx = Index(
            "custom_spatial_idx",
            geometry_table.c.geom,
            mssql_using="GEOMETRY_GRID",
            mssql_grids=("HIGH", "HIGH", "HIGH", "HIGH"),
            mssql_cells_per_object=16,
            mssql_bounding_box=(0, 0, 10, 10),
        )
        assert idx.kwargs["mssql_using"] == "GEOMETRY_GRID"
        assert idx.kwargs["mssql_cells_per_object"] == 16

    def test_mssql_after_create_recreates_non_single_spatial_indexes(self, monkeypatch):
        table = Table(
            "lake",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("geom", Geometry(geometry_type="POINT", srid=4326, spatial_index=False)),
        )
        idx = Index("ix_lake_id_geom", table.c.id, table.c.geom)
        bind = type("Bind", (), {"dialect": self.dialect})()
        created_indexes = []
        spatial_indexes = []

        def create_index(index, bind=None, **kw):
            created_indexes.append((index, bind, kw))

        monkeypatch.setattr(Index, "create", create_index)
        monkeypatch.setattr(mssql_admin, "create_spatial_constraints", lambda *args, **kw: None)
        monkeypatch.setattr(
            mssql_admin,
            "create_spatial_index",
            lambda *args, **kw: spatial_indexes.append((args, kw)),
        )

        mssql_admin.before_create(table, bind)

        assert idx not in table.indexes
        assert table.info["_after_create_indexes"] == [idx]

        mssql_admin.after_create(table, bind)

        assert idx in table.indexes
        assert created_indexes == [(idx, bind, {})]
        assert spatial_indexes == []

    def test_mssql_after_create_recreates_generated_name_custom_indexes(self, monkeypatch):
        table = Table(
            "lake",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("geom", Geometry(geometry_type="POINT", srid=4326)),
        )
        idx = Index("idx_lake_geom", table.c.id, table.c.geom)
        bind = type("Bind", (), {"dialect": self.dialect})()
        created_indexes = []
        spatial_indexes = []

        def create_index(index, bind=None, **kw):
            created_indexes.append((index, bind, kw))

        monkeypatch.setattr(Index, "create", create_index)
        monkeypatch.setattr(mssql_admin, "create_spatial_constraints", lambda *args, **kw: None)
        monkeypatch.setattr(
            mssql_admin,
            "create_spatial_index",
            lambda *args, **kw: spatial_indexes.append((args, kw)),
        )

        mssql_admin.before_create(table, bind)

        assert idx not in table.indexes
        assert table.info["_after_create_indexes"] == [idx]

        mssql_admin.after_create(table, bind)

        assert idx in table.indexes
        assert created_indexes == [(idx, bind, {})]
        assert spatial_indexes == []

    def test_mssql_after_create_uses_spatial_ddl_for_custom_single_spatial_index(
        self,
        monkeypatch,
    ):
        table = Table(
            "lake",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("geom", Geometry(geometry_type="POINT", srid=4326)),
        )
        Index(
            "custom_spatial_idx",
            table.c.geom,
            mssql_using="GEOMETRY_GRID",
        )
        bind = type("Bind", (), {"dialect": self.dialect})()
        created_indexes = []
        spatial_indexes = []

        def create_index(index, bind=None, **kw):
            created_indexes.append((index, bind, kw))

        def create_spatial_index(bind, table_name, column_name, col_type, **kw):
            spatial_indexes.append((bind, table_name, column_name, col_type, kw))

        monkeypatch.setattr(Index, "create", create_index)
        monkeypatch.setattr(mssql_admin, "create_spatial_constraints", lambda *args, **kw: None)
        monkeypatch.setattr(mssql_admin, "create_spatial_index", create_spatial_index)

        mssql_admin.before_create(table, bind)
        mssql_admin.after_create(table, bind)

        assert created_indexes == []
        assert len(spatial_indexes) == 1
        assert spatial_indexes[0][1:3] == ("lake", "geom")
        assert spatial_indexes[0][4] == {
            "schema": None,
            "index_name": "custom_spatial_idx",
            "mssql_using": "GEOMETRY_GRID",
        }


class TestMSSQLReflectionHelpers:
    def test_helper_formats_identifiers_geometry_types_and_index_clauses(self):
        geom_4326 = Geometry(geometry_type="LINESTRING", srid=4326)
        geom_3857 = Geometry(geometry_type="LINESTRING", srid=3857)

        assert mssql_admin._quote_mssql_identifier("schema]name") == "[schema]]name]"
        assert mssql_admin._quote_mssql_string("O'Brien") == "N'O''Brien'"
        assert mssql_admin._quote_mssql_table_name("lake", schema="gis") == "[gis].[lake]"
        assert mssql_admin._get_mssql_full_table_name("lake", schema="gis") == "gis.lake"
        assert mssql_admin._base_mssql_geometry_type(None) is None
        assert mssql_admin._base_mssql_geometry_type("POINTZM") == "POINT"
        assert mssql_admin._mssql_geometry_type_constraint_value(None) is None
        assert mssql_admin._mssql_geometry_type_constraint_value("GEOMETRY") is None
        assert mssql_admin._mssql_geometry_type_constraint_value("LINESTRING") == "LineString"
        assert mssql_admin._mssql_geometry_type_constraint_prefix("GEOMETRY") is None
        assert mssql_admin._mssql_geometry_type_constraint_prefix("LINESTRINGM") == "LINESTRING"
        assert mssql_admin._format_mssql_bounding_box([0, 0, 1, 1]) == "0, 0, 1, 1"
        assert mssql_admin._default_mssql_bounding_box(geom_4326) == (
            -180.0,
            -90.0,
            180.0,
            90.0,
        )
        assert mssql_admin._default_mssql_bounding_box(geom_3857) == (
            -1000000000.0,
            -1000000000.0,
            1000000000.0,
            1000000000.0,
        )
        assert mssql_admin._default_mssql_bounding_box(geom_4326, is_geography=True) is None

        clauses = mssql_admin._get_mssql_spatial_index_with_clauses(
            geom_3857,
            {
                "mssql_with": ["PAD_INDEX = OFF"],
                "mssql_grids": "HIGH, MEDIUM, LOW, LOW",
                "mssql_cells_per_object": 16,
                "mssql_bounding_box": "1, 2, 3, 4",
            },
        )
        assert clauses == [
            "BOUNDING_BOX = (1, 2, 3, 4)",
            "PAD_INDEX = OFF",
            "GRIDS = (HIGH, MEDIUM, LOW, LOW)",
            "CELLS_PER_OBJECT = 16",
        ]

        clauses = mssql_admin._get_mssql_spatial_index_with_clauses(
            geom_3857,
            {
                "mssql_with": "BOUNDING_BOX = (0, 0, 1, 1)",
                "mssql_grids": "GRIDS = (LOW, LOW, LOW, LOW)",
            },
        )
        assert clauses == [
            "BOUNDING_BOX = (0, 0, 1, 1)",
            "GRIDS = (LOW, LOW, LOW, LOW)",
        ]

        expected_bounding_box_error = "xmin, ymin, xmax, ymax"

        with pytest.raises(ArgumentError, match=expected_bounding_box_error):
            mssql_admin._get_mssql_spatial_index_with_clauses(
                geom_3857,
                {"mssql_bounding_box": "BOUNDING_BOX = (0, 0, 1, 1)"},
            )
        with pytest.raises(ArgumentError, match=expected_bounding_box_error):
            mssql_admin._get_mssql_spatial_index_with_clauses(
                geom_3857,
                {"mssql_bounding_box": "0, 0, 1"},
            )
        with pytest.raises(ArgumentError, match=expected_bounding_box_error):
            mssql_admin._get_mssql_spatial_index_with_clauses(
                geom_3857,
                {"mssql_bounding_box": "0, 0, east, 1"},
            )

    def test_get_mssql_spatial_column_constraints_parses_known_constraint_shapes(self):
        class Result:
            def scalars(self):
                return iter(
                    [
                        "([geom].[STSrid]=(3857))",
                        "([geom].[STGeometryType]()=N'LineString')",
                        "(UPPER([other].[AsTextZM]()) LIKE N'POINT%')",
                    ]
                )

        class Bind:
            def execute(self, *args, **kwargs):
                return Result()

        assert mssql_admin._get_mssql_spatial_column_constraints(Bind(), "lake", "geom") == (
            "LINESTRING",
            3857,
        )

    def test_get_mssql_spatial_column_constraints_parses_astextzm_prefix(self):
        class Result:
            def scalars(self):
                return iter(["(UPPER([geom].[AsTextZM]()) LIKE N'MULTIPOLYGON%')"])

        class Bind:
            def execute(self, *args, **kwargs):
                return Result()

        assert mssql_admin._get_mssql_spatial_column_constraints(Bind(), "lake", "geom") == (
            "MULTIPOLYGON",
            -1,
        )

    def test_get_mssql_spatial_indexes_maps_reflected_options(self):
        class Result:
            def mappings(self):
                return iter(
                    [
                        {
                            "index_name": "idx_lake_geom",
                            "column_name": "geom",
                            "tessellation_scheme": "GEOMETRY_GRID",
                            "cells_per_object": 16,
                            "bounding_box_xmin": 0.0,
                            "bounding_box_ymin": 1.0,
                            "bounding_box_xmax": 2.0,
                            "bounding_box_ymax": 3.0,
                            "level_1_grid_desc": "HIGH",
                            "level_2_grid_desc": "MEDIUM",
                            "level_3_grid_desc": "LOW",
                            "level_4_grid_desc": "LOW",
                        }
                    ]
                )

        class Bind:
            def execute(self, statement, params):
                assert params == {"full_table_name": "gis.lake"}
                return Result()

        assert mssql_admin._get_mssql_spatial_indexes(Bind(), "lake", schema="gis") == [
            {
                "name": "idx_lake_geom",
                "column_name": "geom",
                "dialect_options": {
                    "mssql_using": "GEOMETRY_GRID",
                    "mssql_cells_per_object": 16,
                    "mssql_bounding_box": (0.0, 1.0, 2.0, 3.0),
                    "mssql_grids": ("HIGH", "MEDIUM", "LOW", "LOW"),
                },
            }
        ]

    def test_create_spatial_index_uses_reflected_type_for_unknown_column_type(self):
        class TypeResult:
            def scalar(self):
                return "geography"

        class Bind:
            dialect = mssql.dialect()

            def __init__(self):
                self.statements = []

            def execute(self, statement, params=None):
                if params is not None:
                    return TypeResult()
                self.statements.append(str(statement))
                return None

        bind = Bind()
        mssql_admin.create_spatial_index(
            bind,
            "place",
            "geog",
            Integer(),
            index_name="idx_place_geog",
            mssql_using=None,
        )

        assert bind.statements == ["CREATE SPATIAL INDEX [idx_place_geog] ON [place] ([geog])"]

    def test_create_spatial_constraints_skips_generic_metadata(self):
        class Bind:
            dialect = mssql.dialect()

            def __init__(self):
                self.statements = []

            def execute(self, statement):
                self.statements.append(str(statement))

        bind = Bind()
        mssql_admin.create_spatial_constraints(
            bind,
            "lake",
            "geom",
            Geometry(geometry_type=None, srid=-1),
        )

        assert bind.statements == []

    def test_create_spatial_constraints_resolves_typedecorator_metadata(self):
        class Bind:
            dialect = mssql.dialect()

            def __init__(self):
                self.statements = []

            def execute(self, statement):
                self.statements.append(str(statement))

        bind = Bind()
        mssql_admin.create_spatial_constraints(
            bind,
            "place",
            "geog",
            WrappedGeography(),
        )

        assert bind.statements == [
            "ALTER TABLE [place] ADD CONSTRAINT [ck_place_geog_srid] CHECK "
            "([geog] IS NULL OR [geog].STSrid = 4326)",
            "ALTER TABLE [place] ADD CONSTRAINT [ck_place_geog_geometry_type] CHECK "
            "([geog] IS NULL OR UPPER([geog].AsTextZM()) LIKE N'POINT%')",
        ]

    def test_reflect_geometry_column_ignores_non_spatial_column_type(self):
        class Inspector:
            default_schema_name = "dbo"

        column_info = {"name": "payload", "type": Integer()}

        mssql_admin.reflect_geometry_column(
            Inspector(),
            Table("not_spatial", MetaData()),
            column_info,
        )

        assert isinstance(column_info["type"], Integer)

    def test_nulltype_non_spatial_columns_are_not_rewritten(self):
        class Result:
            def one(self):
                return "not_a_spatial_type", True

        class Bind:
            def execute(self, *args, **kwargs):
                return Result()

        class Inspector:
            bind = Bind()
            default_schema_name = "dbo"

        table = Table("not_spatial", MetaData())
        column_info = {"name": "payload", "type": NullType()}

        mssql_admin.reflect_geometry_column(Inspector(), table, column_info)

        assert isinstance(column_info["type"], NullType)


class TestMSSQLBindAndResultProcessing:
    dialect = mssql.dialect()

    def test_bind_processor_strips_ewkt_prefix(self):
        geom = Geometry(geometry_type="LINESTRING", srid=4326)
        bind_processor = geom.bind_processor(self.dialect)

        assert bind_processor("SRID=4326;LINESTRING(0 0,1 1)") == "LINESTRING(0 0,1 1)"
        assert (
            bind_processor("SRID=4326;LINESTRING ZM (0 0 1 2,1 1 3 4)")
            == "LINESTRING (0 0 1 2,1 1 3 4)"
        )

    def test_bind_processor_validates_srid(self):
        geom = Geometry(geometry_type="LINESTRING", srid=4326)
        bind_processor = geom.bind_processor(self.dialect)

        with pytest.raises(ArgumentError):
            bind_processor("SRID=2154;LINESTRING(0 0,1 1)")

        with pytest.raises(ArgumentError):
            bind_processor(WKTElement("LINESTRING(0 0,1 1)", srid=2154))

    def test_bind_processor_accepts_runtime_srid_for_unconstrained_column(self):
        geom = Geometry(geometry_type="LINESTRING", srid=-1)
        bind_processor = geom.bind_processor(self.dialect)
        wkb = bytes.fromhex(
            "01020000000200000000000000000000000000000000000000000000000000f03f000000000000f03f"
        )

        assert bind_processor("SRID=4326;LINESTRING(0 0,1 1)") == "LINESTRING(0 0,1 1)"
        assert bind_processor(WKTElement("LINESTRING(0 0,1 1)", srid=4326)) == (
            "LINESTRING(0 0,1 1)"
        )
        assert bind_processor(WKBElement(wkb, srid=4326)) == "LINESTRING (0 0, 1 1)"

    def test_bind_processor_treats_zero_srid_as_fixed(self):
        geom = Geometry(geometry_type="LINESTRING", srid=0)
        bind_processor = geom.bind_processor(self.dialect)
        wkb = bytes.fromhex(
            "01020000000200000000000000000000000000000000000000000000000000f03f000000000000f03f"
        )

        with pytest.raises(ArgumentError, match=r"column \(0\)"):
            bind_processor("SRID=4326;LINESTRING(0 0,1 1)")

        with pytest.raises(ArgumentError, match=r"column \(0\)"):
            bind_processor(WKTElement("LINESTRING(0 0,1 1)", srid=4326))

        with pytest.raises(ArgumentError, match=r"column \(0\)"):
            bind_processor(WKBElement(wkb, srid=4326))

    def test_bind_processor_resolves_typedecorator_metadata_for_srid_validation(self):
        wrapped_geography = WrappedGeography()

        assert (
            mssql_type.bind_processor_process(
                wrapped_geography,
                "SRID=4326;POINT(1 2)",
                self.dialect,
            )
            == "POINT(1 2)"
        )
        assert (
            mssql_type.bind_processor_process(
                wrapped_geography,
                WKTElement("POINT(1 2)", srid=4326),
                self.dialect,
            )
            == "POINT(1 2)"
        )
        with pytest.raises(ArgumentError, match=r"column \(4326\)"):
            mssql_type.bind_processor_process(
                wrapped_geography,
                WKTElement("POINT(1 2)", srid=3857),
                self.dialect,
            )

    def test_bind_processor_normalizes_wkt_and_wkb_elements(self):
        geom = Geometry(geometry_type="LINESTRING", srid=4326)
        bind_processor = geom.bind_processor(self.dialect)
        wkb = bytes.fromhex(
            "01020000000200000000000000000000000000000000000000000000000000f03f000000000000f03f"
        )

        assert bind_processor(WKTElement("SRID=4326;LINESTRING(0 0,1 1)", extended=True)) == (
            "LINESTRING(0 0,1 1)"
        )
        assert bind_processor(WKTElement("LINESTRING Z (0 0 1,1 1 2)", srid=-1)) == (
            "LINESTRING (0 0 1,1 1 2)"
        )
        assert bind_processor(WKBElement(wkb, srid=4326)) == "LINESTRING (0 0, 1 1)"

    def test_bind_processor_preserves_wkb_coordinate_precision(self):
        geom = Geometry(geometry_type="POINT", srid=4326)
        bind_processor = geom.bind_processor(self.dialect)
        wkb = _pack_iso_wkb(1, _pack_coords(0.12345678901234566, 123456789.12345679))

        assert bind_processor(WKBElement(wkb, srid=4326)) == (
            "POINT (0.12345678901234566 123456789.12345679)"
        )

    def test_wkb_parser_handles_hex_empty_and_geometrycollection_inputs(self):
        empty_linestring_z = _pack_iso_wkb(2, struct.pack("<I", 0), has_z=True)
        empty_linestring_m = _pack_iso_wkb(2, struct.pack("<I", 0), has_m=True)
        empty_polygon = _pack_iso_wkb(3, struct.pack("<I", 0))
        empty_collection = _pack_iso_wkb(7, struct.pack("<I", 0))
        point = _pack_iso_wkb(1, _pack_coords(1, 2))
        collection = _pack_iso_wkb(7, _pack_collection(point))

        assert mssql_type._wkb_to_mssql_wkt(empty_linestring_z) == "LINESTRING EMPTY"
        assert mssql_type._wkb_to_mssql_wkt(empty_linestring_m.hex()) == "LINESTRING EMPTY"
        assert mssql_type._wkb_to_mssql_wkt(bytearray(empty_polygon)) == "POLYGON EMPTY"
        assert mssql_type._wkb_to_mssql_wkt(empty_collection) == "GEOMETRYCOLLECTION EMPTY"
        assert mssql_type._wkb_to_mssql_wkt(collection) == "GEOMETRYCOLLECTION (POINT (1 2))"
        assert (
            mssql_type._wkb_to_mssql_wkt(_pack_iso_wkb(1, _pack_coords(math.nan, math.nan)))
            == "POINT EMPTY"
        )

    def test_wkb_parser_rejects_unsupported_values(self):
        unsupported_wkb = b"\x01" + struct.pack("<I", 999)

        with pytest.raises(ValueError, match="Unsupported WKB geometry type"):
            mssql_type._wkb_to_mssql_wkt(unsupported_wkb)
        with pytest.raises(ValueError, match="Invalid WKB byte order marker"):
            mssql_type._wkb_to_mssql_wkt(b"\x02" + struct.pack("<I", 1) + _pack_coords(1, 2))
        with pytest.raises(TypeError, match="Unsupported WKB value type"):
            mssql_type._wkb_to_mssql_wkt(object())

    def test_to_mssql_wkt_falls_back_to_shape_conversion(self, monkeypatch):
        class Shape:
            wkt = "POINT Z (1 2 3)"

        shaped_values = []

        def raise_parse_error(value):
            raise ValueError("unsupported parser path")

        def fake_to_shape(value):
            shaped_values.append(value)
            return Shape()

        monkeypatch.setattr(mssql_type, "_wkb_to_mssql_wkt", raise_parse_error)
        monkeypatch.setattr(mssql_type, "to_shape", fake_to_shape)

        assert mssql_type._to_mssql_wkt(WKTElement("POINT Z (1 2 3)", srid=4326)) == (
            "POINT (1 2 3)"
        )
        assert isinstance(shaped_values[-1], WKTElement)

        assert mssql_type._to_mssql_wkt(b"\x01") == "POINT (1 2 3)"
        assert isinstance(shaped_values[-1], WKBElement)

    def test_bind_coercion_helpers_keep_non_bind_clauses(self):
        table = Table("raw_values", MetaData(), Column("value", Integer))

        assert mssql_admin._coerce_wkt_bind_clause(table.c.value) is table.c.value
        assert mssql_admin._coerce_wkb_bind_clause(table.c.value) is table.c.value
        assert mssql_admin._is_bindparam_clause(table.c.value) is False
        assert mssql_admin._is_bindparam_clause(bindparam("value")) is True
        assert mssql_admin._should_coerce_wkt_bind_clause(table.c.value) is False
        assert mssql_admin._should_coerce_wkt_bind_clause(bindparam("value", 1)) is False
        assert (
            mssql_admin._should_coerce_wkt_bind_clause_for_text(
                bindparam("value", "SRID=4326;POINT(1 2)"),
                strip_srid=True,
            )
            is True
        )
        assert mssql_admin._should_coerce_wkb_bind_clause(table.c.value) is False
        assert mssql_admin._infer_srid_from_wkb_clause(table.c.value, 4326, extended=True) == 4326

    def test_geom_from_ewkt_bindparam_compiles_to_split_text_and_srid_binds(self):
        stmt = select([func.ST_GeomFromEWKT(bindparam("wkt"))])
        text_key, srid_key = mssql_admin._mssql_dynamic_ewkt_bind_keys(bindparam("wkt"))
        compiled_sql = normalize_sql(stmt.compile(dialect=self.dialect))
        compiled = stmt.compile(dialect=self.dialect)
        text_processor = compiled._bind_processors[text_key]
        srid_processor = compiled._bind_processors[srid_key]

        assert f"geometry::STGeomFromText(:{text_key}, :{srid_key})" in compiled_sql
        assert text_processor("SRID=4326;POINT ZM (1 2 3 4)") == "POINT (1 2 3 4)"
        assert srid_processor("SRID=4326;POINT ZM (1 2 3 4)") == 4326

    def test_geom_from_ewkb_bindparam_compiles_to_split_wkb_and_srid_binds(self):
        source_bind = bindparam("wkb")
        stmt = select([func.ST_GeomFromEWKB(source_bind)])
        wkb_key, srid_key = mssql_admin._mssql_dynamic_ewkb_bind_keys(source_bind)
        compiled_sql = normalize_sql(stmt.compile(dialect=self.dialect))
        compiled = stmt.compile(dialect=self.dialect)
        wkb_processor = compiled._bind_processors[wkb_key]
        srid_processor = compiled._bind_processors[srid_key]
        ewkb = from_shape(Point(1, 2), srid=4326, extended=True)

        assert f"geometry::STGeomFromWKB(:{wkb_key}, :{srid_key})" in compiled_sql
        assert bytes(wkb_processor(ewkb.data)) == bytes(ewkb.as_wkb().data)
        assert srid_processor(ewkb.data) == 4326
        plain_wkb = from_shape(Point(1, 2), extended=False).data
        runtime_wkb = WKBElement(plain_wkb, srid=3857, extended=False)
        assert bytes(wkb_processor(runtime_wkb)) == bytes(plain_wkb)
        assert srid_processor(runtime_wkb) == 3857
        assert wkb_processor(None) is None
        assert srid_processor(None) == 0

    def test_geom_from_ewkb_bindparam_with_explicit_srid_strips_runtime_ewkb(self):
        stmt = select([func.ST_GeomFromEWKB(bindparam("wkb"), 4326)])
        compiled_sql = normalize_sql(stmt.compile(dialect=self.dialect))
        compiled = stmt.compile(dialect=self.dialect)
        processor = compiled._bind_processors["wkb"]
        ewkb = from_shape(Point(1, 2), srid=4326, extended=True)

        assert "geometry::STGeomFromWKB(:wkb, :ST_GeomFromEWKB_2)" in compiled_sql
        assert bytes(processor(ewkb.data)) == bytes(ewkb.as_wkb().data)

    def test_geom_from_ewkb_bindparam_defaults_preserve_compile_time_value(self):
        ewkb = from_shape(Point(1, 2), srid=3857, extended=True)
        source_bind = bindparam("wkb", ewkb.data)
        stmt = select([func.ST_GeomFromEWKB(source_bind)])
        compiled = stmt.compile(dialect=self.dialect)
        wkb_key, srid_key = mssql_admin._mssql_dynamic_ewkb_bind_keys(source_bind)
        wkb_processor = compiled._bind_processors[wkb_key]
        srid_processor = compiled._bind_processors[srid_key]

        assert compiled.params[wkb_key] is ewkb.data
        assert compiled.params[srid_key] is ewkb.data
        assert bytes(wkb_processor(compiled.params[wkb_key])) == bytes(ewkb.as_wkb().data)
        assert srid_processor(compiled.params[srid_key]) == 3857

    def test_geom_from_ewkb_reused_bindparam_uses_distinct_default_srid_binds(self):
        source_bind = bindparam("wkb")
        stmt = select(
            [
                func.ST_GeomFromEWKB(
                    source_bind,
                    type_=DynamicDefaultSRIDGeometry(srid=4326),
                ),
                func.ST_GeomFromEWKB(
                    source_bind,
                    type_=DynamicDefaultSRIDGeometry(srid=3857),
                ),
            ]
        )
        compiled_sql = normalize_sql(stmt.compile(dialect=self.dialect))
        compiled = stmt.compile(dialect=self.dialect)
        wkb_key_4326, srid_key_4326 = mssql_admin._mssql_dynamic_ewkb_bind_keys(
            source_bind,
            default_srid=4326,
        )
        wkb_key_3857, srid_key_3857 = mssql_admin._mssql_dynamic_ewkb_bind_keys(
            source_bind,
            default_srid=3857,
        )
        plain_wkb = from_shape(Point(1, 2), extended=False).data

        assert f"geometry::STGeomFromWKB(:{wkb_key_4326}, :{srid_key_4326})" in compiled_sql
        assert f"geometry::STGeomFromWKB(:{wkb_key_3857}, :{srid_key_3857})" in compiled_sql
        assert srid_key_4326 != srid_key_3857
        assert bytes(compiled._bind_processors[wkb_key_4326](plain_wkb)) == bytes(plain_wkb)
        assert bytes(compiled._bind_processors[wkb_key_3857](plain_wkb)) == bytes(plain_wkb)
        assert compiled._bind_processors[srid_key_4326](plain_wkb) == 4326
        assert compiled._bind_processors[srid_key_3857](plain_wkb) == 3857

    def test_geom_from_ewkb_reused_callable_bind_uses_single_source_value(self):
        calls = []
        wkb = from_shape(Point(1, 2), extended=False).data

        def get_wkb():
            calls.append("called")
            return wkb

        source_bind = bindparam("wkb", callable_=get_wkb)
        stmt = select(
            [
                func.ST_GeomFromEWKB(
                    source_bind,
                    type_=DynamicDefaultSRIDGeometry(srid=4326),
                ),
                func.ST_GeomFromEWKB(
                    source_bind,
                    type_=DynamicDefaultSRIDGeometry(srid=3857),
                ),
            ]
        )
        compiled = stmt.compile(dialect=self.dialect)
        wkb_key_4326, srid_key_4326 = mssql_admin._mssql_dynamic_ewkb_bind_keys(
            source_bind,
            default_srid=4326,
        )
        wkb_key_3857, srid_key_3857 = mssql_admin._mssql_dynamic_ewkb_bind_keys(
            source_bind,
            default_srid=3857,
        )

        assert compiled.construct_params() == {
            wkb_key_4326: wkb,
            srid_key_4326: wkb,
            wkb_key_3857: wkb,
            srid_key_3857: wkb,
        }
        assert calls == ["called"]

    def test_mssql_before_execute_expands_dynamic_ewkt_bindparams(self):
        source_bind = bindparam("wkt")
        stmt = select([func.ST_GeomFromEWKT(source_bind)])
        text_key, srid_key = mssql_admin._mssql_dynamic_ewkt_bind_keys(source_bind)
        clauseelement, multiparams, params = mssql_admin.before_execute(
            type("Conn", (), {"dialect": self.dialect})(),
            stmt,
            (),
            {"wkt": "SRID=4326;POINT(1 2)"},
            {},
        )

        assert clauseelement is stmt
        assert multiparams == ()
        assert params == {
            "wkt": "SRID=4326;POINT(1 2)",
            text_key: "SRID=4326;POINT(1 2)",
            srid_key: "SRID=4326;POINT(1 2)",
        }

    def test_mssql_before_execute_expands_dynamic_ewkb_bindparams(self):
        source_bind = bindparam("wkb")
        stmt = select([func.ST_GeomFromEWKB(source_bind)])
        wkb_key, srid_key = mssql_admin._mssql_dynamic_ewkb_bind_keys(source_bind)
        ewkb = from_shape(Point(1, 2), srid=4326, extended=True).data
        clauseelement, multiparams, params = mssql_admin.before_execute(
            type("Conn", (), {"dialect": self.dialect})(),
            stmt,
            (),
            {"wkb": ewkb},
            {},
        )

        assert clauseelement is stmt
        assert multiparams == ()
        assert params == {
            "wkb": ewkb,
            wkb_key: ewkb,
            srid_key: ewkb,
        }

    def test_mssql_before_execute_expands_reused_ewkb_bind_for_each_default_srid(self):
        source_bind = bindparam("wkb")
        stmt = select(
            [
                func.ST_GeomFromEWKB(
                    source_bind,
                    type_=DynamicDefaultSRIDGeometry(srid=4326),
                ),
                func.ST_GeomFromEWKB(
                    source_bind,
                    type_=DynamicDefaultSRIDGeometry(srid=3857),
                ),
            ]
        )
        wkb_key_4326, srid_key_4326 = mssql_admin._mssql_dynamic_ewkb_bind_keys(
            source_bind,
            default_srid=4326,
        )
        wkb_key_3857, srid_key_3857 = mssql_admin._mssql_dynamic_ewkb_bind_keys(
            source_bind,
            default_srid=3857,
        )
        wkb = from_shape(Point(1, 2), extended=False).data

        _, _, params = mssql_admin.before_execute(
            type("Conn", (), {"dialect": self.dialect})(),
            stmt,
            (),
            {"wkb": wkb},
            {},
        )

        assert params == {
            "wkb": wkb,
            wkb_key_4326: wkb,
            srid_key_4326: wkb,
            wkb_key_3857: wkb,
            srid_key_3857: wkb,
        }

    def test_mssql_before_execute_ignores_auto_ewkb_constructor_bindparams(self):
        ewkb = from_shape(Point(1, 2), srid=4326, extended=True)
        stmt = select([func.ST_AsText(ewkb)])
        compiled = stmt.compile(dialect=self.dialect)
        params = compiled.params
        clauseelement, multiparams, expanded_params = mssql_admin.before_execute(
            type("Conn", (), {"dialect": self.dialect})(),
            stmt,
            (),
            params,
            {},
        )

        assert clauseelement is stmt
        assert multiparams == ()
        assert expanded_params is params

    def test_geom_from_ewkt_bindparam_defaults_preserve_compile_time_value(self):
        source_bind = bindparam("wkt", "SRID=3857;POINT(1 2)")
        stmt = select([func.ST_GeomFromEWKT(source_bind)])
        compiled = stmt.compile(dialect=self.dialect)
        text_key, srid_key = mssql_admin._mssql_dynamic_ewkt_bind_keys(source_bind)
        text_processor = compiled._bind_processors[text_key]
        srid_processor = compiled._bind_processors[srid_key]

        assert compiled.params[text_key] == "SRID=3857;POINT(1 2)"
        assert compiled.params[srid_key] == "SRID=3857;POINT(1 2)"
        assert text_processor(compiled.params[text_key]) == "POINT(1 2)"
        assert srid_processor(compiled.params[srid_key]) == 3857

    def test_geom_from_ewkt_bindparam_preserves_required_semantics(self):
        stmt = select([func.ST_GeomFromEWKT(bindparam("wkt"))])
        compiled = stmt.compile(dialect=self.dialect)

        with pytest.raises(InvalidRequestError, match="bind parameter"):
            compiled.construct_params()

    def test_geom_from_ewkt_bindparam_preserves_callable_default(self):
        calls = []

        def get_ewkt():
            calls.append("called")
            return "SRID=4326;POINT(1 2)"

        source_bind = bindparam("wkt", callable_=get_ewkt)
        stmt = select([func.ST_GeomFromEWKT(source_bind)])
        compiled = stmt.compile(dialect=self.dialect)
        text_key, srid_key = mssql_admin._mssql_dynamic_ewkt_bind_keys(source_bind)

        assert compiled.construct_params() == {
            text_key: "SRID=4326;POINT(1 2)",
            srid_key: "SRID=4326;POINT(1 2)",
        }
        assert calls == ["called"]

    def test_mssql_before_execute_expands_unique_bindparam_compiled_name(self):
        source_bind = bindparam("wkt", unique=True)
        stmt = select([func.ST_GeomFromEWKT(source_bind)])
        text_key, srid_key = mssql_admin._mssql_dynamic_ewkt_bind_keys(source_bind)
        compiled_key = "wkt_1"

        _, _, params = mssql_admin.before_execute(
            type("Conn", (), {"dialect": self.dialect})(),
            stmt,
            (),
            {compiled_key: "SRID=4326;POINT(1 2)"},
            {},
        )

        assert params == {
            compiled_key: "SRID=4326;POINT(1 2)",
            text_key: "SRID=4326;POINT(1 2)",
            srid_key: "SRID=4326;POINT(1 2)",
        }

    def test_mssql_before_execute_keeps_distinct_unique_bindparams_per_statement(self):
        source_bind_1 = bindparam("wkt", unique=True)
        source_bind_2 = bindparam("wkt", unique=True)
        stmt = select(
            [
                func.ST_GeomFromEWKT(source_bind_1),
                func.ST_GeomFromEWKT(source_bind_2),
            ]
        )
        text_key_1, srid_key_1 = mssql_admin._mssql_dynamic_ewkt_bind_keys(source_bind_1)
        text_key_2, srid_key_2 = mssql_admin._mssql_dynamic_ewkt_bind_keys(source_bind_2)

        _, _, params = mssql_admin.before_execute(
            type("Conn", (), {"dialect": self.dialect})(),
            stmt,
            (),
            {
                "wkt_1": "SRID=4326;POINT(1 2)",
                "wkt_2": "SRID=3857;POINT(3 4)",
            },
            {},
        )

        assert params == {
            "wkt_1": "SRID=4326;POINT(1 2)",
            "wkt_2": "SRID=3857;POINT(3 4)",
            text_key_1: "SRID=4326;POINT(1 2)",
            srid_key_1: "SRID=4326;POINT(1 2)",
            text_key_2: "SRID=3857;POINT(3 4)",
            srid_key_2: "SRID=3857;POINT(3 4)",
        }

    def test_mssql_before_execute_reused_unique_bindparam_keeps_single_runtime_key(self):
        source_bind = bindparam("wkt", unique=True)
        stmt = select(
            [
                func.ST_GeomFromEWKT(source_bind),
                func.ST_GeomFromEWKT(source_bind),
            ]
        )
        text_key, srid_key = mssql_admin._mssql_dynamic_ewkt_bind_keys(source_bind)

        _, _, params = mssql_admin.before_execute(
            type("Conn", (), {"dialect": self.dialect})(),
            stmt,
            (),
            {"wkt_1": "SRID=4326;POINT(1 2)"},
            {},
        )

        assert params == {
            "wkt_1": "SRID=4326;POINT(1 2)",
            text_key: "SRID=4326;POINT(1 2)",
            srid_key: "SRID=4326;POINT(1 2)",
        }

    def test_geom_from_ewkt_reused_callable_bind_uses_single_generated_pair(self):
        calls = []

        def get_ewkt():
            calls.append("called")
            return "SRID=4326;POINT(1 2)"

        source_bind = bindparam("wkt", callable_=get_ewkt)
        stmt = select(
            [
                func.ST_GeomFromEWKT(source_bind),
                func.ST_GeomFromEWKT(source_bind),
            ]
        )
        compiled = stmt.compile(dialect=self.dialect)
        text_key, srid_key = mssql_admin._mssql_dynamic_ewkt_bind_keys(source_bind)

        assert compiled.construct_params() == {
            text_key: "SRID=4326;POINT(1 2)",
            srid_key: "SRID=4326;POINT(1 2)",
        }
        assert calls == ["called"]

    def test_geom_from_ewkt_with_explicit_srid_keeps_srid_bind_and_normalizes_wkt(self):
        stmt = select([func.ST_GeomFromEWKT(bindparam("wkt"), bindparam("srid"))])
        compiled_sql = normalize_sql(stmt.compile(dialect=self.dialect))
        compiled = stmt.compile(dialect=self.dialect)
        processor = compiled._bind_processors["wkt"]

        assert "geometry::STGeomFromText(:wkt, :srid)" in compiled_sql
        assert processor("SRID=4326;POINT ZM (1 2 3 4)") == "POINT (1 2 3 4)"
        assert "srid" not in compiled._bind_processors

    def test_geom_from_text_bindparam_uses_runtime_wkt_normalization(self):
        stmt = select([func.ST_GeomFromText(bindparam("wkt"), 4326)])
        compiled = stmt.compile(dialect=self.dialect)
        processor = compiled._bind_processors["wkt"]

        assert processor("POINT ZM (1 2 3 4)") == "POINT (1 2 3 4)"

    def test_bind_processor_passes_through_non_spatial_values(self):
        geom = Geometry(geometry_type="LINESTRING", srid=4326)
        bind_processor = geom.bind_processor(self.dialect)

        assert bind_processor(None) is None

    def test_bind_processor_normalizes_z_dimension_wkb_inputs(self):
        geom = Geometry(geometry_type="POINTZ", srid=4326)
        bind_processor = geom.bind_processor(self.dialect)
        wkbelement = from_shape(Point(1, 2, 3), srid=4326)

        assert bind_processor(wkbelement) == "POINT (1 2 3)"
        assert bind_processor(wkbelement.data) == "POINT (1 2 3)"

    def test_bind_processor_preserves_zm_dimension_for_iso_wkb_inputs(self):
        geom = Geometry(geometry_type="POINTZM", srid=4326)
        bind_processor = geom.bind_processor(self.dialect)
        wkb = _pack_iso_wkb(1, _pack_coords(1, 2, 3, 4), has_z=True, has_m=True)

        assert bind_processor(WKBElement(wkb, srid=4326)) == "POINT (1 2 3 4)"
        assert bind_processor(wkb) == "POINT (1 2 3 4)"
        assert bind_processor(WKBElement(wkb, srid=4326).as_ewkb()) == "POINT (1 2 3 4)"

    def test_bind_processor_preserves_zm_dimension_for_nested_iso_wkb_inputs(self):
        geom = Geometry(geometry_type="MULTIPOLYGONZM", srid=4326)
        bind_processor = geom.bind_processor(self.dialect)
        wkb = _pack_multipolygon(
            [
                [[(1, 2, 3, 4), (5, 6, 7, 8), (9, 10, 11, 12), (1, 2, 3, 4)]],
                [[(10, 20, 30, 40), (50, 60, 70, 80), (90, 100, 110, 120), (10, 20, 30, 40)]],
            ],
            has_z=True,
            has_m=True,
        )

        assert bind_processor(WKBElement(wkb, srid=4326)) == (
            "MULTIPOLYGON (((1 2 3 4, 5 6 7 8, 9 10 11 12, 1 2 3 4)), "
            "((10 20 30 40, 50 60 70 80, 90 100 110 120, 10 20 30 40)))"
        )
        assert bind_processor(WKBElement(wkb, srid=4326).as_ewkb()) == (
            "MULTIPOLYGON (((1 2 3 4, 5 6 7 8, 9 10 11 12, 1 2 3 4)), "
            "((10 20 30 40, 50 60 70 80, 90 100 110 120, 10 20 30 40)))"
        )

    def test_result_processor_marks_values_as_non_extended_wkb(self):
        geom = Geometry(geometry_type="LINESTRING", srid=4326)
        result_processor = geom.result_processor(self.dialect, None)
        wkb = bytes.fromhex(
            "01020000000200000000000000000000000000000000000000000000000000f03f000000000000f03f"
        )

        result = result_processor(wkb)
        assert isinstance(result, WKBElement)
        assert result.srid == 4326
        assert result.extended is False

    def test_result_processor_accepts_memoryview(self):
        geom = Geometry(geometry_type="LINESTRING", srid=4326)
        result_processor = geom.result_processor(self.dialect, None)
        wkb = memoryview(
            bytes.fromhex(
                "01020000000200000000000000000000000000000000000000000000000000f03f000000000000f03f"
            )
        )

        result = result_processor(wkb)
        assert isinstance(result, WKBElement)
        assert result.srid == 4326
        assert result.extended is False

    def test_result_processor_recovers_srid_from_ewkb_for_unknown_srid_columns(self):
        geom = Geometry(geometry_type="POINT")
        result_processor = geom.result_processor(self.dialect, None)
        ewkb = WKBElement(
            bytes.fromhex("0101000000000000000000f03f0000000000000040"),
            srid=4326,
        ).as_ewkb()

        result = result_processor(ewkb.data)
        assert isinstance(result, WKBElement)
        assert result.srid == 4326
        assert result.extended is True
