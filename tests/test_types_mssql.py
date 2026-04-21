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
from sqlalchemy.dialects import mssql
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import func
from sqlalchemy.sql import insert
from sqlalchemy.sql.sqltypes import NullType

from geoalchemy2.admin import select_dialect as select_admin_dialect
from geoalchemy2.admin.dialects import mssql as mssql_admin
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.exc import ArgumentError
from geoalchemy2.shape import from_shape
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry
from geoalchemy2.types import select_dialect as select_type_dialect

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


class TestMSSQLDialectRegistration:
    def test_type_select_dialect(self):
        assert select_type_dialect("mssql").__name__ == "geoalchemy2.types.dialects.mssql"

    def test_admin_select_dialect(self):
        assert select_admin_dialect("mssql").__name__ == "geoalchemy2.admin.dialects.mssql"


class TestMSSQLCompilation:
    dialect = mssql.dialect()

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

    def test_as_ewkb_compiles_to_stasbinary(self, geometry_table):
        stmt = select([geometry_table.c.geom.ST_AsEWKB()])
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))
        assert ".AsBinaryZM()" in compiled
        assert "ST_AsEWKB(" not in compiled

    def test_as_ewkt_compiles_to_astextzm(self, geometry_table):
        stmt = select([geometry_table.c.geom.ST_AsEWKT()])
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))
        assert ".AsTextZM()" in compiled
        assert "ST_AsEWKT(" not in compiled

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


class TestMSSQLReflectionHelpers:
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

    def test_bind_processor_normalizes_wkt_and_wkb_elements(self):
        geom = Geometry(geometry_type="LINESTRING", srid=4326)
        bind_processor = geom.bind_processor(self.dialect)
        wkb = bytes.fromhex(
            "01020000000200000000000000000000000000000000000000000000000000f03f000000000000f03f"
        )

        assert bind_processor(WKTElement("SRID=4326;LINESTRING(0 0,1 1)", extended=True)) == (
            "LINESTRING(0 0,1 1)"
        )
        assert bind_processor(WKBElement(wkb, srid=4326)) == "LINESTRING (0 0, 1 1)"

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
