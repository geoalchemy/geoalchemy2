import re

import pytest
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.dialects import mssql
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import insert

from geoalchemy2.admin import select_dialect as select_admin_dialect
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.exc import ArgumentError
from geoalchemy2.shape import from_shape
from geoalchemy2.types import Geometry
from geoalchemy2.types import select_dialect as select_type_dialect
from shapely.geometry import Point

from . import select


def normalize_sql(sql):
    return re.sub(r"\s+", " ", str(sql)).strip()


@pytest.fixture
def geometry_table():
    return Table(
        "lake",
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("geom", Geometry(geometry_type="LINESTRING", srid=4326)),
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

    def test_functions_compile_to_mssql_methods(self, geometry_table):
        stmt = select(
            [
                geometry_table.c.geom.ST_Buffer(2),
                geometry_table.c.geom.ST_GeometryType(),
                geometry_table.c.geom.ST_SRID(),
                geometry_table.c.geom.ST_AsText(),
            ]
        )
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))
        assert ".STBuffer(" in compiled
        assert ".STGeometryType()" in compiled
        assert ".STSrid" in compiled
        assert ".AsTextZM()" in compiled

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

    def test_binary_predicates_compile_to_mssql_methods(self, geometry_table):
        stmt = select(
            [
                geometry_table.c.geom.ST_Equals(WKTElement("LINESTRING(0 0,1 1)", srid=4326)),
                geometry_table.c.geom.ST_Within(WKTElement("POLYGON((0 0,2 0,2 2,0 0))", srid=4326)),
            ]
        )
        compiled = normalize_sql(stmt.compile(dialect=self.dialect))
        assert ".STEquals(" in compiled
        assert ".STWithin(" in compiled
        assert "ST_Equals(" not in compiled
        assert "ST_Within(" not in compiled

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
