import re

import pytest
from sqlalchemy import Column
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import bindparam
from sqlalchemy.dialects import mysql
from sqlalchemy.dialects.mysql import mariadb as mariadb_dialect
from sqlalchemy.sql import func
from sqlalchemy.sql import insert
from sqlalchemy.sql import text

from geoalchemy2.admin.dialects import mariadb as _mariadb_admin  # noqa: F401
from geoalchemy2.admin.dialects import mysql as _mysql_admin  # noqa: F401
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.exc import ArgumentError
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry
from geoalchemy2.types import Raster

from . import select

WKB_HEX = "0101000000000000000000f03f0000000000000040"
EWKB_HEX = "0101000020e6100000000000000000f03f0000000000000040"
WEB_MERCATOR_EWKB_HEX = "0101000020110f0000000000000000f03f0000000000000040"
ZERO_SRID_EWKB_HEX = "010100002000000000000000000000f03f0000000000000040"


def eq_sql(a, b):
    a = re.sub(r"[\n\t]", "", str(a))
    assert a == b


@pytest.fixture
def geometry_table():
    table = Table("table", MetaData(), Column("geom", Geometry))
    return table


@pytest.fixture
def geography_table():
    table = Table("table", MetaData(), Column("geom", Geography))
    return table


@pytest.fixture
def raster_table():
    table = Table("table", MetaData(), Column("rast", Raster))
    return table


class TestGeometry:
    def test_get_col_spec(self):
        g = Geometry(srid=900913)
        assert g.get_col_spec() == "geometry(GEOMETRY,900913)"

    def test_get_col_spec_no_srid(self):
        g = Geometry(srid=None)
        assert g.get_col_spec() == "geometry(GEOMETRY,-1)"

    def test_get_col_spec_invalid_srid(self):
        with pytest.raises(ArgumentError) as e:
            g = Geometry(srid="foo")
            g.get_col_spec()
        assert str(e.value) == "srid must be convertible to an integer"

    def test_get_col_spec_no_typmod(self):
        g = Geometry(geometry_type=None)
        assert g.get_col_spec() == "geometry"

    def test_check_ctor_args_bad_srid(self):
        with pytest.raises(ArgumentError):
            Geometry(srid="foo")

    def test_get_col_spec_geometryzm(self):
        g = Geometry(geometry_type="GEOMETRYZM", srid=900913)
        assert g.get_col_spec() == "geometry(GEOMETRYZM,900913)"

    def test_get_col_spec_geometryz(self):
        g = Geometry(geometry_type="GEOMETRYZ", srid=900913)
        assert g.get_col_spec() == "geometry(GEOMETRYZ,900913)"

    def test_get_col_spec_geometrym(self):
        g = Geometry(geometry_type="GEOMETRYM", srid=900913)
        assert g.get_col_spec() == "geometry(GEOMETRYM,900913)"

    def test_check_ctor_args_srid_not_enforced(self):
        with pytest.warns(UserWarning):
            Geometry(geometry_type=None, srid=4326)

    def test_check_ctor_args_use_typmod_nullable(self):
        with pytest.raises(
            ArgumentError,
            match='The "nullable" and "use_typmod" arguments can not be used together',
        ):
            Geometry(use_typmod=True, nullable=False)

    def test_column_expression(self, geometry_table):
        s = select([geometry_table.c.geom])
        eq_sql(s, 'SELECT ST_AsEWKB("table".geom) AS geom FROM "table"')

    def test_select_bind_expression(self, geometry_table):
        s = select([text("foo")]).where(geometry_table.c.geom == "POINT(1 2)")
        eq_sql(
            s,
            'SELECT foo FROM "table" WHERE "table".geom = ST_GeomFromEWKT(:geom_1)',
        )
        assert s.compile().params == {"geom_1": "POINT(1 2)"}

    def test_insert_bind_expression(self, geometry_table):
        i = insert(geometry_table).values(geom="POINT(1 2)")
        eq_sql(i, 'INSERT INTO "table" (geom) VALUES (ST_GeomFromEWKT(:geom))')
        assert i.compile().params == {"geom": "POINT(1 2)"}

    def test_function_call(self, geometry_table):
        s = select([geometry_table.c.geom.ST_Buffer(2)])
        eq_sql(
            s,
            'SELECT ST_AsEWKB(ST_Buffer("table".geom, :ST_Buffer_2)) AS "ST_Buffer_1" FROM "table"',
        )

    def test_non_ST_function_call(self, geometry_table):
        with pytest.raises(AttributeError):
            geometry_table.c.geom.Buffer(2)

    def test_subquery(self, geometry_table):
        # test for geometry columns not delivered to the result
        # http://hg.sqlalchemy.org/sqlalchemy/rev/f1efb20c6d61
        s = select([geometry_table]).alias("name").select()
        eq_sql(
            s,
            "SELECT ST_AsEWKB(name.geom) AS geom FROM "
            '(SELECT "table".geom AS geom FROM "table") AS name',
        )


class TestGeography:
    def test_get_col_spec(self):
        g = Geography(srid=900913)
        assert g.get_col_spec() == "geography(GEOMETRY,900913)"

    def test_get_col_spec_no_typmod(self):
        g = Geography(geometry_type=None)
        assert g.get_col_spec() == "geography"

    def test_column_expression(self, geography_table):
        s = select([geography_table.c.geom])
        eq_sql(s, 'SELECT ST_AsBinary("table".geom) AS geom FROM "table"')

    def test_select_bind_expression(self, geography_table):
        s = select([text("foo")]).where(geography_table.c.geom == "POINT(1 2)")
        eq_sql(
            s,
            'SELECT foo FROM "table" WHERE "table".geom = ST_GeogFromText(:geom_1)',
        )
        assert s.compile().params == {"geom_1": "POINT(1 2)"}

    def test_insert_bind_expression(self, geography_table):
        i = insert(geography_table).values(geom="POINT(1 2)")
        eq_sql(i, 'INSERT INTO "table" (geom) VALUES (ST_GeogFromText(:geom))')
        assert i.compile().params == {"geom": "POINT(1 2)"}

    def test_function_call(self, geography_table):
        s = select([geography_table.c.geom.ST_Buffer(2)])
        eq_sql(
            s,
            'SELECT ST_AsEWKB(ST_Buffer("table".geom, :ST_Buffer_2)) AS "ST_Buffer_1" FROM "table"',
        )

    def test_non_ST_function_call(self, geography_table):
        with pytest.raises(AttributeError):
            geography_table.c.geom.Buffer(2)

    def test_subquery(self, geography_table):
        # test for geography columns not delivered to the result
        # http://hg.sqlalchemy.org/sqlalchemy/rev/f1efb20c6d61
        s = select([geography_table]).alias("name").select()
        eq_sql(
            s,
            "SELECT ST_AsBinary(name.geom) AS geom FROM "
            '(SELECT "table".geom AS geom FROM "table") AS name',
        )


class TestPoint:
    def test_get_col_spec(self):
        g = Geometry(geometry_type="POINT", srid=900913)
        assert g.get_col_spec() == "geometry(POINT,900913)"


class TestCurve:
    def test_get_col_spec(self):
        g = Geometry(geometry_type="CURVE", srid=900913)
        assert g.get_col_spec() == "geometry(CURVE,900913)"


class TestLineString:
    def test_get_col_spec(self):
        g = Geometry(geometry_type="LINESTRING", srid=900913)
        assert g.get_col_spec() == "geometry(LINESTRING,900913)"


class TestPolygon:
    def test_get_col_spec(self):
        g = Geometry(geometry_type="POLYGON", srid=900913)
        assert g.get_col_spec() == "geometry(POLYGON,900913)"


class TestMultiPoint:
    def test_get_col_spec(self):
        g = Geometry(geometry_type="MULTIPOINT", srid=900913)
        assert g.get_col_spec() == "geometry(MULTIPOINT,900913)"


class TestMultiLineString:
    def test_get_col_spec(self):
        g = Geometry(geometry_type="MULTILINESTRING", srid=900913)
        assert g.get_col_spec() == "geometry(MULTILINESTRING,900913)"


class TestMultiPolygon:
    def test_get_col_spec(self):
        g = Geometry(geometry_type="MULTIPOLYGON", srid=900913)
        assert g.get_col_spec() == "geometry(MULTIPOLYGON,900913)"


class TestGeometryCollection:
    def test_get_col_spec(self):
        g = Geometry(geometry_type="GEOMETRYCOLLECTION", srid=900913)
        assert g.get_col_spec() == "geometry(GEOMETRYCOLLECTION,900913)"


class TestMySQLWKBConstructors:
    @staticmethod
    def normalize_sql(sql):
        return re.sub(r"\s+", " ", str(sql)).strip()

    def test_geom_from_ewkb_compiles_to_supported_wkb_constructor(self):
        expr = func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX), type_=Geometry(srid=4326))

        compiled_expr = expr.compile(dialect=mysql.dialect())

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(%s, 4326)"
        assert compiled_expr.params == {"param_1": bytes.fromhex(WKB_HEX)}

    def test_geom_from_ewkb_literal_compile_strips_ewkb_srid(self):
        expr = func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX), type_=Geometry(srid=4326))

        compiled = self.normalize_sql(
            expr.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True})
        )

        assert compiled == f"ST_GeomFromWKB(unhex('{WKB_HEX}'), 4326)"

    @pytest.mark.parametrize(
        "value",
        [
            WKBElement(bytes.fromhex(WKB_HEX)),
            WKTElement("POINT (1 2)", srid=4326).as_wkb(),
            WKTElement("POINT (1 2)", srid=4326).as_ewkb(),
        ],
    )
    def test_geom_from_ewkb_literal_compile_unwraps_wkbelement_constructor(self, value):
        expr = func.ST_GeomFromEWKB(value, type_=Geometry(srid=4326))

        compiled = self.normalize_sql(
            expr.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True})
        )

        assert compiled == f"ST_GeomFromWKB(unhex('{WKB_HEX}'), 4326)"

    def test_geom_from_wkb_omits_unknown_srid(self):
        expr = func.ST_GeomFromWKB(bytes.fromhex(WKB_HEX), -1)

        compiled_expr = expr.compile(dialect=mysql.dialect())
        compiled_literal = expr.compile(
            dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}
        )

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(%s)"
        assert self.normalize_sql(compiled_literal) == f"ST_GeomFromWKB(unhex('{WKB_HEX}'))"

    @pytest.mark.parametrize("srid", [None, -1, 0])
    def test_geom_from_wkb_omits_fixed_unknown_srid_bindparam(self, srid):
        expr = func.ST_GeomFromWKB(bytes.fromhex(WKB_HEX), bindparam("srid", srid))

        compiled_expr = expr.compile(dialect=mysql.dialect())

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(%s)"
        assert "srid" not in compiled_expr.params

    def test_geom_from_wkb_keeps_runtime_srid_bindparam(self):
        expr = func.ST_GeomFromWKB(bytes.fromhex(WKB_HEX), bindparam("srid"))

        compiled_expr = expr.compile(dialect=mysql.dialect())

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(%s, %s)"
        assert "srid" in compiled_expr.params

    def test_wkbelement_literal_compile_omits_unknown_srid(self):
        query = select([func.ST_AsText(WKBElement(bytes.fromhex(WKB_HEX)))])

        compiled = self.normalize_sql(
            query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True})
        )

        assert f"ST_GeomFromWKB(unhex('{WKB_HEX}'))" in compiled
        assert ", -1" not in compiled

    def test_geom_from_ewkb_prefers_embedded_srid_over_return_type_srid(self):
        expr = func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX), type_=Geometry(srid=3857))

        compiled = self.normalize_sql(expr.compile(dialect=mysql.dialect()))

        assert compiled == "ST_GeomFromWKB(%s, 4326)"

    def test_geom_from_ewkb_uses_embedded_srid_without_return_type_srid(self):
        expr = func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX))

        compiled = self.normalize_sql(expr.compile(dialect=mysql.dialect()))

        assert compiled == "ST_GeomFromWKB(%s, 4326)"

    @pytest.mark.parametrize("srid", [None, -1, 0])
    def test_geom_from_ewkb_omitted_explicit_srid_uses_embedded_srid(self, srid):
        expr = func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX), srid)

        compiled_expr = expr.compile(dialect=mysql.dialect())

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(%s, 4326)"
        assert list(compiled_expr.params.values()) == [bytes.fromhex(WKB_HEX)]

    @pytest.mark.parametrize("srid", [None, -1, 0])
    def test_geom_from_ewkb_runtime_bind_with_omitted_explicit_srid_rejects_ewkb(
        self,
        srid,
    ):
        expr = func.ST_GeomFromEWKB(bindparam("wkb"), srid)
        compiled_expr = expr.compile(dialect=mysql.dialect())
        wkb_processor = compiled_expr._bind_processors["wkb"]

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(%s)"
        assert set(compiled_expr.params) == {"wkb"}
        assert wkb_processor(bytes.fromhex(WKB_HEX)) == bytes.fromhex(WKB_HEX)
        with pytest.raises(ArgumentError, match="fixed column SRID or an explicit SRID"):
            wkb_processor(bytes.fromhex(EWKB_HEX))

    def test_geom_from_ewkb_defaulted_bindparam_preserves_key_and_processor(self):
        source_bind = bindparam("wkb", bytes.fromhex(EWKB_HEX))
        expr = func.ST_GeomFromEWKB(source_bind)
        compiled_expr = expr.compile(dialect=mysql.dialect())
        wkb_processor = compiled_expr._bind_processors["wkb"]
        override = bytes.fromhex(WKB_HEX)

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(%s, 4326)"
        assert compiled_expr.params == {"wkb": bytes.fromhex(EWKB_HEX)}
        assert compiled_expr.construct_params({"wkb": override}) == {"wkb": override}
        assert wkb_processor(compiled_expr.params["wkb"]) == bytes.fromhex(WKB_HEX)
        assert wkb_processor(bytes.fromhex(EWKB_HEX)) == bytes.fromhex(WKB_HEX)
        with pytest.raises(ArgumentError, match=r"column \(4326\)"):
            wkb_processor(bytes.fromhex(WEB_MERCATOR_EWKB_HEX))

    def test_geom_from_ewkb_fixed_srid_bindparam_strips_runtime_ewkb(self):
        source_bind = bindparam("wkb")
        expr = func.ST_GeomFromEWKB(source_bind, type_=Geometry(srid=3857))
        compiled_expr = expr.compile(dialect=mysql.dialect())

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(%s, 3857)"
        assert set(compiled_expr.params) == {"wkb"}
        assert compiled_expr._bind_processors["wkb"](bytes.fromhex(WKB_HEX)) == bytes.fromhex(
            WKB_HEX
        )
        assert compiled_expr._bind_processors["wkb"](bytes.fromhex(ZERO_SRID_EWKB_HEX)) == (
            bytes.fromhex(WKB_HEX)
        )
        with pytest.raises(ArgumentError, match=r"column \(3857\)"):
            compiled_expr._bind_processors["wkb"](bytes.fromhex(EWKB_HEX))

    @pytest.mark.parametrize("srids", [(3857, 4326), (4326, 3857)])
    def test_geom_from_ewkb_reused_fixed_srid_bind_validates_all_contexts(self, srids):
        source_bind = bindparam("wkb")
        stmt = select(
            [
                func.ST_GeomFromEWKB(source_bind, type_=Geometry(srid=srids[0])),
                func.ST_GeomFromEWKB(source_bind, type_=Geometry(srid=srids[1])),
            ]
        )
        compiled_expr = stmt.compile(dialect=mysql.dialect())
        compiled = self.normalize_sql(compiled_expr)
        wkb_processor = compiled_expr._bind_processors["wkb"]

        assert f"ST_GeomFromWKB(%s, {srids[0]})" in compiled
        assert f"ST_GeomFromWKB(%s, {srids[1]})" in compiled
        assert wkb_processor(bytes.fromhex(WKB_HEX)) == bytes.fromhex(WKB_HEX)
        with pytest.raises(ArgumentError, match=r"column \(3857\)"):
            wkb_processor(bytes.fromhex(EWKB_HEX))

    def test_geom_from_ewkb_runtime_bind_without_srid_rejects_embedded_srid(self):
        expr = func.ST_GeomFromEWKB(bindparam("wkb"))
        compiled_expr = expr.compile(dialect=mysql.dialect())
        wkb_processor = compiled_expr._bind_processors["wkb"]

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(%s)"
        assert wkb_processor(bytes.fromhex(WKB_HEX)) == bytes.fromhex(WKB_HEX)
        with pytest.raises(ArgumentError, match="fixed column SRID or an explicit SRID"):
            wkb_processor(bytes.fromhex(EWKB_HEX))
        with pytest.raises(ArgumentError, match="fixed column SRID or an explicit SRID"):
            wkb_processor(WKBElement(bytes.fromhex(WKB_HEX), srid=4326))

    def test_geom_from_ewkb_explicit_srid_bindparam_strips_runtime_ewkb(self):
        expr = func.ST_GeomFromEWKB(bindparam("wkb"), bindparam("srid"))
        compiled_expr = expr.compile(dialect=mysql.dialect())

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(%s, %s)"
        assert set(compiled_expr.params) == {"wkb", "srid"}
        assert compiled_expr._bind_processors["wkb"](bytes.fromhex(EWKB_HEX)) == bytes.fromhex(
            WKB_HEX
        )


class TestMariaDBWKBConstructors:
    @staticmethod
    def normalize_sql(sql):
        return re.sub(r"\s+", " ", str(sql)).strip()

    @staticmethod
    def dialect():
        return mariadb_dialect.MariaDBDialect()

    def test_geom_from_ewkb_compiles_to_supported_wkb_constructor(self):
        expr = func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX), type_=Geometry(srid=4326))

        compiled_expr = expr.compile(dialect=self.dialect())

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(unhex(%s), 4326)"
        assert compiled_expr.params == {"param_1": WKB_HEX}

    def test_geom_from_ewkb_literal_compile_strips_ewkb_srid(self):
        expr = func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX), type_=Geometry(srid=4326))

        compiled = self.normalize_sql(
            expr.compile(dialect=self.dialect(), compile_kwargs={"literal_binds": True})
        )

        assert compiled == f"ST_GeomFromWKB(unhex('{WKB_HEX}'), 4326)"

    @pytest.mark.parametrize(
        "value",
        [
            WKBElement(bytes.fromhex(WKB_HEX)),
            WKTElement("POINT (1 2)", srid=4326).as_wkb(),
            WKTElement("POINT (1 2)", srid=4326).as_ewkb(),
        ],
    )
    def test_geom_from_ewkb_literal_compile_unwraps_wkbelement_constructor(self, value):
        expr = func.ST_GeomFromEWKB(value, type_=Geometry(srid=4326))

        compiled = self.normalize_sql(
            expr.compile(dialect=self.dialect(), compile_kwargs={"literal_binds": True})
        )

        assert compiled == f"ST_GeomFromWKB(unhex('{WKB_HEX}'), 4326)"

    def test_geom_from_wkb_omits_unknown_srid(self):
        expr = func.ST_GeomFromWKB(bytes.fromhex(WKB_HEX), -1)

        compiled_expr = expr.compile(dialect=self.dialect())
        compiled_literal = expr.compile(
            dialect=self.dialect(), compile_kwargs={"literal_binds": True}
        )

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(unhex(%s))"
        assert self.normalize_sql(compiled_literal) == f"ST_GeomFromWKB(unhex('{WKB_HEX}'))"

    @pytest.mark.parametrize("srid", [None, -1, 0])
    def test_geom_from_wkb_omits_fixed_unknown_srid_bindparam(self, srid):
        expr = func.ST_GeomFromWKB(bytes.fromhex(WKB_HEX), bindparam("srid", srid))

        compiled_expr = expr.compile(dialect=self.dialect())

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(unhex(%s))"
        assert "srid" not in compiled_expr.params

    def test_geom_from_wkb_keeps_runtime_srid_bindparam(self):
        expr = func.ST_GeomFromWKB(bytes.fromhex(WKB_HEX), bindparam("srid"))

        compiled_expr = expr.compile(dialect=self.dialect())

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(unhex(%s), %s)"
        assert "srid" in compiled_expr.params

    def test_wkbelement_literal_compile_omits_unknown_srid(self):
        query = select([func.ST_AsText(WKBElement(bytes.fromhex(WKB_HEX)))])

        compiled = self.normalize_sql(
            query.compile(dialect=self.dialect(), compile_kwargs={"literal_binds": True})
        )

        assert f"ST_GeomFromWKB(unhex('{WKB_HEX}'))" in compiled
        assert ", -1" not in compiled

    def test_geom_from_ewkb_prefers_embedded_srid_over_return_type_srid(self):
        expr = func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX), type_=Geometry(srid=3857))

        compiled = self.normalize_sql(expr.compile(dialect=self.dialect()))

        assert compiled == "ST_GeomFromWKB(unhex(%s), 4326)"

    def test_geom_from_ewkb_uses_embedded_srid_without_return_type_srid(self):
        expr = func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX))

        compiled = self.normalize_sql(expr.compile(dialect=self.dialect()))

        assert compiled == "ST_GeomFromWKB(unhex(%s), 4326)"

    @pytest.mark.parametrize("srid", [None, -1, 0])
    def test_geom_from_ewkb_omitted_explicit_srid_uses_embedded_srid(self, srid):
        expr = func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX), srid)

        compiled_expr = expr.compile(dialect=self.dialect())

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(unhex(%s), 4326)"
        assert list(compiled_expr.params.values()) == [WKB_HEX]

    @pytest.mark.parametrize("srid", [None, -1, 0])
    def test_geom_from_ewkb_runtime_bind_with_omitted_explicit_srid_rejects_ewkb(
        self,
        srid,
    ):
        expr = func.ST_GeomFromEWKB(bindparam("wkb"), srid)
        compiled_expr = expr.compile(dialect=self.dialect())
        wkb_processor = compiled_expr._bind_processors["wkb"]

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(unhex(%s))"
        assert set(compiled_expr.params) == {"wkb"}
        assert wkb_processor(bytes.fromhex(WKB_HEX)) == WKB_HEX
        with pytest.raises(ArgumentError, match="fixed column SRID or an explicit SRID"):
            wkb_processor(bytes.fromhex(EWKB_HEX))

    def test_geom_from_ewkb_defaulted_bindparam_preserves_key_and_processor(self):
        source_bind = bindparam("wkb", bytes.fromhex(EWKB_HEX))
        expr = func.ST_GeomFromEWKB(source_bind)
        compiled_expr = expr.compile(dialect=self.dialect())
        wkb_processor = compiled_expr._bind_processors["wkb"]
        override = bytes.fromhex(WKB_HEX)

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(unhex(%s), 4326)"
        assert compiled_expr.params == {"wkb": bytes.fromhex(EWKB_HEX)}
        assert compiled_expr.construct_params({"wkb": override}) == {"wkb": override}
        assert wkb_processor(compiled_expr.params["wkb"]) == WKB_HEX
        assert wkb_processor(bytes.fromhex(EWKB_HEX)) == WKB_HEX
        with pytest.raises(ArgumentError, match=r"column \(4326\)"):
            wkb_processor(bytes.fromhex(WEB_MERCATOR_EWKB_HEX))

    def test_geom_from_ewkb_fixed_srid_bindparam_strips_runtime_ewkb(self):
        source_bind = bindparam("wkb")
        expr = func.ST_GeomFromEWKB(source_bind, type_=Geometry(srid=3857))
        compiled_expr = expr.compile(dialect=self.dialect())

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(unhex(%s), 3857)"
        assert set(compiled_expr.params) == {"wkb"}
        assert compiled_expr._bind_processors["wkb"](bytes.fromhex(WKB_HEX)) == WKB_HEX
        assert compiled_expr._bind_processors["wkb"](bytes.fromhex(ZERO_SRID_EWKB_HEX)) == WKB_HEX
        with pytest.raises(ArgumentError, match=r"column \(3857\)"):
            compiled_expr._bind_processors["wkb"](bytes.fromhex(EWKB_HEX))

    @pytest.mark.parametrize("srids", [(3857, 4326), (4326, 3857)])
    def test_geom_from_ewkb_reused_fixed_srid_bind_validates_all_contexts(self, srids):
        source_bind = bindparam("wkb")
        stmt = select(
            [
                func.ST_GeomFromEWKB(source_bind, type_=Geometry(srid=srids[0])),
                func.ST_GeomFromEWKB(source_bind, type_=Geometry(srid=srids[1])),
            ]
        )
        compiled_expr = stmt.compile(dialect=self.dialect())
        compiled = self.normalize_sql(compiled_expr)
        wkb_processor = compiled_expr._bind_processors["wkb"]

        assert f"ST_GeomFromWKB(unhex(%s), {srids[0]})" in compiled
        assert f"ST_GeomFromWKB(unhex(%s), {srids[1]})" in compiled
        assert wkb_processor(bytes.fromhex(WKB_HEX)) == WKB_HEX
        with pytest.raises(ArgumentError, match=r"column \(3857\)"):
            wkb_processor(bytes.fromhex(EWKB_HEX))

    def test_geom_from_ewkb_runtime_bind_without_srid_rejects_embedded_srid(self):
        expr = func.ST_GeomFromEWKB(bindparam("wkb"))
        compiled_expr = expr.compile(dialect=self.dialect())
        wkb_processor = compiled_expr._bind_processors["wkb"]

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(unhex(%s))"
        assert wkb_processor(bytes.fromhex(WKB_HEX)) == WKB_HEX
        with pytest.raises(ArgumentError, match="fixed column SRID or an explicit SRID"):
            wkb_processor(bytes.fromhex(EWKB_HEX))
        with pytest.raises(ArgumentError, match="fixed column SRID or an explicit SRID"):
            wkb_processor(WKBElement(bytes.fromhex(WKB_HEX), srid=4326))

    def test_geom_from_ewkb_explicit_srid_bindparam_strips_runtime_ewkb(self):
        expr = func.ST_GeomFromEWKB(bindparam("wkb"), bindparam("srid"))
        compiled_expr = expr.compile(dialect=self.dialect())

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(unhex(%s), %s)"
        assert set(compiled_expr.params) == {"wkb", "srid"}
        assert compiled_expr._bind_processors["wkb"](bytes.fromhex(EWKB_HEX)) == WKB_HEX


class TestRaster:
    def test_get_col_spec(self):
        r = Raster()
        assert r.get_col_spec() == "raster"

    def test_column_expression(self, raster_table):
        s = select([raster_table.c.rast])
        eq_sql(s, 'SELECT raster("table".rast) AS rast FROM "table"')

    def test_insert_bind_expression(self, raster_table):
        i = insert(raster_table).values(rast=b"\x01\x02")
        eq_sql(i, 'INSERT INTO "table" (rast) VALUES (raster(:rast))')
        assert i.compile().params == {"rast": b"\x01\x02"}

    def test_function_call(self, raster_table):
        s = select([raster_table.c.rast.ST_Height()])
        eq_sql(s, 'SELECT ST_Height("table".rast) AS "ST_Height_1" FROM "table"')

    def test_non_ST_function_call(self, raster_table):
        with pytest.raises(AttributeError):
            raster_table.c.geom.Height()


class TestCompositeType:
    def test_ST_Dump(self, geography_table):
        s = select([func.ST_Dump(geography_table.c.geom).geom.label("geom")])

        eq_sql(s, 'SELECT ST_AsEWKB((ST_Dump("table".geom)).geom) AS geom FROM "table"')
