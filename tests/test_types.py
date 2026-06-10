import re

import pytest
from sqlalchemy import Column
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import bindparam
from sqlalchemy.dialects import mysql
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite
from sqlalchemy.dialects.mysql import mariadb as mariadb_dialect
from sqlalchemy.sql import func
from sqlalchemy.sql import insert
from sqlalchemy.sql import text

from geoalchemy2 import _wkb_wkt
from geoalchemy2._wkb_wkt import is_known_srid
from geoalchemy2.admin.dialects import mariadb as _mariadb_admin  # noqa: F401
from geoalchemy2.admin.dialects import mysql as _mysql_admin  # noqa: F401
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.exc import ArgumentError
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry
from geoalchemy2.types import Raster
from geoalchemy2.types.dialects.common import as_binary_ewkb
from geoalchemy2.types.dialects.common import as_binary_wkb
from geoalchemy2.types.dialects.common import as_ewkb_hex
from geoalchemy2.types.dialects.common import as_wkb_hex
from geoalchemy2.types.dialects.common import is_ewkb_constructor
from geoalchemy2.types.dialects.common import is_wkb_constructor
from geoalchemy2.types.dialects.common import validate_wkb_srid

from . import select

WKB_HEX = "0101000000000000000000f03f0000000000000040"
EWKB_HEX = "0101000020e6100000000000000000f03f0000000000000040"
WEB_MERCATOR_EWKB_HEX = "0101000020110f0000000000000000f03f0000000000000040"
ZERO_SRID_EWKB_HEX = "010100002000000000000000000000f03f0000000000000040"
UNKNOWN_SRID_EWKB_HEX = "0101000020ffffffff000000000000f03f0000000000000040"


class _GeoPackageDialect:
    name = "geopackage"


def eq_sql(a, b):
    a = re.sub(r"[\n\t]", "", str(a))
    assert a == b


def test_split_wkb_srid_treats_strings_as_hex_wkb_only():
    assert _wkb_wkt.split_wkb_srid(EWKB_HEX) == ("POINT (1 2)", 4326)

    with pytest.raises(ValueError):
        _wkb_wkt.split_wkb_srid("SRID=4326;POINT (1 2)")


def test_wkb_srid_can_include_unknown_srid_values():
    assert _wkb_wkt.wkb_srid(ZERO_SRID_EWKB_HEX) is None
    assert _wkb_wkt.wkb_srid(ZERO_SRID_EWKB_HEX, include_unknown=True) == 0
    assert _wkb_wkt.wkb_srid(UNKNOWN_SRID_EWKB_HEX) is None
    assert _wkb_wkt.wkb_srid(UNKNOWN_SRID_EWKB_HEX, include_unknown=True) == -1


@pytest.mark.parametrize(
    ("srid", "expected"),
    [
        (None, False),
        (-1, False),
        (0, False),
        (1, True),
        (4326, True),
        (0xFFFFFFFF, True),
    ],
)
def test_is_known_srid(srid, expected):
    assert is_known_srid(srid) is expected


@pytest.mark.parametrize(
    ("spatial_type", "expected_wkb", "expected_ewkb"),
    [
        (Geometry(), False, False),
        (Geometry(from_text="ST_GeomFromWKB"), True, False),
        (Geometry(from_text="ST_GeomFromEWKB"), True, True),
    ],
)
def test_wkb_constructor_helpers(spatial_type, expected_wkb, expected_ewkb):
    assert is_wkb_constructor(spatial_type) is expected_wkb
    assert is_ewkb_constructor(spatial_type) is expected_ewkb


@pytest.mark.parametrize(
    "bindvalue",
    [
        bytes.fromhex(EWKB_HEX),
        bytearray(bytes.fromhex(EWKB_HEX)),
        memoryview(bytes.fromhex(EWKB_HEX)),
        EWKB_HEX,
        WKBElement(bytes.fromhex(EWKB_HEX), extended=True),
    ],
)
def test_as_binary_wkb_strips_or_validates_srid(bindvalue):
    assert as_binary_wkb(bindvalue, strip_srid=True, column_srid=4326) == bytes.fromhex(WKB_HEX)


def test_as_binary_wkb_validates_column_srid():
    with pytest.raises(ArgumentError, match=r"column \(3857\)"):
        as_binary_wkb(bytes.fromhex(EWKB_HEX), strip_srid=True, column_srid=3857)


@pytest.mark.parametrize(
    "bindvalue",
    [
        bytes.fromhex(EWKB_HEX),
        memoryview(bytes.fromhex(EWKB_HEX)),
        EWKB_HEX,
        WKBElement(bytes.fromhex(EWKB_HEX), extended=True),
    ],
)
def test_as_wkb_hex_strips_or_validates_srid(bindvalue):
    assert as_wkb_hex(bindvalue, column_srid=4326) == WKB_HEX


def test_as_wkb_hex_validates_column_srid():
    with pytest.raises(ArgumentError, match=r"column \(3857\)"):
        as_wkb_hex(bytes.fromhex(EWKB_HEX), column_srid=3857)


@pytest.mark.parametrize(
    ("bindvalue", "column_srid", "expected"),
    [
        (WKBElement(bytes.fromhex(WKB_HEX), srid=4326), None, bytes.fromhex(EWKB_HEX)),
        (bytes.fromhex(WKB_HEX), 4326, bytes.fromhex(EWKB_HEX)),
        (memoryview(bytes.fromhex(WKB_HEX)), 4326, bytes.fromhex(EWKB_HEX)),
        (bytes.fromhex(EWKB_HEX), None, bytes.fromhex(EWKB_HEX)),
    ],
)
def test_as_binary_ewkb_embeds_or_preserves_known_srid(bindvalue, column_srid, expected):
    assert as_binary_ewkb(bindvalue, column_srid=column_srid) == expected


def test_as_binary_ewkb_respects_wkbelement_srid_override_for_ewkb():
    bindvalue = WKBElement(bytes.fromhex(EWKB_HEX), srid=3857, extended=True)
    expected = bindvalue.as_ewkb().data

    assert expected == bytes.fromhex(WEB_MERCATOR_EWKB_HEX)
    assert as_binary_ewkb(bindvalue) == expected


@pytest.mark.parametrize(
    ("bindvalue", "column_srid", "expected"),
    [
        (bytes.fromhex(WKB_HEX), 4326, EWKB_HEX),
        (memoryview(bytes.fromhex(WKB_HEX)), 4326, EWKB_HEX),
        (WKB_HEX, 4326, EWKB_HEX),
        (None, 4326, None),
    ],
)
def test_as_ewkb_hex_embeds_or_preserves_known_srid(bindvalue, column_srid, expected):
    assert as_ewkb_hex(bindvalue, column_srid=column_srid) == expected


def test_as_ewkb_hex_respects_wkbelement_srid_override_for_ewkb():
    bindvalue = WKBElement(bytes.fromhex(EWKB_HEX), srid=3857, extended=True)

    assert as_ewkb_hex(bindvalue) == WEB_MERCATOR_EWKB_HEX


@pytest.mark.parametrize(
    "bindvalue",
    [
        WKBElement(bytes.fromhex(WKB_HEX), srid=4326),
        bytes.fromhex(EWKB_HEX),
    ],
)
def test_as_binary_ewkb_validates_column_srid(bindvalue):
    with pytest.raises(ArgumentError, match=r"column \(3857\)"):
        as_binary_ewkb(bindvalue, column_srid=3857)


def test_validate_wkb_srid_ignores_unknown_srid_values():
    validate_wkb_srid(3857, 4326, has_fixed_srid=False)
    validate_wkb_srid(None, 4326)
    validate_wkb_srid(3857, 0)
    validate_wkb_srid(3857, -1)


@pytest.mark.parametrize(
    ("dialect", "bindvalue", "expected"),
    [
        (mysql.dialect(), bytes.fromhex(EWKB_HEX), bytes.fromhex(WKB_HEX)),
        (mariadb_dialect.MariaDBDialect(), memoryview(bytes.fromhex(EWKB_HEX)), WKB_HEX),
        (postgresql.dialect(), WKB_HEX, bytes.fromhex(EWKB_HEX)),
        (sqlite.dialect(), bytes.fromhex(WKB_HEX), EWKB_HEX),
        (_GeoPackageDialect(), WKB_HEX, EWKB_HEX),
    ],
    ids=["mysql", "mariadb", "postgresql", "sqlite", "geopackage"],
)
def test_ewkb_constructor_bind_processors_use_shared_helpers(dialect, bindvalue, expected):
    bind_processor = Geometry(srid=4326, from_text="ST_GeomFromEWKB").bind_processor(dialect)

    assert bind_processor(bindvalue) == expected


@pytest.mark.parametrize(
    "dialect",
    [
        mysql.dialect(),
        mariadb_dialect.MariaDBDialect(),
        postgresql.dialect(),
        sqlite.dialect(),
        _GeoPackageDialect(),
    ],
    ids=["mysql", "mariadb", "postgresql", "sqlite", "geopackage"],
)
def test_ewkb_constructor_bind_processors_validate_shared_helper_srid(dialect):
    bind_processor = Geometry(srid=4326, from_text="ST_GeomFromEWKB").bind_processor(dialect)

    with pytest.raises(ArgumentError, match=r"column \(4326\)"):
        bind_processor(bytes.fromhex(WEB_MERCATOR_EWKB_HEX))


def _cached_dynamic_ewkb_srid(dialect_module, dialect, stmt, cache, params=None):
    dialect.supports_statement_cache = True
    conn = type("Conn", (), {"dialect": dialect})()
    clauseelement, _, expanded_params = dialect_module.before_execute(
        conn,
        stmt,
        (),
        params or {},
        {},
    )
    compiled, extracted_params, cache_hit = clauseelement._compile_w_cache(
        dialect,
        compiled_cache=cache,
        column_keys=[],
        for_executemany=False,
        schema_translate_map=None,
    )
    final_params = compiled.construct_params(
        params=expanded_params,
        extracted_parameters=extracted_params,
    )
    srid_key = next(key for key in compiled.positiontup if key.endswith("_srid"))
    return cache_hit, compiled._bind_processors[srid_key](final_params[srid_key])


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
        wkb_key, srid_key = compiled_expr.positiontup

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(%s, %s)"
        assert compiled_expr.params == {
            wkb_key: bytes.fromhex(EWKB_HEX),
            srid_key: (bytes.fromhex(EWKB_HEX), 4326),
        }
        assert compiled_expr._bind_processors[wkb_key](bytes.fromhex(EWKB_HEX)) == bytes.fromhex(
            WKB_HEX
        )
        assert compiled_expr._bind_processors[srid_key](bytes.fromhex(EWKB_HEX)) == 4326

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

        compiled_expr = expr.compile(dialect=mysql.dialect())
        srid_key = compiled_expr.positiontup[1]

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(%s, %s)"
        assert compiled_expr._bind_processors[srid_key](bytes.fromhex(EWKB_HEX)) == 4326

    def test_geom_from_ewkb_uses_embedded_srid_without_return_type_srid(self):
        expr = func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX))

        compiled_expr = expr.compile(dialect=mysql.dialect())
        srid_key = compiled_expr.positiontup[1]

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(%s, %s)"
        assert compiled_expr._bind_processors[srid_key](bytes.fromhex(EWKB_HEX)) == 4326

    @pytest.mark.parametrize("srid", [None, -1, 0])
    def test_geom_from_ewkb_omitted_explicit_srid_uses_embedded_srid(self, srid):
        expr = func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX), srid)

        compiled_expr = expr.compile(dialect=mysql.dialect())
        wkb_key, srid_key = compiled_expr.positiontup

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(%s, %s)"
        assert compiled_expr.params == {
            wkb_key: bytes.fromhex(EWKB_HEX),
            srid_key: (bytes.fromhex(EWKB_HEX), 4326),
        }
        assert compiled_expr._bind_processors[wkb_key](bytes.fromhex(EWKB_HEX)) == bytes.fromhex(
            WKB_HEX
        )
        assert compiled_expr._bind_processors[srid_key](bytes.fromhex(EWKB_HEX)) == 4326

    def test_geom_from_ewkb_inferred_srid_bind_keeps_cache_shape(self):
        stmt_4326 = select([func.ST_GeomFromEWKB(bindparam("wkb", bytes.fromhex(EWKB_HEX)))])
        stmt_3857 = select(
            [func.ST_GeomFromEWKB(bindparam("wkb", bytes.fromhex(WEB_MERCATOR_EWKB_HEX)))]
        )

        compiled_4326 = self.normalize_sql(stmt_4326.compile(dialect=mysql.dialect()))
        compiled_3857 = self.normalize_sql(stmt_3857.compile(dialect=mysql.dialect()))

        assert compiled_4326 == compiled_3857
        assert "ST_GeomFromWKB(%s, %s)" in compiled_4326
        assert "4326" not in compiled_4326
        assert "3857" not in compiled_3857
        assert stmt_4326._generate_cache_key().key == stmt_3857._generate_cache_key().key

    def test_geom_from_ewkb_cached_literal_values_use_current_srid(self):
        cache = {}
        dialect = mysql.dialect()
        stmt_4326 = select([func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX))])
        stmt_3857 = select([func.ST_GeomFromEWKB(bytes.fromhex(WEB_MERCATOR_EWKB_HEX))])

        _, srid_4326 = _cached_dynamic_ewkb_srid(_mysql_admin, dialect, stmt_4326, cache)
        _, srid_3857 = _cached_dynamic_ewkb_srid(_mysql_admin, dialect, stmt_3857, cache)

        assert len(cache) == 1
        assert srid_4326 == 4326
        assert srid_3857 == 3857

    def test_geom_from_ewkb_cached_defaulted_bindparam_override_uses_current_srid(self):
        cache = {}
        dialect = mysql.dialect()
        stmt = select([func.ST_GeomFromEWKB(bindparam("wkb", bytes.fromhex(EWKB_HEX)))])

        _, default_srid = _cached_dynamic_ewkb_srid(_mysql_admin, dialect, stmt, cache)
        _, override_srid = _cached_dynamic_ewkb_srid(
            _mysql_admin,
            dialect,
            stmt,
            cache,
            params={"wkb": bytes.fromhex(WEB_MERCATOR_EWKB_HEX)},
        )

        assert len(cache) == 1
        assert default_srid == 4326
        assert override_srid == 3857

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

    def test_geom_from_ewkb_defaulted_bindparam_uses_dynamic_srid_processor(self):
        source_bind = bindparam("wkb", bytes.fromhex(EWKB_HEX))
        expr = func.ST_GeomFromEWKB(source_bind)
        compiled_expr = expr.compile(dialect=mysql.dialect())
        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(
            source_bind,
            default_srid=4326,
        )
        wkb_processor = compiled_expr._bind_processors[wkb_key]
        srid_processor = compiled_expr._bind_processors[srid_key]
        override = bytes.fromhex(WKB_HEX)

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(%s, %s)"
        assert compiled_expr.params == {
            wkb_key: bytes.fromhex(EWKB_HEX),
            srid_key: (bytes.fromhex(EWKB_HEX), 4326),
        }
        assert wkb_processor(compiled_expr.params[wkb_key]) == bytes.fromhex(WKB_HEX)
        assert wkb_processor(bytes.fromhex(EWKB_HEX)) == bytes.fromhex(WKB_HEX)
        assert wkb_processor(bytes.fromhex(WEB_MERCATOR_EWKB_HEX)) == bytes.fromhex(WKB_HEX)
        assert srid_processor(bytes.fromhex(EWKB_HEX)) == 4326
        assert srid_processor(bytes.fromhex(WEB_MERCATOR_EWKB_HEX)) == 3857
        assert srid_processor(override) == 4326

    def test_mysql_before_execute_expands_dynamic_ewkb_bindparams(self):
        source_bind = bindparam("wkb", bytes.fromhex(EWKB_HEX))
        stmt = select([func.ST_GeomFromEWKB(source_bind)])
        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(
            source_bind,
            default_srid=4326,
        )
        override = bytes.fromhex(WEB_MERCATOR_EWKB_HEX)

        clauseelement, multiparams, params = _mysql_admin.before_execute(
            type("Conn", (), {"dialect": mysql.dialect()})(),
            stmt,
            (),
            {"wkb": override},
            {},
        )

        assert clauseelement is stmt
        assert multiparams == ()
        assert params == {
            "wkb": override,
            wkb_key: override,
            srid_key: (override, 4326),
        }

    def test_geom_from_ewkb_callable_bind_uses_dynamic_srid_processor(self):
        calls = []
        value = bytes.fromhex(WEB_MERCATOR_EWKB_HEX)

        def get_wkb():
            calls.append("called")
            return value

        source_bind = bindparam("wkb", callable_=get_wkb)
        expr = func.ST_GeomFromEWKB(source_bind)
        compiled_expr = expr.compile(dialect=mysql.dialect())
        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(
            source_bind,
            default_srid=0,
        )

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(%s, %s)"
        assert compiled_expr.construct_params() == {
            wkb_key: value,
            srid_key: (value, 0),
        }
        assert calls == ["called"]
        assert compiled_expr._bind_processors[wkb_key](value) == bytes.fromhex(WKB_HEX)
        assert compiled_expr._bind_processors[srid_key](value) == 3857

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
        wkb_key, srid_key = compiled_expr.positiontup

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(unhex(%s), %s)"
        assert compiled_expr.params == {
            wkb_key: bytes.fromhex(EWKB_HEX),
            srid_key: (bytes.fromhex(EWKB_HEX), 4326),
        }
        assert compiled_expr._bind_processors[wkb_key](bytes.fromhex(EWKB_HEX)) == WKB_HEX
        assert compiled_expr._bind_processors[srid_key](bytes.fromhex(EWKB_HEX)) == 4326

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

        compiled_expr = expr.compile(dialect=self.dialect())
        srid_key = compiled_expr.positiontup[1]

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(unhex(%s), %s)"
        assert compiled_expr._bind_processors[srid_key](bytes.fromhex(EWKB_HEX)) == 4326

    def test_geom_from_ewkb_uses_embedded_srid_without_return_type_srid(self):
        expr = func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX))

        compiled_expr = expr.compile(dialect=self.dialect())
        srid_key = compiled_expr.positiontup[1]

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(unhex(%s), %s)"
        assert compiled_expr._bind_processors[srid_key](bytes.fromhex(EWKB_HEX)) == 4326

    @pytest.mark.parametrize("srid", [None, -1, 0])
    def test_geom_from_ewkb_omitted_explicit_srid_uses_embedded_srid(self, srid):
        expr = func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX), srid)

        compiled_expr = expr.compile(dialect=self.dialect())
        wkb_key, srid_key = compiled_expr.positiontup

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(unhex(%s), %s)"
        assert compiled_expr.params == {
            wkb_key: bytes.fromhex(EWKB_HEX),
            srid_key: (bytes.fromhex(EWKB_HEX), 4326),
        }
        assert compiled_expr._bind_processors[wkb_key](bytes.fromhex(EWKB_HEX)) == WKB_HEX
        assert compiled_expr._bind_processors[srid_key](bytes.fromhex(EWKB_HEX)) == 4326

    def test_geom_from_ewkb_inferred_srid_bind_keeps_cache_shape(self):
        stmt_4326 = select([func.ST_GeomFromEWKB(bindparam("wkb", bytes.fromhex(EWKB_HEX)))])
        stmt_3857 = select(
            [func.ST_GeomFromEWKB(bindparam("wkb", bytes.fromhex(WEB_MERCATOR_EWKB_HEX)))]
        )

        compiled_4326 = self.normalize_sql(stmt_4326.compile(dialect=self.dialect()))
        compiled_3857 = self.normalize_sql(stmt_3857.compile(dialect=self.dialect()))

        assert compiled_4326 == compiled_3857
        assert "ST_GeomFromWKB(unhex(%s), %s)" in compiled_4326
        assert "4326" not in compiled_4326
        assert "3857" not in compiled_3857
        assert stmt_4326._generate_cache_key().key == stmt_3857._generate_cache_key().key

    def test_geom_from_ewkb_cached_literal_values_use_current_srid(self):
        cache = {}
        dialect = self.dialect()
        stmt_4326 = select([func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX))])
        stmt_3857 = select([func.ST_GeomFromEWKB(bytes.fromhex(WEB_MERCATOR_EWKB_HEX))])

        _, srid_4326 = _cached_dynamic_ewkb_srid(_mariadb_admin, dialect, stmt_4326, cache)
        _, srid_3857 = _cached_dynamic_ewkb_srid(_mariadb_admin, dialect, stmt_3857, cache)

        assert len(cache) == 1
        assert srid_4326 == 4326
        assert srid_3857 == 3857

    def test_geom_from_ewkb_cached_defaulted_bindparam_override_uses_current_srid(self):
        cache = {}
        dialect = self.dialect()
        stmt = select([func.ST_GeomFromEWKB(bindparam("wkb", bytes.fromhex(EWKB_HEX)))])

        _, default_srid = _cached_dynamic_ewkb_srid(_mariadb_admin, dialect, stmt, cache)
        _, override_srid = _cached_dynamic_ewkb_srid(
            _mariadb_admin,
            dialect,
            stmt,
            cache,
            params={"wkb": bytes.fromhex(WEB_MERCATOR_EWKB_HEX)},
        )

        assert len(cache) == 1
        assert default_srid == 4326
        assert override_srid == 3857

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

    def test_geom_from_ewkb_defaulted_bindparam_uses_dynamic_srid_processor(self):
        source_bind = bindparam("wkb", bytes.fromhex(EWKB_HEX))
        expr = func.ST_GeomFromEWKB(source_bind)
        compiled_expr = expr.compile(dialect=self.dialect())
        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(
            source_bind,
            default_srid=4326,
        )
        wkb_processor = compiled_expr._bind_processors[wkb_key]
        srid_processor = compiled_expr._bind_processors[srid_key]
        override = bytes.fromhex(WKB_HEX)

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(unhex(%s), %s)"
        assert compiled_expr.params == {
            wkb_key: bytes.fromhex(EWKB_HEX),
            srid_key: (bytes.fromhex(EWKB_HEX), 4326),
        }
        assert wkb_processor(compiled_expr.params[wkb_key]) == WKB_HEX
        assert wkb_processor(bytes.fromhex(EWKB_HEX)) == WKB_HEX
        assert wkb_processor(bytes.fromhex(WEB_MERCATOR_EWKB_HEX)) == WKB_HEX
        assert srid_processor(bytes.fromhex(EWKB_HEX)) == 4326
        assert srid_processor(bytes.fromhex(WEB_MERCATOR_EWKB_HEX)) == 3857
        assert srid_processor(override) == 4326

    def test_mariadb_before_execute_expands_dynamic_ewkb_bindparams(self):
        source_bind = bindparam("wkb", bytes.fromhex(EWKB_HEX))
        stmt = select([func.ST_GeomFromEWKB(source_bind)])
        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(
            source_bind,
            default_srid=4326,
        )
        override = bytes.fromhex(WEB_MERCATOR_EWKB_HEX)

        clauseelement, multiparams, params = _mariadb_admin.before_execute(
            type("Conn", (), {"dialect": self.dialect()})(),
            stmt,
            (),
            {"wkb": override},
            {},
        )

        assert clauseelement is stmt
        assert multiparams == ()
        assert params == {
            "wkb": override,
            wkb_key: override,
            srid_key: (override, 4326),
        }

    def test_geom_from_ewkb_callable_bind_uses_dynamic_srid_processor(self):
        calls = []
        value = bytes.fromhex(WEB_MERCATOR_EWKB_HEX)

        def get_wkb():
            calls.append("called")
            return value

        source_bind = bindparam("wkb", callable_=get_wkb)
        expr = func.ST_GeomFromEWKB(source_bind)
        compiled_expr = expr.compile(dialect=self.dialect())
        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(
            source_bind,
            default_srid=0,
        )

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(unhex(%s), %s)"
        assert compiled_expr.construct_params() == {
            wkb_key: value,
            srid_key: (value, 0),
        }
        assert calls == ["called"]
        assert compiled_expr._bind_processors[wkb_key](value) == WKB_HEX
        assert compiled_expr._bind_processors[srid_key](value) == 3857

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
