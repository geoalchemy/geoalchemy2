import re
import weakref

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
from sqlalchemy.sql import update

from geoalchemy2.admin.dialects import mariadb as _mariadb_admin  # noqa: F401
from geoalchemy2.admin.dialects import mysql as _mysql_admin  # noqa: F401
from geoalchemy2.admin.dialects import postgresql as _postgresql_admin  # noqa: F401
from geoalchemy2.admin.dialects import sqlite as _sqlite_admin  # noqa: F401
from geoalchemy2.admin.dialects.geopackage import GeoPackageDialect
from geoalchemy2.elements import WKBElement
from geoalchemy2.exc import ArgumentError
from geoalchemy2.types import Geography
from geoalchemy2.types import Geometry
from geoalchemy2.types import Raster
from geoalchemy2.types.dialects import geopackage as geopackage_type
from geoalchemy2.types.dialects import mariadb as mariadb_type
from geoalchemy2.types.dialects import mysql as mysql_type
from geoalchemy2.types.dialects import postgresql as postgresql_type
from geoalchemy2.types.dialects import sqlite as sqlite_type

from . import select

WKB_HEX = "0101000000000000000000f03f0000000000000040"
EWKB_HEX = "0101000020e6100000000000000000f03f0000000000000040"


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
        compiled = self.normalize_sql(compiled_expr)

        assert compiled == "ST_GeomFromWKB(%s, 4326)"
        assert compiled_expr.params == {"param_1": bytes.fromhex(WKB_HEX)}

    def test_geom_from_ewkb_literal_compile_strips_ewkb_srid(self):
        expr = func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX), type_=Geometry(srid=4326))

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

    @pytest.mark.parametrize(
        ("runtime_value", "expected_wkb", "expected_srid"),
        [
            (bytes.fromhex(EWKB_HEX), bytes.fromhex(WKB_HEX), 4326),
            (memoryview(bytes.fromhex(EWKB_HEX)), bytes.fromhex(WKB_HEX), 4326),
            (EWKB_HEX, bytes.fromhex(WKB_HEX), 4326),
            (WKBElement(bytes.fromhex(EWKB_HEX), extended=True), bytes.fromhex(WKB_HEX), 4326),
            (WKBElement(bytes.fromhex(WKB_HEX), srid=3857), bytes.fromhex(WKB_HEX), 3857),
            (bytes.fromhex(WKB_HEX), bytes.fromhex(WKB_HEX), 0),
            (None, None, 0),
        ],
    )
    def test_geom_from_ewkb_dynamic_bindparam_processes_runtime_values(
        self,
        runtime_value,
        expected_wkb,
        expected_srid,
    ):
        source_bind = bindparam("wkb")
        expr = func.ST_GeomFromEWKB(source_bind)
        compiled_expr = expr.compile(dialect=mysql.dialect())
        compiled = self.normalize_sql(compiled_expr)
        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(source_bind)

        assert compiled == "ST_GeomFromWKB(%s, %s)"
        assert set(compiled_expr.params) == {wkb_key, srid_key}
        wkb_processor = compiled_expr._bind_processors[wkb_key]
        srid_processor = compiled_expr._bind_processors[srid_key]

        processed_wkb = wkb_processor(runtime_value)
        assert processed_wkb == expected_wkb
        assert srid_processor(runtime_value) == expected_srid

    def test_geom_from_ewkb_dynamic_bindparam_uses_type_srid_for_plain_wkb(self):
        source_bind = bindparam("wkb")
        expr = func.ST_GeomFromEWKB(source_bind, type_=Geometry(srid=3857))
        compiled_expr = expr.compile(dialect=mysql.dialect())
        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(
            source_bind,
            default_srid=3857,
        )

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(%s, %s)"
        assert compiled_expr._bind_processors[wkb_key](bytes.fromhex(WKB_HEX)) == bytes.fromhex(
            WKB_HEX
        )
        assert compiled_expr._bind_processors[srid_key](bytes.fromhex(WKB_HEX)) == 3857

    def test_geom_from_ewkb_dynamic_bindparam_with_explicit_srid_literal(self):
        expr = func.ST_GeomFromEWKB(bindparam("wkb"), 3857)
        compiled_expr = expr.compile(dialect=mysql.dialect())

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(%s, 3857)"
        assert compiled_expr._bind_processors["wkb"](bytes.fromhex(EWKB_HEX)) == bytes.fromhex(
            WKB_HEX
        )

    def test_geom_from_ewkb_dynamic_bindparam_with_explicit_srid_bindparam(self):
        expr = func.ST_GeomFromEWKB(bindparam("wkb"), bindparam("srid"))
        compiled_expr = expr.compile(dialect=mysql.dialect())

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(%s, %s)"
        assert set(compiled_expr.params) == {"wkb", "srid"}
        assert compiled_expr._bind_processors["wkb"](bytes.fromhex(EWKB_HEX)) == bytes.fromhex(
            WKB_HEX
        )

    @pytest.mark.parametrize("srid", [0, 3857])
    def test_geom_from_ewkb_dynamic_bindparam_with_defaulted_srid_bindparam(self, srid):
        expr = func.ST_GeomFromEWKB(bindparam("wkb"), bindparam("srid", srid))
        compiled_expr = expr.compile(dialect=mysql.dialect())

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(%s, %s)"
        assert set(compiled_expr.params) == {"wkb", "srid"}
        assert compiled_expr.params["srid"] == srid
        assert (
            compiled_expr.construct_params(params={"wkb": bytes.fromhex(EWKB_HEX), "srid": 4326})[
                "srid"
            ]
            == 4326
        )

    @pytest.mark.parametrize(
        ("statement_factory", "param_key"),
        [
            (lambda table: insert(table), "geom"),
            (lambda table: insert(table).values(geom=bindparam("wkb")), "wkb"),
            (lambda table: update(table).values(geom=bindparam("wkb")), "wkb"),
            (
                lambda table: update(table).ordered_values((table.c.geom, bindparam("wkb"))),
                "wkb",
            ),
        ],
    )
    def test_before_execute_expands_dml_generated_dynamic_ewkb_binds(
        self,
        statement_factory,
        param_key,
    ):
        class Conn:
            dialect = mysql.dialect()

        table = Table(
            "lake",
            MetaData(),
            Column("geom", Geometry(srid=3857, from_text="ST_GeomFromEWKB")),
        )
        stmt = statement_factory(table)
        source_bind = bindparam(param_key)
        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(
            source_bind,
            default_srid=3857,
        )
        ewkb = bytes.fromhex(EWKB_HEX)

        _, _, expanded_params = _mysql_admin.before_execute(
            Conn(),
            stmt,
            (),
            {param_key: ewkb},
            {},
        )
        compiled = stmt.compile(dialect=mysql.dialect())

        assert expanded_params[wkb_key] is ewkb
        assert expanded_params[srid_key] is ewkb
        assert compiled.construct_params(params=expanded_params)[wkb_key] is ewkb

    def test_before_execute_expands_multivalue_dml_generated_dynamic_ewkb_binds(self):
        class Conn:
            dialect = mysql.dialect()

        table = Table(
            "lake",
            MetaData(),
            Column("geom", Geometry(srid=3857, from_text="ST_GeomFromEWKB")),
        )
        stmt = insert(table).values(
            [
                {"geom": bindparam("wkb1")},
                {"geom": bindparam("wkb2")},
            ]
        )
        ewkb1 = bytes.fromhex(EWKB_HEX)
        ewkb2 = memoryview(bytes.fromhex(EWKB_HEX))
        params = {"wkb1": ewkb1, "wkb2": ewkb2}

        clauseelement, _, expanded_params = _mysql_admin.before_execute(
            Conn(),
            stmt,
            (),
            params,
            {},
        )
        compiled = clauseelement.compile(dialect=mysql.dialect())

        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(
            bindparam("wkb1"),
            default_srid=3857,
        )
        assert expanded_params[wkb_key] is ewkb1
        assert expanded_params[srid_key] is ewkb1
        assert compiled.construct_params(params=expanded_params)[wkb_key] is ewkb1
        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(
            bindparam("wkb2"),
            default_srid=3857,
        )
        assert expanded_params[wkb_key] is ewkb2
        assert expanded_params[srid_key] is ewkb2
        assert compiled.construct_params(params=expanded_params)[wkb_key] is ewkb2
        assert self.normalize_sql(compiled).count("ST_GeomFromWKB") == 2

    def test_before_execute_expands_dynamic_ewkb_params_without_mutating_source(self):
        class Conn:
            dialect = mysql.dialect()

        source_bind = bindparam("wkb")
        stmt = select([func.ST_GeomFromEWKB(source_bind, type_=Geometry(srid=3857))])
        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(
            source_bind,
            default_srid=3857,
        )
        ewkb = bytes.fromhex(EWKB_HEX)
        params = {"wkb": ewkb}

        _, multiparams, expanded_params = _mysql_admin.before_execute(Conn(), stmt, (), params, {})

        assert multiparams == ()
        assert params == {"wkb": ewkb}
        assert expanded_params["wkb"] is ewkb
        assert expanded_params[wkb_key] is ewkb
        assert expanded_params[srid_key] is ewkb

    def test_before_execute_skips_non_wkb_runtime_params(self, monkeypatch):
        class Conn:
            dialect = mysql.dialect()

        def collect_source_binds(clauseelement):
            raise AssertionError("non-WKB params should not traverse the statement")

        monkeypatch.setattr(
            _mysql_admin,
            "_collect_mysql_dynamic_ewkb_source_binds",
            collect_source_binds,
        )
        stmt = select([bindparam("id")])
        params = {"id": 1}

        result = _mysql_admin.before_execute(Conn(), stmt, (), params, {})

        assert result == (stmt, (), params)

    def test_before_execute_skips_text_clause(self, monkeypatch):
        class Conn:
            dialect = mysql.dialect()

        def collect_source_binds(clauseelement):
            raise AssertionError("text clauses should not traverse the statement")

        monkeypatch.setattr(
            _mysql_admin,
            "_collect_mysql_dynamic_ewkb_source_binds",
            collect_source_binds,
        )
        stmt = text("SELECT :wkb")
        params = {"wkb": bytes.fromhex(EWKB_HEX)}

        result = _mysql_admin.before_execute(Conn(), stmt, (), params, {})

        assert result == (stmt, (), params)

    def test_before_execute_caches_dynamic_bind_discovery(self, monkeypatch):
        class Conn:
            dialect = mysql.dialect()

        calls = []

        def collect_source_binds(clauseelement):
            calls.append(clauseelement)
            return ()

        monkeypatch.setattr(
            _mysql_admin,
            "_MYSQL_DYNAMIC_EWKB_SOURCE_BIND_CACHE",
            weakref.WeakKeyDictionary(),
        )
        monkeypatch.setattr(
            _mysql_admin,
            "_collect_mysql_dynamic_ewkb_source_binds_uncached",
            collect_source_binds,
        )
        stmt = select([bindparam("blob")])
        params = {"blob": None}

        _mysql_admin.before_execute(Conn(), stmt, (), params, {})
        _mysql_admin.before_execute(Conn(), stmt, (), params, {})

        assert calls == [stmt]

    def test_before_execute_avoids_statement_compile_for_named_bindparam(self, monkeypatch):
        class Conn:
            dialect = mysql.dialect()

        def compile_bind_name_map(clauseelement, dialect):
            raise AssertionError("named bindparams should not need a statement compile")

        monkeypatch.setattr(
            _mysql_admin,
            "_compile_mysql_statement_bind_name_map",
            compile_bind_name_map,
        )
        source_bind = bindparam("wkb")
        stmt = select([func.ST_GeomFromEWKB(source_bind)])
        ewkb = bytes.fromhex(EWKB_HEX)

        _, _, expanded_params = _mysql_admin.before_execute(
            Conn(),
            stmt,
            (),
            {"wkb": ewkb},
            {},
        )

        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(source_bind)
        assert expanded_params[wkb_key] is ewkb
        assert expanded_params[srid_key] is ewkb

    def test_before_execute_wraps_multivalue_dml_without_runtime_params(self):
        class Conn:
            dialect = mysql.dialect()

        table = Table(
            "lake",
            MetaData(),
            Column("geom", Geometry(srid=3857, from_text="ST_GeomFromEWKB")),
        )
        stmt = insert(table).values([{"geom": bindparam("wkb", bytes.fromhex(EWKB_HEX))}])

        clauseelement, multiparams, params = _mysql_admin.before_execute(
            Conn(),
            stmt,
            (),
            {},
            {},
        )

        assert multiparams == ()
        assert params == {}
        assert "ST_GeomFromWKB" in self.normalize_sql(
            clauseelement.compile(dialect=mysql.dialect())
        )

    def test_before_execute_expands_defaulted_user_bindparam_override(self):
        class Conn:
            dialect = mysql.dialect()

        source_bind = bindparam("wkb", bytes.fromhex(WKB_HEX))
        stmt = select([func.ST_GeomFromEWKB(source_bind)])
        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(source_bind)
        ewkb = bytes.fromhex(EWKB_HEX)

        _, _, expanded_params = _mysql_admin.before_execute(
            Conn(),
            stmt,
            (),
            {"wkb": ewkb},
            {},
        )

        assert expanded_params[wkb_key] is ewkb
        assert expanded_params[srid_key] is ewkb

    def test_before_execute_expands_dynamic_ewkb_multiparams(self):
        class Conn:
            dialect = mysql.dialect()

        source_bind = bindparam("wkb")
        stmt = select([func.ST_GeomFromEWKB(source_bind)])
        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(source_bind)
        ewkb = bytes.fromhex(EWKB_HEX)
        wkb = bytes.fromhex(WKB_HEX)
        multiparams = ({"wkb": ewkb}, {"wkb": wkb})

        _, expanded_multiparams, expanded_params = _mysql_admin.before_execute(
            Conn(),
            stmt,
            multiparams,
            {},
            {},
        )

        assert expanded_params == {}
        assert multiparams == ({"wkb": ewkb}, {"wkb": wkb})
        assert expanded_multiparams[0][wkb_key] is ewkb
        assert expanded_multiparams[0][srid_key] is ewkb
        assert expanded_multiparams[1][wkb_key] is wkb
        assert expanded_multiparams[1][srid_key] is wkb

    def test_before_execute_expands_dynamic_ewkb_unique_compiled_name(self):
        class Conn:
            dialect = mysql.dialect()

        source_bind = bindparam("wkb", unique=True)
        stmt = select([func.ST_GeomFromEWKB(source_bind)])
        bind_name_map = _mysql_admin._compile_mysql_statement_bind_name_map(stmt, Conn.dialect)
        compiled_name = bind_name_map[_mysql_admin._mysql_dynamic_ewkb_bind_identifier(source_bind)]
        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(source_bind)
        ewkb = bytes.fromhex(EWKB_HEX)

        _, _, expanded_params = _mysql_admin.before_execute(
            Conn(),
            stmt,
            (),
            {compiled_name: ewkb},
            {},
        )

        assert expanded_params[wkb_key] is ewkb
        assert expanded_params[srid_key] is ewkb

    def test_before_execute_expands_reused_dynamic_ewkb_bind_for_distinct_default_srids(self):
        class Conn:
            dialect = mysql.dialect()

        source_bind = bindparam("wkb")
        stmt = select(
            [
                func.ST_GeomFromEWKB(source_bind, type_=Geometry(srid=4326)),
                func.ST_GeomFromEWKB(source_bind, type_=Geometry(srid=3857)),
            ]
        )
        wkb_key_4326, srid_key_4326 = _mysql_admin._mysql_dynamic_ewkb_bind_keys(
            source_bind,
            default_srid=4326,
        )
        wkb_key_3857, srid_key_3857 = _mysql_admin._mysql_dynamic_ewkb_bind_keys(
            source_bind,
            default_srid=3857,
        )
        ewkb = bytes.fromhex(EWKB_HEX)

        _, _, expanded_params = _mysql_admin.before_execute(
            Conn(),
            stmt,
            (),
            {"wkb": ewkb},
            {},
        )

        assert expanded_params[wkb_key_4326] is ewkb
        assert expanded_params[srid_key_4326] is ewkb
        assert expanded_params[wkb_key_3857] is ewkb
        assert expanded_params[srid_key_3857] is ewkb

    def test_before_execute_leaves_already_expanded_dynamic_ewkb_params_unchanged(self):
        class Conn:
            dialect = mysql.dialect()

        source_bind = bindparam("wkb")
        stmt = select([func.ST_GeomFromEWKB(source_bind)])
        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(source_bind)
        ewkb = bytes.fromhex(EWKB_HEX)
        params = {"wkb": ewkb, wkb_key: ewkb, srid_key: ewkb}

        _, multiparams, expanded_params = _mysql_admin.before_execute(Conn(), stmt, (), params, {})

        assert multiparams == ()
        assert expanded_params is params

    def test_before_execute_ignores_auto_constructor_ewkb_bind(self):
        class Conn:
            dialect = mysql.dialect()

        stmt = select([func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX))])
        params = {}

        _, multiparams, expanded_params = _mysql_admin.before_execute(Conn(), stmt, (), params, {})

        assert multiparams == ()
        assert expanded_params is params

    def test_bind_processor_converts_wkbelement_for_wkb_constructor(self):
        spatial_type = Geometry(srid=4326, from_text="ST_GeomFromWKB")

        assert mysql_type.bind_processor_process(
            spatial_type, WKBElement(bytes.fromhex(WKB_HEX), srid=4326)
        ) == bytes.fromhex(WKB_HEX)

    def test_bind_processor_strips_ewkb_for_ewkb_constructor(self):
        spatial_type = Geometry(srid=4326, from_text="ST_GeomFromEWKB")

        assert mysql_type.bind_processor_process(
            spatial_type, WKBElement(bytes.fromhex(EWKB_HEX), extended=True)
        ) == bytes.fromhex(WKB_HEX)


class TestMariaDBWKBConstructors:
    @staticmethod
    def normalize_sql(sql):
        return re.sub(r"\s+", " ", str(sql)).strip()

    def test_geom_from_ewkb_compiles_to_supported_wkb_constructor(self):
        expr = func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX), type_=Geometry(srid=4326))

        compiled_expr = expr.compile(dialect=mariadb_dialect.MariaDBDialect())
        compiled = self.normalize_sql(compiled_expr)

        assert compiled == "ST_GeomFromWKB(unhex(%s), 4326)"
        assert compiled_expr.params == {"param_1": WKB_HEX}

    def test_geom_from_ewkb_literal_compile_strips_ewkb_srid(self):
        expr = func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX), type_=Geometry(srid=4326))

        compiled = self.normalize_sql(
            expr.compile(
                dialect=mariadb_dialect.MariaDBDialect(), compile_kwargs={"literal_binds": True}
            )
        )

        assert compiled == f"ST_GeomFromWKB(unhex('{WKB_HEX}'), 4326)"

    def test_geom_from_wkb_omits_unknown_srid(self):
        expr = func.ST_GeomFromWKB(bytes.fromhex(WKB_HEX), -1)

        compiled_expr = expr.compile(dialect=mariadb_dialect.MariaDBDialect())
        compiled_literal = expr.compile(
            dialect=mariadb_dialect.MariaDBDialect(), compile_kwargs={"literal_binds": True}
        )

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(unhex(%s))"
        assert self.normalize_sql(compiled_literal) == f"ST_GeomFromWKB(unhex('{WKB_HEX}'))"

    def test_wkbelement_literal_compile_omits_unknown_srid(self):
        query = select([func.ST_AsText(WKBElement(bytes.fromhex(WKB_HEX)))])

        compiled = self.normalize_sql(
            query.compile(
                dialect=mariadb_dialect.MariaDBDialect(),
                compile_kwargs={"literal_binds": True},
            )
        )

        assert f"ST_GeomFromWKB(unhex('{WKB_HEX}'))" in compiled
        assert ", -1" not in compiled

    def test_geom_from_ewkb_prefers_embedded_srid_over_return_type_srid(self):
        expr = func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX), type_=Geometry(srid=3857))

        compiled = self.normalize_sql(expr.compile(dialect=mariadb_dialect.MariaDBDialect()))

        assert compiled == "ST_GeomFromWKB(unhex(%s), 4326)"

    def test_geom_from_ewkb_uses_embedded_srid_without_return_type_srid(self):
        expr = func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX))

        compiled = self.normalize_sql(expr.compile(dialect=mariadb_dialect.MariaDBDialect()))

        assert compiled == "ST_GeomFromWKB(unhex(%s), 4326)"

    @pytest.mark.parametrize(
        ("runtime_value", "expected_wkb", "expected_srid"),
        [
            (bytes.fromhex(EWKB_HEX), WKB_HEX, 4326),
            (memoryview(bytes.fromhex(EWKB_HEX)), WKB_HEX, 4326),
            (EWKB_HEX, WKB_HEX, 4326),
            (WKBElement(bytes.fromhex(EWKB_HEX), extended=True), WKB_HEX, 4326),
            (WKBElement(bytes.fromhex(WKB_HEX), srid=3857), WKB_HEX, 3857),
            (bytes.fromhex(WKB_HEX), WKB_HEX, 0),
            (None, None, 0),
        ],
    )
    def test_geom_from_ewkb_dynamic_bindparam_processes_runtime_values(
        self,
        runtime_value,
        expected_wkb,
        expected_srid,
    ):
        source_bind = bindparam("wkb")
        expr = func.ST_GeomFromEWKB(source_bind)
        compiled_expr = expr.compile(dialect=mariadb_dialect.MariaDBDialect())
        compiled = self.normalize_sql(compiled_expr)
        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(source_bind)

        assert compiled == "ST_GeomFromWKB(unhex(%s), %s)"
        assert set(compiled_expr.params) == {wkb_key, srid_key}
        wkb_processor = compiled_expr._bind_processors[wkb_key]
        srid_processor = compiled_expr._bind_processors[srid_key]

        assert wkb_processor(runtime_value) == expected_wkb
        assert srid_processor(runtime_value) == expected_srid

    def test_geom_from_ewkb_dynamic_bindparam_uses_type_srid_for_plain_wkb(self):
        source_bind = bindparam("wkb")
        expr = func.ST_GeomFromEWKB(source_bind, type_=Geometry(srid=3857))
        compiled_expr = expr.compile(dialect=mariadb_dialect.MariaDBDialect())
        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(
            source_bind,
            default_srid=3857,
        )

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(unhex(%s), %s)"
        assert compiled_expr._bind_processors[wkb_key](bytes.fromhex(WKB_HEX)) == WKB_HEX
        assert compiled_expr._bind_processors[srid_key](bytes.fromhex(WKB_HEX)) == 3857

    def test_geom_from_ewkb_dynamic_bindparam_with_explicit_srid_literal(self):
        expr = func.ST_GeomFromEWKB(bindparam("wkb"), 3857)
        compiled_expr = expr.compile(dialect=mariadb_dialect.MariaDBDialect())

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(unhex(%s), 3857)"
        assert compiled_expr._bind_processors["wkb"](bytes.fromhex(EWKB_HEX)) == WKB_HEX

    def test_geom_from_ewkb_dynamic_bindparam_with_explicit_srid_bindparam(self):
        expr = func.ST_GeomFromEWKB(bindparam("wkb"), bindparam("srid"))
        compiled_expr = expr.compile(dialect=mariadb_dialect.MariaDBDialect())

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(unhex(%s), %s)"
        assert set(compiled_expr.params) == {"wkb", "srid"}
        assert compiled_expr._bind_processors["wkb"](bytes.fromhex(EWKB_HEX)) == WKB_HEX

    @pytest.mark.parametrize("srid", [0, 3857])
    def test_geom_from_ewkb_dynamic_bindparam_with_defaulted_srid_bindparam(self, srid):
        expr = func.ST_GeomFromEWKB(bindparam("wkb"), bindparam("srid", srid))
        compiled_expr = expr.compile(dialect=mariadb_dialect.MariaDBDialect())

        assert self.normalize_sql(compiled_expr) == "ST_GeomFromWKB(unhex(%s), %s)"
        assert set(compiled_expr.params) == {"wkb", "srid"}
        assert compiled_expr.params["srid"] == srid
        assert (
            compiled_expr.construct_params(params={"wkb": bytes.fromhex(EWKB_HEX), "srid": 4326})[
                "srid"
            ]
            == 4326
        )

    @pytest.mark.parametrize(
        ("statement_factory", "param_key"),
        [
            (lambda table: insert(table), "geom"),
            (lambda table: insert(table).values(geom=bindparam("wkb")), "wkb"),
            (lambda table: update(table).values(geom=bindparam("wkb")), "wkb"),
            (
                lambda table: update(table).ordered_values((table.c.geom, bindparam("wkb"))),
                "wkb",
            ),
        ],
    )
    def test_before_execute_expands_dml_generated_dynamic_ewkb_binds(
        self,
        statement_factory,
        param_key,
    ):
        class Conn:
            dialect = mariadb_dialect.MariaDBDialect()

        table = Table(
            "lake",
            MetaData(),
            Column("geom", Geometry(srid=3857, from_text="ST_GeomFromEWKB")),
        )
        stmt = statement_factory(table)
        source_bind = bindparam(param_key)
        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(
            source_bind,
            default_srid=3857,
        )
        ewkb = bytes.fromhex(EWKB_HEX)

        _, _, expanded_params = _mariadb_admin.before_execute(
            Conn(),
            stmt,
            (),
            {param_key: ewkb},
            {},
        )
        compiled = stmt.compile(dialect=mariadb_dialect.MariaDBDialect())

        assert expanded_params[wkb_key] is ewkb
        assert expanded_params[srid_key] is ewkb
        assert compiled.construct_params(params=expanded_params)[wkb_key] is ewkb

    def test_before_execute_expands_multivalue_dml_generated_dynamic_ewkb_binds(self):
        class Conn:
            dialect = mariadb_dialect.MariaDBDialect()

        table = Table(
            "lake",
            MetaData(),
            Column("geom", Geometry(srid=3857, from_text="ST_GeomFromEWKB")),
        )
        stmt = insert(table).values(
            [
                {"geom": bindparam("wkb1")},
                {"geom": bindparam("wkb2")},
            ]
        )
        ewkb1 = bytes.fromhex(EWKB_HEX)
        ewkb2 = memoryview(bytes.fromhex(EWKB_HEX))
        params = {"wkb1": ewkb1, "wkb2": ewkb2}

        clauseelement, _, expanded_params = _mariadb_admin.before_execute(
            Conn(),
            stmt,
            (),
            params,
            {},
        )
        compiled = clauseelement.compile(dialect=mariadb_dialect.MariaDBDialect())

        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(
            bindparam("wkb1"),
            default_srid=3857,
        )
        assert expanded_params[wkb_key] is ewkb1
        assert expanded_params[srid_key] is ewkb1
        assert compiled.construct_params(params=expanded_params)[wkb_key] is ewkb1
        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(
            bindparam("wkb2"),
            default_srid=3857,
        )
        assert expanded_params[wkb_key] is ewkb2
        assert expanded_params[srid_key] is ewkb2
        assert compiled.construct_params(params=expanded_params)[wkb_key] is ewkb2
        assert self.normalize_sql(compiled).count("ST_GeomFromWKB") == 2

    def test_before_execute_expands_dynamic_ewkb_params_for_mariadb(self):
        class Conn:
            dialect = mariadb_dialect.MariaDBDialect()

        source_bind = bindparam("wkb")
        stmt = select([func.ST_GeomFromEWKB(source_bind, type_=Geometry(srid=3857))])
        wkb_key, srid_key = _mysql_admin._mysql_dynamic_ewkb_bind_keys(
            source_bind,
            default_srid=3857,
        )
        ewkb = bytes.fromhex(EWKB_HEX)
        params = {"wkb": ewkb}

        _, multiparams, expanded_params = _mariadb_admin.before_execute(
            Conn(),
            stmt,
            (),
            params,
            {},
        )

        assert multiparams == ()
        assert params == {"wkb": ewkb}
        assert expanded_params[wkb_key] is ewkb
        assert expanded_params[srid_key] is ewkb

    def test_bind_processor_converts_wkbelement_for_wkb_constructor(self):
        spatial_type = Geometry(srid=4326, from_text="ST_GeomFromWKB")

        assert (
            mariadb_type.bind_processor_process(
                spatial_type, WKBElement(bytes.fromhex(WKB_HEX), srid=4326)
            )
            == WKB_HEX
        )

    def test_bind_processor_strips_ewkb_for_ewkb_constructor(self):
        spatial_type = Geometry(srid=4326, from_text="ST_GeomFromEWKB")

        assert (
            mariadb_type.bind_processor_process(
                spatial_type, WKBElement(bytes.fromhex(EWKB_HEX), extended=True)
            )
            == WKB_HEX
        )


class TestPostgreSQLWKBConstructors:
    @staticmethod
    def normalize_sql(sql):
        return re.sub(r"\s+", " ", str(sql)).strip()

    def test_geom_from_ewkb_compile_omits_srid_parameter(self):
        expr = func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX), type_=Geometry(srid=4326))

        compiled = self.normalize_sql(expr.compile(dialect=postgresql.dialect()))

        assert compiled == "ST_GeomFromEWKB(%(ST_GeomFromEWKB_1)s)"

    def test_bind_processor_preserves_wkbelement_for_wkb_constructor(self):
        spatial_type = Geometry(srid=4326, from_text="ST_GeomFromWKB")

        assert postgresql_type.bind_processor_process(
            spatial_type, WKBElement(bytes.fromhex(WKB_HEX), srid=4326)
        ) == bytes.fromhex(WKB_HEX)


class TestSQLiteWKBConstructors:
    @staticmethod
    def normalize_sql(sql):
        return re.sub(r"\s+", " ", str(sql)).strip()

    def test_geom_from_ewkb_compile_omits_srid_parameter(self):
        expr = func.ST_GeomFromEWKB(bytes.fromhex(EWKB_HEX), type_=Geometry(srid=4326))

        compiled = self.normalize_sql(expr.compile(dialect=sqlite.dialect()))

        assert compiled == "GeomFromEWKB(?)"

    def test_bind_processor_preserves_wkbelement_for_wkb_constructor(self):
        spatial_type = Geometry(srid=4326, from_text="ST_GeomFromWKB")

        assert sqlite_type.bind_processor_process(
            spatial_type, WKBElement(bytes.fromhex(WKB_HEX), srid=4326)
        ) == bytes.fromhex(WKB_HEX)


class TestGeoPackage:
    WKB_HEX = WKB_HEX
    EWKB_HEX = EWKB_HEX

    def normalize_sql(self, sql):
        return re.sub(r"\s+", " ", str(sql)).strip()

    def test_geom_from_wkb_literal_compile_keeps_srid(self):
        wkb = bytes.fromhex(self.WKB_HEX)
        expr = func.ST_GeomFromWKB(wkb, 4326)

        compiled = self.normalize_sql(
            expr.compile(dialect=GeoPackageDialect(), compile_kwargs={"literal_binds": True})
        )

        assert compiled == f"GeomFromWKB(unhex('{self.WKB_HEX}'), 4326)"

    def test_geom_from_ewkb_literal_compile_omits_srid(self):
        ewkb = bytes.fromhex(self.EWKB_HEX)
        expr = func.ST_GeomFromEWKB(ewkb, type_=Geometry(srid=4326))

        compiled = self.normalize_sql(
            expr.compile(dialect=GeoPackageDialect(), compile_kwargs={"literal_binds": True})
        )

        assert compiled == f"GeomFromEWKB(unhex('{self.EWKB_HEX}'))"

    def test_geom_from_ewkb_compile_omits_srid_parameter(self):
        ewkb = bytes.fromhex(self.EWKB_HEX)
        expr = func.ST_GeomFromEWKB(ewkb, type_=Geometry(srid=4326))

        compiled = self.normalize_sql(expr.compile(dialect=GeoPackageDialect()))

        assert compiled == "GeomFromEWKB(?)"

    @pytest.mark.parametrize(
        "bindvalue",
        [
            bytes.fromhex(WKB_HEX),
            memoryview(bytes.fromhex(WKB_HEX)),
            WKB_HEX,
            WKBElement(bytes.fromhex(WKB_HEX)),
            WKBElement(WKB_HEX),
        ],
    )
    def test_bind_processor_preserves_wkb_for_wkb_constructor(self, bindvalue):
        wkb = bytes.fromhex(self.WKB_HEX)
        spatial_type = Geometry(srid=4326, from_text="ST_GeomFromWKB")

        assert geopackage_type.bind_processor_process(spatial_type, bindvalue) == wkb

    @pytest.mark.parametrize(
        "bindvalue",
        [
            bytes.fromhex(EWKB_HEX),
            memoryview(bytes.fromhex(EWKB_HEX)),
            EWKB_HEX,
            WKBElement(
                bytes.fromhex(EWKB_HEX),
                extended=True,
            ),
            WKBElement(EWKB_HEX, extended=True),
        ],
    )
    def test_bind_processor_preserves_ewkb_for_ewkb_constructor(self, bindvalue):
        ewkb = bytes.fromhex(self.EWKB_HEX)
        spatial_type = Geometry(srid=4326, from_text="ST_GeomFromEWKB")

        assert geopackage_type.bind_processor_process(spatial_type, bindvalue) == ewkb


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
