import pytest
import re

from sqlalchemy import Table, MetaData, Column
from sqlalchemy.sql import select, insert, func, text
from geoalchemy2.types import Geometry, Geography, Raster
from geoalchemy2.exc import ArgumentError


def eq_sql(a, b):
    a = re.sub(r'[\n\t]', '', str(a))
    assert a == b


@pytest.fixture
def geometry_table():
    table = Table('table', MetaData(), Column('geom', Geometry))
    return table


@pytest.fixture
def geography_table():
    table = Table('table', MetaData(), Column('geom', Geography))
    return table


@pytest.fixture
def raster_table():
    table = Table('table', MetaData(), Column('rast', Raster))
    return table


class TestGeometry():

    def test_get_col_spec(self):
        g = Geometry(srid=900913)
        assert g.get_col_spec() == 'geometry(GEOMETRY,900913)'

    def test_get_col_spec_no_typmod(self):
        g = Geometry(geometry_type=None)
        assert g.get_col_spec() == 'geometry'

    def test_check_ctor_args_bad_srid(self):
        with pytest.raises(ArgumentError):
            Geometry(srid='foo')

    def test_get_col_spec_geometryzm(self):
        g = Geometry(geometry_type='GEOMETRYZM', srid=900913)
        assert g.get_col_spec() == 'geometry(GEOMETRYZM,900913)'

    def test_get_col_spec_geometryz(self):
        g = Geometry(geometry_type='GEOMETRYZ', srid=900913)
        assert g.get_col_spec() == 'geometry(GEOMETRYZ,900913)'

    def test_get_col_spec_geometrym(self):
        g = Geometry(geometry_type='GEOMETRYM', srid=900913)
        assert g.get_col_spec() == 'geometry(GEOMETRYM,900913)'

    def test_check_ctor_args_management_zm(self):
        with pytest.raises(ArgumentError):
            Geometry(geometry_type='POINTZM', management=True)

    def test_check_ctor_args_management_z(self):
        with pytest.raises(ArgumentError):
            Geometry(geometry_type='POINTZ', dimension=2, management=True)

    def test_check_ctor_args_management_m(self):
        with pytest.raises(ArgumentError):
            Geometry(geometry_type='POINTM', dimension=2, management=True)

    def test_check_ctor_args_incompatible_arguments(self):
        with pytest.raises(ArgumentError):
            Geometry(geometry_type=None, management=True)

    def test_check_ctor_args_srid_not_enforced(self):
        with pytest.warns(UserWarning):
            Geometry(geometry_type=None, srid=4326)

    def test_check_ctor_args_use_typmod_ignored(self):
        with pytest.warns(UserWarning):
            Geometry(management=False, use_typmod=True)

    def test_check_ctor_args_use_typmod_nullable(self):
        with pytest.raises(ArgumentError):
            Geometry(use_typmod=True, nullable=False)

    def test_column_expression(self, geometry_table):
        s = select([geometry_table.c.geom])
        eq_sql(s, 'SELECT ST_AsEWKB("table".geom) AS geom FROM "table"')

    def test_select_bind_expression(self, geometry_table):
        s = select([text('foo')]).where(geometry_table.c.geom == 'POINT(1 2)')
        eq_sql(s, 'SELECT foo FROM "table" WHERE '
                  '"table".geom = ST_GeomFromEWKT(:geom_1)')
        assert s.compile().params == {'geom_1': 'POINT(1 2)'}

    def test_insert_bind_expression(self, geometry_table):
        i = insert(geometry_table).values(geom='POINT(1 2)')
        eq_sql(i, 'INSERT INTO "table" (geom) VALUES (ST_GeomFromEWKT(:geom))')
        assert i.compile().params == {'geom': 'POINT(1 2)'}

    def test_function_call(self, geometry_table):
        s = select([geometry_table.c.geom.ST_Buffer(2)])
        eq_sql(s,
               'SELECT ST_AsEWKB(ST_Buffer("table".geom, :ST_Buffer_2)) '
               'AS "ST_Buffer_1" FROM "table"')

    def test_non_ST_function_call(self, geometry_table):
        with pytest.raises(AttributeError):
            geometry_table.c.geom.Buffer(2)

    def test_subquery(self, geometry_table):
        # test for geometry columns not delivered to the result
        # http://hg.sqlalchemy.org/sqlalchemy/rev/f1efb20c6d61
        from sqlalchemy.sql import select
        s = select([geometry_table]).alias('name').select()
        eq_sql(s,
               'SELECT ST_AsEWKB(name.geom) AS geom FROM '
               '(SELECT "table".geom AS geom FROM "table") AS name')


class TestGeography():

    def test_get_col_spec(self):
        g = Geography(srid=900913)
        assert g.get_col_spec() == 'geography(GEOMETRY,900913)'

    def test_get_col_spec_no_typmod(self):
        g = Geography(geometry_type=None)
        assert g.get_col_spec() == 'geography'

    def test_column_expression(self, geography_table):
        s = select([geography_table.c.geom])
        eq_sql(s, 'SELECT ST_AsBinary("table".geom) AS geom FROM "table"')

    def test_select_bind_expression(self, geography_table):
        s = select([text('foo')]).where(geography_table.c.geom == 'POINT(1 2)')
        eq_sql(s, 'SELECT foo FROM "table" WHERE '
                  '"table".geom = ST_GeogFromText(:geom_1)')
        assert s.compile().params == {'geom_1': 'POINT(1 2)'}

    def test_insert_bind_expression(self, geography_table):
        i = insert(geography_table).values(geom='POINT(1 2)')
        eq_sql(i, 'INSERT INTO "table" (geom) VALUES (ST_GeogFromText(:geom))')
        assert i.compile().params == {'geom': 'POINT(1 2)'}

    def test_function_call(self, geography_table):
        s = select([geography_table.c.geom.ST_Buffer(2)])
        eq_sql(s,
               'SELECT ST_AsEWKB(ST_Buffer("table".geom, :ST_Buffer_2)) '
               'AS "ST_Buffer_1" FROM "table"')

    def test_non_ST_function_call(self, geography_table):
        with pytest.raises(AttributeError):
            geography_table.c.geom.Buffer(2)

    def test_subquery(self, geography_table):
        # test for geography columns not delivered to the result
        # http://hg.sqlalchemy.org/sqlalchemy/rev/f1efb20c6d61
        s = select([geography_table]).alias('name').select()
        eq_sql(s,
               'SELECT ST_AsBinary(name.geom) AS geom FROM '
               '(SELECT "table".geom AS geom FROM "table") AS name')


class TestPoint():

    def test_get_col_spec(self):
        g = Geometry(geometry_type='POINT', srid=900913)
        assert g.get_col_spec() == 'geometry(POINT,900913)'


class TestCurve():

    def test_get_col_spec(self):
        g = Geometry(geometry_type='CURVE', srid=900913)
        assert g.get_col_spec() == 'geometry(CURVE,900913)'


class TestLineString():

    def test_get_col_spec(self):
        g = Geometry(geometry_type='LINESTRING', srid=900913)
        assert g.get_col_spec() == 'geometry(LINESTRING,900913)'


class TestPolygon():

    def test_get_col_spec(self):
        g = Geometry(geometry_type='POLYGON', srid=900913)
        assert g.get_col_spec() == 'geometry(POLYGON,900913)'


class TestMultiPoint():

    def test_get_col_spec(self):
        g = Geometry(geometry_type='MULTIPOINT', srid=900913)
        assert g.get_col_spec() == 'geometry(MULTIPOINT,900913)'


class TestMultiLineString():

    def test_get_col_spec(self):
        g = Geometry(geometry_type='MULTILINESTRING', srid=900913)
        assert g.get_col_spec() == 'geometry(MULTILINESTRING,900913)'


class TestMultiPolygon():

    def test_get_col_spec(self):
        g = Geometry(geometry_type='MULTIPOLYGON', srid=900913)
        assert g.get_col_spec() == 'geometry(MULTIPOLYGON,900913)'


class TestGeometryCollection():

    def test_get_col_spec(self):
        g = Geometry(geometry_type='GEOMETRYCOLLECTION', srid=900913)
        assert g.get_col_spec() == 'geometry(GEOMETRYCOLLECTION,900913)'


class TestRaster():

    def test_get_col_spec(self):
        r = Raster()
        assert r.get_col_spec() == 'raster'

    def test_column_expression(self, raster_table):
        s = select([raster_table.c.rast])
        eq_sql(s, 'SELECT raster("table".rast) AS rast FROM "table"')

    def test_insert_bind_expression(self, raster_table):
        i = insert(raster_table).values(rast=b'\x01\x02')
        eq_sql(i, 'INSERT INTO "table" (rast) VALUES (raster(:rast))')
        assert i.compile().params == {'rast': b'\x01\x02'}

    def test_function_call(self, raster_table):
        s = select([raster_table.c.rast.ST_Height()])
        eq_sql(s,
               'SELECT ST_Height("table".rast) '
               'AS "ST_Height_1" FROM "table"')

    def test_non_ST_function_call(self, raster_table):

        with pytest.raises(AttributeError):
            raster_table.c.geom.Height()


class TestCompositeType():

    def test_ST_Dump(self, geography_table):
        s = select([func.ST_Dump(geography_table.c.geom).geom.label("geom")])

        eq_sql(s,
               'SELECT ST_AsEWKB((ST_Dump("table".geom)).geom) AS geom '
               'FROM "table"')
