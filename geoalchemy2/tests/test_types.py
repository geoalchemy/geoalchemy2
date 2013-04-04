import unittest
import re

from nose.tools import eq_, raises


def eq_sql(a, b, msg=None):
    a = re.sub(r'[\n\t]', '', str(a))
    eq_(a, b, msg)


def _create_geometry_table():
    from sqlalchemy import Table, MetaData, Column
    from geoalchemy2.types import Geometry
    table = Table('table', MetaData(), Column('geom', Geometry))
    return table


def _create_geography_table():
    from sqlalchemy import Table, MetaData, Column
    from geoalchemy2.types import Geography
    table = Table('table', MetaData(), Column('geom', Geography))
    return table


def _create_raster_table():
    from sqlalchemy import Table, MetaData, Column
    from geoalchemy2.types import Raster
    table = Table('table', MetaData(), Column('rast', Raster))
    return table


class TestGeometry(unittest.TestCase):

    def test_get_col_spec(self):
        from geoalchemy2 import Geometry
        g = Geometry(srid=900913)
        eq_(g.get_col_spec(), 'geometry(GEOMETRY,900913)')

    def test_column_expression(self):
        from sqlalchemy.sql import select
        table = _create_geometry_table()
        s = select([table.c.geom])
        eq_sql(s, 'SELECT ST_AsBinary("table".geom) AS geom FROM "table"')

    def test_select_bind_expression(self):
        from sqlalchemy.sql import select
        table = _create_geometry_table()
        s = select(['foo']).where(table.c.geom == 'POINT(1 2)')
        eq_sql(s, 'SELECT foo FROM "table" WHERE '
                  '"table".geom = ST_GeomFromText(:geom_1)')
        eq_(s.compile().params, {'geom_1': 'POINT(1 2)'})

    def test_insert_bind_expression(self):
        from sqlalchemy.sql import insert
        table = _create_geometry_table()
        i = insert(table).values(geom='POINT(1 2)')
        eq_sql(i, 'INSERT INTO "table" (geom) VALUES (ST_GeomFromText(:geom))')
        eq_(i.compile().params, {'geom': 'POINT(1 2)'})

    def test_function_call(self):
        from sqlalchemy.sql import select
        table = _create_geometry_table()
        s = select([table.c.geom.ST_Buffer(2)])
        eq_sql(s,
               'SELECT ST_AsBinary(ST_Buffer("table".geom, :param_1)) '
               'AS "ST_Buffer_1" FROM "table"')

    @raises(AttributeError)
    def test_non_ST_function_call(self):
        table = _create_geometry_table()
        table.c.geom.Buffer(2)

    def test_subquery(self):
        # test for geometry columns not delivered to the result
        # http://hg.sqlalchemy.org/sqlalchemy/rev/f1efb20c6d61
        from sqlalchemy.sql import select
        table = _create_geometry_table()
        s = select([table]).alias('name').select()
        eq_sql(s,
               'SELECT ST_AsBinary(name.geom) AS geom FROM '
               '(SELECT "table".geom AS geom FROM "table") AS name')


class TestGeography(unittest.TestCase):

    def test_get_col_spec(self):
        from geoalchemy2 import Geography
        g = Geography(srid=900913)
        eq_(g.get_col_spec(), 'geography(GEOMETRY,900913)')

    def test_column_expression(self):
        from sqlalchemy.sql import select
        table = _create_geography_table()
        s = select([table.c.geom])
        eq_sql(s, 'SELECT ST_AsBinary("table".geom) AS geom FROM "table"')

    def test_select_bind_expression(self):
        from sqlalchemy.sql import select
        table = _create_geography_table()
        s = select(['foo']).where(table.c.geom == 'POINT(1 2)')
        eq_sql(s, 'SELECT foo FROM "table" WHERE '
                  '"table".geom = ST_GeogFromText(:geom_1)')
        eq_(s.compile().params, {'geom_1': 'POINT(1 2)'})

    def test_insert_bind_expression(self):
        from sqlalchemy.sql import insert
        table = _create_geography_table()
        i = insert(table).values(geom='POINT(1 2)')
        eq_sql(i, 'INSERT INTO "table" (geom) VALUES (ST_GeogFromText(:geom))')
        eq_(i.compile().params, {'geom': 'POINT(1 2)'})

    def test_function_call(self):
        from sqlalchemy.sql import select
        table = _create_geography_table()
        s = select([table.c.geom.ST_Buffer(2)])
        eq_sql(s,
               'SELECT ST_AsBinary(ST_Buffer("table".geom, :param_1)) '
               'AS "ST_Buffer_1" FROM "table"')

    @raises(AttributeError)
    def test_non_ST_function_call(self):
        table = _create_geography_table()
        table.c.geom.Buffer(2)

    def test_subquery(self):
        # test for geography columns not delivered to the result
        # http://hg.sqlalchemy.org/sqlalchemy/rev/f1efb20c6d61
        from sqlalchemy.sql import select
        table = _create_geography_table()
        s = select([table]).alias('name').select()
        eq_sql(s,
               'SELECT ST_AsBinary(name.geom) AS geom FROM '
               '(SELECT "table".geom AS geom FROM "table") AS name')


class TestPoint(unittest.TestCase):

    def test_get_col_spec(self):
        from geoalchemy2.types import Geometry
        g = Geometry(geometry_type='POINT', srid=900913)
        eq_(g.get_col_spec(), 'geometry(POINT,900913)')


class TestCurve(unittest.TestCase):

    def test_get_col_spec(self):
        from geoalchemy2.types import Geometry
        g = Geometry(geometry_type='CURVE', srid=900913)
        eq_(g.get_col_spec(), 'geometry(CURVE,900913)')


class TestLineString(unittest.TestCase):

    def test_get_col_spec(self):
        from geoalchemy2.types import Geometry
        g = Geometry(geometry_type='LINESTRING', srid=900913)
        eq_(g.get_col_spec(), 'geometry(LINESTRING,900913)')


class TestPolygon(unittest.TestCase):

    def test_get_col_spec(self):
        from geoalchemy2.types import Geometry
        g = Geometry(geometry_type='POLYGON', srid=900913)
        eq_(g.get_col_spec(), 'geometry(POLYGON,900913)')


class TestMultiPoint(unittest.TestCase):

    def test_get_col_spec(self):
        from geoalchemy2.types import Geometry
        g = Geometry(geometry_type='MULTIPOINT', srid=900913)
        eq_(g.get_col_spec(), 'geometry(MULTIPOINT,900913)')


class TestMultiLineString(unittest.TestCase):

    def test_get_col_spec(self):
        from geoalchemy2.types import Geometry
        g = Geometry(geometry_type='MULTILINESTRING', srid=900913)
        eq_(g.get_col_spec(), 'geometry(MULTILINESTRING,900913)')


class TestMultiPolygon(unittest.TestCase):

    def test_get_col_spec(self):
        from geoalchemy2.types import Geometry
        g = Geometry(geometry_type='MULTIPOLYGON', srid=900913)
        eq_(g.get_col_spec(), 'geometry(MULTIPOLYGON,900913)')


class TestGeometryCollection(unittest.TestCase):

    def test_get_col_spec(self):
        from geoalchemy2.types import Geometry
        g = Geometry(geometry_type='GEOMETRYCOLLECTION', srid=900913)
        eq_(g.get_col_spec(), 'geometry(GEOMETRYCOLLECTION,900913)')


class TestFunction(unittest.TestCase):

    def test_ST_Equal_WKTElement_WKTElement(self):
        from sqlalchemy import func
        from geoalchemy2.elements import WKTElement
        expr = func.ST_Equals(WKTElement('POINT(1 2)'),
                              WKTElement('POINT(1 2)'))
        eq_sql(expr, 'ST_Equals('
               'ST_GeomFromText(:ST_GeomFromText_1, :ST_GeomFromText_2), '
               'ST_GeomFromText(:ST_GeomFromText_3, :ST_GeomFromText_4))')
        eq_(expr.compile().params,
            {u'ST_GeomFromText_1': 'POINT(1 2)',
             u'ST_GeomFromText_2': -1,
             u'ST_GeomFromText_3': 'POINT(1 2)',
             u'ST_GeomFromText_4': -1})

    def test_ST_Equal_Column_WKTElement(self):
        from sqlalchemy import func
        from geoalchemy2.elements import WKTElement
        table = _create_geometry_table()
        expr = func.ST_Equals(table.c.geom, WKTElement('POINT(1 2)'))
        eq_sql(expr,
               'ST_Equals("table".geom, '
               'ST_GeomFromText(:ST_GeomFromText_1, :ST_GeomFromText_2))')
        eq_(expr.compile().params, {u'ST_GeomFromText_1': 'POINT(1 2)',
                                    u'ST_GeomFromText_2': -1})


class TestRaster(unittest.TestCase):

    def test_get_col_spec(self):
        from geoalchemy2 import Raster
        r = Raster()
        eq_(r.get_col_spec(), 'raster')

    def test_column_expression(self):
        from sqlalchemy.sql import select
        table = _create_raster_table()
        s = select([table.c.rast])
        eq_sql(s, 'SELECT "table".rast FROM "table"')

    def test_insert_bind_expression(self):
        from sqlalchemy.sql import insert
        table = _create_raster_table()
        i = insert(table).values(rast=b'\x01\x02')
        eq_sql(i, 'INSERT INTO "table" (rast) VALUES (:rast)')
        eq_(i.compile().params, {'rast': b'\x01\x02'})

    def test_function_call(self):
        from sqlalchemy.sql import select
        table = _create_raster_table()
        s = select([table.c.rast.ST_Height()])
        eq_sql(s,
               'SELECT ST_Height("table".rast) '
               'AS "ST_Height_1" FROM "table"')

    @raises(AttributeError)
    def test_non_ST_function_call(self):
        table = _create_raster_table()
        table.c.geom.Height()


class TestCompositeType(unittest.TestCase):

    def test_ST_Dump(self):
        from sqlalchemy import func
        from sqlalchemy.sql import select

        table = _create_geography_table()
        s = select([func.ST_Dump(table.c.geom).geom])
        eq_sql(s,
               'SELECT ST_AsBinary((ST_Dump("table".geom)).geom) AS geom '
               'FROM "table"')
