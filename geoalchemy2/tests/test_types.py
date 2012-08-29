import unittest
import re

from nose.tools import eq_, raises


def eq_sql(a, b, msg=None):
    a = re.sub(r'[\n\t]', '', str(a))
    eq_(a, b, msg)


def _create_table():
    from sqlalchemy import Table, MetaData, Column
    from geoalchemy2.types import Geometry
    table = Table('table', MetaData(), Column('geom', Geometry))
    return table


class TestGeometry(unittest.TestCase):

    def test_get_col_spec(self):
        from geoalchemy2 import Geometry
        g = Geometry(srid=900913)
        eq_(g.get_col_spec(), 'geometry(GEOMETRY,900913)')

    def test_column_expression(self):
        from sqlalchemy.sql import select
        table = _create_table()
        s = select([table.c.geom])
        eq_sql(s, 'SELECT ST_AsBinary("table".geom) AS geom_1 FROM "table"')

    def test_select_bind_expression(self):
        from sqlalchemy.sql import select
        table = _create_table()
        s = select(['foo']).where(table.c.geom == 'POINT(1 2)')
        eq_sql(s, 'SELECT foo FROM "table" WHERE '
                  '"table".geom = ST_GeomFromText(:geom_1)')
        eq_(s.compile().params, {'geom_1': 'POINT(1 2)'})

    def test_insert_bind_expression(self):
        from sqlalchemy.sql import insert
        table = _create_table()
        i = insert(table).values(geom='POINT(1 2)')
        eq_sql(i, 'INSERT INTO "table" (geom) VALUES (ST_GeomFromText(:geom))')
        eq_(i.compile().params, {'geom': 'POINT(1 2)'})

    def test_function_call(self):
        from sqlalchemy.sql import select
        table = _create_table()
        s = select([table.c.geom.ST_Buffer(2)])
        eq_sql(s,
               'SELECT ST_AsBinary(ST_Buffer("table".geom, :param_1)) '
               'AS "ST_Buffer_1" FROM "table"')

    @raises(AttributeError)
    def test_non_ST_function_call(self):
        table = _create_table()
        table.c.geom.Buffer(2)


class TestPoint(unittest.TestCase):

    def test_get_col_spec(self):
        from geoalchemy2.types import Point
        g = Point(srid=900913)
        eq_(g.get_col_spec(), 'geometry(POINT,900913)')


class TestCurve(unittest.TestCase):

    def test_get_col_spec(self):
        from geoalchemy2.types import Curve
        g = Curve(srid=900913)
        eq_(g.get_col_spec(), 'geometry(CURVE,900913)')


class TestLineString(unittest.TestCase):

    def test_get_col_spec(self):
        from geoalchemy2.types import LineString
        g = LineString(srid=900913)
        eq_(g.get_col_spec(), 'geometry(LINESTRING,900913)')


class TestPolygon(unittest.TestCase):

    def test_get_col_spec(self):
        from geoalchemy2.types import Polygon
        g = Polygon(srid=900913)
        eq_(g.get_col_spec(), 'geometry(POLYGON,900913)')


class TestMultiPoint(unittest.TestCase):

    def test_get_col_spec(self):
        from geoalchemy2.types import MultiPoint
        g = MultiPoint(srid=900913)
        eq_(g.get_col_spec(), 'geometry(MULTIPOINT,900913)')


class TestMultiLineString(unittest.TestCase):

    def test_get_col_spec(self):
        from geoalchemy2.types import MultiLineString
        g = MultiLineString(srid=900913)
        eq_(g.get_col_spec(), 'geometry(MULTILINESTRING,900913)')


class TestMultiPolygon(unittest.TestCase):

    def test_get_col_spec(self):
        from geoalchemy2.types import MultiPolygon
        g = MultiPolygon(srid=900913)
        eq_(g.get_col_spec(), 'geometry(MULTIPOLYGON,900913)')


class TestGeometryCollection(unittest.TestCase):

    def test_get_col_spec(self):
        from geoalchemy2.types import GeometryCollection
        g = GeometryCollection(srid=900913)
        eq_(g.get_col_spec(), 'geometry(GEOMETRYCOLLECTION,900913)')


class TestFunction(unittest.TestCase):

    def test_ST_Equal_WKTElement_WKTElement(self):
        from sqlalchemy import func
        from geoalchemy2.types import WKTElement
        expr = func.ST_Equals(WKTElement('POINT(1 2)'),
                              WKTElement('POINT(1 2)'))
        eq_sql(expr,
               'ST_Equals(ST_GeomFromText(:ST_GeomFromText_1), '
               'ST_GeomFromText(:ST_GeomFromText_2))')
        eq_(expr.compile().params,
            {u'ST_GeomFromText_1': 'POINT(1 2)',
             u'ST_GeomFromText_2': 'POINT(1 2)'})

    def test_ST_Equal_Column_WKTElement(self):
        from sqlalchemy import func
        from geoalchemy2.types import WKTElement
        table = _create_table()
        expr = func.ST_Equals(table.c.geom, WKTElement('POINT(1 2)'))
        eq_sql(expr,
               'ST_Equals("table".geom, '
               'ST_GeomFromText(:ST_GeomFromText_1))')
        eq_(expr.compile().params, {u'ST_GeomFromText_1': 'POINT(1 2)'})


class TestOperator(unittest.TestCase):

    def test_eq(self):
        table = _create_table()
        expr = table.c.geom == 'POINT(1 2)'
        eq_sql(expr, '"table".geom = ST_GeomFromText(:geom_1)')

    def test_eq_with_None(self):
        table = _create_table()
        expr = table.c.geom == None
        eq_sql(expr, '"table".geom IS NULL')

    def test_ne(self):
        table = _create_table()
        expr = table.c.geom != 'POINT(1 2)'
        eq_sql(expr, '"table".geom != ST_GeomFromText(:geom_1)')

    def test_ne_with_None(self):
        table = _create_table()
        expr = table.c.geom != None
        eq_sql(expr, '"table".geom IS NOT NULL')

    def test_intersects(self):
        table = _create_table()
        expr = table.c.geom.intersects('POINT(1 2)')
        eq_sql(expr, '"table".geom && ST_GeomFromText(:geom_1)')

    def test_overlaps_or_left(self):
        table = _create_table()
        expr = table.c.geom.overlaps_or_left('POINT(1 2)')
        eq_sql(expr, '"table".geom &< ST_GeomFromText(:geom_1)')

    def test_overlaps_or_right(self):
        table = _create_table()
        expr = table.c.geom.overlaps_or_right('POINT(1 2)')
        eq_sql(expr, '"table".geom &> ST_GeomFromText(:geom_1)')

    def test_distance_between_points(self):
        table = _create_table()
        expr = table.c.geom.distance_between_points('POINT(1 2)')
        eq_sql(expr, '"table".geom <-> ST_GeomFromText(:geom_1)')
        s = table.select().order_by(
                table.c.geom.distance_between_points('POINT(1 2)')).limit(10)
        eq_sql(s, 'SELECT ST_AsBinary("table".geom) AS geom_1 '
                  'FROM "table" '
                  'ORDER BY "table".geom <-> ST_GeomFromText(:geom_2) '
                  'LIMIT :param_1')
        eq_(s.compile().params, {u'geom_2': 'POINT(1 2)', u'param_1': 10})

    def test_distance_between_bbox(self):
        table = _create_table()
        expr = table.c.geom.distance_between_bbox('POINT(1 2)')
        eq_sql(expr, '"table".geom <#> ST_GeomFromText(:geom_1)')
        s = table.select().order_by(
                table.c.geom.distance_between_bbox('POINT(1 2)')).limit(10)
        eq_sql(s, 'SELECT ST_AsBinary("table".geom) AS geom_1 '
                  'FROM "table" '
                  'ORDER BY "table".geom <#> ST_GeomFromText(:geom_2) '
                  'LIMIT :param_1')
        eq_(s.compile().params, {u'geom_2': 'POINT(1 2)', u'param_1': 10})


class TestWKTElement(unittest.TestCase):

    def test_desc(self):
        from geoalchemy2.types import WKTElement
        e = WKTElement('POINT(1 2)')
        eq_(e.desc, 'POINT(1 2)')


class TestWKBElement(unittest.TestCase):

    def test_desc(self):
        from geoalchemy2.types import WKBElement
        e = WKBElement('\x01\x02')
        eq_(e.desc, '0102')

    def test_function_call(self):
        from geoalchemy2.types import WKBElement
        e = WKBElement('\x01\x02')
        f = e.ST_Buffer(2)
        eq_sql(f,
               'ST_Buffer(ST_GeomFromWKB(:ST_GeomFromWKB_1), :param_1)')
        eq_(f.compile().params,
            {u'ST_GeomFromWKB_1': '\x01\x02', u'param_1': 2})

    @raises(AttributeError)
    def test_non_ST_function_call(self):
        from geoalchemy2.types import WKBElement
        e = WKBElement('\x01\x02')
        e.Buffer(2)
