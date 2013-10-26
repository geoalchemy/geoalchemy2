import unittest
import re

from nose.tools import eq_


def eq_sql(a, b, msg=None):
    a = re.sub(r'[\n\t]', '', str(a))
    eq_(a, b, msg)


def _create_geometry_table():
    from sqlalchemy import Table, MetaData, Column
    from geoalchemy2.types import Geometry
    table = Table('table', MetaData(), Column('geom', Geometry))
    return table


class TestOperator(unittest.TestCase):

    def test_eq(self):
        table = _create_geometry_table()
        expr = table.c.geom == 'POINT(1 2)'
        eq_sql(expr, '"table".geom = ST_GeomFromEWKT(:geom_1)')

    def test_eq_with_None(self):
        table = _create_geometry_table()
        expr = table.c.geom == None
        eq_sql(expr, '"table".geom IS NULL')

    def test_ne(self):
        table = _create_geometry_table()
        expr = table.c.geom != 'POINT(1 2)'
        eq_sql(expr, '"table".geom != ST_GeomFromEWKT(:geom_1)')

    def test_ne_with_None(self):
        table = _create_geometry_table()
        expr = table.c.geom != None
        eq_sql(expr, '"table".geom IS NOT NULL')

    def test_intersects(self):
        table = _create_geometry_table()
        expr = table.c.geom.intersects('POINT(1 2)')
        eq_sql(expr, '"table".geom && ST_GeomFromEWKT(:geom_1)')

    def test_overlaps_or_to_left(self):
        table = _create_geometry_table()
        expr = table.c.geom.overlaps_or_to_left('POINT(1 2)')
        eq_sql(expr, '"table".geom &< ST_GeomFromEWKT(:geom_1)')

    def test_overlaps_or_below(self):
        table = _create_geometry_table()
        expr = table.c.geom.overlaps_or_below('POINT(1 2)')
        eq_sql(expr, '"table".geom &<| ST_GeomFromEWKT(:geom_1)')

    def test_overlaps_or_to_right(self):
        table = _create_geometry_table()
        expr = table.c.geom.overlaps_or_to_right('POINT(1 2)')
        eq_sql(expr, '"table".geom &> ST_GeomFromEWKT(:geom_1)')

    def test_to_left(self):
        table = _create_geometry_table()
        expr = table.c.geom.to_left('POINT(1 2)')
        eq_sql(expr, '"table".geom << ST_GeomFromEWKT(:geom_1)')

    def test_lshift(self):
        table = _create_geometry_table()
        expr = table.c.geom << 'POINT(1 2)'
        eq_sql(expr, '"table".geom << ST_GeomFromEWKT(:geom_1)')

    def test_below(self):
        table = _create_geometry_table()
        expr = table.c.geom.below('POINT(1 2)')
        eq_sql(expr, '"table".geom <<| ST_GeomFromEWKT(:geom_1)')

    def test_to_right(self):
        table = _create_geometry_table()
        expr = table.c.geom.to_right('POINT(1 2)')
        eq_sql(expr, '"table".geom >> ST_GeomFromEWKT(:geom_1)')

    def test_rshift(self):
        table = _create_geometry_table()
        expr = table.c.geom >> 'POINT(1 2)'
        eq_sql(expr, '"table".geom >> ST_GeomFromEWKT(:geom_1)')

    def test_contained(self):
        table = _create_geometry_table()
        expr = table.c.geom.contained('POINT(1 2)')
        eq_sql(expr, '"table".geom @ ST_GeomFromEWKT(:geom_1)')

    def test_overlaps_or_above(self):
        table = _create_geometry_table()
        expr = table.c.geom.overlaps_or_above('POINT(1 2)')
        eq_sql(expr, '"table".geom |&> ST_GeomFromEWKT(:geom_1)')

    def test_above(self):
        table = _create_geometry_table()
        expr = table.c.geom.above('POINT(1 2)')
        eq_sql(expr, '"table".geom |>> ST_GeomFromEWKT(:geom_1)')

    def test_contains(self):
        table = _create_geometry_table()
        expr = table.c.geom.contains('POINT(1 2)')
        eq_sql(expr, '"table".geom ~ ST_GeomFromEWKT(:geom_1)')

    def test_same(self):
        table = _create_geometry_table()
        expr = table.c.geom.same('POINT(1 2)')
        eq_sql(expr, '"table".geom ~= ST_GeomFromEWKT(:geom_1)')

    def test_distance_centroid(self):
        table = _create_geometry_table()
        expr = table.c.geom.distance_centroid('POINT(1 2)')
        eq_sql(expr, '"table".geom <-> ST_GeomFromEWKT(:geom_1)')
        s = table.select().order_by(
            table.c.geom.distance_centroid('POINT(1 2)')).limit(10)
        eq_sql(s, 'SELECT ST_AsBinary("table".geom) AS geom '
                  'FROM "table" '
                  'ORDER BY "table".geom <-> ST_GeomFromEWKT(:geom_1) '
                  'LIMIT :param_1')
        eq_(s.compile().params, {u'geom_1': 'POINT(1 2)', u'param_1': 10})

    def test_distance_box(self):
        table = _create_geometry_table()
        expr = table.c.geom.distance_box('POINT(1 2)')
        eq_sql(expr, '"table".geom <#> ST_GeomFromEWKT(:geom_1)')
        s = table.select().order_by(
            table.c.geom.distance_box('POINT(1 2)')).limit(10)
        eq_sql(s, 'SELECT ST_AsBinary("table".geom) AS geom '
                  'FROM "table" '
                  'ORDER BY "table".geom <#> ST_GeomFromEWKT(:geom_1) '
                  'LIMIT :param_1')
        eq_(s.compile().params, {u'geom_1': 'POINT(1 2)', u'param_1': 10})
