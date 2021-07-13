import re
import pytest

from sqlalchemy import Table, MetaData, Column, select
from geoalchemy2.types import Geometry


def eq_sql(a, b):
    a = re.sub(r'[\n\t]', '', str(a))
    assert a == b


@pytest.fixture
def geometry_table():
    table = Table('table', MetaData(), Column('geom', Geometry))
    return table


class TestOperator():

    def test_eq(self, geometry_table):
        expr = geometry_table.c.geom == 'POINT(1 2)'
        eq_sql(expr, '"table".geom = ST_GeomFromEWKT(:geom_1)')

    def test_eq_with_None(self, geometry_table):
        expr = geometry_table.c.geom == None  # NOQA
        eq_sql(expr, '"table".geom IS NULL')

    def test_ne(self, geometry_table):
        expr = geometry_table.c.geom != 'POINT(1 2)'
        eq_sql(expr, '"table".geom != ST_GeomFromEWKT(:geom_1)')

    def test_ne_with_None(self, geometry_table):
        expr = geometry_table.c.geom != None  # NOQA
        eq_sql(expr, '"table".geom IS NOT NULL')

    def test_intersects(self, geometry_table):
        expr = geometry_table.c.geom.intersects('POINT(1 2)')
        eq_sql(expr, '"table".geom && ST_GeomFromEWKT(:geom_1)')

    def test_overlaps_or_to_left(self, geometry_table):
        expr = geometry_table.c.geom.overlaps_or_to_left('POINT(1 2)')
        eq_sql(expr, '"table".geom &< ST_GeomFromEWKT(:geom_1)')

    def test_overlaps_or_below(self, geometry_table):
        expr = geometry_table.c.geom.overlaps_or_below('POINT(1 2)')
        eq_sql(expr, '"table".geom &<| ST_GeomFromEWKT(:geom_1)')

    def test_overlaps_or_to_right(self, geometry_table):
        expr = geometry_table.c.geom.overlaps_or_to_right('POINT(1 2)')
        eq_sql(expr, '"table".geom &> ST_GeomFromEWKT(:geom_1)')

    def test_to_left(self, geometry_table):
        expr = geometry_table.c.geom.to_left('POINT(1 2)')
        eq_sql(expr, '"table".geom << ST_GeomFromEWKT(:geom_1)')

    def test_lshift(self, geometry_table):
        expr = geometry_table.c.geom << 'POINT(1 2)'
        eq_sql(expr, '"table".geom << ST_GeomFromEWKT(:geom_1)')

    def test_below(self, geometry_table):
        expr = geometry_table.c.geom.below('POINT(1 2)')
        eq_sql(expr, '"table".geom <<| ST_GeomFromEWKT(:geom_1)')

    def test_to_right(self, geometry_table):
        expr = geometry_table.c.geom.to_right('POINT(1 2)')
        eq_sql(expr, '"table".geom >> ST_GeomFromEWKT(:geom_1)')

    def test_rshift(self, geometry_table):
        expr = geometry_table.c.geom >> 'POINT(1 2)'
        eq_sql(expr, '"table".geom >> ST_GeomFromEWKT(:geom_1)')

    def test_contained(self, geometry_table):
        expr = geometry_table.c.geom.contained('POINT(1 2)')
        eq_sql(expr, '"table".geom @ ST_GeomFromEWKT(:geom_1)')

    def test_overlaps_or_above(self, geometry_table):
        expr = geometry_table.c.geom.overlaps_or_above('POINT(1 2)')
        eq_sql(expr, '"table".geom |&> ST_GeomFromEWKT(:geom_1)')

    def test_above(self, geometry_table):
        expr = geometry_table.c.geom.above('POINT(1 2)')
        eq_sql(expr, '"table".geom |>> ST_GeomFromEWKT(:geom_1)')

    def test_contains(self, geometry_table):
        expr = geometry_table.c.geom.contains('POINT(1 2)')
        eq_sql(expr, '"table".geom ~ ST_GeomFromEWKT(:geom_1)')

    def test_same(self, geometry_table):
        expr = geometry_table.c.geom.same('POINT(1 2)')
        eq_sql(expr, '"table".geom ~= ST_GeomFromEWKT(:geom_1)')

    def test_distance_centroid(self, geometry_table):
        expr = geometry_table.c.geom.distance_centroid('POINT(1 2)')
        eq_sql(expr, '"table".geom <-> ST_GeomFromEWKT(:geom_1)')

    def test_distance_centroid_select(self, geometry_table):
        s = geometry_table.select().order_by(
            geometry_table.c.geom.distance_centroid('POINT(1 2)')).limit(10)
        eq_sql(s, 'SELECT ST_AsEWKB("table".geom) AS geom '
                  'FROM "table" '
                  'ORDER BY "table".geom <-> ST_GeomFromEWKT(:geom_1) '
                  'LIMIT :param_1')
        assert s.compile().params == {u'geom_1': 'POINT(1 2)', u'param_1': 10}

    def test_distance_centroid_select_with_label(self, geometry_table):
        s = select([geometry_table.c.geom.distance_centroid('POINT(1 2)').
                    label('dc')])
        s = s.order_by('dc').limit(10)
        eq_sql(s, 'SELECT "table".geom <-> ST_GeomFromEWKT(:geom_1) AS dc '
                  'FROM "table" ORDER BY dc LIMIT :param_1')
        assert s.compile().params == {u'geom_1': 'POINT(1 2)', u'param_1': 10}

    def test_distance_box(self, geometry_table):
        expr = geometry_table.c.geom.distance_box('POINT(1 2)')
        eq_sql(expr, '"table".geom <#> ST_GeomFromEWKT(:geom_1)')

    def test_distance_box_select(self, geometry_table):
        s = geometry_table.select().order_by(
            geometry_table.c.geom.distance_box('POINT(1 2)')).limit(10)
        eq_sql(s, 'SELECT ST_AsEWKB("table".geom) AS geom '
                  'FROM "table" '
                  'ORDER BY "table".geom <#> ST_GeomFromEWKT(:geom_1) '
                  'LIMIT :param_1')
        assert s.compile().params == {u'geom_1': 'POINT(1 2)', u'param_1': 10}

    def test_distance_box_select_with_label(self, geometry_table):
        s = select([geometry_table.c.geom.distance_box('POINT(1 2)').
                    label('dc')])
        s = s.order_by('dc').limit(10)
        eq_sql(s, 'SELECT "table".geom <#> ST_GeomFromEWKT(:geom_1) AS dc '
                  'FROM "table" ORDER BY dc LIMIT :param_1')
        assert s.compile().params == {u'geom_1': 'POINT(1 2)', u'param_1': 10}

    def test_intersects_nd(self, geometry_table):
        expr = geometry_table.c.geom.intersects_nd(
            "Box3D(ST_GeomFromEWKT('LINESTRING(1 2 3, 3 4 5, 5 6 5)'));")
        eq_sql(expr, '"table".geom &&& ST_GeomFromEWKT(:geom_1)')
