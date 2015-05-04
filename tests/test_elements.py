import re
import pytest

from sqlalchemy import Table, MetaData, Column, String, func
from geoalchemy2.types import Geometry
from geoalchemy2.elements import (
    WKTElement, WKBElement, RasterElement, CompositeElement
)


@pytest.fixture
def geometry_table():
    table = Table('table', MetaData(), Column('geom', Geometry))
    return table


def eq_sql(a, b):
    a = re.sub(r'[\n\t]', '', str(a))
    assert a == b


class TestWKTElement():

    def test_desc(self):
        e = WKTElement('POINT(1 2)')
        assert e.desc == 'POINT(1 2)'

    def test_function_call(self):
        e = WKTElement('POINT(1 2)')
        f = e.ST_Buffer(2)
        eq_sql(f, 'ST_Buffer('
               'ST_GeomFromText(:ST_GeomFromText_1, :ST_GeomFromText_2), '
               ':param_1)')
        assert f.compile().params == {
            u'param_1': 2,
            u'ST_GeomFromText_1': 'POINT(1 2)',
            u'ST_GeomFromText_2': -1
        }

    def test_compare(self):
        # Two identical points
        e0 = WKTElement('POINT(1 2)')
        e1 = WKTElement('POINT(1 2)')
        assert e0 == e1

        # A diffenent point
        e2 = WKTElement('POINT(2 1)')
        assert e0 != e2
        
        # Same point but wiht different projections, should not be equivalent
        e3 = WKTElement('POINT(1 2)', srid=4326)
        assert e0 != e3

        # Same points of the same SRID
        e4 = WKTElement('POINT(1 2)', srid=4326)
        assert e3 == e4

        # Can make safe comparisons to None (aka NULL value)
        assert (e0 == None) == False


class TestWKTElementFunction():

    def test_ST_Equal_WKTElement_WKTElement(self):
        expr = func.ST_Equals(WKTElement('POINT(1 2)'),
                              WKTElement('POINT(1 2)'))
        eq_sql(expr, 'ST_Equals('
               'ST_GeomFromText(:ST_GeomFromText_1, :ST_GeomFromText_2), '
               'ST_GeomFromText(:ST_GeomFromText_3, :ST_GeomFromText_4))')
        assert expr.compile().params == {
            u'ST_GeomFromText_1': 'POINT(1 2)',
            u'ST_GeomFromText_2': -1,
            u'ST_GeomFromText_3': 'POINT(1 2)',
            u'ST_GeomFromText_4': -1,
        }

    def test_ST_Equal_Column_WKTElement(self, geometry_table):
        expr = func.ST_Equals(geometry_table.c.geom, WKTElement('POINT(1 2)'))
        eq_sql(expr,
               'ST_Equals("table".geom, '
               'ST_GeomFromText(:ST_GeomFromText_1, :ST_GeomFromText_2))')
        assert expr.compile().params == {
            u'ST_GeomFromText_1': 'POINT(1 2)',
            u'ST_GeomFromText_2': -1
        }


class TestWKBElement():

    def test_desc(self):
        e = WKBElement(b'\x01\x02')
        assert e.desc == b'0102'

    def test_function_call(self):
        e = WKBElement(b'\x01\x02')
        f = e.ST_Buffer(2)
        eq_sql(f, 'ST_Buffer('
               'ST_GeomFromWKB(:ST_GeomFromWKB_1, :ST_GeomFromWKB_2), '
               ':param_1)')
        assert f.compile().params == {
            u'param_1': 2, u'ST_GeomFromWKB_1': b'\x01\x02',
            u'ST_GeomFromWKB_2': -1
        }

    def test_compare(self):
        # Two identical points
        e0 = WKBElement(b'\x01\x02')
        e1 = WKBElement(b'\x01\x02')
        assert e0 == e1

        # A diffenent point
        e2 = WKBElement(b'\x02\x01')
        assert e0 != e2
        
        # Same point but wiht different projections, should not be equivalent
        e3 = WKBElement(b'\x01\x02', srid=4326)
        assert e0 != e3

        # Same points of the same SRID
        e4 = WKBElement(b'\x01\x02', srid=4326)
        assert e3 == e4

        # Can make safe comparisons to None (aka NULL value)
        assert (e0 == None) == False



class TestRasterElement():

    def test_desc(self):
        e = RasterElement(b'\x01\x02')
        assert e.desc == b'0102'

    def test_function_call(self):
        e = RasterElement(b'\x01\x02')
        f = e.ST_Height()
        eq_sql(f, 'ST_Height(:raster_1::raster)')
        assert f.compile().params == {u'raster_1': b'\x01\x02'}


class TestCompositeElement():

    def test_compile(self):
        # text fixture
        metadata = MetaData()
        foo = Table('foo', metadata, Column('one', String))

        e = CompositeElement(foo.c.one, 'geom', String)
        assert str(e) == '(foo.one).geom'
