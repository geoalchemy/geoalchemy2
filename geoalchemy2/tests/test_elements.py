import unittest
import re

from nose.tools import eq_


def eq_sql(a, b, msg=None):
    a = re.sub(r'[\n\t]', '', str(a))
    eq_(a, b, msg)


class TestWKTElement(unittest.TestCase):

    def test_desc(self):
        from geoalchemy2.elements import WKTElement
        e = WKTElement('POINT(1 2)')
        eq_(e.desc, 'POINT(1 2)')

    def test_function_call(self):
        from geoalchemy2.elements import WKTElement
        e = WKTElement('POINT(1 2)')
        f = e.ST_Buffer(2)
        eq_sql(f, 'ST_Buffer('
               'ST_GeomFromText(:ST_GeomFromText_1, :ST_GeomFromText_2), '
               ':param_1)')
        eq_(f.compile().params,
            {u'param_1': 2, u'ST_GeomFromText_1': 'POINT(1 2)',
             u'ST_GeomFromText_2': -1})


class TestWKBElement(unittest.TestCase):

    def test_desc(self):
        from geoalchemy2.elements import WKBElement
        e = WKBElement(b'\x01\x02')
        eq_(e.desc, b'0102')

    def test_function_call(self):
        from geoalchemy2.elements import WKBElement
        e = WKBElement(b'\x01\x02')
        f = e.ST_Buffer(2)
        eq_sql(f, 'ST_Buffer('
               'ST_GeomFromWKB(:ST_GeomFromWKB_1, :ST_GeomFromWKB_2), '
               ':param_1)')
        eq_(f.compile().params,
            {u'param_1': 2, u'ST_GeomFromWKB_1': b'\x01\x02',
             u'ST_GeomFromWKB_2': -1})


class TestRasterElement(unittest.TestCase):

    def test_desc(self):
        from geoalchemy2.elements import RasterElement
        e = RasterElement(b'\x01\x02')
        eq_(e.desc, b'0102')

    def test_function_call(self):
        from geoalchemy2.elements import RasterElement
        e = RasterElement(b'\x01\x02')
        f = e.ST_Height()
        eq_sql(f, 'ST_Height(:raster_1::raster)')
        eq_(f.compile().params, {u'raster_1': b'\x01\x02'})


class TestCompositeElement(unittest.TestCase):

    def test_compile(self):
        from sqlalchemy import MetaData, Table, Column, String
        from geoalchemy2.elements import CompositeElement

        # text fixture
        metadata = MetaData()
        foo = Table('foo', metadata, Column('one', String))

        e = CompositeElement(foo.c.one, 'geom', String)
        eq_(str(e), '(foo.one).geom')
