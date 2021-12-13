from itertools import permutations
import re
import pytest

from shapely import wkb
from sqlalchemy import Table, MetaData, Column, String, func
from geoalchemy2.types import Geometry
from geoalchemy2.elements import (
    WKTElement, WKBElement, RasterElement, CompositeElement
)
from geoalchemy2.exc import ArgumentError


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
               ':ST_Buffer_1)')
        assert f.compile().params == {
            u'ST_Buffer_1': 2,
            u'ST_GeomFromText_1': 'POINT(1 2)',
            u'ST_GeomFromText_2': -1
        }

    def test_attribute_error(self):
        e = WKTElement('POINT(1 2)')
        assert not hasattr(e, 'foo')

    def test_pickle_unpickle(self):
        import pickle
        e = WKTElement('POINT(1 2)', srid=3, extended=True)
        pickled = pickle.dumps(e)
        unpickled = pickle.loads(pickled)
        assert unpickled.srid == 3
        assert unpickled.extended is True
        assert unpickled.data == 'POINT(1 2)'
        f = unpickled.ST_Buffer(2)
        eq_sql(f, 'ST_Buffer('
               'ST_GeomFromEWKT(:ST_GeomFromEWKT_1), '
               ':ST_Buffer_1)')
        assert f.compile().params == {
            u'ST_Buffer_1': 2,
            u'ST_GeomFromEWKT_1': 'POINT(1 2)',
        }

    def test_eq(self):
        a = WKTElement('POINT(1 2)')
        b = WKTElement('POINT(1 2)')
        assert a == b


class TestExtendedWKTElement():

    _srid = 3857  # expected srid
    _wkt = 'POINT (1 2 3)'  # expected wkt
    _ewkt = 'SRID=3857;POINT (1 2 3)'  # expected ewkt

    def test_desc(self):
        e = WKTElement(self._ewkt, extended=True)
        assert e.desc == self._ewkt

    def test_function_call(self):
        e = WKTElement(self._ewkt, extended=True)
        f = e.ST_Buffer(2)
        eq_sql(f, 'ST_Buffer('
               'ST_GeomFromEWKT(:ST_GeomFromEWKT_1), '
               ':ST_Buffer_1)')
        assert f.compile().params == {
            u'ST_Buffer_1': 2,
            u'ST_GeomFromEWKT_1': self._ewkt
        }

    def test_pickle_unpickle(self):
        import pickle
        e = WKTElement(self._ewkt, extended=True)
        pickled = pickle.dumps(e)
        unpickled = pickle.loads(pickled)
        assert unpickled.srid == self._srid
        assert unpickled.extended is True
        assert unpickled.data == self._ewkt
        f = unpickled.ST_Buffer(2)
        eq_sql(f, 'ST_Buffer('
               'ST_GeomFromEWKT(:ST_GeomFromEWKT_1), '
               ':ST_Buffer_1)')
        assert f.compile().params == {
            u'ST_Buffer_1': 2,
            u'ST_GeomFromEWKT_1': self._ewkt,
        }

    def test_unpack_srid_from_ewkt(self):
        """
        Unpack SRID from WKT struct (when it is not provided as arg)
        to ensure geometry result processor preserves query SRID.
        """
        e = WKTElement(self._ewkt, extended=True)
        assert e.srid == self._srid
        assert e.desc == self._ewkt

    def test_unpack_srid_from_ewkt_forcing_srid(self):
        e = WKTElement(self._ewkt, srid=9999, extended=True)
        assert e.srid == 9999
        assert e.desc == self._ewkt

    def test_unpack_srid_from_bad_ewkt(self):
        with pytest.raises(ArgumentError):
            WKTElement('SRID=BAD SRID;POINT (1 2 3)', extended=True)

    def test_eq(self):
        a = WKTElement(self._ewkt, extended=True)
        b = WKTElement(self._ewkt, extended=True)
        assert a == b


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


class TestExtendedWKTElementFunction():

    def test_ST_Equal_WKTElement_WKTElement(self):
        expr = func.ST_Equals(WKTElement('SRID=3857;POINT(1 2 3)',
                                         extended=True),
                              WKTElement('SRID=3857;POINT(1 2 3)',
                                         extended=True))
        eq_sql(expr, 'ST_Equals('
               'ST_GeomFromEWKT(:ST_GeomFromEWKT_1), '
               'ST_GeomFromEWKT(:ST_GeomFromEWKT_2))')
        assert expr.compile().params == {
            u'ST_GeomFromEWKT_1': 'SRID=3857;POINT(1 2 3)',
            u'ST_GeomFromEWKT_2': 'SRID=3857;POINT(1 2 3)',
        }

    def test_ST_Equal_Column_WKTElement(self, geometry_table):
        expr = func.ST_Equals(geometry_table.c.geom,
                              WKTElement('SRID=3857;POINT(1 2 3)',
                                         extended=True))
        eq_sql(expr,
               'ST_Equals("table".geom, '
               'ST_GeomFromEWKT(:ST_GeomFromEWKT_1))')
        assert expr.compile().params == {
            u'ST_GeomFromEWKT_1': 'SRID=3857;POINT(1 2 3)',
        }


class TestExtendedWKBElement():

    # _bin/_hex computed by following query:
    # SELECT ST_GeomFromEWKT('SRID=3;POINT(1 2)');
    _bin = memoryview(b'\x01\x01\x00\x00 \x03\x00\x00\x00\x00\x00\x00'
                      b'\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@')
    _hex = str('010100002003000000000000000000f03f0000000000000040')
    _srid = 3  # expected srid
    _wkt = 'POINT (1 2)'  # expected wkt

    def test_desc(self):
        e = WKBElement(self._bin, extended=True)
        assert e.desc == self._hex

    def test_desc_str(self):
        e = WKBElement(self._hex)
        assert e.desc == self._hex

    def test_function_call(self):
        e = WKBElement(self._bin, extended=True)
        f = e.ST_Buffer(2)
        eq_sql(f, 'ST_Buffer('
               'ST_GeomFromEWKB(:ST_GeomFromEWKB_1), '
               ':ST_Buffer_1)')
        assert f.compile().params == {
            u'ST_Buffer_1': 2,
            u'ST_GeomFromEWKB_1': self._bin,
        }

    def test_function_str(self):
        e = WKBElement(self._bin, extended=True)
        assert isinstance(str(e), str)

    def test_pickle_unpickle(self):
        import pickle
        e = WKBElement(self._bin, srid=self._srid, extended=True)
        pickled = pickle.dumps(e)
        unpickled = pickle.loads(pickled)
        assert unpickled.srid == self._srid
        assert unpickled.extended is True
        assert unpickled.data == bytes(self._bin)
        f = unpickled.ST_Buffer(2)
        eq_sql(f, 'ST_Buffer('
               'ST_GeomFromEWKB(:ST_GeomFromEWKB_1), '
               ':ST_Buffer_1)')
        assert f.compile().params == {
            u'ST_Buffer_1': 2,
            u'ST_GeomFromEWKB_1': bytes(self._bin),
        }

    def test_unpack_srid_from_bin(self):
        """
        Unpack SRID from WKB struct (when it is not provided as arg)
        to ensure geometry result processor preserves query SRID.
        """
        e = WKBElement(self._bin, extended=True)
        assert e.srid == self._srid
        assert wkb.loads(bytes(e.data)).wkt == self._wkt

    def test_unpack_srid_from_bin_forcing_srid(self):
        e = WKBElement(self._bin, srid=9999, extended=True)
        assert e.srid == 9999
        assert wkb.loads(bytes(e.data)).wkt == self._wkt

    def test_unpack_srid_from_hex(self):
        e = WKBElement(self._hex, extended=True)
        assert e.srid == self._srid
        assert wkb.loads(e.data, hex=True).wkt == self._wkt

    def test_eq(self):
        a = WKBElement(self._bin, extended=True)
        b = WKBElement(self._bin, extended=True)
        assert a == b


class TestWKBElement():

    def test_desc(self):
        e = WKBElement(b'\x01\x02')
        assert e.desc == '0102'

    def test_function_call(self):
        e = WKBElement(b'\x01\x02')
        f = e.ST_Buffer(2)
        eq_sql(f, 'ST_Buffer('
               'ST_GeomFromWKB(:ST_GeomFromWKB_1, :ST_GeomFromWKB_2), '
               ':ST_Buffer_1)')
        assert f.compile().params == {
            u'ST_Buffer_1': 2, u'ST_GeomFromWKB_1': b'\x01\x02',
            u'ST_GeomFromWKB_2': -1
        }

    def test_attribute_error(self):
        e = WKBElement(b'\x01\x02')
        assert not hasattr(e, 'foo')

    def test_function_str(self):
        e = WKBElement(b'\x01\x02')
        assert isinstance(str(e), str)

    def test_eq(self):
        a = WKBElement(b'\x01\x02')
        b = WKBElement(b'\x01\x02')
        assert a == b


class TestNotEqualSpatialElement():

    # _bin/_hex computed by following query:
    # SELECT ST_GeomFromEWKT('SRID=3;POINT(1 2)');
    _ewkb = memoryview(b'\x01\x01\x00\x00 \x03\x00\x00\x00\x00\x00\x00'
                       b'\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@')
    _wkb = wkb.loads(bytes(_ewkb)).wkb
    _hex = str('010100002003000000000000000000f03f0000000000000040')
    _srid = 3
    _wkt = 'POINT (1 2)'
    _ewkt = 'SRID=3;POINT (1 2)'

    def test_eq(self):
        a = WKBElement(self._ewkb, extended=True)
        b = WKBElement(self._wkb, srid=self._srid)
        c = WKTElement(self._wkt, srid=self._srid)
        d = WKTElement(self._ewkt, extended=True)
        e = WKBElement(self._hex, extended=True)
        assert a == a
        assert b == b
        assert c == c
        assert d == d
        assert e == e
        assert a == e and e == a

    def test_neq_other_types(self):
        a = WKBElement(self._ewkb, extended=True)
        b = WKBElement(self._wkb, srid=self._srid)
        c = WKTElement(self._wkt, srid=self._srid)
        d = WKTElement(self._ewkt, extended=True)
        e = WKBElement(self._hex, extended=True)
        all_elements = [a, b, c, d, None, 1, "test"]
        for i, j in permutations(all_elements, 2):
            assert i != j
        for i in all_elements[1:]:
            assert i != e and e != i


class TestRasterElement():

    rast_data = (
        b'\x01\x00\x00\x01\x00\x9a\x99\x99\x99\x99\x99\xc9?\x9a\x99\x99\x99\x99\x99'
        b'\xc9\xbf\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00'
        b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xe6\x10\x00'
        b'\x00\x05\x00\x05\x00D\x00\x01\x01\x01\x01\x01\x01\x01\x01\x01\x00\x01\x01'
        b'\x01\x00\x00\x01\x01\x00\x00\x00\x01\x00\x00\x00\x00')

    hex_rast_data = (
        '01000001009a9999999999c93f9a9999999999c9bf0000000000000000000000000000f03'
        'f00000000000000000000000000000000e610000005000500440001010101010101010100'
        '010101000001010000000100000000')

    def test_desc(self):
        e = RasterElement(self.rast_data)
        assert e.desc == self.hex_rast_data
        assert e.srid == 4326
        e = RasterElement(self.hex_rast_data)
        assert e.desc == self.hex_rast_data
        assert e.srid == 4326

    def test_function_call(self):
        e = RasterElement(self.rast_data)
        f = e.ST_Height()
        eq_sql(f, 'ST_Height(raster(:raster_1))')
        assert f.compile().params == {u'raster_1': self.hex_rast_data}

    def test_pickle_unpickle(self):
        import pickle
        e = RasterElement(self.rast_data)
        assert e.srid == 4326
        assert e.extended is True
        assert e.data == self.hex_rast_data
        pickled = pickle.dumps(e)
        unpickled = pickle.loads(pickled)
        assert unpickled.srid == 4326
        assert unpickled.extended is True
        assert unpickled.data == self.hex_rast_data
        f = unpickled.ST_Height()
        eq_sql(f, 'ST_Height(raster(:raster_1))')
        assert f.compile().params == {
            u'raster_1': self.hex_rast_data,
        }


class TestCompositeElement():

    def test_compile(self):
        # text fixture
        metadata = MetaData()
        foo = Table('foo', metadata, Column('one', String))

        e = CompositeElement(foo.c.one, 'geom', String)
        assert str(e) == '(foo.one).geom'
