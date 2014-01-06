from nose.tools import ok_, eq_
from geoalchemy2.compat import buffer, bytes


def test_to_shape_WKBElement():
    from geoalchemy2.elements import WKBElement
    from geoalchemy2.shape import to_shape
    from shapely.geometry import Point

    e = WKBElement('\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00'
                   '\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@')
    s = to_shape(e)
    ok_(isinstance(s, Point))
    eq_(s.x, 1)
    eq_(s.y, 2)


def test_to_shape_WKTElement():
    from geoalchemy2.elements import WKTElement
    from geoalchemy2.shape import to_shape
    from shapely.geometry import Point

    e = WKTElement('POINT(1 2)')
    s = to_shape(e)
    ok_(isinstance(s, Point))
    eq_(s.x, 1)
    eq_(s.y, 2)


def test_from_shape():
    from geoalchemy2.elements import WKBElement
    from geoalchemy2.shape import from_shape
    import shapely.wkb
    from shapely.geometry import Point

    p = Point(1, 2)
    e = from_shape(p)
    ok_(isinstance(e, WKBElement))
    ok_(isinstance(e.data, buffer))
    s = shapely.wkb.loads(bytes(e.data))
    ok_(isinstance(s, Point))
    ok_(p.equals(p))
