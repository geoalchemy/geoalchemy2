from geoalchemy2.compat import buffer, bytes, str
from geoalchemy2.elements import WKBElement, WKTElement
from geoalchemy2.shape import from_shape, to_shape

import shapely.wkb
from shapely.geometry import Point


def test_to_shape_WKBElement():
    # POINT(1 2)
    e = WKBElement(b'\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00'
                   b'\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@')
    s = to_shape(e)
    assert isinstance(s, Point)
    assert s.x == 1
    assert s.y == 2


def test_to_shape_WKBElement_str():
    # POINT(1 2)
    e = WKBElement(str('0101000000000000000000f03f0000000000000040'))
    s = to_shape(e)
    assert isinstance(s, Point)
    assert s.x == 1
    assert s.y == 2


def test_to_shape_ExtendedWKBElement():
    # SRID=3857;POINT(1 2 3)
    e = WKBElement(b'\x01\x01\x00\x00\xa0\x11\x0f\x00\x00\x00'
                   b'\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00'
                   b'\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x08@',
                   extended=True)
    s = to_shape(e)
    assert isinstance(s, Point)
    assert s.x == 1
    assert s.y == 2
    assert s.z == 3


def test_to_shape_WKTElement():
    e = WKTElement('POINT(1 2)')
    s = to_shape(e)
    assert isinstance(s, Point)
    assert s.x == 1
    assert s.y == 2


def test_from_shape():
    p = Point(1, 2)
    e = from_shape(p)
    assert isinstance(e, WKBElement)
    assert isinstance(e.data, buffer)

    s = shapely.wkb.loads(bytes(e.data))
    assert isinstance(s, Point)
    assert p.equals(p)
