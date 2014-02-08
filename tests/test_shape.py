from geoalchemy2.compat import buffer, bytes
from geoalchemy2.elements import WKBElement, WKTElement
from geoalchemy2.shape import from_shape, to_shape

import shapely.wkb
from shapely.geometry import Point


def test_to_shape_WKBElement():
    e = WKBElement(b'\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00'
                   b'\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@')
    s = to_shape(e)
    assert isinstance(s, Point)
    assert s.x == 1
    assert s.y == 2


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
