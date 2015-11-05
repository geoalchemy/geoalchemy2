from geoalchemy2.compat import buffer, bytes
from geoalchemy2.elements import WKBElement, EWKBElement, WKTElement
from geoalchemy2.shape import from_shape, to_shape

from copy import copy
import shapely.wkb
import shapely.geos
from shapely.geometry import Point


def test_to_shape_WKBElement():
    e = WKBElement(b'\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00'
                   b'\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@')
    s = to_shape(e)
    assert isinstance(s, Point)
    assert s.x == 1
    assert s.y == 2


def test_to_shape_EWKBElement():
    e = EWKBElement(b'\001\001\000\000\200\000\000\000\000\000\000\000'
                    b'\000\000\000\000\000\000\000\360?\000\000\000\000'
                    b'\000\000\000@')
    s = to_shape(e)
    assert isinstance(s, Point)
    assert s.x == 0
    assert s.y == 1
    assert s.z == 2


def test_to_shape_WKTElement():
    e = WKTElement('POINT(1 2)')
    s = to_shape(e)
    assert isinstance(s, Point)
    assert s.x == 1
    assert s.y == 2


def test_from_shape_WKBElement():
    p = Point(1, 2)
    e = from_shape(p)
    assert isinstance(e, WKBElement)
    assert isinstance(e.data, buffer)

    s = shapely.wkb.loads(bytes(e.data))
    assert isinstance(s, Point)
    assert p.equals(p)


def test_from_shape_EWKBElement():
    p = Point(1, 2, 3)
    wkbwriter_defaults = copy(shapely.geos.WKBWriter.defaults)
    e = from_shape(p, use_ewkb=True)
    assert isinstance(e, EWKBElement)
    assert isinstance(e.data, buffer)
    wkbwriter_new_defaults = shapely.geos.WKBWriter.defaults
    # Make sur the WKBWriter defaults have not changed
    for k in wkbwriter_new_defaults.keys():
        assert wkbwriter_defaults[k] == wkbwriter_new_defaults[k]

    s = shapely.wkb.loads(bytes(e.data))
    assert isinstance(s, Point)
    assert p.equals(p)
