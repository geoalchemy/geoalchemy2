import pytest
import shapely.wkb
from shapely.geometry import Point

import geoalchemy2.shape
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import from_shape
from geoalchemy2.shape import to_shape


def test_check_shapely(monkeypatch):

    @geoalchemy2.shape.check_shapely()
    def f():
        return "ok"

    assert f() == "ok"

    with monkeypatch.context() as m:
        m.setattr(geoalchemy2.shape, "HAS_SHAPELY", False)
        with pytest.raises(ImportError):
            f()


def test_to_shape_WKBElement():
    # POINT(1 2)
    e = WKBElement(
        b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00" b"\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@"
    )
    s = to_shape(e)
    assert isinstance(s, Point)
    assert s.x == 1
    assert s.y == 2


def test_to_shape_WKBElement_str():
    # POINT(1 2)
    e = WKBElement(str("0101000000000000000000f03f0000000000000040"))
    s = to_shape(e)
    assert isinstance(s, Point)
    assert s.x == 1
    assert s.y == 2


def test_to_shape_ExtendedWKBElement():
    # SRID=3857;POINT(1 2 3)
    e = WKBElement(
        b"\x01\x01\x00\x00\xa0\x11\x0f\x00\x00\x00"
        b"\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00"
        b"\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x08@",
        extended=True,
    )
    s = to_shape(e)
    assert isinstance(s, Point)
    assert s.x == 1
    assert s.y == 2
    assert s.z == 3


def test_to_shape_ExtendedWKTElement():
    e = WKTElement("SRID=3857;POINT(1 2)", extended=True)
    s = to_shape(e)
    assert isinstance(s, Point)
    assert s.x == 1
    assert s.y == 2


def test_to_shape_WKTElement():
    e = WKTElement("POINT(1 2)")
    s = to_shape(e)
    assert isinstance(s, Point)
    assert s.x == 1
    assert s.y == 2


def test_to_shape_wrong_type():
    with pytest.raises(TypeError, match="Only WKBElement and WKTElement objects are supported"):
        to_shape(0)


def test_from_shape():
    # Standard case: POINT(1 2)
    expected = WKBElement(str("0101000000000000000000f03f0000000000000040"))
    p = Point(1, 2)
    e = from_shape(p)
    assert isinstance(e, WKBElement)
    assert isinstance(e.data, memoryview)
    assert e == expected

    s = shapely.wkb.loads(bytes(e.data))
    assert isinstance(s, Point)
    assert s.equals(p)

    # Standard case with SRID: SRID=2145;POINT(1 2)
    expected2 = WKBElement(str("0101000000000000000000f03f0000000000000040"), srid=2154)
    p = Point(1, 2)
    e2 = from_shape(p, srid=2154)
    assert isinstance(e2, WKBElement)
    assert isinstance(e2.data, memoryview)
    assert e2 == expected2

    s2 = shapely.wkb.loads(bytes(e2.data))
    assert isinstance(s2, Point)
    assert s2.equals(p)

    # Extended case: SRID=2145;POINT(1 2)
    expected3 = WKBElement(str("01010000206a080000000000000000f03f0000000000000040"), extended=True)
    e3 = from_shape(p, srid=2154, extended=True)
    assert isinstance(e3, WKBElement)
    assert isinstance(e3.data, memoryview)
    assert e3 == expected3

    s3 = shapely.wkb.loads(bytes(e3.data))
    assert isinstance(s, Point)
    assert s3.equals(p)
