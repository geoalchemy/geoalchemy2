import re
from itertools import permutations

import pytest
from shapely import wkb
from sqlalchemy import Column
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import func

from geoalchemy2.elements import CompositeElement
from geoalchemy2.elements import RasterElement
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.exc import ArgumentError
from geoalchemy2.types import Geometry


@pytest.fixture
def geometry_table():
    table = Table("table", MetaData(), Column("geom", Geometry))
    return table


def eq_sql(a, b):
    a = re.sub(r"[\n\t]", "", str(a))
    assert a == b


class TestWKTElement:
    _srid = 3857  # expected srid
    _wkt = "POINT (1 2)"  # expected wkt
    _ewkt = "SRID=3857;POINT (1 2)"  # expected ewkt

    def test_ctor(self):
        e1 = WKTElement(self._wkt)
        e2 = WKTElement(self._wkt, extended=False)
        e3 = WKTElement(self._wkt, srid=self._srid)
        e4 = WKTElement(self._wkt, srid=self._srid, extended=True)
        e5 = WKTElement(self._wkt, srid=self._srid, extended=False)
        assert e1.desc == e2.desc == e3.desc == e4.desc == e5.desc == self._wkt
        assert e1.srid == e2.srid == -1
        assert e3.srid == e4.srid == e5.srid == self._srid
        assert e1.extended == e2.extended == e3.extended == (not e4.extended) == e5.extended

    def test_desc(self):
        e = WKTElement(self._wkt)
        assert e.desc == self._wkt

    def test_function_call(self):
        e = WKTElement(self._wkt)
        f = e.ST_Buffer(2)
        eq_sql(
            f,
            "ST_Buffer("
            "ST_GeomFromText(:ST_GeomFromText_1, :ST_GeomFromText_2), "
            ":ST_Buffer_1)",
        )
        assert f.compile().params == {
            "ST_Buffer_1": 2,
            "ST_GeomFromText_1": self._wkt,
            "ST_GeomFromText_2": -1,
        }

    def test_attribute_error(self):
        e = WKTElement(self._wkt)
        assert not hasattr(e, "foo")

    def test_pickle_unpickle(self):
        import pickle

        e = WKTElement(self._wkt, srid=3, extended=False)
        pickled = pickle.dumps(e)
        unpickled = pickle.loads(pickled)
        assert unpickled.srid == 3
        assert unpickled.extended is False
        assert unpickled.data == self._wkt
        f = unpickled.ST_Buffer(2)
        eq_sql(
            f,
            "ST_Buffer("
            "ST_GeomFromText(:ST_GeomFromText_1, :ST_GeomFromText_2), "
            ":ST_Buffer_1)",
        )
        assert f.compile().params == {
            "ST_Buffer_1": 2,
            "ST_GeomFromText_1": self._wkt,
            "ST_GeomFromText_2": 3,
        }

    def test_eq(self):
        a = WKTElement(self._wkt)
        b = WKTElement(self._wkt)
        assert a == b

    def test_hash(self):
        a = WKTElement(self._wkt)
        b = WKTElement("POINT(10 20)")
        c = WKTElement("POINT(10 20)")
        assert set([a, b, c]) == set([a, b, c])
        assert len(set([a, b, c])) == 2

    def test_as_wkt_as_ewkt(self):
        e1 = WKTElement(self._wkt)
        e2 = WKTElement(self._wkt, srid=self._srid)

        # No SRID
        e3 = e1.as_ewkt()
        assert e3.desc == self._wkt
        assert e3.srid == -1
        assert e3.extended is False

        # With SRID
        e4 = e2.as_ewkt()
        assert e4.desc == f"SRID={self._srid};{self._wkt}"
        assert e4.srid == self._srid
        assert e4.extended is True

        assert e3.as_wkt() == e3.as_wkt().as_wkt() == e1
        assert e4.as_wkt() == e4.as_wkt().as_wkt() == e2


class TestExtendedWKTElement:
    _srid = 3857  # expected srid
    _wkt = "POINT (1 2 3)"  # expected wkt
    _ewkt = "SRID=3857;POINT (1 2 3)"  # expected ewkt

    def test_ctor(self):
        arbitrary_srid = self._srid + 1
        e1 = WKTElement(self._ewkt)
        e2 = WKTElement(self._ewkt, extended=True)
        e3 = WKTElement(self._ewkt, srid=arbitrary_srid)
        e4 = WKTElement(self._ewkt, srid=arbitrary_srid, extended=True)
        e5 = WKTElement(self._ewkt, srid=arbitrary_srid, extended=False)
        assert e1.desc == e2.desc == e3.desc == e4.desc == e5.desc == self._ewkt
        assert e1.srid == e2.srid == self._srid
        assert e3.srid == e4.srid == e5.srid == arbitrary_srid
        assert e1.extended == e2.extended == e3.extended == e4.extended == (not e5.extended)

    def test_desc(self):
        e = WKTElement(self._ewkt)
        assert e.desc == self._ewkt

    def test_function_call(self):
        e = WKTElement(self._ewkt, extended=True)
        f = e.ST_Buffer(2)
        eq_sql(f, "ST_Buffer(" "ST_GeomFromEWKT(:ST_GeomFromEWKT_1), " ":ST_Buffer_1)")
        assert f.compile().params == {"ST_Buffer_1": 2, "ST_GeomFromEWKT_1": self._ewkt}

    def test_pickle_unpickle(self):
        import pickle

        e = WKTElement(self._ewkt, extended=True)
        pickled = pickle.dumps(e)
        unpickled = pickle.loads(pickled)
        assert unpickled.srid == self._srid
        assert unpickled.extended is True
        assert unpickled.data == self._ewkt
        f = unpickled.ST_Buffer(2)
        eq_sql(f, "ST_Buffer(" "ST_GeomFromEWKT(:ST_GeomFromEWKT_1), " ":ST_Buffer_1)")
        assert f.compile().params == {
            "ST_Buffer_1": 2,
            "ST_GeomFromEWKT_1": self._ewkt,
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
            WKTElement("SRID=BAD SRID;POINT (1 2 3)", extended=True)
        with pytest.raises(ArgumentError):
            WKTElement("SRID=BAD SRID;POINT (1 2 3)", extended=True)
        with pytest.raises(ArgumentError):
            WKTElement("SRID=1234;1234;1234;POINT (1 2 3)", extended=True)

    def test_eq(self):
        a = WKTElement(self._ewkt, extended=True)
        b = WKTElement(self._ewkt, extended=True)
        assert a == b

    def test_hash(self):
        a = WKTElement("SRID=3857;POINT (1 2 3)", extended=True)
        b = WKTElement("SRID=3857;POINT (10 20 30)", extended=True)
        c = WKTElement("SRID=3857;POINT (10 20 30)", extended=True)
        assert set([a, b, c]) == set([a, b, c])
        assert len(set([a, b, c])) == 2

    def test_missing_srid(self):
        with pytest.raises(ArgumentError, match="invalid EWKT string"):
            WKTElement(self._wkt, extended=True)

    def test_missing_semi_colon(self):
        with pytest.raises(ArgumentError, match="invalid EWKT string"):
            WKTElement("SRID=3857" + self._wkt, extended=True)

    def test_as_wkt_as_ewkt(self):
        arbitrary_srid = self._srid + 1
        e1 = WKTElement(self._ewkt)
        e2 = WKTElement(self._ewkt, srid=arbitrary_srid)

        # No arbitrary SRID
        e3 = e1.as_wkt()
        assert e3.desc == self._wkt
        assert e3.srid == self._srid
        assert e3.extended is False
        assert e3.as_ewkt() == e1

        # With arbitrary SRID
        e4 = e2.as_wkt()
        assert e4.desc == self._wkt
        assert e4.srid == arbitrary_srid
        assert e4.extended is False
        # The arbitrary SRID overwrites the original SRID in the EWKT string
        assert e4.as_ewkt() == WKTElement(f"SRID={arbitrary_srid};{self._wkt}")


class TestWKTElementFunction:
    def test_ST_Equal_WKTElement_WKTElement(self):
        expr = func.ST_Equals(WKTElement("POINT(1 2)"), WKTElement("POINT(1 2)"))
        eq_sql(
            expr,
            "ST_Equals("
            "ST_GeomFromText(:ST_GeomFromText_1, :ST_GeomFromText_2), "
            "ST_GeomFromText(:ST_GeomFromText_3, :ST_GeomFromText_4))",
        )
        assert expr.compile().params == {
            "ST_GeomFromText_1": "POINT(1 2)",
            "ST_GeomFromText_2": -1,
            "ST_GeomFromText_3": "POINT(1 2)",
            "ST_GeomFromText_4": -1,
        }

    def test_ST_Equal_Column_WKTElement(self, geometry_table):
        expr = func.ST_Equals(geometry_table.c.geom, WKTElement("POINT(1 2)"))
        eq_sql(
            expr,
            'ST_Equals("table".geom, ' "ST_GeomFromText(:ST_GeomFromText_1, :ST_GeomFromText_2))",
        )
        assert expr.compile().params == {
            "ST_GeomFromText_1": "POINT(1 2)",
            "ST_GeomFromText_2": -1,
        }


class TestExtendedWKTElementFunction:
    def test_ST_Equal_WKTElement_WKTElement(self):
        expr = func.ST_Equals(
            WKTElement("SRID=3857;POINT(1 2 3)", extended=True),
            WKTElement("SRID=3857;POINT(1 2 3)", extended=True),
        )
        eq_sql(
            expr,
            "ST_Equals("
            "ST_GeomFromEWKT(:ST_GeomFromEWKT_1), "
            "ST_GeomFromEWKT(:ST_GeomFromEWKT_2))",
        )
        assert expr.compile().params == {
            "ST_GeomFromEWKT_1": "SRID=3857;POINT(1 2 3)",
            "ST_GeomFromEWKT_2": "SRID=3857;POINT(1 2 3)",
        }

    def test_ST_Equal_Column_WKTElement(self, geometry_table):
        expr = func.ST_Equals(
            geometry_table.c.geom, WKTElement("SRID=3857;POINT(1 2 3)", extended=True)
        )
        eq_sql(expr, 'ST_Equals("table".geom, ' "ST_GeomFromEWKT(:ST_GeomFromEWKT_1))")
        assert expr.compile().params == {
            "ST_GeomFromEWKT_1": "SRID=3857;POINT(1 2 3)",
        }


class TestExtendedWKBElement:
    # _bin/_hex computed by following query:
    # SELECT ST_GeomFromEWKT('SRID=3;POINT(1 2)');
    _bin_ewkb = memoryview(
        b"\x01\x01\x00\x00 \x03\x00\x00\x00\x00\x00\x00"
        b"\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@"
    )
    _bin_wkb = memoryview(
        b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@"
    )
    _hex_ewkb = str("010100002003000000000000000000f03f0000000000000040")
    _hex_wkb = str("0101000000000000000000f03f0000000000000040")
    _srid = 3  # expected srid
    _wkt = "POINT (1 2)"  # expected wkt

    def test_desc(self):
        e1 = WKBElement(self._bin_ewkb, extended=True)
        e2 = WKBElement(self._bin_ewkb)
        assert e1.desc == self._hex_ewkb
        assert e2.desc == self._hex_ewkb

    def test_desc_str(self):
        e = WKBElement(self._hex_ewkb)
        assert e.desc == self._hex_ewkb

    def test_function_call(self):
        e = WKBElement(self._bin_ewkb, extended=True)
        f = e.ST_Buffer(2)
        eq_sql(f, "ST_Buffer(" "ST_GeomFromEWKB(:ST_GeomFromEWKB_1), " ":ST_Buffer_1)")
        assert f.compile().params == {
            "ST_Buffer_1": 2,
            "ST_GeomFromEWKB_1": self._bin_ewkb,
        }

    def test_function_str(self):
        e = WKBElement(self._bin_ewkb, extended=True)
        assert isinstance(str(e), str)

    def test_pickle_unpickle(self):
        import pickle

        e = WKBElement(self._bin_ewkb, srid=self._srid, extended=True)
        pickled = pickle.dumps(e)
        unpickled = pickle.loads(pickled)
        assert unpickled.srid == self._srid
        assert unpickled.extended is True
        assert unpickled.data == bytes(self._bin_ewkb)
        f = unpickled.ST_Buffer(2)
        eq_sql(f, "ST_Buffer(" "ST_GeomFromEWKB(:ST_GeomFromEWKB_1), " ":ST_Buffer_1)")
        assert f.compile().params == {
            "ST_Buffer_1": 2,
            "ST_GeomFromEWKB_1": bytes(self._bin_ewkb),
        }

    def test_unpack_srid_from_bin(self):
        """
        Unpack SRID from WKB struct (when it is not provided as arg)
        to ensure geometry result processor preserves query SRID.
        """
        e = WKBElement(self._bin_ewkb, extended=True)
        assert e.srid == self._srid
        assert wkb.loads(bytes(e.data)).wkt == self._wkt

    def test_unpack_srid_from_bin_forcing_srid(self):
        e = WKBElement(self._bin_ewkb, srid=9999, extended=True)
        assert e.srid == 9999
        assert wkb.loads(bytes(e.data)).wkt == self._wkt

    def test_unpack_srid_from_hex(self):
        e = WKBElement(self._hex_ewkb, extended=True)
        assert e.srid == self._srid
        assert wkb.loads(e.data, hex=True).wkt == self._wkt

    def test_eq(self):
        a = WKBElement(self._bin_ewkb, extended=True)
        b = WKBElement(self._bin_ewkb, extended=True)
        assert a == b

    def test_hash(self):
        a = WKBElement(str("010100002003000000000000000000f03f0000000000000040"), extended=True)
        b = WKBElement(str("010100002003000000000000000000f02f0000000000000040"), extended=True)
        c = WKBElement(str("010100002003000000000000000000f02f0000000000000040"), extended=True)
        assert set([a, b, c]) == set([a, b, c])
        assert len(set([a, b, c])) == 2

    def test_as_wkt_as_ewkt(self):
        arbitrary_srid = self._srid + 1
        e1 = WKBElement(self._bin_ewkb)
        e2 = WKBElement(self._bin_ewkb, srid=arbitrary_srid)
        e3 = WKBElement(self._hex_ewkb)
        e4 = WKBElement(self._hex_ewkb, srid=arbitrary_srid)

        # Bin with no arbitrary SRID
        e5 = e1.as_wkb()
        assert e5.desc == self._hex_wkb
        assert e5.srid == self._srid
        assert e5.extended is False
        assert e5.as_ewkb() == e1

        # Bin with arbitrary SRID
        e6 = e2.as_wkb()
        assert e6.desc == self._hex_wkb
        assert e6.srid == arbitrary_srid
        assert e6.extended is False
        # The arbitrary SRID overwrites the original SRID in the EWKB string
        e6_ewkb = WKBElement(self._bin_ewkb, srid=arbitrary_srid)
        data = bytearray(e6_ewkb.data)
        data[5] = 4
        e6_ewkb.data = memoryview(data)
        assert e6.as_ewkb() == e6_ewkb

        # Hex with no arbitrary SRID
        e7 = e3.as_wkb()
        assert e7.desc == self._hex_wkb
        assert e7.srid == self._srid
        assert e7.extended is False
        assert e7.as_ewkb() == e3

        # Hex with arbitrary SRID
        e8 = e4.as_wkb()
        assert e8.desc == self._hex_wkb
        assert e8.srid == arbitrary_srid
        assert e8.extended is False
        # The arbitrary SRID overwrites the original SRID in the EWKB string
        e8_ewkb = WKBElement(self._hex_ewkb, srid=arbitrary_srid)
        e8_ewkb.data = e8_ewkb.data[:11] + "4" + e8_ewkb.data[12:]
        assert e8.as_ewkb() == e8_ewkb


class TestWKBElement:
    def test_desc(self):
        e = WKBElement(b"\x01\x02")
        assert e.desc == "0102"

    def test_function_call(self):
        e = WKBElement(b"\x01\x02")
        f = e.ST_Buffer(2)
        eq_sql(
            f,
            "ST_Buffer(" "ST_GeomFromWKB(:ST_GeomFromWKB_1, :ST_GeomFromWKB_2), " ":ST_Buffer_1)",
        )
        assert f.compile().params == {
            "ST_Buffer_1": 2,
            "ST_GeomFromWKB_1": b"\x01\x02",
            "ST_GeomFromWKB_2": -1,
        }

    def test_attribute_error(self):
        e = WKBElement(b"\x01\x02")
        assert not hasattr(e, "foo")

    def test_function_str(self):
        e = WKBElement(b"\x01\x02")
        assert isinstance(str(e), str)

    def test_eq(self):
        a = WKBElement(b"\x01\x02")
        b = WKBElement(b"\x01\x02")
        assert a == b

    def test_hash(self):
        a = WKBElement(b"\x01\x02")
        b = WKBElement(b"\x01\x03")
        c = WKBElement(b"\x01\x03")
        assert set([a, b, c]) == set([a, b, c])
        assert len(set([a, b, c])) == 2


class TestNotEqualSpatialElement:
    # _bin/_hex computed by following query:
    # SELECT ST_GeomFromEWKT('SRID=3;POINT(1 2)');
    _ewkb = memoryview(
        b"\x01\x01\x00\x00 \x03\x00\x00\x00\x00\x00\x00"
        b"\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@"
    )
    _wkb = wkb.loads(bytes(_ewkb)).wkb
    _hex = str("010100002003000000000000000000f03f0000000000000040")
    _srid = 3
    _wkt = "POINT (1 2)"
    _ewkt = "SRID=3;POINT (1 2)"

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


class TestRasterElement:
    rast_data = (
        b"\x01\x00\x00\x01\x00\x9a\x99\x99\x99\x99\x99\xc9?\x9a\x99\x99\x99\x99\x99"
        b"\xc9\xbf\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00"
        b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xe6\x10\x00"
        b"\x00\x05\x00\x05\x00D\x00\x01\x01\x01\x01\x01\x01\x01\x01\x01\x00\x01\x01"
        b"\x01\x00\x00\x01\x01\x00\x00\x00\x01\x00\x00\x00\x00"
    )

    hex_rast_data = (
        "01000001009a9999999999c93f9a9999999999c9bf0000000000000000000000000000f03"
        "f00000000000000000000000000000000e610000005000500440001010101010101010100"
        "010101000001010000000100000000"
    )

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
        eq_sql(f, "ST_Height(raster(:raster_1))")
        assert f.compile().params == {"raster_1": self.hex_rast_data}

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
        eq_sql(f, "ST_Height(raster(:raster_1))")
        assert f.compile().params == {
            "raster_1": self.hex_rast_data,
        }

    def test_hash(self):
        new_hex_rast_data = self.hex_rast_data.replace("f", "e")
        a = WKBElement(self.hex_rast_data)
        b = WKBElement(new_hex_rast_data)
        c = WKBElement(new_hex_rast_data)
        assert set([a, b, c]) == set([a, b, c])
        assert len(set([a, b, c])) == 2


class TestCompositeElement:
    def test_compile(self):
        # text fixture
        metadata = MetaData()
        foo = Table("foo", metadata, Column("one", String))

        e = CompositeElement(foo.c.one, "geom", String)
        assert str(e) == "(foo.one).geom"
