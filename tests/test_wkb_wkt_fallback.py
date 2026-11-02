"""Unit tests for geoalchemy2._wkb_wkt_fallback."""

import pytest

from geoalchemy2 import _wkb_wkt_fallback as fallback

HEX_WKB_LE = "0101000000000000000000f03f0000000000000040"
HEX_EWKB_LE_4326 = "0101000020e6100000000000000000f03f0000000000000040"
HEX_EWKB_LE_3857 = "0101000020110f0000000000000000f03f0000000000000040"
HEX_EWKB_LE_ZERO = "010100002000000000000000000000f03f0000000000000040"
HEX_EWKB_LE_NEG1 = "0101000020ffffffff000000000000f03f0000000000000040"

HEX_WKB_LE_Z = "0101000080000000000000f03f00000000000000400000000000000840"
HEX_EWKB_LE_Z_4326 = "01010000a0e6100000000000000000f03f00000000000000400000000000000840"
HEX_EWKB_LE_Z_3857 = "01010000a0110f0000000000000000f03f00000000000000400000000000000840"

BIN_WKB_LE = bytes.fromhex(HEX_WKB_LE)
BIN_EWKB_LE_4326 = bytes.fromhex(HEX_EWKB_LE_4326)
BIN_EWKB_LE_3857 = bytes.fromhex(HEX_EWKB_LE_3857)
BIN_EWKB_LE_ZERO = bytes.fromhex(HEX_EWKB_LE_ZERO)
BIN_EWKB_LE_NEG1 = bytes.fromhex(HEX_EWKB_LE_NEG1)

BIN_WKB_LE_Z = bytes.fromhex(HEX_WKB_LE_Z)
BIN_EWKB_LE_Z_4326 = bytes.fromhex(HEX_EWKB_LE_Z_4326)
BIN_EWKB_LE_Z_3857 = bytes.fromhex(HEX_EWKB_LE_Z_3857)

HEX_WKB_BE = "000000000100000000000000000000000000000000"
HEX_EWKB_BE_4326 = "0020000001000010e600000000000000000000000000000000"
HEX_EWKB_BE_3857 = "002000000100000f1100000000000000000000000000000000"

HEX_WKB_BE_Z = "0080000001000000000000000000000000000000000000000000000000"
HEX_EWKB_BE_Z_4326 = "00a0000001000010e6000000000000000000000000000000000000000000000000"

BIN_WKB_BE = bytes.fromhex(HEX_WKB_BE)
BIN_EWKB_BE_4326 = bytes.fromhex(HEX_EWKB_BE_4326)
BIN_EWKB_BE_3857 = bytes.fromhex(HEX_EWKB_BE_3857)
BIN_WKB_BE_Z = bytes.fromhex(HEX_WKB_BE_Z)
BIN_EWKB_BE_Z_4326 = bytes.fromhex(HEX_EWKB_BE_Z_4326)


class TestByteOrderMarker:
    def test_byte_order_marker(self):
        assert fallback._byte_order_marker(1) == "<I"
        assert fallback._byte_order_marker(0) == ">I"
        assert fallback._byte_order_marker(1, signed=True) == "<i"
        assert fallback._byte_order_marker(0, signed=True) == ">i"


class TestWkbHeaderSrid:
    def test_valid_ewkb_little_endian(self):
        assert fallback.wkb_header_srid(HEX_EWKB_LE_4326) == 4326
        assert fallback.wkb_header_srid(BIN_EWKB_LE_4326) == 4326

    def test_valid_ewkb_big_endian(self):
        assert fallback.wkb_header_srid(HEX_EWKB_BE_4326) == 4326
        assert fallback.wkb_header_srid(BIN_EWKB_BE_4326) == 4326

    def test_valid_ewkb_3d_little_endian(self):
        assert fallback.wkb_header_srid(HEX_EWKB_LE_Z_4326) == 4326
        assert fallback.wkb_header_srid(BIN_EWKB_LE_Z_4326) == 4326

    def test_valid_ewkb_3d_big_endian(self):
        assert fallback.wkb_header_srid(HEX_EWKB_BE_Z_4326) == 4326
        assert fallback.wkb_header_srid(BIN_EWKB_BE_Z_4326) == 4326

    def test_negative_srid(self):
        assert fallback.wkb_header_srid(HEX_EWKB_LE_NEG1) is None
        assert fallback.wkb_header_srid(HEX_EWKB_LE_NEG1, include_unknown=True) == -1
        assert fallback.wkb_header_srid(BIN_EWKB_LE_NEG1) is None
        assert fallback.wkb_header_srid(BIN_EWKB_LE_NEG1, include_unknown=True) == -1

    def test_wkb_no_srid(self):
        assert fallback.wkb_header_srid(HEX_WKB_LE) is None
        assert fallback.wkb_header_srid(BIN_WKB_LE) is None
        assert fallback.wkb_header_srid(HEX_WKB_BE) is None
        assert fallback.wkb_header_srid(BIN_WKB_BE) is None

    def test_zero_srid(self):
        assert fallback.wkb_header_srid(HEX_EWKB_LE_ZERO) is None
        assert fallback.wkb_header_srid(HEX_EWKB_LE_ZERO, include_unknown=True) == 0
        assert fallback.wkb_header_srid(BIN_EWKB_LE_ZERO) is None
        assert fallback.wkb_header_srid(BIN_EWKB_LE_ZERO, include_unknown=True) == 0

    def test_short_headers(self):
        # len(header) < 5
        assert fallback.wkb_header_srid(b"\x01\x01\x00") is None
        assert fallback.wkb_header_srid("010100") is None
        assert fallback.wkb_header_srid(b"") is None
        assert fallback.wkb_header_srid("") is None

        # len(wkb_srid_bytes) != 4 (header length between 5 and 8 bytes)
        assert fallback.wkb_header_srid(b"\x01\x01\x00\x00\x20\x01\x02") is None
        assert fallback.wkb_header_srid("01010000200102") is None

    def test_invalid_input_raises_value_error(self):
        with pytest.raises(ValueError):
            fallback.wkb_header_srid("invalid_hex!")
        with pytest.raises(ValueError):
            fallback.wkb_header_srid(12345)


class TestToWkbNoSridHeader:
    def test_strip_srid_little_endian(self):
        assert fallback.to_wkb_no_srid_header(HEX_EWKB_LE_4326) == HEX_WKB_LE
        assert fallback.to_wkb_no_srid_header(BIN_EWKB_LE_4326) == BIN_WKB_LE

    def test_strip_srid_big_endian(self):
        assert fallback.to_wkb_no_srid_header(HEX_EWKB_BE_4326) == HEX_WKB_BE
        assert fallback.to_wkb_no_srid_header(BIN_EWKB_BE_4326) == BIN_WKB_BE

    def test_strip_srid_3d_little_endian(self):
        assert fallback.to_wkb_no_srid_header(HEX_EWKB_LE_Z_4326) == HEX_WKB_LE_Z
        assert fallback.to_wkb_no_srid_header(BIN_EWKB_LE_Z_4326) == BIN_WKB_LE_Z

    def test_strip_srid_3d_big_endian(self):
        assert fallback.to_wkb_no_srid_header(HEX_EWKB_BE_Z_4326) == HEX_WKB_BE_Z
        assert fallback.to_wkb_no_srid_header(BIN_EWKB_BE_Z_4326) == BIN_WKB_BE_Z

    def test_no_srid_returns_full(self):
        assert fallback.to_wkb_no_srid_header(HEX_WKB_LE) == HEX_WKB_LE
        assert fallback.to_wkb_no_srid_header(BIN_WKB_LE) == BIN_WKB_LE
        assert fallback.to_wkb_no_srid_header(HEX_WKB_BE) == HEX_WKB_BE
        assert fallback.to_wkb_no_srid_header(BIN_WKB_BE) == BIN_WKB_BE

    def test_short_input(self):
        with pytest.raises(ValueError, match="too short"):
            fallback.to_wkb_no_srid_header(b"\x01\x01\x00")
        with pytest.raises(ValueError, match="too short"):
            fallback.to_wkb_no_srid_header("")


class TestToEwkbHeader:
    def test_embed_srid_into_plain_wkb_little_endian(self):
        assert fallback.to_ewkb_header(HEX_WKB_LE, 4326) == HEX_EWKB_LE_4326
        assert fallback.to_ewkb_header(BIN_WKB_LE, 4326) == BIN_EWKB_LE_4326

    def test_embed_srid_into_plain_wkb_big_endian(self):
        assert fallback.to_ewkb_header(HEX_WKB_BE, 4326) == HEX_EWKB_BE_4326
        assert fallback.to_ewkb_header(BIN_WKB_BE, 4326) == BIN_EWKB_BE_4326

    def test_embed_srid_into_plain_wkb_3d_little_endian(self):
        assert fallback.to_ewkb_header(HEX_WKB_LE_Z, 4326) == HEX_EWKB_LE_Z_4326
        assert fallback.to_ewkb_header(BIN_WKB_LE_Z, 4326) == BIN_EWKB_LE_Z_4326

    def test_embed_srid_into_plain_wkb_3d_big_endian(self):
        assert fallback.to_ewkb_header(HEX_WKB_BE_Z, 4326) == HEX_EWKB_BE_Z_4326
        assert fallback.to_ewkb_header(BIN_WKB_BE_Z, 4326) == BIN_EWKB_BE_Z_4326

    def test_embed_negative_srid(self):
        assert fallback.to_ewkb_header(HEX_WKB_LE, -1) == HEX_EWKB_LE_NEG1
        assert fallback.to_ewkb_header(BIN_WKB_LE, -1) == BIN_EWKB_LE_NEG1

    def test_replace_srid_in_ewkb_little_endian(self):
        assert fallback.to_ewkb_header(HEX_EWKB_LE_4326, 3857) == HEX_EWKB_LE_3857
        assert fallback.to_ewkb_header(BIN_EWKB_LE_4326, 3857) == BIN_EWKB_LE_3857

    def test_replace_srid_in_ewkb_3d_little_endian(self):
        assert fallback.to_ewkb_header(HEX_EWKB_LE_Z_4326, 3857) == HEX_EWKB_LE_Z_3857
        assert fallback.to_ewkb_header(BIN_EWKB_LE_Z_4326, 3857) == BIN_EWKB_LE_Z_3857

    def test_replace_srid_in_ewkb_big_endian(self):
        assert fallback.to_ewkb_header(HEX_EWKB_BE_4326, 3857) == HEX_EWKB_BE_3857
        assert fallback.to_ewkb_header(BIN_EWKB_BE_4326, 3857) == BIN_EWKB_BE_3857

    def test_short_input(self):
        with pytest.raises(ValueError, match="too short"):
            fallback.to_ewkb_header(b"\x01\x01\x00", 4326)
        with pytest.raises(ValueError, match="too short"):
            fallback.to_ewkb_header("010100", 4326)
        with pytest.raises(ValueError, match="too short"):
            fallback.to_ewkb_header(b"", 4326)


class TestToHexWkbNoSrid:
    def test_hex_input(self):
        assert fallback.to_hex_wkb_no_srid(HEX_EWKB_LE_4326.upper()) == HEX_WKB_LE.lower()

    def test_bytes_input(self):
        assert fallback.to_hex_wkb_no_srid(BIN_EWKB_LE_4326) == HEX_WKB_LE.lower()


class TestWktSridHandling:
    def test_wkt_strip_srid(self):
        assert fallback.wkt_strip_srid("SRID=4326;POINT(1 2)") == "POINT(1 2)"
        assert fallback.wkt_strip_srid("SRID=4326; POINT(1 2)") == "POINT(1 2)"
        assert fallback.wkt_strip_srid("srid=4326;POINT(1 2)") == "POINT(1 2)"
        assert fallback.wkt_strip_srid("SRID=-1;POINT(1 2)") == "POINT(1 2)"
        assert fallback.wkt_strip_srid("POINT(1 2)") == "POINT(1 2)"

    def test_wkt_add_srid(self):
        assert fallback.wkt_add_srid("POINT(1 2)", 4326) == "SRID=4326;POINT(1 2)"
        assert fallback.wkt_add_srid("SRID=3857;POINT(1 2)", 4326) == "SRID=4326;POINT(1 2)"
        assert fallback.wkt_add_srid("srid=3857;POINT(1 2)", 4326) == "SRID=4326;POINT(1 2)"
        assert fallback.wkt_add_srid("SRID=-1;POINT(1 2)", 4326) == "SRID=4326;POINT(1 2)"
        assert fallback.wkt_add_srid("SRID=3857; POINT(1 2)", 4326) == "SRID=4326;POINT(1 2)"
