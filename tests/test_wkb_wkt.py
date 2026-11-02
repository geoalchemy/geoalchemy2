"""Tests for the ``wkb-wkt-converter``-optional fallback behavior.

``wkb-wkt-converter`` is an optional runtime dependency
(``geoalchemy2[wkb-wkt-converter]``). These tests simulate it being absent
(mirroring the ``sys.modules`` blanking + ``importlib.reload`` technique used
in ``tests/test_shape.py`` for Shapely) and check that:

* header-only WKB/EWKB SRID operations and WKT SRID-prefix handling keep
  working via the pure-Python fallback, matching the converter-backed result,
* dialect bind-time WKB->WKT conversion falls back to Shapely,
* the new converter-only conversion methods (``WKTElement.as_wkb/as_ewkb``,
  ``WKBElement.as_wkt/as_ewkt``) raise a clear ``ImportError`` instead of
  silently degrading, even when Shapely is available.
"""

import contextlib
import importlib
import os
import sys

import pytest

import geoalchemy2._wkb_wkt
import geoalchemy2.shape
from geoalchemy2 import _wkb_wkt
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.types.dialects.common import wkb_bytes_to_wkt
from geoalchemy2.types.dialects.common import wkb_bytes_to_wkt_split_srid

from . import skip_without_wkb_wkt_converter

WKB_HEX = "0101000000000000000000f03f0000000000000040"
EWKB_HEX = "0101000020e6100000000000000000f03f0000000000000040"
ZERO_SRID_EWKB_HEX = "010100002000000000000000000000f03f0000000000000040"
UNKNOWN_SRID_EWKB_HEX = "0101000020ffffffff000000000000f03f0000000000000040"

EWKB_BYTES = bytes.fromhex(EWKB_HEX)
WKB_BYTES = bytes.fromhex(WKB_HEX)


@contextlib.contextmanager
def wkb_wkt_converter_disabled(monkeypatch):
    """Simulate ``wkb_wkt_converter`` not being installed, then restore it."""
    converter_modules = {
        k: v
        for k, v in sys.modules.items()
        if k == "wkb_wkt_converter" or k.startswith("wkb_wkt_converter.")
    }
    for k in converter_modules:
        monkeypatch.setitem(sys.modules, k, None)
    importlib.reload(geoalchemy2._wkb_wkt)
    assert not geoalchemy2._wkb_wkt.HAS_WKB_WKT_CONVERTER
    try:
        yield
    finally:
        for k, v in converter_modules.items():
            monkeypatch.setitem(sys.modules, k, v)
        importlib.reload(geoalchemy2._wkb_wkt)


@contextlib.contextmanager
def shapely_disabled(monkeypatch):
    """Simulate Shapely not being installed, then restore it."""
    import geoalchemy2.shape

    shapely_modules = {
        k: v for k, v in sys.modules.items() if k == "shapely" or k.startswith("shapely.")
    }
    for k in shapely_modules:
        monkeypatch.setitem(sys.modules, k, None)
    importlib.reload(geoalchemy2.shape)
    assert not geoalchemy2.shape.HAS_SHAPELY
    try:
        yield
    finally:
        for k, v in shapely_modules.items():
            monkeypatch.setitem(sys.modules, k, v)
        importlib.reload(geoalchemy2.shape)


def test_import_without_wkb_wkt_converter(monkeypatch):
    """Ensure geoalchemy2._wkb_wkt can be imported when the converter is absent."""
    initial_has = geoalchemy2._wkb_wkt.HAS_WKB_WKT_CONVERTER
    with wkb_wkt_converter_disabled(monkeypatch):
        assert not geoalchemy2._wkb_wkt.HAS_WKB_WKT_CONVERTER
        with pytest.raises(ImportError, match="wkb-wkt-converter"):
            _wkb_wkt.wkt_to_wkb("POINT (1 2)")

    assert geoalchemy2._wkb_wkt.HAS_WKB_WKT_CONVERTER is initial_has


@skip_without_wkb_wkt_converter()
def test_import_with_wkb_wkt_converter():
    assert geoalchemy2._wkb_wkt.HAS_WKB_WKT_CONVERTER
    assert _wkb_wkt.wkt_to_wkb("POINT (1 2)") is not None


def test_fallback_wkb_srid_matches_converter(monkeypatch):
    with_converter = {
        "ewkb": _wkb_wkt.wkb_srid(EWKB_HEX),
        "zero_srid": _wkb_wkt.wkb_srid(ZERO_SRID_EWKB_HEX),
        "zero_srid_unknown": _wkb_wkt.wkb_srid(ZERO_SRID_EWKB_HEX, include_unknown=True),
        "wkb": _wkb_wkt.wkb_srid(WKB_HEX),
    }
    with wkb_wkt_converter_disabled(monkeypatch):
        assert _wkb_wkt.wkb_srid(EWKB_HEX) == with_converter["ewkb"] == 4326
        assert _wkb_wkt.wkb_srid(ZERO_SRID_EWKB_HEX) == with_converter["zero_srid"] is None
        assert (
            _wkb_wkt.wkb_srid(ZERO_SRID_EWKB_HEX, include_unknown=True)
            == with_converter["zero_srid_unknown"]
            == 0
        )
        assert _wkb_wkt.wkb_srid(WKB_HEX) == with_converter["wkb"] is None
        # bytes input works too
        assert _wkb_wkt.wkb_srid(EWKB_BYTES) == 4326


def test_fallback_unknown_srid_matches_converter(monkeypatch):
    """The 0xffffffff sentinel is interpreted as signed (-1) by both backends."""
    assert _wkb_wkt.wkb_srid(UNKNOWN_SRID_EWKB_HEX, include_unknown=True) == -1
    assert _wkb_wkt.wkb_srid(UNKNOWN_SRID_EWKB_HEX, include_unknown=False) is None
    with wkb_wkt_converter_disabled(monkeypatch):
        assert _wkb_wkt.wkb_srid(UNKNOWN_SRID_EWKB_HEX, include_unknown=True) == -1
        assert _wkb_wkt.wkb_srid(UNKNOWN_SRID_EWKB_HEX, include_unknown=False) is None


def test_fallback_header_rewrite_matches_converter(monkeypatch):
    # Hex letter casing is not a semantic guarantee of either backend
    # (callers that care already normalize with .lower()), so compare
    # case-insensitively.
    stripped_with_converter = _wkb_wkt.to_wkb_no_srid_header(EWKB_HEX).lower()
    embedded_with_converter = _wkb_wkt.to_ewkb_header(WKB_HEX, 4326).lower()
    with wkb_wkt_converter_disabled(monkeypatch):
        stripped = _wkb_wkt.to_wkb_no_srid_header(EWKB_HEX).lower()
        embedded = _wkb_wkt.to_ewkb_header(WKB_HEX, 4326).lower()
        assert stripped == stripped_with_converter == WKB_HEX
        assert embedded == embedded_with_converter == EWKB_HEX
        # bytes input works too
        assert _wkb_wkt.to_wkb_no_srid_header(EWKB_BYTES) == WKB_BYTES
        assert _wkb_wkt.to_ewkb_header(WKB_BYTES, 4326) == EWKB_BYTES


def test_fallback_to_hex_wkb_no_srid_matches_converter(monkeypatch):
    with_converter = _wkb_wkt.to_hex_wkb_no_srid(EWKB_HEX).lower()
    with wkb_wkt_converter_disabled(monkeypatch):
        assert _wkb_wkt.to_hex_wkb_no_srid(EWKB_HEX).lower() == with_converter == WKB_HEX


def test_fallback_wkt_srid_prefix(monkeypatch):
    """wkt_add_srid/wkt_strip_srid operate on WKT text and never need the converter."""
    with_converter_added = _wkb_wkt.wkt_add_srid("POINT (1 2)", 4326)
    with_converter_stripped = _wkb_wkt.wkt_strip_srid("SRID=4326;POINT (1 2)")
    with wkb_wkt_converter_disabled(monkeypatch):
        assert _wkb_wkt.wkt_add_srid("POINT (1 2)", 4326) == with_converter_added
        assert _wkb_wkt.wkt_strip_srid("SRID=4326;POINT (1 2)") == with_converter_stripped
        assert _wkb_wkt.wkt_add_srid("POINT (1 2)") == "POINT (1 2)"
        assert _wkb_wkt.wkt_strip_srid("POINT (1 2)") == "POINT (1 2)"


def test_fallback_to_wkt_always_requires_converter(monkeypatch):
    """to_wkt/to_wkt_no_srid/to_wkt_for_column convert WKB source: no string-only fallback."""
    with wkb_wkt_converter_disabled(monkeypatch):
        with pytest.raises(ImportError, match="wkb-wkt-converter"):
            _wkb_wkt.to_wkt(EWKB_BYTES)
        with pytest.raises(ImportError, match="wkb-wkt-converter"):
            _wkb_wkt.to_wkt_no_srid(EWKB_BYTES)
        with pytest.raises(ImportError, match="wkb-wkt-converter"):
            _wkb_wkt.to_wkt_for_column(EWKB_BYTES, 4326)
        with pytest.raises(ImportError, match="wkb-wkt-converter"):
            _wkb_wkt.split_wkb_srid(EWKB_HEX)


def test_wkb_element_srid_sniffing_without_converter(monkeypatch):
    with wkb_wkt_converter_disabled(monkeypatch):
        e = WKBElement(EWKB_HEX)
        assert e.srid == 4326
        assert e.extended is True

        e_zero = WKBElement(ZERO_SRID_EWKB_HEX)
        assert e_zero.srid == -1
        assert e_zero.extended is True

        e_plain = WKBElement(WKB_HEX)
        assert e_plain.srid == -1
        assert e_plain.extended is False


def test_wkt_element_as_wkt_ewkt_without_converter(monkeypatch):
    with wkb_wkt_converter_disabled(monkeypatch):
        extended = WKTElement("SRID=4326;POINT (1 2)", extended=True)
        assert extended.as_wkt().data == "POINT (1 2)"
        assert extended.as_wkt().srid == 4326

        plain = WKTElement("POINT (1 2)", srid=4326)
        assert plain.as_ewkt().data == "SRID=4326;POINT (1 2)"


def test_wkt_element_as_wkb_ewkb_requires_converter(monkeypatch):
    e = WKTElement("POINT (1 2)", srid=4326)
    with wkb_wkt_converter_disabled(monkeypatch):
        with pytest.raises(ImportError, match="wkb-wkt-converter"):
            e.as_wkb()
        with pytest.raises(ImportError, match="wkb-wkt-converter"):
            e.as_ewkb()


def test_wkb_element_as_wkt_ewkt_requires_converter(monkeypatch):
    e = WKBElement(EWKB_HEX)
    with wkb_wkt_converter_disabled(monkeypatch):
        with pytest.raises(ImportError, match="wkb-wkt-converter"):
            e.as_wkt()
        with pytest.raises(ImportError, match="wkb-wkt-converter"):
            e.as_ewkt()


def test_wkb_element_as_wkt_ewkt_requires_converter_even_with_shapely(monkeypatch):
    """The new as_wkt/as_ewkt methods never existed pre-0.21: no Shapely fallback."""
    e = WKBElement(EWKB_HEX)
    with wkb_wkt_converter_disabled(monkeypatch):
        assert geoalchemy2.shape.HAS_SHAPELY
        with pytest.raises(ImportError, match="wkb-wkt-converter"):
            e.as_wkt()


def test_wkb_element_as_wkb_ewkb_works_without_converter(monkeypatch):
    e_extended = WKBElement(EWKB_HEX)
    e_plain = WKBElement(WKB_HEX, srid=4326)
    expected_stripped = e_extended.as_wkb()
    expected_embedded = e_plain.as_ewkb()
    with wkb_wkt_converter_disabled(monkeypatch):
        stripped = WKBElement(EWKB_HEX).as_wkb()
        assert stripped == expected_stripped
        assert stripped.extended is False
        assert stripped.srid == 4326

        embedded = WKBElement(WKB_HEX, srid=4326).as_ewkb()
        assert embedded == expected_embedded
        assert embedded.extended is True


def test_wkb_bytes_to_wkt_falls_back_to_shapely(monkeypatch):
    with wkb_wkt_converter_disabled(monkeypatch):
        assert wkb_bytes_to_wkt(EWKB_BYTES) == "POINT (1 2)"
        assert wkb_bytes_to_wkt_split_srid(EWKB_HEX) == ("POINT (1 2)", 4326)


def test_converter_presence_matches_tox_env():
    expected = os.environ.get("GEOALCHEMY2_EXPECT_WKB_WKT_CONVERTER")
    if expected is None:
        pytest.skip("not running under tox")
    assert _wkb_wkt.HAS_WKB_WKT_CONVERTER is (expected == "1")


def test_wkb_bytes_to_wkt_raises_when_both_missing(monkeypatch):
    combined_error = "wkb-wkt-converter.*shapely|shapely.*wkb-wkt-converter"
    with wkb_wkt_converter_disabled(monkeypatch), shapely_disabled(monkeypatch):
        with pytest.raises(ImportError, match=combined_error):
            wkb_bytes_to_wkt(EWKB_BYTES)
        with pytest.raises(ImportError, match=combined_error):
            wkb_bytes_to_wkt_split_srid(EWKB_HEX)


def test_to_wkb_no_srid_returns_bytes_for_hex(monkeypatch):
    with wkb_wkt_converter_disabled(monkeypatch):
        res = _wkb_wkt.to_wkb_no_srid(EWKB_HEX)
        assert isinstance(res, bytes)
        assert res == WKB_BYTES


def test_mysql_bind_processor_with_hex_wkb_in_fallback(monkeypatch):
    from geoalchemy2.types import Geometry
    from geoalchemy2.types.dialects import mysql

    t = Geometry("POINT", srid=4326, from_text="ST_GeomFromWKB")
    v = WKBElement(EWKB_HEX, extended=True)
    with wkb_wkt_converter_disabled(monkeypatch):
        processed = mysql.bind_processor_process(t, v)
        assert isinstance(processed, bytes)
        assert processed == WKB_BYTES
