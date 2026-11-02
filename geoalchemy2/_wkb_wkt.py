"""Private wrappers around :mod:`wkb_wkt_converter`.

GeoAlchemy2 uses ``-1`` and sometimes ``0`` as "unknown/no SRID" sentinels in
Python objects and SQL compilation. The converter accepts those values,
but centralising the mapping keeps that policy explicit for callers.

``wkb_wkt_converter`` is an optional dependency (``geoalchemy2[wkb-wkt-converter]``).
When it is not installed, header-only operations (SRID sniff/strip/inject on
WKB/EWKB data, SRID prefixing on WKT/EWKT text) fall back to the pure-Python
implementation in :mod:`geoalchemy2._wkb_wkt_fallback`, restoring the behavior
GeoAlchemy2 had at tag ``0.20.0``. Real WKB<->WKT geometry conversion has no
such fallback here and raises :class:`ImportError`; callers that need it with
the converter absent (dialect bind processors) fall back to Shapely instead,
see :mod:`geoalchemy2.types.dialects.common`.
"""

from __future__ import annotations

import binascii
from typing import NoReturn

from geoalchemy2 import _wkb_wkt_fallback as _fallback

try:
    from wkb_wkt_converter import to_ewkb_header as _to_ewkb_header
    from wkb_wkt_converter import to_hex_wkb as _to_hex_wkb
    from wkb_wkt_converter import to_wkb as _to_wkb
    from wkb_wkt_converter import to_wkb_no_srid_header as _to_wkb_no_srid_header
    from wkb_wkt_converter import to_wkt as _to_wkt
    from wkb_wkt_converter import wkb_header_srid as _wkb_header_srid
    from wkb_wkt_converter import wkb_to_wkt_split_srid
    from wkb_wkt_converter import wkt_to_wkb_split_srid

    HAS_WKB_WKT_CONVERTER = True
    _wkb_wkt_converter_exc: ImportError | None = None
except ImportError as exc:
    HAS_WKB_WKT_CONVERTER = False
    _wkb_wkt_converter_exc = exc


def is_known_srid(srid: int | None) -> bool:
    return srid is not None and srid > 0


def _srid_arg(srid: int | None) -> int | bool:
    if srid is None or not is_known_srid(srid):
        return False
    return srid


def _raise_missing_converter() -> NoReturn:
    raise ImportError(
        "This feature needs the optional wkb-wkt-converter dependency. "
        "Please install it with 'pip install geoalchemy2[wkb-wkt-converter]'."
    ) from _wkb_wkt_converter_exc


def wkb_srid(
    source: str | bytes | bytearray | memoryview, *, include_unknown: bool = False
) -> int | None:
    """Return the embedded EWKB SRID without parsing the full geometry."""
    if not HAS_WKB_WKT_CONVERTER:
        return _fallback.wkb_header_srid(source, include_unknown=include_unknown)
    return _wkb_header_srid(source, include_unknown=include_unknown)


def to_wkb_no_srid_header(source: str | bytes | bytearray | memoryview) -> str | bytes:
    """Strip the EWKB SRID with native header rewrite/full-conversion fallback."""
    if not HAS_WKB_WKT_CONVERTER:
        return _fallback.to_wkb_no_srid_header(source)
    try:
        return _to_wkb_no_srid_header(source)
    except ValueError as exc:
        if "unexpected end of data reading u32" in str(exc):
            raise ValueError("WKB value is too short to read header") from None
        raise


def to_ewkb_header(source: str | bytes | bytearray | memoryview, srid: int) -> str | bytes:
    """Embed or replace an EWKB SRID with native header rewrite/full-conversion fallback."""
    if not is_known_srid(srid):
        return to_wkb_no_srid_header(source)
    if not HAS_WKB_WKT_CONVERTER:
        return _fallback.to_ewkb_header(source, srid)
    return _to_ewkb_header(source, srid)


def to_wkb_no_srid(source: str | bytes | bytearray | memoryview) -> bytes:
    """Convert WKB/EWKB source to plain WKB, stripping any embedded SRID.

    ``source`` must already be WKB/EWKB (bytes or a SpatiaLite-style hex
    string); for converting WKT text to WKB, use :func:`wkt_to_wkb`.
    """
    if not HAS_WKB_WKT_CONVERTER:
        res = _fallback.to_wkb_no_srid_header(source)
        if isinstance(res, str):
            return binascii.unhexlify(res)
        return bytes(res)
    return _to_wkb(source, srid=False)


def wkt_to_wkb(source: str, srid: int | None = None) -> bytes:
    """Convert WKT/EWKT text to WKB/EWKB bytes, embedding only a known SRID."""
    if not HAS_WKB_WKT_CONVERTER:
        _raise_missing_converter()
    return _to_wkb(source, srid=_srid_arg(srid))


def to_wkt(source, srid: int | None = None, *, normalize_wkt: bool = False) -> str:
    """Convert WKB/EWKB source to WKT/EWKT, prefixing only positive SRIDs.

    ``source`` may be WKB/EWKB (bytes or a hex string, e.g. ``WKBElement.data``);
    for adjusting the SRID prefix of WKT/EWKT text instead, use
    :func:`wkt_add_srid`/:func:`wkt_strip_srid`, which work without the
    converter installed.
    """
    if not HAS_WKB_WKT_CONVERTER:
        _raise_missing_converter()
    return _to_wkt(source, srid=_srid_arg(srid), normalize_wkt=normalize_wkt)


def to_wkt_no_srid(source, *, normalize_wkt: bool = False) -> str:
    """Convert WKB/EWKB source to plain WKT, stripping any SRID.

    See :func:`to_wkt` for the ``source`` contract.
    """
    if not HAS_WKB_WKT_CONVERTER:
        _raise_missing_converter()
    return _to_wkt(source, srid=False, normalize_wkt=normalize_wkt)


def to_wkt_for_column(source, srid: int | None, *, normalize_wkt: bool = False) -> str:
    """Convert WKB/EWKB source to WKT/EWKT, preserving SRID unless the column has one."""
    if not HAS_WKB_WKT_CONVERTER:
        _raise_missing_converter()
    return _to_wkt(
        source,
        srid=srid if is_known_srid(srid) else None,
        normalize_wkt=normalize_wkt,
    )


def wkt_add_srid(source: str, srid: int | None = None) -> str:
    """Add or replace the SRID prefix of WKT/EWKT ``source`` text.

    Pure string manipulation: works with or without the converter installed.
    """
    if not HAS_WKB_WKT_CONVERTER:
        if srid is not None and is_known_srid(srid):
            return _fallback.wkt_add_srid(source, srid)
        return _fallback.wkt_strip_srid(source)
    return _to_wkt(source, srid=_srid_arg(srid))


def wkt_strip_srid(source: str) -> str:
    """Strip the SRID prefix, if any, from WKT/EWKT ``source`` text.

    Pure string manipulation: works with or without the converter installed.
    """
    if not HAS_WKB_WKT_CONVERTER:
        return _fallback.wkt_strip_srid(source)
    return _to_wkt(source, srid=False)


def to_hex_wkb_no_srid(source: str | bytes | bytearray | memoryview) -> str:
    """Convert to plain hex WKB, stripping any embedded SRID."""
    if not HAS_WKB_WKT_CONVERTER:
        return _fallback.to_hex_wkb_no_srid(source)
    return _to_hex_wkb(source, srid=False)


def split_wkb_srid(source) -> tuple[str, int | None]:
    """Return ``(plain_wkt, embedded_srid)`` for WKB/EWKB input."""
    if not HAS_WKB_WKT_CONVERTER:
        _raise_missing_converter()
    if isinstance(source, str):
        source = bytes.fromhex(source)
    return wkb_to_wkt_split_srid(source)


def split_wkt_srid(source: str) -> tuple[bytes, int | None]:
    """Return ``(plain_wkb, srid)`` for WKT/EWKT input."""
    if not HAS_WKB_WKT_CONVERTER:
        _raise_missing_converter()
    return wkt_to_wkb_split_srid(source)
