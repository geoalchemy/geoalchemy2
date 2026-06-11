"""Private wrappers around :mod:`wkb_wkt_converter`.

GeoAlchemy2 uses ``-1`` and sometimes ``0`` as "unknown/no SRID" sentinels in
Python objects and SQL compilation. The converter accepts those values,
but centralising the mapping keeps that policy explicit for callers.
"""

from __future__ import annotations

from wkb_wkt_converter import to_ewkb_header as _to_ewkb_header
from wkb_wkt_converter import to_hex_wkb as _to_hex_wkb
from wkb_wkt_converter import to_wkb as _to_wkb
from wkb_wkt_converter import to_wkb_no_srid_header as _to_wkb_no_srid_header
from wkb_wkt_converter import to_wkt as _to_wkt
from wkb_wkt_converter import wkb_header_srid as _wkb_header_srid
from wkb_wkt_converter import wkb_to_wkt_split_srid
from wkb_wkt_converter import wkt_to_wkb_split_srid


def is_known_srid(srid: int | None) -> bool:
    return srid is not None and srid > 0


def _srid_arg(srid: int | None) -> int | bool:
    if srid is None or not is_known_srid(srid):
        return False
    return srid


def wkb_srid(source, *, include_unknown: bool = False) -> int | None:
    """Return the embedded EWKB SRID without parsing the full geometry."""
    return _wkb_header_srid(source, include_unknown=include_unknown)


def to_wkb_no_srid_header(source):
    """Strip the EWKB SRID with native header rewrite/full-conversion fallback."""
    try:
        return _to_wkb_no_srid_header(source)
    except ValueError as exc:
        if "unexpected end of data reading u32" in str(exc):
            raise ValueError("WKB value is too short to read header") from None
        raise


def to_ewkb_header(source, srid: int):
    """Embed or replace an EWKB SRID with native header rewrite/full-conversion fallback."""
    if not is_known_srid(srid):
        return to_wkb_no_srid_header(source)
    return _to_ewkb_header(source, srid)


def to_wkb(source, srid: int | None = None) -> bytes:
    """Convert to WKB/EWKB, embedding only positive SRIDs."""
    return _to_wkb(source, srid=_srid_arg(srid))


def to_wkb_no_srid(source) -> bytes:
    """Convert to plain WKB, stripping any embedded SRID."""
    return _to_wkb(source, srid=False)


def to_wkt(source, srid: int | None = None, *, normalize_wkt: bool = False) -> str:
    """Convert to WKT/EWKT, prefixing only positive SRIDs."""
    return _to_wkt(source, srid=_srid_arg(srid), normalize_wkt=normalize_wkt)


def to_wkt_no_srid(source, *, normalize_wkt: bool = False) -> str:
    """Convert to plain WKT, stripping any SRID prefix."""
    return _to_wkt(source, srid=False, normalize_wkt=normalize_wkt)


def to_wkt_for_column(source, srid: int | None, *, normalize_wkt: bool = False) -> str:
    """Convert to WKT/EWKT, preserving source SRID unless the column has one."""
    return _to_wkt(
        source,
        srid=srid if is_known_srid(srid) else None,
        normalize_wkt=normalize_wkt,
    )


def to_hex_wkb_no_srid(source) -> str:
    """Convert to plain hex WKB, stripping any embedded SRID."""
    return _to_hex_wkb(source, srid=False)


def split_wkb_srid(source) -> tuple[str, int | None]:
    """Return ``(plain_wkt, embedded_srid)`` for WKB/EWKB input."""
    if isinstance(source, str):
        source = bytes.fromhex(source)
    return wkb_to_wkt_split_srid(source)


def split_wkt_srid(source: str) -> tuple[bytes, int | None]:
    """Return ``(plain_wkb, srid)`` for WKT/EWKT input."""
    return wkt_to_wkb_split_srid(source)
