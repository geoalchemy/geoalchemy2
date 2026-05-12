"""Private wrappers around :mod:`wkb_wkt_converter`.

GeoAlchemy2 uses ``-1`` and sometimes ``0`` as "unknown/no SRID" sentinels in
Python objects and SQL compilation. The converter accepts those values in 0.5,
but centralising the mapping keeps that policy explicit for callers.
"""

from __future__ import annotations

import struct

from wkb_wkt_converter import to_hex_wkb as _to_hex_wkb
from wkb_wkt_converter import to_wkb as _to_wkb
from wkb_wkt_converter import to_wkt as _to_wkt
from wkb_wkt_converter import wkb_to_wkt_split_srid
from wkb_wkt_converter import wkt_to_wkb_split_srid

_EWKB_SRID_FLAG = 0x20000000
_EWKB_M_FLAG = 0x40000000
_EWKB_Z_FLAG = 0x80000000
_EWKB_TYPE_FLAGS = _EWKB_SRID_FLAG | _EWKB_M_FLAG | _EWKB_Z_FLAG
_WKB_HEADER_LENGTH = 5
_EWKB_HEADER_LENGTH = 9
_SIMPLE_WKB_TYPES = frozenset({1, 2, 3})


def _srid_arg(srid: int | None) -> int | bool:
    return srid if srid is not None and srid > 0 else False


def _header_bytes(source, length: int) -> bytes:
    if isinstance(source, str):
        if len(source) < length * 2:
            raise ValueError("WKB value is too short to read header")
        return bytes.fromhex(source[: length * 2])

    if len(source) < length:
        raise ValueError("WKB value is too short to read header")

    data = source[:length]
    return data.tobytes() if isinstance(data, memoryview) else bytes(data)


def _unpack_wkb_header(source) -> tuple[int, str, int, int | None]:
    header = _header_bytes(source, _WKB_HEADER_LENGTH)
    byte_order = header[0]
    if byte_order not in (0, 1):
        raise ValueError(f"Invalid WKB byte order marker: {byte_order}")

    endian = "little" if byte_order else "big"
    marker = "<I" if byte_order else ">I"
    wkb_type = struct.unpack(marker, header[1:5])[0]
    srid = None
    if wkb_type & _EWKB_SRID_FLAG:
        srid_header = _header_bytes(source, _EWKB_HEADER_LENGTH)
        srid = struct.unpack(marker, srid_header[5:9])[0]
    return byte_order, endian, wkb_type, srid


def _can_header_rewrite_type(wkb_type: int) -> bool:
    return (wkb_type & ~_EWKB_TYPE_FLAGS) in _SIMPLE_WKB_TYPES


def can_header_rewrite(source) -> bool:
    """Return whether SRID edits can be done without parsing nested geometry."""
    _, _, wkb_type, _ = _unpack_wkb_header(source)
    return _can_header_rewrite_type(wkb_type)


def wkb_srid(source) -> int | None:
    """Return the embedded EWKB SRID without parsing the full geometry."""
    _, _, _, srid = _unpack_wkb_header(source)
    return srid


def to_wkb_no_srid_header(source):
    """Strip the EWKB SRID with a header rewrite when that is equivalent."""
    byte_order, endian, wkb_type, srid = _unpack_wkb_header(source)
    if not _can_header_rewrite_type(wkb_type):
        return to_wkb_no_srid(source)
    if srid is None:
        return source

    wkb_type &= ~_EWKB_SRID_FLAG
    type_bytes = wkb_type.to_bytes(4, endian)
    if isinstance(source, str):
        return source[:2] + type_bytes.hex() + source[_EWKB_HEADER_LENGTH * 2 :]

    buffer = bytearray()
    buffer.append(byte_order)
    buffer.extend(type_bytes)
    buffer.extend(source[_EWKB_HEADER_LENGTH:])
    return memoryview(buffer)


def to_ewkb_header(source, srid: int):
    """Embed or replace an EWKB SRID with a header rewrite when equivalent."""
    if srid <= 0:
        return to_wkb_no_srid_header(source)

    byte_order, endian, wkb_type, embedded_srid = _unpack_wkb_header(source)
    if not _can_header_rewrite_type(wkb_type):
        return to_wkb(source, srid=srid)
    wkb_type |= _EWKB_SRID_FLAG
    type_bytes = wkb_type.to_bytes(4, endian)
    srid_bytes = srid.to_bytes(4, endian)
    payload_offset = _EWKB_HEADER_LENGTH if embedded_srid is not None else _WKB_HEADER_LENGTH

    if isinstance(source, str):
        return source[:2] + type_bytes.hex() + srid_bytes.hex() + source[payload_offset * 2 :]

    buffer = bytearray()
    buffer.append(byte_order)
    buffer.extend(type_bytes)
    buffer.extend(srid_bytes)
    buffer.extend(source[payload_offset:])
    return memoryview(buffer)


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
        srid=srid if srid is not None and srid > 0 else None,
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
