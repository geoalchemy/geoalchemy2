"""Pure-Python fallback for header-only WKB/EWKB and WKT SRID handling.

Restores the behavior GeoAlchemy2 had at tag ``0.20.0``, before the optional
``wkb-wkt-converter`` dependency existed: SRID/EPSG extraction and injection
work directly on the EWKB header bytes (the SRID bit in the WKB type flag and
the 4 SRID bytes that follow it), without parsing the full geometry. WKT SRID
prefixing/stripping is pure string/regex work.

Used automatically by :mod:`geoalchemy2._wkb_wkt` when ``wkb-wkt-converter``
is not installed. Real WKB<->WKT geometry conversion is not reproducible here
(it requires either the converter or Shapely) and is out of scope.
"""

from __future__ import annotations

import binascii
import re
import struct

_HAS_SRID_BIT = 0x20000000
_CLEAR_SRID_BIT_MASK = 0xDFFFFFFF

_WKT_SRID_PREFIX = re.compile(r"(SRID=(-?[0-9]+); ?)?(.*)", re.IGNORECASE | re.DOTALL)


def _byte_order_marker(byte_order: int, *, signed: bool = False) -> str:
    if signed:
        return "<i" if byte_order else ">i"
    return "<I" if byte_order else ">I"


def wkb_header_srid(
    source: str | bytes | bytearray | memoryview,
    *,
    include_unknown: bool = False,
) -> int | None:
    """Sniff the embedded EWKB SRID from the header without parsing the geometry."""
    try:
        header = binascii.unhexlify(source[:18]) if isinstance(source, str) else bytes(source[:9])
        if len(header) < 5:
            return None
        byte_order, wkb_type = header[0], header[1:5]
        if len(wkb_type) != 4:
            return None
        wkb_type_int = struct.unpack(_byte_order_marker(byte_order), wkb_type)[0]
        if not wkb_type_int & _HAS_SRID_BIT:
            return None
        wkb_srid_bytes = header[5:9]
        if len(wkb_srid_bytes) != 4:
            return None
        srid = struct.unpack(_byte_order_marker(byte_order, signed=True), wkb_srid_bytes)[0]
    except (struct.error, binascii.Error, IndexError, TypeError) as exc:
        raise ValueError(str(exc)) from exc
    if (srid == 0 or srid == -1) and not include_unknown:
        return None
    return int(srid)


def to_wkb_no_srid_header(source: str | bytes | bytearray | memoryview) -> str | bytes:
    """Strip the SRID bit and 4-byte SRID field from an EWKB header, if present."""
    if isinstance(source, str):
        try:
            header = binascii.unhexlify(source[:18])
        except (binascii.Error, TypeError) as exc:
            raise ValueError(str(exc)) from exc

        if len(header) < 5:
            raise ValueError("WKB value is too short to read header")

        byte_order = header[0]
        wkb_type = header[1:5]
        wkb_type_int = struct.unpack(_byte_order_marker(byte_order), wkb_type)[0]

        if not wkb_type_int & _HAS_SRID_BIT:
            return source

        if len(header) < 9:
            raise ValueError("WKB value is too short to read header")

        wkb_type_int &= _CLEAR_SRID_BIT_MASK
        little_endian = bool(byte_order)
        wkb_type_bytes = wkb_type_int.to_bytes(4, "little" if little_endian else "big")
        return source[:2] + binascii.hexlify(wkb_type_bytes).decode("ascii") + source[18:]

    full = bytes(source)
    header = full[:9]
    if len(header) < 5:
        raise ValueError("WKB value is too short to read header")

    byte_order = header[0]
    wkb_type = header[1:5]
    wkb_type_int = struct.unpack(_byte_order_marker(byte_order), wkb_type)[0]

    if not wkb_type_int & _HAS_SRID_BIT:
        return full

    if len(header) < 9:
        raise ValueError("WKB value is too short to read header")

    wkb_type_int &= _CLEAR_SRID_BIT_MASK
    little_endian = bool(byte_order)
    wkb_type_bytes = wkb_type_int.to_bytes(4, "little" if little_endian else "big")

    buffer = bytearray()
    buffer.extend(full[:1])
    buffer.extend(wkb_type_bytes)
    buffer.extend(full[9:])
    return bytes(buffer)


def to_ewkb_header(source: str | bytes | bytearray | memoryview, srid: int) -> str | bytes:
    """Embed or replace the EWKB SRID header for WKB/EWKB ``source``."""
    if isinstance(source, str):
        try:
            header = binascii.unhexlify(source[:18])
        except (binascii.Error, TypeError) as exc:
            raise ValueError(str(exc)) from exc

        if len(header) < 5:
            raise ValueError("WKB value is too short to read header")

        byte_order = header[0]
        wkb_type = header[1:5]
        wkb_type_int = struct.unpack(_byte_order_marker(byte_order), wkb_type)[0]
        has_srid = bool(wkb_type_int & _HAS_SRID_BIT)
        if has_srid and len(header) < 9:
            raise ValueError("WKB value is too short to read header")

        wkb_type_int |= _HAS_SRID_BIT

        little_endian = bool(byte_order)
        wkb_type_bytes = wkb_type_int.to_bytes(4, "little" if little_endian else "big")
        srid_bytes = int(srid).to_bytes(4, "little" if little_endian else "big", signed=True)

        rest_str = source[18:] if has_srid else source[10:]
        return (
            source[:2]
            + binascii.hexlify(wkb_type_bytes).decode("ascii")
            + binascii.hexlify(srid_bytes).decode("ascii")
            + rest_str
        )

    full = bytes(source)
    header = full[:9]
    if len(header) < 5:
        raise ValueError("WKB value is too short to read header")

    byte_order = header[0]
    wkb_type = header[1:5]
    wkb_type_int = struct.unpack(_byte_order_marker(byte_order), wkb_type)[0]
    has_srid = bool(wkb_type_int & _HAS_SRID_BIT)
    if has_srid and len(header) < 9:
        raise ValueError("WKB value is too short to read header")

    wkb_type_int |= _HAS_SRID_BIT

    little_endian = bool(byte_order)
    wkb_type_bytes = wkb_type_int.to_bytes(4, "little" if little_endian else "big")
    srid_bytes = int(srid).to_bytes(4, "little" if little_endian else "big", signed=True)

    rest_bytes = full[9:] if has_srid else full[5:]
    buffer = bytearray()
    buffer.extend(full[:1])
    buffer.extend(wkb_type_bytes)
    buffer.extend(srid_bytes)
    buffer.extend(rest_bytes)
    return bytes(buffer)


def to_hex_wkb_no_srid(source: str | bytes | bytearray | memoryview) -> str:
    """Strip the SRID header and return the remaining WKB as a lowercase hex string."""
    stripped = to_wkb_no_srid_header(source)
    if isinstance(stripped, str):
        return stripped.lower()
    return binascii.hexlify(bytes(stripped)).decode("ascii").lower()


def wkt_strip_srid(source: str) -> str:
    """Return the WKT text with any leading ``SRID=n;`` prefix removed."""
    match = _WKT_SRID_PREFIX.match(source)
    assert match is not None
    return match.group(3)


def wkt_add_srid(source: str, srid: int) -> str:
    """Return the WKT text with any existing SRID prefix replaced by ``srid``."""
    return f"SRID={srid};{wkt_strip_srid(source)}"
