"""This module defines specific functions for MSSQL dialect."""

import binascii
import re
import struct

from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.elements import _SpatialElement
from geoalchemy2.exc import ArgumentError
from geoalchemy2.shape import to_shape


_WKT_DIMENSION_SUFFIX = re.compile(
    r"^([A-Z]+?)\s*(ZM|Z|M)(\s*\(.*)$",
    re.IGNORECASE | re.DOTALL,
)
_WKB_TYPE_NAMES = {
    1: "POINT",
    2: "LINESTRING",
    3: "POLYGON",
    4: "MULTIPOINT",
    5: "MULTILINESTRING",
    6: "MULTIPOLYGON",
    7: "GEOMETRYCOLLECTION",
}


def _normalize_wkt_for_mssql(wkt):
    return _WKT_DIMENSION_SUFFIX.sub(r"\1\3", wkt)


def _format_wkb_number(value):
    if value == 0:
        value = 0.0
    return format(value, ".15g")


def _coerce_wkb_data(value):
    if isinstance(value, WKBElement):
        value = value.data
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, str):
        return binascii.unhexlify(value)
    raise TypeError("Unsupported WKB value type")


def _decode_wkb_type(raw_type):
    has_srid = bool(raw_type & 0x20000000)
    has_z = bool(raw_type & 0x80000000)
    has_m = bool(raw_type & 0x40000000)
    base_type = raw_type & 0x1FFFFFFF

    if base_type not in _WKB_TYPE_NAMES:
        if base_type >= 3000:
            base_type -= 3000
            has_z = True
            has_m = True
        elif base_type >= 2000:
            base_type -= 2000
            has_m = True
        elif base_type >= 1000:
            base_type -= 1000
            has_z = True

    type_name = _WKB_TYPE_NAMES.get(base_type)
    if type_name is None:
        raise ValueError(f"Unsupported WKB geometry type: {raw_type}")

    return type_name, 2 + int(has_z) + int(has_m), has_srid


def _read_uint32(data, offset, byte_order):
    return struct.unpack_from(f"{byte_order}I", data, offset)[0], offset + 4


def _read_coord(data, offset, byte_order, dimension):
    values = struct.unpack_from(f"{byte_order}{'d' * dimension}", data, offset)
    coord = " ".join(_format_wkb_number(value) for value in values)
    return coord, offset + (8 * dimension)


def _read_coords(data, offset, byte_order, dimension, count):
    coords = []
    for _ in range(count):
        coord, offset = _read_coord(data, offset, byte_order, dimension)
        coords.append(coord)
    return coords, offset


def _parse_wkb_geometry(data, offset=0):
    byte_order = "<" if data[offset] == 1 else ">"
    offset += 1

    raw_type, offset = _read_uint32(data, offset, byte_order)
    type_name, dimension, has_srid = _decode_wkb_type(raw_type)

    if has_srid:
        _, offset = _read_uint32(data, offset, byte_order)

    if type_name == "POINT":
        coord, offset = _read_coord(data, offset, byte_order, dimension)
        return type_name, f" ({coord})", offset

    if type_name == "LINESTRING":
        point_count, offset = _read_uint32(data, offset, byte_order)
        if point_count == 0:
            return type_name, " EMPTY", offset
        coords, offset = _read_coords(data, offset, byte_order, dimension, point_count)
        return type_name, f" ({', '.join(coords)})", offset

    if type_name == "POLYGON":
        ring_count, offset = _read_uint32(data, offset, byte_order)
        if ring_count == 0:
            return type_name, " EMPTY", offset
        rings = []
        for _ in range(ring_count):
            point_count, offset = _read_uint32(data, offset, byte_order)
            coords, offset = _read_coords(data, offset, byte_order, dimension, point_count)
            rings.append(f"({', '.join(coords)})")
        return type_name, f" ({', '.join(rings)})", offset

    if type_name in {"MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION"}:
        geometry_count, offset = _read_uint32(data, offset, byte_order)
        if geometry_count == 0:
            return type_name, " EMPTY", offset
        geometries = []
        for _ in range(geometry_count):
            child_type, child_text, offset = _parse_wkb_geometry(data, offset)
            if type_name == "GEOMETRYCOLLECTION":
                geometries.append(f"{child_type}{child_text}")
            else:
                geometries.append(child_text.strip())
        return type_name, f" ({', '.join(geometries)})", offset

    raise ValueError(f"Unsupported WKB geometry type: {type_name}")


def _wkb_to_mssql_wkt(value):
    data = _coerce_wkb_data(value)
    type_name, body, _ = _parse_wkb_geometry(data)
    return f"{type_name}{body}"


def _to_mssql_wkt(value):
    try:
        return _wkb_to_mssql_wkt(value)
    except (TypeError, ValueError, struct.error, binascii.Error):
        return _normalize_wkt_for_mssql(to_shape(value).wkt)


def bind_processor_process(spatial_type, bindvalue):
    if isinstance(bindvalue, str):
        wkt_match = WKTElement._REMOVE_SRID.match(bindvalue)
        srid = wkt_match.group(2)
        try:
            if srid is not None:
                srid = int(srid)
        except (ValueError, TypeError):  # pragma: no cover
            raise ArgumentError(
                f"The SRID ({srid}) of the supplied value can not be casted to integer"
            ) from None

        if srid is not None and srid != spatial_type.srid:
            raise ArgumentError(
                f"The SRID ({srid}) of the supplied value is different "
                f"from the one of the column ({spatial_type.srid})"
            )
        return _normalize_wkt_for_mssql(wkt_match.group(3))

    if (
        isinstance(bindvalue, _SpatialElement)
        and bindvalue.srid != -1
        and bindvalue.srid != spatial_type.srid
    ):
        raise ArgumentError(
            f"The SRID ({bindvalue.srid}) of the supplied value is different "
            f"from the one of the column ({spatial_type.srid})"
        )

    if isinstance(bindvalue, WKTElement):
        bindvalue = bindvalue.as_wkt()
        if bindvalue.srid <= 0:
            bindvalue.srid = spatial_type.srid
        return _normalize_wkt_for_mssql(bindvalue.data)
    elif isinstance(bindvalue, WKBElement):
        return _to_mssql_wkt(bindvalue)
    elif isinstance(bindvalue, (bytes, bytearray, memoryview)):
        return _to_mssql_wkt(bindvalue)
    return bindvalue
