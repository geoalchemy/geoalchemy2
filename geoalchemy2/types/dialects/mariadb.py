"""This module defines specific functions for MySQL dialect."""

from geoalchemy2 import _wkb_wkt
from geoalchemy2._wkb_wkt import is_known_srid
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.elements import _SpatialElement
from geoalchemy2.exc import ArgumentError
from geoalchemy2.types.dialects.common import as_wkb_hex
from geoalchemy2.types.dialects.common import is_wkb_constructor
from geoalchemy2.types.dialects.common import validate_wkb_srid


def _normalize_mariadb_wkt(wkt):
    if "multipoint" in wkt[:20].lower() and "empty" not in wkt[:30].lower():
        # MariaDB does not support ISO WKT with parentheses around each sub-point.
        first_idx = wkt.find("(")
        last_idx = wkt.rfind(")")
        if first_idx == -1 or last_idx == -1:
            return wkt
        wkt = (
            wkt[: first_idx + 1]
            + wkt[first_idx:last_idx].replace("(", "").replace(")", "")
            + wkt[last_idx:]
        )
    return wkt


def bind_processor_process(spatial_type, bindvalue):
    if isinstance(bindvalue, str):
        if is_wkb_constructor(spatial_type):
            return as_wkb_hex(bindvalue, column_srid=spatial_type.srid)

        wkt_match = WKTElement._REMOVE_SRID.match(bindvalue)
        srid = wkt_match.group(2)
        try:
            if srid is not None:
                srid = int(srid)
        except (ValueError, TypeError):  # pragma: no cover
            raise ArgumentError(
                f"The SRID ({srid}) of the supplied value can not be casted to integer"
            ) from None

        validate_wkb_srid(spatial_type.srid, srid)
        return wkt_match.group(3)

    if isinstance(bindvalue, _SpatialElement):
        validate_wkb_srid(spatial_type.srid, bindvalue.srid)

    if isinstance(bindvalue, WKTElement):
        bindvalue = bindvalue.as_wkt()
        if not is_known_srid(bindvalue.srid):
            bindvalue.srid = spatial_type.srid
        return bindvalue
    elif isinstance(bindvalue, WKBElement):
        if not is_wkb_constructor(spatial_type):
            return _normalize_mariadb_wkt(_wkb_wkt.to_wkt_no_srid(bindvalue.data))
        # MariaDB does not support raw binary data so we use the hex representation
        return as_wkb_hex(bindvalue, column_srid=spatial_type.srid)
    elif isinstance(bindvalue, (bytes, bytearray, memoryview)):
        if is_wkb_constructor(spatial_type):
            return as_wkb_hex(bindvalue, column_srid=spatial_type.srid)
        wkt, srid = _wkb_wkt.split_wkb_srid(bindvalue)
        validate_wkb_srid(spatial_type.srid, srid)
        return _normalize_mariadb_wkt(wkt)
    return bindvalue
