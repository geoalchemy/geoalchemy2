"""This module defines specific functions for MySQL dialect."""

from geoalchemy2 import _wkb_wkt
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.elements import _SpatialElement
from geoalchemy2.exc import ArgumentError


def _is_wkb_constructor(spatial_type):
    return "wkb" in (getattr(spatial_type, "from_text", "") or "").lower()


def _as_wkb_hex(bindvalue):
    wkb_element = bindvalue if isinstance(bindvalue, WKBElement) else WKBElement(bindvalue)
    return wkb_element.as_wkb().desc


def _validate_wkb_srid(spatial_type, srid):
    if srid is not None and srid != spatial_type.srid:
        raise ArgumentError(
            f"The SRID ({srid}) of the supplied value is different "
            f"from the one of the column ({spatial_type.srid})"
        )


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
        return wkt_match.group(3)

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
        return bindvalue
    elif isinstance(bindvalue, WKBElement):
        if not _is_wkb_constructor(spatial_type):
            return _normalize_mariadb_wkt(_wkb_wkt.to_wkt_no_srid(bindvalue.data))
        # MariaDB does not support raw binary data so we use the hex representation
        return _as_wkb_hex(bindvalue)
    elif isinstance(bindvalue, (bytes, bytearray, memoryview)):
        if _is_wkb_constructor(spatial_type):
            return _as_wkb_hex(bindvalue)
        wkt, srid = _wkb_wkt.split_wkb_srid(bindvalue)
        _validate_wkb_srid(spatial_type, srid)
        return _normalize_mariadb_wkt(wkt)
    return bindvalue
