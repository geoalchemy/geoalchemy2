"""This module defines specific functions for MySQL dialect."""

from geoalchemy2 import _wkb_wkt
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.elements import _SpatialElement
from geoalchemy2.exc import ArgumentError


def _is_wkb_constructor(spatial_type):
    return "wkb" in (getattr(spatial_type, "from_text", "") or "").lower()


def _as_binary_wkb(bindvalue):
    wkb_element = bindvalue if isinstance(bindvalue, WKBElement) else WKBElement(bindvalue)
    bindvalue = wkb_element.as_wkb().data
    if isinstance(bindvalue, memoryview):
        return bindvalue.tobytes()
    if isinstance(bindvalue, str):
        return WKBElement._data_from_desc(bindvalue)
    return bytes(bindvalue)


def _validate_raw_wkb_srid(spatial_type, bindvalue):
    _, srid = _wkb_wkt.split_wkb_srid(bindvalue)
    if srid is not None and srid != spatial_type.srid:
        raise ArgumentError(
            f"The SRID ({srid}) of the supplied value is different "
            f"from the one of the column ({spatial_type.srid})"
        )


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
        if _is_wkb_constructor(spatial_type):
            return _as_binary_wkb(bindvalue)
        else:
            return _wkb_wkt.to_wkt_no_srid(bindvalue.data)
    elif isinstance(bindvalue, (bytes, bytearray, memoryview)):
        if _is_wkb_constructor(spatial_type):
            return _as_binary_wkb(bindvalue)
        _validate_raw_wkb_srid(spatial_type, bindvalue)
        return _wkb_wkt.to_wkt_no_srid(bindvalue)
    return bindvalue
