"""This module defines specific functions for Postgresql dialect."""

from geoalchemy2 import _wkb_wkt
from geoalchemy2.elements import RasterElement
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement


def _is_wkb_constructor(spatial_type):
    return "wkb" in (getattr(spatial_type, "from_text", "") or "").lower()


def _as_binary_wkb(bindvalue):
    if isinstance(bindvalue, WKBElement):
        bindvalue = bindvalue.data
    if isinstance(bindvalue, memoryview):
        return bindvalue.tobytes()
    if isinstance(bindvalue, str):
        return WKBElement._data_from_desc(bindvalue)
    return bytes(bindvalue)


def bind_processor_process(spatial_type, bindvalue):
    if isinstance(bindvalue, WKTElement):
        if bindvalue.extended:
            return bindvalue.data
        else:
            return _wkb_wkt.to_wkt(bindvalue.data, srid=bindvalue.srid)
    elif isinstance(bindvalue, WKBElement):
        if _is_wkb_constructor(spatial_type):
            return _as_binary_wkb(bindvalue)
        elif not bindvalue.extended:
            return _wkb_wkt.to_wkt(bindvalue.data, srid=bindvalue.srid)
        else:
            # PostGIS ST_GeomFromEWKT works with EWKT strings as well
            # as EWKB hex strings
            return bindvalue.desc
    elif isinstance(bindvalue, RasterElement):
        return f"{bindvalue.data}"
    elif isinstance(bindvalue, (bytes, bytearray, memoryview)):
        if _is_wkb_constructor(spatial_type):
            return _as_binary_wkb(bindvalue)
        return _wkb_wkt.to_wkt_for_column(bindvalue, srid=spatial_type.srid)
    else:
        return bindvalue
