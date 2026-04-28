"""This module defines specific functions for Postgresql dialect."""

from geoalchemy2.elements import RasterElement
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import to_shape


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
            return f"{bindvalue.data}"
        else:
            return f"SRID={bindvalue.srid};{bindvalue.data}"
    elif isinstance(bindvalue, WKBElement):
        if _is_wkb_constructor(spatial_type):
            return _as_binary_wkb(bindvalue)
        elif not bindvalue.extended:
            # When the WKBElement includes a WKB value rather
            # than a EWKB value we use Shapely to convert the WKBElement to an
            # EWKT string
            shape = to_shape(bindvalue)
            return f"SRID={bindvalue.srid};{shape.wkt}"
        else:
            # PostGIS ST_GeomFromEWKT works with EWKT strings as well
            # as EWKB hex strings
            return bindvalue.desc
    elif isinstance(bindvalue, RasterElement):
        return f"{bindvalue.data}"
    elif isinstance(bindvalue, (bytes, memoryview)) and _is_wkb_constructor(spatial_type):
        return _as_binary_wkb(bindvalue)
    else:
        return bindvalue
