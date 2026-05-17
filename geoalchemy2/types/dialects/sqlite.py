"""This module defines specific functions for SQLite dialect."""

import re
import warnings

from geoalchemy2 import _wkb_wkt
from geoalchemy2.elements import RasterElement
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.types.dialects.common import as_binary_wkb
from geoalchemy2.types.dialects.common import as_wkb_hex
from geoalchemy2.types.dialects.common import is_ewkb_constructor
from geoalchemy2.types.dialects.common import is_wkb_constructor


def format_geom_type(wkt, default_srid=None):
    """Format the Geometry type for SQLite."""
    match = re.match(WKTElement.SPLIT_WKT_PATTERN, wkt)
    if match is None:
        warnings.warn(
            "The given WKT could not be parsed by GeoAlchemy2, this could lead to undefined "
            f"behavior with Z, M or ZM geometries or with incorrect SRID. The WKT string is: {wkt}",
            stacklevel=1,
        )
        return wkt
    _, srid, geom_type, coords = match.groups()
    geom_type = geom_type.replace(" ", "")
    if geom_type.endswith("ZM"):
        geom_type = geom_type[:-2]
    elif geom_type.endswith("Z"):
        geom_type = geom_type[:-1]
    if srid is None and default_srid is not None:
        srid = f"SRID={default_srid}"
    if srid is not None:
        return f"{srid};{geom_type}{coords}"
    else:
        return f"{geom_type}{coords}"


def bind_processor_process(spatial_type, bindvalue):
    use_ewkb_constructor = is_ewkb_constructor(spatial_type)
    if isinstance(bindvalue, WKTElement):
        return format_geom_type(
            bindvalue.data,
            default_srid=bindvalue.srid if bindvalue.srid >= 0 else spatial_type.srid,
        )
    elif isinstance(bindvalue, WKBElement):
        if is_wkb_constructor(spatial_type):
            if use_ewkb_constructor:
                return as_wkb_hex(bindvalue, strip_srid=False)
            return as_binary_wkb(bindvalue)
        res = format_geom_type(
            _wkb_wkt.to_wkt_no_srid(bindvalue.data),
            default_srid=bindvalue.srid if bindvalue.srid >= 0 else spatial_type.srid,
        )
        return res
    elif isinstance(bindvalue, RasterElement):
        return f"{bindvalue.data}"
    elif isinstance(bindvalue, str):
        if is_wkb_constructor(spatial_type):
            if use_ewkb_constructor:
                return as_wkb_hex(bindvalue, strip_srid=False)
            return as_binary_wkb(bindvalue)
        return format_geom_type(bindvalue, default_srid=spatial_type.srid)
    elif isinstance(bindvalue, (bytes, bytearray, memoryview)):
        if is_wkb_constructor(spatial_type):
            if use_ewkb_constructor:
                return as_wkb_hex(bindvalue, strip_srid=False)
            return as_binary_wkb(bindvalue)
        wkt, srid = _wkb_wkt.split_wkb_srid(bindvalue)
        return format_geom_type(
            wkt,
            default_srid=srid if srid is not None and srid >= 0 else spatial_type.srid,
        )
    else:
        return bindvalue
