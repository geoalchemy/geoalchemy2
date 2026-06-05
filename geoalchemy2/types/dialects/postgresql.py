"""This module defines specific functions for Postgresql dialect."""

from geoalchemy2 import _wkb_wkt
from geoalchemy2._wkb_wkt import is_known_srid
from geoalchemy2.elements import RasterElement
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.types.dialects.common import as_binary_ewkb
from geoalchemy2.types.dialects.common import as_binary_wkb
from geoalchemy2.types.dialects.common import is_ewkb_constructor
from geoalchemy2.types.dialects.common import is_wkb_constructor
from geoalchemy2.types.dialects.common import validate_wkb_srid


def bind_processor_process(spatial_type, bindvalue):
    if isinstance(bindvalue, WKTElement):
        if bindvalue.extended:
            return bindvalue.data
        else:
            return _wkb_wkt.to_wkt(bindvalue.data, srid=bindvalue.srid)
    elif isinstance(bindvalue, WKBElement):
        if is_ewkb_constructor(spatial_type):
            return as_binary_ewkb(bindvalue, column_srid=spatial_type.srid)
        elif is_wkb_constructor(spatial_type):
            return as_binary_wkb(bindvalue)
        elif not bindvalue.extended:
            return _wkb_wkt.to_wkt(bindvalue.data, srid=bindvalue.srid)
        else:
            # PostGIS ST_GeomFromEWKT works with EWKT strings as well
            # as EWKB hex strings
            return bindvalue.desc
    elif isinstance(bindvalue, RasterElement):
        return f"{bindvalue.data}"
    elif isinstance(bindvalue, str):
        if is_ewkb_constructor(spatial_type):
            return as_binary_ewkb(bindvalue, column_srid=spatial_type.srid)
        elif is_wkb_constructor(spatial_type):
            return as_binary_wkb(bindvalue)
        return bindvalue
    elif isinstance(bindvalue, (bytes, bytearray, memoryview)):
        if is_ewkb_constructor(spatial_type):
            return as_binary_ewkb(bindvalue, column_srid=spatial_type.srid)
        elif is_wkb_constructor(spatial_type):
            return as_binary_wkb(bindvalue)
        wkt, srid = _wkb_wkt.split_wkb_srid(bindvalue)
        if is_known_srid(srid):
            validate_wkb_srid(spatial_type.srid, srid)
            return _wkb_wkt.to_wkt(wkt, srid=srid)
        return _wkb_wkt.to_wkt(wkt, srid=spatial_type.srid)
    else:
        return bindvalue
