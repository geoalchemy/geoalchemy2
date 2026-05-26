"""This module defines specific functions for Postgresql dialect."""

from geoalchemy2 import _wkb_wkt
from geoalchemy2.elements import RasterElement
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.exc import ArgumentError
from geoalchemy2.types.dialects.common import as_binary_wkb
from geoalchemy2.types.dialects.common import is_wkb_constructor


def bind_processor_process(spatial_type, bindvalue):
    if isinstance(bindvalue, WKTElement):
        if bindvalue.extended:
            return bindvalue.data
        else:
            return _wkb_wkt.to_wkt(bindvalue.data, srid=bindvalue.srid)
    elif isinstance(bindvalue, WKBElement):
        if is_wkb_constructor(spatial_type):
            return as_binary_wkb(bindvalue)
        elif not bindvalue.extended:
            return _wkb_wkt.to_wkt(bindvalue.data, srid=bindvalue.srid)
        else:
            # PostGIS ST_GeomFromEWKT works with EWKT strings as well
            # as EWKB hex strings
            return bindvalue.desc
    elif isinstance(bindvalue, RasterElement):
        return f"{bindvalue.data}"
    elif isinstance(bindvalue, (bytes, bytearray, memoryview)):
        if is_wkb_constructor(spatial_type):
            return as_binary_wkb(bindvalue)
        wkt, srid = _wkb_wkt.split_wkb_srid(bindvalue)
        column_srid = spatial_type.srid
        if srid is not None and srid > 0:
            if column_srid > 0 and srid != column_srid:
                raise ArgumentError(
                    f"The SRID ({srid}) of the supplied value is different "
                    f"from the one of the column ({column_srid})"
                )
            return _wkb_wkt.to_wkt(wkt, srid=srid)
        return _wkb_wkt.to_wkt(wkt, srid=column_srid)
    else:
        return bindvalue
