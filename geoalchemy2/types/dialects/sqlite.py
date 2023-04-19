"""This module defines specific functions for SQLite dialect."""
from geoalchemy2.elements import RasterElement
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import to_shape


def bind_processor_process(spatial_type, bindvalue):
    if isinstance(bindvalue, WKTElement):
        if bindvalue.extended:
            return "%s" % (bindvalue.data)
        else:
            return "SRID=%d;%s" % (bindvalue.srid, bindvalue.data)
    elif isinstance(bindvalue, WKBElement):
        # With SpatiaLite we use Shapely to convert the WKBElement to an EWKT string
        shape = to_shape(bindvalue)
        return "SRID=%d;%s" % (bindvalue.srid, shape.wkt)
    elif isinstance(bindvalue, RasterElement):
        return "%s" % (bindvalue.data)
    else:
        return bindvalue
