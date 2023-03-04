"""This module defines specific functions for SQLite dialect."""
from geoalchemy2.elements import RasterElement
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.exc import ArgumentError

try:
    from geoalchemy2.shape import to_shape

    SHAPELY = True
except ImportError:
    SHAPELY = False


def bind_processor_process(spatial_type, bindvalue):
    if isinstance(bindvalue, WKTElement):
        if bindvalue.extended:
            return "%s" % (bindvalue.data)
        else:
            return "SRID=%d;%s" % (bindvalue.srid, bindvalue.data)
    elif isinstance(bindvalue, WKBElement):
        # With SpatiaLite or when the WKBElement includes a WKB value rather
        # than a EWKB value we use Shapely to convert the WKBElement to an
        # EWKT string
        if not SHAPELY:
            raise ArgumentError(
                "Shapely is required for handling WKBElement bind "
                "values when using SpatiaLite or when the bind value "
                "is a WKB rather than an EWKB"
            )
        shape = to_shape(bindvalue)
        return "SRID=%d;%s" % (bindvalue.srid, shape.wkt)
    elif isinstance(bindvalue, RasterElement):
        return "%s" % (bindvalue.data)
    else:
        return bindvalue
