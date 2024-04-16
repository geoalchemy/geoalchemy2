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
        if bindvalue.srid == -1:
            bindvalue.srid = spatial_type.srid
        # With SpatiaLite we use Shapely to convert the WKBElement to an EWKT string
        shape = to_shape(bindvalue)
        result = "SRID=%d;%s" % (bindvalue.srid, shape.wkt)
        if shape.has_z:
            # shapely.wkb.loads returns geom_type with a 'Z', for example, 'LINESTRING Z'
            # which is a limitation with SpatiaLite. Hence, a temporary fix.
            result = result.replace("Z ", "")
        return result
    elif isinstance(bindvalue, RasterElement):
        return "%s" % (bindvalue.data)
    else:
        return bindvalue
