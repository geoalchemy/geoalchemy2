"""This module defines specific functions for SQLite dialect."""

import re

from geoalchemy2.elements import RasterElement
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import to_shape


def format_geom_type(wkt, forced_srid=None):
    """Format the Geometry type for SQLite."""
    match = re.match(WKTElement.SPLIT_WKT_PATTERN, wkt)
    if match is None:
        return wkt
    _, srid, geom_type, coords = match.groups()
    geom_type = geom_type.replace(" ", "")
    if geom_type.endswith("M"):
        geom_type = geom_type[:-1]
    if geom_type.endswith("Z"):
        geom_type = geom_type[:-1]
    if forced_srid is not None:
        srid = f"SRID={forced_srid}"
    if srid is not None:
        return "%s;%s%s" % (srid, geom_type, coords)
    else:
        return "%s%s" % (geom_type, coords)


def bind_processor_process(spatial_type, bindvalue):
    if isinstance(bindvalue, WKTElement):
        return format_geom_type(bindvalue.data, forced_srid=bindvalue.srid)
    elif isinstance(bindvalue, WKBElement):
        if bindvalue.srid == -1:
            bindvalue.srid = spatial_type.srid
        # With SpatiaLite we use Shapely to convert the WKBElement to an EWKT string
        shape = to_shape(bindvalue)
        # shapely.wkb.loads returns geom_type with a 'Z', for example, 'LINESTRING Z'
        # which is a limitation with SpatiaLite. Hence, a temporary fix.
        return format_geom_type(shape.wkt, forced_srid=bindvalue.srid)
    elif isinstance(bindvalue, RasterElement):
        return "%s" % (bindvalue.data)
    else:
        return format_geom_type(bindvalue)
