"""
This module provides utility functions for integrating with Shapely.

.. note::

    As GeoAlchemy 2 itself has no dependency on `Shapely`, applications using
    functions of this module have to ensure that `Shapely` is available.
"""

from copy import copy
import shapely.wkb
import shapely.wkt
import shapely.geos

from .elements import WKBElement, EWKBElement, WKTElement
from .compat import buffer, bytes


def to_shape(element):
    """
    Function to convert a :class:`geoalchemy2.types.SpatialElement`
    to a Shapely geometry.

    Example::

        lake = Session.query(Lake).get(1)
        polygon = to_shape(lake.geom)
    """
    assert isinstance(element, (WKBElement, EWKBElement, WKTElement))
    if isinstance(element, WKBElement) or isinstance(element, EWKBElement):
        return shapely.wkb.loads(bytes(element.data))
    elif isinstance(element, WKTElement):
        return shapely.wkt.loads(element.data)


def from_shape(shape, srid=-1, use_ewkb=False):
    """
    Function to convert a Shapely geometry to a
    :class:`geoalchemy2.types.WKBElement` or a
    :class:`geoalchemy2.types.EWKBElement.

    Additional arguments:

    ``srid``

        An integer representing the spatial reference system. E.g. 4326.
        Default value is -1, which means no/unknown reference system.

    ``use_ewkb``

        Boolean indicating whether converting the shape to
        a WKBElement or to a EWKBElement. srid is ignored if use_ewkb is set.
        Default value to False.

    Example::

        from shapely.geometry import Point
        wkb_element = from_shape(Point(5, 45), srid=4326)
    """
    if use_ewkb:
        wkbwriter_defaults = shapely.geos.WKBWriter.defaults
        wkbwriter_defaults_copy = copy(wkbwriter_defaults)
        wkbwriter_defaults_copy['include_srid'] = True
        shapely.geos.WKBWriter.defaults = wkbwriter_defaults_copy
        try:
            element = EWKBElement(buffer(shape.wkb))
        finally:
            shapely.geos.WKBWriter.defaults = wkbwriter_defaults
        return element
    else:
        element = WKBElement(buffer(shape.wkb), srid=srid)
    return element
