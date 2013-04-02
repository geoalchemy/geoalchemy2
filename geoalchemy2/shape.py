"""
This module provides utility functions for integrating with Shapely.
"""

import shapely.wkb
import shapely.wkt

from .elements import WKBElement, WKTElement


def to_shape(element):
    """
    Function to convert a :class:`geoalchemy2.types.SpatialElement`
    to a Shapely geometry.

    Example::

        lake = Session.query(Lake).get(1)
        polygon = to_shape(lake.geom)
    """
    assert isinstance(element, (WKBElement, WKTElement))
    if isinstance(element, WKBElement):
        return shapely.wkb.loads(str(element.data))
    elif isinstance(element, WKTElement):
        return shapely.wkt.loads(element.data)


def from_shape(shape, srid=-1):
    """
    Function to convert a Shapely geometry to a
    :class:`geoalchemy2.types.WKBElement`.

    Additional arguments:

    ``srid``

        An integer representing the spatial reference system. E.g. 4326.
        Default value is -1, which means no/unknown reference system.

    Example::

        from shapely.geometry import Point
        wkb_element = from_shape(Point(5, 45), srid=4326)
    """
    return WKBElement(buffer(shape.wkb), srid=srid)  # flake8: noqa
