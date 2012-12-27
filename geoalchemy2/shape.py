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
    """
    return WKBElement(buffer(shape.wkb), srid=srid)
