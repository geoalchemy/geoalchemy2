import binascii

from sqlalchemy.sql import expression
from .comparator import ST_FunctionMixin


class _SpatialElement(ST_FunctionMixin):
    """
    The base class for :class:`geoalchemy2.elements.WKTElement` and
    :class:`geoalchemy2.elements.WKBElement`.

    The first argument passed to the constructor is the data wrapped
    by the ``_SpatialElement` object being constructed.

    Additional arguments:

    ``srid``

        An integer representing the spatial reference system. E.g. 4326.
        Default value is -1, which means no/unknown reference system.

    """

    def __init__(self, data, srid=-1):
        self.srid = srid
        self.data = data

    def __str__(self):
        return self.desc  # pragma: no cover

    def __repr__(self):
        return "<%s at 0x%x; %r>" % \
            (self.__class__.__name__, id(self), self.desc)  # pragma: no cover


class WKTElement(_SpatialElement, expression.Function):
    """
    Instances of this class wrap a WKT value.

    Usage examples::

        wkt_element_1 = WKTElement('POINT(5 45)')
        wkt_element_2 = WKTElement('POINT(5 45)', srid=4326)

    """

    def __init__(self, *args, **kwargs):
        _SpatialElement.__init__(self, *args, **kwargs)
        expression.Function.__init__(self, "ST_GeomFromText",
                                     self.data, self.srid)

    @property
    def desc(self):
        """
        This element's description string.
        """
        return self.data


class WKBElement(_SpatialElement, expression.Function):
    """
    Instances of this class wrap a WKB value. Geometry values read
    from the database are converted to instances of this type. In
    most cases you won't need to create ``WKBElement`` instances
    yourself.

    Note: you can create ``WKBElement`` objects from Shapely geometries
    using the :func:`geoalchemy2.shape.from_shape` function.
    """

    def __init__(self, *args, **kwargs):
        _SpatialElement.__init__(self, *args, **kwargs)
        expression.Function.__init__(self, "ST_GeomFromWKB",
                                     self.data, self.srid)

    @property
    def desc(self):
        """
        This element's description string.
        """
        return binascii.hexlify(self.data)
