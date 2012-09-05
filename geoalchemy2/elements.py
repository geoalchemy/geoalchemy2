import binascii

from sqlalchemy.sql import expression


class _SpatialElement(object):
    """
    The base class for :class:`geoalchemy2.elements.WKTElement` and
    :class:`geoalchemy2.elements.WKBElement`.
    """

    def __str__(self):
        return self.desc  # pragma: no cover

    def __repr__(self):
        return "<%s at 0x%x; %r>" % \
            (self.__class__.__name__, id(self), self.desc)  # pragma: no cover


class WKTElement(_SpatialElement, expression.Function):
    """
    Instances of this class wrap a WKT value.
    """

    def __init__(self, data, srid=-1):
        self.srid = srid
        self.data = data
        expression.Function.__init__(self, "ST_GeomFromText", data)

    @property
    def desc(self):
        """
        This element's description string.
        """
        return self.data


class WKBElement(_SpatialElement, expression.Function):
    """
    Instances of this class wrap a WKB value. Geometry values read
    from the database are converted to instances of this type.
    """

    def __init__(self, data):
        self.data = data
        expression.Function.__init__(self, "ST_GeomFromWKB", data)

    @property
    def desc(self):
        """
        This element's description string.
        """
        return binascii.hexlify(self.data)

    def __getattr__(self, name):
        #
        # This is how things like lake.geom.ST_Buffer(2) creates
        # SQL expressions of this form:
        #
        # ST_Buffer(ST_GeomFromWKB(:ST_GeomFromWKB_1), :param_1)
        #

        # We create our own _FunctionGenerator here, and use it in place of
        # SQLAlchemy's "func" object. This is to be able to "bind" the
        # function to the SQL expression. See also GenericFunction above.

        func_ = expression._FunctionGenerator(expr=self)
        return getattr(func_, name)
