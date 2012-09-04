"""

This module defines a ``Comparator`` class for use with geometry and geography
objects. This is where spatial operators like ``&&``, ``&<`` are defined.

"""

from sqlalchemy.types import UserDefinedType
from sqlalchemy.sql import expression


class Comparator(UserDefinedType.Comparator):
    """
    A custom comparator class. Used in :class:`geoalchemy2.types.Geometry`
    and :class:`geoalchemy2.types.Geography`.

    This is where spatial operators like ``&&`` and ``&<`` are defined.
    """

    def __getattr__(self, name):

        # Function names that don't start with "ST_" are rejected.
        # This is not to mess up with SQLAlchemy's use of
        # hasattr/getattr on Column objects.

        if not name.startswith('ST_'):
            raise AttributeError

        # We create our own _FunctionGenerator here, and use it in place of
        # SQLAlchemy's "func" object. This is to be able to "bind" the
        # function to the SQL expression. See also GenericFunction above.

        func_ = expression._FunctionGenerator(expr=self.expr)
        return getattr(func_, name)

    def intersects(self, other):
        """
        The ``&&`` operator. A's BBOX intersects B's.
        """
        return self.op('&&')(other)

    def overlaps_or_left(self, other):
        """
        The ``&<`` operator. A's BBOX overlaps or is to the left of B's.
        """
        return self.op('&<')(other)

    def overlaps_or_below(self, other):
        """
        The ``&<|`` operator. A's BBOX overlaps or is below B's.
        """
        return self.op('&<|')(other)

    def overlaps_or_right(self, other):
        """
        The ``&>`` operator. A's BBOX overlaps or is to the right of B's.
        """
        return self.op('&>')(other)

    def left(self, other):
        """
        The ``<<`` operator. A's BBOX is strictly to the left of B's.
        """
        return self.op('<<')(other)

    def __lshift__(self, other):
        """
        The ``<<`` operator. A's BBOX is strictly to the left of B's.
        Same as ``left``.
        """
        return self.left(other)

    def below(self, other):
        """
        The ``<<|`` operator. A's BBOX is strictly below B's.
        """
        return self.op('<<|')(other)

    def right(self, other):
        """
        The ``>>`` operator. A's BBOX is strictly to the right of B's.
        """
        return self.op('>>')(other)

    def __rshift__(self, other):
        """
        The ``>>`` operator. A's BBOX is strictly to the left of B's.
        Same as ``right``.
        """
        return self.right(other)

    def contained(self, other):
        """
        The ``@`` operator. A's BBOX is contained by B's.
        """
        return self.op('@')(other)

    def overlaps_or_above(self, other):
        """
        The ``|&>`` operator. A's BBOX overlaps or is to the right of B's.
        """
        return self.op('|&>')(other)

    def above(self, other):
        """
        The ``|>>`` operator. A's BBOX is strictly above B's.
        """
        return self.op('|>>')(other)

    #def contains(self, other):
        #"""
        #The ``~`` operator. A's BBOX contains B's.
        #"""
        #return self.op('~')(other)

    def same(self, other):
        """
        The ``~=`` operator. A's BBOX is the same as B's.
        """
        return self.op('~=')(other)

    def distance_centroid(self, other):
        """
        The ``<->`` operator. The distance between two points.
        """
        return self.op('<->')(other)

    def distance_box(self, other):
        """
        The ``<#>`` operator. The distance between bounding box of two
        geometries.
        """
        return self.op('<#>')(other)
