"""

This module defines a ``Comparator`` class for use with geometry and geography
objects. This is where spatial operators, like ``&&``, ``&<``, are defined.
Spatial operators very often apply to the bounding boxes of geometries. For
example, ``geom1 && geom2`` indicates if geom1's bounding box intersects
geom2's.

Examples
--------

Select the objects whose bounding boxes are to the left of the
bounding box of ``POLYGON((-5 45,5 45,5 -45,-5 -45,-5 45))``::

    select([table]).where(table.c.geom.to_left(
        'POLYGON((-5 45,5 45,5 -45,-5 -45,-5 45))'))

The ``<<`` and ``>>`` operators are a bit specific, because they have
corresponding Python operator (``__lshift__`` and ``__rshift__``). The
above ``SELECT`` expression can thus be rewritten like this::

    select([table]).where(
        table.c.geom << 'POLYGON((-5 45,5 45,5 -45,-5 -45,-5 45))')

Operators can also be used when using the ORM. For example::

    Session.query(Cls).filter(
        Cls.geom << 'POLYGON((-5 45,5 45,5 -45,-5 -45,-5 45))')

Now some other examples with the ``<#>`` operator.

Select the ten objects that are the closest to ``POINT(0 0)`` (typical
closed neighbors problem)::

    select([table]).order_by(table.c.geom.distance_box('POINT(0 0)')).limit(10)

Using the ORM::

    Session.query(Cls).order_by(Cls.geom.distance_box('POINT(0 0)')).limit(10)

Reference
---------
"""

from sqlalchemy.types import UserDefinedType
try:
    from sqlalchemy.sql.functions import _FunctionGenerator
except ImportError:  # SQLA < 0.9
    from sqlalchemy.sql.expression import _FunctionGenerator


class BaseComparator(UserDefinedType.Comparator):
    """
    A custom comparator base class. It adds the ability to call spatial
    functions on columns that use this kind of comparator. It also defines
    functions that map to operators supported by ``Geometry``, ``Geography``
    and ``Raster`` columns.

    This comparator is used by the :class:`geoalchemy2.types.Raster`.
    """

    key = None

    def __getattr__(self, name):

        # Function names that don't start with "ST_" are rejected.
        # This is not to mess up with SQLAlchemy's use of
        # hasattr/getattr on Column objects.

        if not name.startswith('ST_'):
            raise AttributeError

        # We create our own _FunctionGenerator here, and use it in place of
        # SQLAlchemy's "func" object. This is to be able to "bind" the
        # function to the SQL expression. See also GenericFunction above.

        func_ = _FunctionGenerator(expr=self.expr)
        return getattr(func_, name)

    def intersects(self, other):
        """
        The ``&&`` operator. A's BBOX intersects B's.
        """
        return self.op('&&')(other)

    def overlaps_or_to_left(self, other):
        """
        The ``&<`` operator. A's BBOX overlaps or is to the left of B's.
        """
        return self.op('&<')(other)

    def overlaps_or_to_right(self, other):
        """
        The ``&>`` operator. A's BBOX overlaps or is to the right of B's.
        """
        return self.op('&>')(other)


class Comparator(BaseComparator):
    """
    A custom comparator class. Used in :class:`geoalchemy2.types.Geometry`
    and :class:`geoalchemy2.types.Geography`.

    This is where spatial operators like ``<<`` and ``<->`` are defined.
    """

    def overlaps_or_below(self, other):
        """
        The ``&<|`` operator. A's BBOX overlaps or is below B's.
        """
        return self.op('&<|')(other)

    def to_left(self, other):
        """
        The ``<<`` operator. A's BBOX is strictly to the left of B's.
        """
        return self.op('<<')(other)

    def __lshift__(self, other):
        """
        The ``<<`` operator. A's BBOX is strictly to the left of B's.
        Same as ``to_left``, so::

            table.c.geom << 'POINT(1 2)'

        is the same as::

            table.c.geom.to_left('POINT(1 2)')
        """
        return self.to_left(other)

    def below(self, other):
        """
        The ``<<|`` operator. A's BBOX is strictly below B's.
        """
        return self.op('<<|')(other)

    def to_right(self, other):
        """
        The ``>>`` operator. A's BBOX is strictly to the right of B's.
        """
        return self.op('>>')(other)

    def __rshift__(self, other):
        """
        The ``>>`` operator. A's BBOX is strictly to the left of B's.
        Same as `to_`right``, so::

            table.c.geom >> 'POINT(1 2)'

        is the same as::

            table.c.geom.to_right('POINT(1 2)')
        """
        return self.to_right(other)

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

    def contains(self, other, **kw):
        """
        The ``~`` operator. A's BBOX contains B's.
        """
        return self.op('~')(other)

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
