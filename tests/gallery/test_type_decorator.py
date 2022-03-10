"""
Automatically use a function at insert or select
================================================

Sometimes the application wants to apply a function in an insert or in a select.
For example, the application might need the geometry with lat/lon coordinates while they
are projected in the DB. To avoid having to always tweak the query with a
``ST_Transform()``, it is possible to define a `TypeDecorator
<https://docs.sqlalchemy.org/en/13/core/custom_types.html#sqlalchemy.types.TypeDecorator>`_
"""
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import func
from sqlalchemy import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import TypeDecorator

from geoalchemy2 import Geometry
from geoalchemy2 import shape

# Tests imports
from tests import test_only_with_dialects

metadata = MetaData()

Base = declarative_base(metadata=metadata)


class TransformedGeometry(TypeDecorator):
    """This class is used to insert a ST_Transform() in each insert or select."""
    impl = Geometry

    cache_ok = True

    def __init__(self, db_srid, app_srid, **kwargs):
        kwargs["srid"] = db_srid
        super().__init__(**kwargs)
        self.app_srid = app_srid
        self.db_srid = db_srid

    def column_expression(self, col):
        """The column_expression() method is overridden to set the correct type.

        This is needed so that the returned element will also be decorated. In this case we don't
        want to transform it again afterwards so we set the same SRID to both the ``db_srid`` and
        ``app_srid`` arguments.
        Without this the SRID of the WKBElement would be wrong.
        """
        return getattr(func, self.impl.as_binary)(
            func.ST_Transform(col, self.app_srid),
            type_=self.__class__(db_srid=self.app_srid, app_srid=self.app_srid)
        )

    def bind_expression(self, bindvalue):
        return func.ST_Transform(
            self.impl.bind_expression(bindvalue), self.db_srid,
            type_=self,
        )


class ThreeDGeometry(TypeDecorator):
    """This class is used to insert a ST_Force3D() in each insert."""
    impl = Geometry

    cache_ok = True

    def column_expression(self, col):
        """The column_expression() method is overridden to set the correct type.

        This is not needed in this example but it is needed if one wants to override other methods
        of the TypeDecorator class, like ``process_result_value()`` for example.
        """
        return getattr(func, self.impl.as_binary)(col, type_=self)

    def bind_expression(self, bindvalue):
        return func.ST_Force3D(
            self.impl.bind_expression(bindvalue),
            type=self,
        )


class Point(Base):
    __tablename__ = "point"
    id = Column(Integer, primary_key=True)
    raw_geom = Column(Geometry(srid=4326, geometry_type="POINT"))
    geom = Column(
        TransformedGeometry(
            db_srid=2154, app_srid=4326, geometry_type="POINT"))
    three_d_geom = Column(
        ThreeDGeometry(srid=4326, geometry_type="POINTZ", dimension=3))


def check_wkb(wkb, x, y):
    pt = shape.to_shape(wkb)
    assert round(pt.x, 5) == x
    assert round(pt.y, 5) == y


@test_only_with_dialects("postgresql")
class TestTypeDecorator():

    def _create_one_point(self, session, conn):
        metadata.drop_all(conn, checkfirst=True)
        metadata.create_all(conn)

        # Create new point instance
        p = Point()
        p.raw_geom = "SRID=4326;POINT(5 45)"
        p.geom = "SRID=4326;POINT(5 45)"
        p.three_d_geom = "SRID=4326;POINT(5 45)"  # Insert 2D geometry into 3D column

        # Insert point
        session.add(p)
        session.flush()
        session.expire(p)

        return p.id

    def test_transform(self, session, conn):
        self._create_one_point(session, conn)

        # Query the point and check the result
        pt = session.query(Point).one()
        assert pt.id == 1
        assert pt.raw_geom.srid == 4326
        check_wkb(pt.raw_geom, 5, 45)

        assert pt.geom.srid == 4326
        check_wkb(pt.geom, 5, 45)

        # Check that the data is correct in DB using raw query
        q = text("SELECT id, ST_AsEWKT(geom) AS geom FROM point;")
        res_q = session.execute(q).fetchone()
        assert res_q.id == 1
        assert res_q.geom == "SRID=2154;POINT(857581.899319668 6435414.7478354)"

        # Compare geom, raw_geom with auto transform and explicit transform
        pt_trans = session.query(
            Point,
            Point.raw_geom,
            func.ST_Transform(Point.raw_geom, 2154).label("trans"),
        ).one()

        assert pt_trans[0].id == 1

        assert pt_trans[0].geom.srid == 4326
        check_wkb(pt_trans[0].geom, 5, 45)

        assert pt_trans[0].raw_geom.srid == 4326
        check_wkb(pt_trans[0].raw_geom, 5, 45)

        assert pt_trans[1].srid == 4326
        check_wkb(pt_trans[1], 5, 45)

        assert pt_trans[2].srid == 2154
        check_wkb(pt_trans[2], 857581.89932, 6435414.74784)

    def test_force_3d(self, session, conn):
        self._create_one_point(session, conn)

        # Query the point and check the result
        pt = session.query(Point).one()

        assert pt.id == 1
        assert pt.three_d_geom.srid == 4326
        assert pt.three_d_geom.desc.lower() == (
            '01010000a0e6100000000000000000144000000000008046400000000000000000')
