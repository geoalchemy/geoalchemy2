"""
Automatically use a function at insert or select
================================================

Sometimes the application wants to apply a function in an insert or in a select.
For example, the application might need the geometry with lat/lon coordinates while they
are projected in the DB. To avoid having to always tweak the query with a
``ST_Transform()``, it is possible to define a `TypeDecorator
<https://docs.sqlalchemy.org/en/13/core/custom_types.html#sqlalchemy.types.TypeDecorator>`_
"""
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeDecorator

from geoalchemy2 import Geometry
from geoalchemy2 import shape


engine = create_engine('postgresql://gis:gis@localhost/gis', echo=True)
metadata = MetaData(engine)

Base = declarative_base(metadata=metadata)


class TransformedGeometry(TypeDecorator):
    """This class is used to insert a ST_Transform() in each insert or select."""
    impl = Geometry

    def __init__(self, db_srid, app_srid, **kwargs):
        kwargs["srid"] = db_srid
        self.impl = self.__class__.impl(**kwargs)
        self.app_srid = app_srid
        self.db_srid = db_srid

    def column_expression(self, col):
        """The column_expression() method is overrided to ensure that the
        SRID of the resulting WKBElement is correct"""
        return getattr(func, self.impl.as_binary)(
            func.ST_Transform(col, self.app_srid),
            type_=self.__class__.impl(srid=self.app_srid)
            # srid could also be -1 so that the SRID is deduced from the
            # WKB data
        )

    def bind_expression(self, bindvalue):
        return func.ST_Transform(
            self.impl.bind_expression(bindvalue), self.db_srid)


class ThreeDGeometry(TypeDecorator):
    """This class is used to insert a ST_Force3D() in each insert."""
    impl = Geometry

    def bind_expression(self, bindvalue):
        return func.ST_Force3D(self.impl.bind_expression(bindvalue))


class Point(Base):
    __tablename__ = "point"
    id = Column(Integer, primary_key=True)
    raw_geom = Column(Geometry(srid=4326, geometry_type="POINT"))
    geom = Column(
        TransformedGeometry(
            db_srid=2154, app_srid=4326, geometry_type="POINT"))
    three_d_geom = Column(
        ThreeDGeometry(srid=4326, geometry_type="POINTZ", dimension=3))


session = sessionmaker(bind=engine)()


def check_wkb(wkb, x, y):
    pt = shape.to_shape(wkb)
    assert round(pt.x, 5) == x
    assert round(pt.y, 5) == y


class TestTypeDecorator():

    def setup(self):
        metadata.drop_all(checkfirst=True)
        metadata.create_all()

    def teardown(self):
        session.rollback()
        metadata.drop_all()

    def _create_one_point(self):
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

    def test_transform(self):
        self._create_one_point()

        # Query the point and check the result
        pt = session.query(Point).one()
        assert pt.id == 1
        assert pt.raw_geom.srid == 4326
        check_wkb(pt.raw_geom, 5, 45)

        assert pt.geom.srid == 4326
        check_wkb(pt.geom, 5, 45)

        # Check that the data is correct in DB using raw query
        q = "SELECT id, ST_AsEWKT(geom) AS geom FROM point;"
        res_q = session.execute(q).fetchone()
        assert res_q.id == 1
        assert res_q.geom == "SRID=2154;POINT(857581.899319668 6435414.7478354)"

        # Compare geom, raw_geom with auto transform and explicit transform
        pt_trans = session.query(
            Point,
            Point.raw_geom,
            func.ST_Transform(Point.raw_geom, 2154).label("trans")
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

    def test_force_3d(self):
        self._create_one_point()

        # Query the point and check the result
        pt = session.query(Point).one()

        assert pt.id == 1
        assert pt.three_d_geom.srid == 4326
        assert pt.three_d_geom.desc.lower() == (
            '01010000a0e6100000000000000000144000000000008046400000000000000000')
