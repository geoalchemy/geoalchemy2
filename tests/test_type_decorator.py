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
    impl = Geometry

    def __init__(self, db_srid, app_srid, **kwargs):
        kwargs["srid"] = db_srid
        self.impl = self.__class__.impl(**kwargs)
        self.app_srid = app_srid
        self.db_srid = db_srid

    def column_expression(self, col):
        return func.ST_Transform(col, self.app_srid)

    def bind_expression(self, bindvalue):
        return func.ST_Transform(
            self.impl.bind_expression(bindvalue), self.impl.srid)


class ThreeDGeometry(TypeDecorator):
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


class TestTypeDecorator():

    def setup(self):
        metadata.drop_all(checkfirst=True)
        metadata.create_all()

    def teardown(self):
        session.rollback()
        metadata.drop_all()

    def test_decorator(self):

        # Create new point instance
        p = Point()
        p.raw_geom = "SRID=4326;POINT(5 45)"
        p.geom = "SRID=4326;POINT(5 45)"
        p.three_d_geom = "SRID=4326;POINT(5 45)"  # Insert 2D geometry into 3D column

        # Create session
        Session = sessionmaker(bind=engine)
        session = Session()

        # Insert point
        session.add(p)
        session.commit()

        # Reset session
        session = Session()

        # Query the point and check the result
        pt = session.query(Point).one()
        assert pt.id == 1
        assert pt.geom.srid == 4326
        pt_shape = shape.to_shape(pt.geom)
        assert round(pt_shape.x, 5) == 5
        assert round(pt_shape.y, 5) == 45
        pt_shape_three_d = shape.to_shape(pt.three_d_geom)
        assert pt.three_d_geom.srid == 4326
        assert pt_shape_three_d.wkt == "POINT Z (5 45 0)"

        # Check that the data is correct in DB using raw query
        q = "SELECT id, ST_AsEWKT(geom) AS geom FROM point;"
        res_q = session.execute(q).fetchone()
        assert res_q.id == 1
        assert res_q.geom == "SRID=2154;POINT(857581.899319668 6435414.7478354)"

        # Compare geom, raw_geom with auto transform and manual transform
        pt_trans = session.query(
            Point,
            Point.raw_geom,
            func.ST_Transform(Point.raw_geom, 2154).label("trans")
        ).one()

        assert pt_trans[0].id == 1
        assert pt_trans[0].geom.srid == 4326
        assert pt_trans[0].geom.desc == "0101000020E61000000100000000001440F3FFFFFFFF7F4640"
        assert pt_trans[0].raw_geom.srid == 4326
        assert pt_trans[0].raw_geom.desc == "0101000020e610000000000000000014400000000000804640"
        assert pt_trans[1].srid == 4326
        assert pt_trans[1].desc == "0101000020e610000000000000000014400000000000804640"
        assert pt_trans[2].srid == 2154
        assert pt_trans[2].desc == "01010000206a080000a6a073ccdb2b2a410289dcaf958c5841"
