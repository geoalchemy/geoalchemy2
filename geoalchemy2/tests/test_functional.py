import unittest
from nose.tools import eq_, ok_

from sqlalchemy import create_engine, MetaData, Column, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from geoalchemy2 import LineString


engine = create_engine('postgresql://gis:gis@localhost/gis', echo=True)
metadata = MetaData(engine)
Base = declarative_base(metadata=metadata)


class Lake(Base):
    __tablename__ = 'lake'
    id = Column(Integer, primary_key=True)
    geom = Column(LineString)

    def __init__(self, geom):
        self.geom = geom


session = sessionmaker(bind=engine)()


class FunctionalTest(unittest.TestCase):

    def setUp(self):
        metadata.drop_all(checkfirst=True)
        metadata.create_all()

    def tearDown(self):
        session.rollback()
        metadata.drop_all()
        pass

    def _create_one(self):
        l = Lake('LINESTRING(0 0,1 1)')
        session.add(l)
        session.flush()
        return l.id

    def test_geometry_type(self):
        from sqlalchemy.sql import select, func

        lake_id = self._create_one()

        s = select([func.ST_GeometryType(Lake.__table__.c.geom)])
        r1 = session.execute(s).scalar()
        eq_(r1, 'ST_LineString')

        lake = session.query(Lake).get(lake_id)
        r2 = session.execute(lake.geom.ST_GeometryType()).scalar()
        eq_(r2, 'ST_LineString')

        r3 = session.query(Lake.geom.ST_GeometryType()).scalar()
        eq_(r3, 'ST_LineString')

        r4 = session.query(Lake).filter(
            Lake.geom.ST_GeometryType() == 'ST_LineString').one()
        ok_(isinstance(r4, Lake))
        eq_(r4.id, lake_id)

    def test_buffer(self):
        from sqlalchemy.sql import select, func
        from geoalchemy2 import WKBElement

        lake_id = self._create_one()

        s = select([func.ST_Buffer(Lake.__table__.c.geom, 2)])
        r1 = session.execute(s).scalar()
        ok_(isinstance(r1, WKBElement))

        lake = session.query(Lake).get(lake_id)
        r2 = session.execute(lake.geom.ST_Buffer(2)).scalar()
        ok_(isinstance(r2, WKBElement))

        r3 = session.query(Lake.geom.ST_Buffer(2)).scalar()
        ok_(isinstance(r3, WKBElement))

        ok_(r1.data == r2.data == r3.data)

        r4 = session.query(Lake).filter(
                func.ST_Within('POINT(0 0)', Lake.geom.ST_Buffer(2))).one()
        ok_(isinstance(r4, Lake))
        eq_(r4.id, lake_id)
