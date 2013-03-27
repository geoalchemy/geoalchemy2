import unittest
from nose.tools import eq_, ok_, raises

from sqlalchemy import create_engine, MetaData, Column, Integer, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from geoalchemy2 import Geometry
from sqlalchemy.exc import DataError, IntegrityError, InternalError


engine = create_engine('postgresql://gis:gis@localhost/gis', echo=True)
metadata = MetaData(engine)
Base = declarative_base(metadata=metadata)


class Lake(Base):
    __tablename__ = 'lake'
    id = Column(Integer, primary_key=True)
    geom = Column(Geometry(geometry_type='LINESTRING', srid=4326))

    def __init__(self, geom):
        self.geom = geom


session = sessionmaker(bind=engine)()

postgis_version = session.execute(func.postgis_version()).scalar()
if not postgis_version.startswith('2.'):
    # With PostGIS 1.x the AddGeometryColumn and DropGeometryColumn
    # management functions should be used.
    Lake.__table__.c.geom.type.management = True


class InsertionTest(unittest.TestCase):

    def setUp(self):
        metadata.drop_all(checkfirst=True)
        metadata.create_all()

    def tearDown(self):
        session.rollback()
        metadata.drop_all()

    @raises(DataError, IntegrityError)
    def test_WKT(self):
        # With PostGIS 1.5:
        # IntegrityError: (IntegrityError) new row for relation "lake" violates
        # check constraint "enforce_srid_geom"
        #
        # With PostGIS 2.0:
        # DataError: (DataError) Geometry SRID (0) does not match column SRID
        # (4326)
        l = Lake('LINESTRING(0 0,1 1)')
        session.add(l)
        session.flush()

    def test_WKTElement(self):
        from geoalchemy2 import WKTElement, WKBElement
        l = Lake(WKTElement('LINESTRING(0 0,1 1)', srid=4326))
        session.add(l)
        session.flush()
        session.expire(l)
        ok_(isinstance(l.geom, WKBElement))
        wkt = session.execute(l.geom.ST_AsText()).scalar()
        eq_(wkt, 'LINESTRING(0 0,1 1)')
        srid = session.execute(l.geom.ST_SRID()).scalar()
        eq_(srid, 4326)


class CallFunctionTest(unittest.TestCase):

    def setUp(self):
        metadata.drop_all(checkfirst=True)
        metadata.create_all()

    def tearDown(self):
        session.rollback()
        metadata.drop_all()

    def _create_one(self):
        from geoalchemy2 import WKTElement
        l = Lake(WKTElement('LINESTRING(0 0,1 1)', srid=4326))
        session.add(l)
        session.flush()
        return l.id

    def test_ST_GeometryType(self):
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

    def test_ST_Buffer(self):
        from sqlalchemy.sql import select, func
        from geoalchemy2 import WKBElement, WKTElement

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
                func.ST_Within(WKTElement('POINT(0 0)', srid=4326),
                        Lake.geom.ST_Buffer(2))).one()
        ok_(isinstance(r4, Lake))
        eq_(r4.id, lake_id)

    def test_ST_Dump(self):
        from sqlalchemy.sql import select, func
        from geoalchemy2 import WKBElement

        lake_id = self._create_one()
        lake = session.query(Lake).get(lake_id)

        s = select([func.ST_Dump(Lake.__table__.c.geom)])
        r1 = session.execute(s).scalar()
        ok_(isinstance(r1, str))

        s = select([func.ST_Dump(Lake.__table__.c.geom).geom])
        r2 = session.execute(s).scalar()
        ok_(isinstance(r2, WKBElement))
        eq_(r2.data, lake.geom.data)

        r3 = session.execute(func.ST_Dump(lake.geom).geom).scalar()
        ok_(isinstance(r3, WKBElement))
        eq_(r3.data, lake.geom.data)

        r4 = session.query(func.ST_Dump(Lake.geom).geom).scalar()
        ok_(isinstance(r4, WKBElement))
        eq_(r4.data, lake.geom.data)

        r5 = session.query(Lake.geom.ST_Dump().geom).scalar()
        ok_(isinstance(r5, WKBElement))
        eq_(r5.data, lake.geom.data)

    def test_ST_DumpPoints(self):
        from sqlalchemy.sql import func
        from geoalchemy2 import WKBElement

        lake_id = self._create_one()
        lake = session.query(Lake).get(lake_id)

        q = session.query(lake.geom.ST_DumpPoints().geom).all()
        eq_(len(q), 2)

        p1 = q[0]
        ok_(isinstance(p1[0], WKBElement))
        p1 = session.execute(func.ST_AsText(p1[0])).scalar()
        eq_(p1, 'POINT(0 0)')

        p2 = q[1]
        ok_(isinstance(p2[0], WKBElement))
        p2 = session.execute(func.ST_AsText(p2[0])).scalar()
        eq_(p2, 'POINT(1 1)')

    @raises(InternalError)
    def test_ST_Buffer_Mixed_SRID(self):
        from sqlalchemy.sql import func
        self._create_one()
        session.query(Lake).filter(
                func.ST_Within('POINT(0 0)',
                      Lake.geom.ST_Buffer(2))).one()


class ReflectionTest(unittest.TestCase):

    def setUp(self):
        metadata.drop_all(checkfirst=True)
        metadata.create_all()

    def tearDown(self):
        metadata.drop_all()

    def test_reflection(self):
        from sqlalchemy import Table
        from geoalchemy2 import Geometry

        t = Table('lake', MetaData(), autoload=True, autoload_with=engine)
        type_ = t.c.geom.type
        ok_(isinstance(type_, Geometry))
        if not postgis_version.startswith('2.'):
            eq_(type_.geometry_type, 'GEOMETRY')
            eq_(type_.srid, -1)
        else:
            eq_(type_.geometry_type, 'LINESTRING')
            eq_(type_.srid, 4326)
