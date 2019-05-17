from pkg_resources import parse_version
import pytest

try:
    from psycopg2cffi import compat
except ImportError:
    pass
else:
    compat.register()
    del compat

from sqlalchemy import __version__ as SA_VERSION
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, bindparam
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine import reflection
from sqlalchemy.exc import DataError, IntegrityError, InternalError
from sqlalchemy.sql import select, func
from sqlalchemy.sql.expression import type_coerce

from geoalchemy2 import Geometry, Geography, Raster
from geoalchemy2.elements import WKTElement, WKBElement, RasterElement
from geoalchemy2.shape import from_shape

from shapely.geometry import LineString


engine = create_engine('postgresql://gis:gis@localhost/gis', echo=True)
metadata = MetaData(engine)
Base = declarative_base(metadata=metadata)


class Lake(Base):
    __tablename__ = 'lake'
    __table_args__ = {'schema': 'gis'}
    id = Column(Integer, primary_key=True)
    geom = Column(Geometry(geometry_type='LINESTRING', srid=4326))

    def __init__(self, geom):
        self.geom = geom


class Poi(Base):
    __tablename__ = 'poi'
    __table_args__ = {'schema': 'gis'}
    id = Column(Integer, primary_key=True)
    geog = Column(Geography(geometry_type='POINT', srid=4326))

    def __init__(self, geog):
        self.geog = geog


class Summit(Base):
    __tablename__ = 'summit'
    __table_args__ = {'schema': 'gis'}
    id = Column(Integer, primary_key=True)
    geom = Column(Geometry(
        geometry_type='POINT', srid=4326, management=True))

    def __init__(self, geom):
        self.geom = geom


class IndexTestWithSchema(Base):
    __tablename__ = 'indextestwithschema'
    __table_args__ = {'schema': 'gis'}
    id = Column(Integer, primary_key=True)
    geom1 = Column(Geometry(geometry_type='POINT', srid=4326))
    geom2 = Column(Geometry(geometry_type='POINT', srid=4326, management=True))


class IndexTestWithoutSchema(Base):
    __tablename__ = 'indextestwithoutschema'
    id = Column(Integer, primary_key=True)
    geom1 = Column(Geometry(geometry_type='POINT', srid=4326))
    geom2 = Column(Geometry(geometry_type='POINT', srid=4326, management=True))


session = sessionmaker(bind=engine)()

postgis_version = session.execute(func.postgis_version()).scalar()
if not postgis_version.startswith('2.'):
    # With PostGIS 1.x the AddGeometryColumn and DropGeometryColumn
    # management functions should be used.
    Lake.__table__.c.geom.type.management = True
else:
    # parameter use_typmod for AddGeometryColumn was added in PostGIS 2.0
    Summit.__table__.c.geom.type.use_typmod = False

    # The raster type is only available on PostGIS 2.0 and above
    class Ocean(Base):
        __tablename__ = 'ocean'
        __table_args__ = {'schema': 'public'}
        id = Column(Integer, primary_key=True)
        rast = Column(Raster)

        def __init__(self, rast):
            self.rast = rast

postgis2_required = pytest.mark.skipif(
    not postgis_version.startswith('2.'),
    reason="requires PostGIS 2.x",
)


class TestIndex():

    def setup(self):
        metadata.drop_all(checkfirst=True)
        metadata.create_all()

    def teardown(self):
        session.rollback()
        metadata.drop_all()

    def test_index_with_schema(self):
        inspector = reflection.Inspector.from_engine(engine)
        indices = inspector.get_indexes(IndexTestWithSchema.__tablename__, schema='gis')
        assert len(indices) == 2
        assert not indices[0].get('unique')
        assert indices[0].get('column_names')[0] in (u'geom1', u'geom2')
        assert not indices[1].get('unique')
        assert indices[1].get('column_names')[0] in (u'geom1', u'geom2')

    def test_index_without_schema(self):
        inspector = reflection.Inspector.from_engine(engine)
        indices = inspector.get_indexes(IndexTestWithSchema.__tablename__)
        assert len(indices) == 2
        assert not indices[0].get('unique')
        assert indices[0].get('column_names')[0] in (u'geom1', u'geom2')
        assert not indices[1].get('unique')
        assert indices[1].get('column_names')[0] in (u'geom1', u'geom2')


class TestTypMod():

    def setup(self):
        metadata.drop_all(checkfirst=True)
        metadata.create_all()

    def teardown(self):
        session.rollback()
        metadata.drop_all()

    def test_SummitConstraints(self):
        """ Make sure the geometry column of table Summit is created with
        `use_typmod=false` (explicit constraints are created).
         """

        inspector = reflection.Inspector.from_engine(engine)
        constraints = inspector.get_check_constraints(
            Summit.__tablename__, schema='gis')
        assert len(constraints) == 3

        constraint_names = {c['name'] for c in constraints}
        assert 'enforce_srid_geom' in constraint_names
        assert 'enforce_dims_geom' in constraint_names
        assert 'enforce_geotype_geom' in constraint_names


class TestInsertionCore():

    def setup(self):
        metadata.drop_all(checkfirst=True)
        metadata.create_all()
        self.conn = engine.connect()

    def teardown(self):
        self.conn.close()
        metadata.drop_all()

    def test_insert(self):
        conn = self.conn

        # Issue inserts using DBAPI's executemany() method. This tests the
        # Geometry type's bind_processor and bind_expression functions.
        conn.execute(Lake.__table__.insert(), [
            {'geom': 'SRID=4326;LINESTRING(0 0,1 1)'},
            {'geom': WKTElement('LINESTRING(0 0,2 2)', srid=4326)},
            {'geom': WKTElement('SRID=4326;LINESTRING(0 0,2 2)', extended=True)},
            {'geom': from_shape(LineString([[0, 0], [3, 3]]), srid=4326)}
        ])

        results = conn.execute(Lake.__table__.select())
        rows = results.fetchall()

        row = rows[0]
        assert isinstance(row[1], WKBElement)
        wkt = session.execute(row[1].ST_AsText()).scalar()
        assert wkt == 'LINESTRING(0 0,1 1)'
        srid = session.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

        row = rows[1]
        assert isinstance(row[1], WKBElement)
        wkt = session.execute(row[1].ST_AsText()).scalar()
        assert wkt == 'LINESTRING(0 0,2 2)'
        srid = session.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

        row = rows[2]
        assert isinstance(row[1], WKBElement)
        wkt = session.execute(row[1].ST_AsText()).scalar()
        assert wkt == 'LINESTRING(0 0,2 2)'
        srid = session.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

        row = rows[3]
        assert isinstance(row[1], WKBElement)
        wkt = session.execute(row[1].ST_AsText()).scalar()
        assert wkt == 'LINESTRING(0 0,3 3)'
        srid = session.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326


class TestSelectBindParam():

    def setup(self):
        metadata.drop_all(checkfirst=True)
        metadata.create_all()
        self.conn = engine.connect()
        self.conn.execute(Lake.__table__.insert(), {'geom': 'SRID=4326;LINESTRING(0 0,1 1)'})

    def teardown(self):
        self.conn.close()
        metadata.drop_all()

    def test_select_bindparam(self):
        s = Lake.__table__.select().where(Lake.__table__.c.geom == bindparam('geom'))
        results = self.conn.execute(s, geom='SRID=4326;LINESTRING(0 0,1 1)')
        rows = results.fetchall()

        row = rows[0]
        assert isinstance(row[1], WKBElement)
        wkt = session.execute(row[1].ST_AsText()).scalar()
        assert wkt == 'LINESTRING(0 0,1 1)'
        srid = session.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

    def test_select_bindparam_WKBElement(self):
        s = Lake.__table__.select().where(Lake.__table__.c.geom == bindparam('geom'))
        wkbelement = from_shape(LineString([[0, 0], [1, 1]]), srid=4326)
        results = self.conn.execute(s, geom=wkbelement)
        rows = results.fetchall()

        row = rows[0]
        assert isinstance(row[1], WKBElement)
        wkt = session.execute(row[1].ST_AsText()).scalar()
        assert wkt == 'LINESTRING(0 0,1 1)'
        srid = session.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

    def test_select_bindparam_WKBElement_extented(self):
        s = Lake.__table__.select()
        results = self.conn.execute(s)
        rows = results.fetchall()
        geom = rows[0][1]
        assert isinstance(geom, WKBElement)
        assert geom.extented

        s = Lake.__table__.select().where(Lake.__table__.c.geom == bindparam('geom'))
        results = self.conn.execute(s, geom=geom)
        rows = results.fetchall()

        row = rows[0]
        assert isinstance(row[1], WKBElement)
        wkt = session.execute(row[1].ST_AsText()).scalar()
        assert wkt == 'LINESTRING(0 0,1 1)'
        srid = session.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326


class TestInsertionORM():

    def setup(self):
        metadata.drop_all(checkfirst=True)
        metadata.create_all()

    def teardown(self):
        session.rollback()
        metadata.drop_all()

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

        with pytest.raises((DataError, IntegrityError)):
            session.flush()

    def test_WKTElement(self):
        l = Lake(WKTElement('LINESTRING(0 0,1 1)', srid=4326))
        session.add(l)
        session.flush()
        session.expire(l)
        assert isinstance(l.geom, WKBElement)
        wkt = session.execute(l.geom.ST_AsText()).scalar()
        assert wkt == 'LINESTRING(0 0,1 1)'
        srid = session.execute(l.geom.ST_SRID()).scalar()
        assert srid == 4326

    def test_WKBElement(self):
        shape = LineString([[0, 0], [1, 1]])
        l = Lake(from_shape(shape, srid=4326))
        session.add(l)
        session.flush()
        session.expire(l)
        assert isinstance(l.geom, WKBElement)
        wkt = session.execute(l.geom.ST_AsText()).scalar()
        assert wkt == 'LINESTRING(0 0,1 1)'
        srid = session.execute(l.geom.ST_SRID()).scalar()
        assert srid == 4326

    @postgis2_required
    def test_Raster(self):
        polygon = WKTElement('POLYGON((0 0,1 1,0 1,0 0))', srid=4326)
        o = Ocean(polygon.ST_AsRaster(5, 5))
        session.add(o)
        session.flush()
        session.expire(o)

        assert isinstance(o.rast, RasterElement)

        height = session.execute(o.rast.ST_Height()).scalar()
        assert height == 5

        width = session.execute(o.rast.ST_Width()).scalar()
        assert width == 5

        # The top left corner is covered by the polygon
        top_left_point = WKTElement('Point(0 1)', srid=4326)
        top_left = session.execute(
            o.rast.ST_Value(top_left_point)).scalar()
        assert top_left == 1

        # The bottom right corner has NODATA
        bottom_right_point = WKTElement('Point(1 0)', srid=4326)
        bottom_right = session.execute(
            o.rast.ST_Value(bottom_right_point)).scalar()
        assert bottom_right is None


class TestPickle():

    def setup(self):
        metadata.drop_all(checkfirst=True)
        metadata.create_all()

    def teardown(self):
        session.rollback()
        metadata.drop_all()

    def _create_one_lake(self):
        l = Lake(WKTElement('LINESTRING(0 0,1 1)', srid=4326))
        session.add(l)
        session.flush()
        return l.id

    def test_pickle_unpickle(self):
        import pickle

        lake_id = self._create_one_lake()

        lake = session.query(Lake).get(lake_id)
        assert isinstance(lake.geom, WKBElement)
        data_desc = str(lake.geom)

        pickled = pickle.dumps(lake)
        unpickled = pickle.loads(pickled)
        assert unpickled.geom.srid == 4326
        assert str(unpickled.geom) == data_desc
        assert unpickled.geom.extended
        assert unpickled.geom.name == 'ST_GeomFromEWKB'


class TestCallFunction():

    def setup(self):
        metadata.drop_all(checkfirst=True)
        metadata.create_all()

    def teardown(self):
        session.rollback()
        metadata.drop_all()

    def _create_one_lake(self):
        l = Lake(WKTElement('LINESTRING(0 0,1 1)', srid=4326))
        session.add(l)
        session.flush()
        return l.id

    def _create_one_poi(self):
        p = Poi('POINT(5 45)')
        session.add(p)
        session.flush()
        return p.id

    def test_ST_GeometryType(self):
        lake_id = self._create_one_lake()

        s = select([func.ST_GeometryType(Lake.__table__.c.geom)])
        r1 = session.execute(s).scalar()
        assert r1 == 'ST_LineString'

        lake = session.query(Lake).get(lake_id)
        r2 = session.execute(lake.geom.ST_GeometryType()).scalar()
        assert r2 == 'ST_LineString'

        r3 = session.query(Lake.geom.ST_GeometryType()).scalar()
        assert r3 == 'ST_LineString'

        r4 = session.query(Lake).filter(
            Lake.geom.ST_GeometryType() == 'ST_LineString').one()
        assert isinstance(r4, Lake)
        assert r4.id == lake_id

    def test_ST_Buffer(self):
        lake_id = self._create_one_lake()

        s = select([func.ST_Buffer(Lake.__table__.c.geom, 2)])
        r1 = session.execute(s).scalar()
        assert isinstance(r1, WKBElement)

        lake = session.query(Lake).get(lake_id)
        r2 = session.execute(lake.geom.ST_Buffer(2)).scalar()
        assert isinstance(r2, WKBElement)

        r3 = session.query(Lake.geom.ST_Buffer(2)).scalar()
        assert isinstance(r3, WKBElement)

        assert r1.data == r2.data == r3.data

        r4 = session.query(Lake).filter(
            func.ST_Within(WKTElement('POINT(0 0)', srid=4326),
                           Lake.geom.ST_Buffer(2))).one()
        assert isinstance(r4, Lake)
        assert r4.id == lake_id

    def test_ST_Dump(self):
        lake_id = self._create_one_lake()
        lake = session.query(Lake).get(lake_id)

        s = select([func.ST_Dump(Lake.__table__.c.geom)])
        r1 = session.execute(s).scalar()
        assert isinstance(r1, str)

        s = select([func.ST_Dump(Lake.__table__.c.geom).path])
        r2 = session.execute(s).scalar()
        assert isinstance(r2, list)
        assert r2 == []

        s = select([func.ST_Dump(Lake.__table__.c.geom).geom])
        r2 = session.execute(s).scalar()
        assert isinstance(r2, WKBElement)
        assert r2.data == lake.geom.data

        r3 = session.execute(func.ST_Dump(lake.geom).geom).scalar()
        assert isinstance(r3, WKBElement)
        assert r3.data == lake.geom.data

        r4 = session.query(func.ST_Dump(Lake.geom).geom).scalar()
        assert isinstance(r4, WKBElement)
        assert r4.data == lake.geom.data

        r5 = session.query(Lake.geom.ST_Dump().geom).scalar()
        assert isinstance(r5, WKBElement)
        assert r5.data == lake.geom.data

        assert r2.data == r3.data == r4.data == r5.data

    def test_ST_DumpPoints(self):
        lake_id = self._create_one_lake()
        lake = session.query(Lake).get(lake_id)

        dump = lake.geom.ST_DumpPoints()

        q = session.query(dump.path.label('path'),
                          dump.geom.label('geom')).all()
        assert len(q) == 2

        p1 = q[0]
        assert isinstance(p1.path, list)
        assert p1.path == [1]
        assert isinstance(p1.geom, WKBElement)
        p1 = session.execute(func.ST_AsText(p1.geom)).scalar()
        assert p1 == 'POINT(0 0)'

        p2 = q[1]
        assert isinstance(p2.path, list)
        assert p2.path == [2]
        assert isinstance(p2.geom, WKBElement)
        p2 = session.execute(func.ST_AsText(p2.geom)).scalar()
        assert p2 == 'POINT(1 1)'

    def test_ST_Buffer_Mixed_SRID(self):
        self._create_one_lake()

        with pytest.raises(InternalError):
            session.query(Lake).filter(
                func.ST_Within('POINT(0 0)',
                               Lake.geom.ST_Buffer(2))).one()

    def test_ST_Distance_type_coerce(self):
        poi_id = self._create_one_poi()
        poi = session.query(Poi) \
            .filter(Poi.geog.ST_Distance(
                type_coerce('POINT(5 45)', Geography)) < 1000).one()
        assert poi.id == poi_id

    @pytest.mark.skipif(
        parse_version(SA_VERSION) < parse_version("1.3.4"),
        reason='Case-insensitivity is only available for sqlalchemy>=1.3.4')
    def test_comparator_case_insensitivity(self):
        lake_id = self._create_one_lake()

        s = select([func.ST_Buffer(Lake.__table__.c.geom, 2)])
        r1 = session.execute(s).scalar()
        assert isinstance(r1, WKBElement)

        lake = session.query(Lake).get(lake_id)

        r2 = session.execute(lake.geom.ST_Buffer(2)).scalar()
        assert isinstance(r2, WKBElement)

        r3 = session.execute(lake.geom.st_buffer(2)).scalar()
        assert isinstance(r3, WKBElement)

        r4 = session.execute(lake.geom.St_BuFfEr(2)).scalar()
        assert isinstance(r4, WKBElement)

        r5 = session.query(Lake.geom.ST_Buffer(2)).scalar()
        assert isinstance(r5, WKBElement)

        r6 = session.query(Lake.geom.st_buffer(2)).scalar()
        assert isinstance(r6, WKBElement)

        r7 = session.query(Lake.geom.St_BuFfEr(2)).scalar()
        assert isinstance(r7, WKBElement)

        assert (
            r1.data == r2.data == r3.data == r4.data == r5.data == r6.data
            == r7.data)


class TestReflection():

    def setup(self):
        metadata.drop_all(checkfirst=True)
        metadata.create_all()

    def teardown(self):
        metadata.drop_all()

    def test_reflection(self):
        t = Table(
            'lake',
            MetaData(),
            schema='gis',
            autoload=True,
            autoload_with=engine)
        type_ = t.c.geom.type
        assert isinstance(type_, Geometry)
        if not postgis_version.startswith('2.'):
            assert type_.geometry_type == 'GEOMETRY'
            assert type_.srid == -1
        else:
            assert type_.geometry_type == 'LINESTRING'
            assert type_.srid == 4326

    @postgis2_required
    def test_raster_reflection(self):
        t = Table('ocean', MetaData(), autoload=True, autoload_with=engine)
        type_ = t.c.rast.type
        assert isinstance(type_, Raster)
