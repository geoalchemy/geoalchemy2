import re
from json import loads

import pytest

try:
    from psycopg2cffi import compat
except ImportError:
    pass
else:
    compat.register()
    del compat

from pkg_resources import parse_version
from shapely.geometry import LineString
from shapely.geometry import Point
from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import __version__ as SA_VERSION
from sqlalchemy import bindparam
from sqlalchemy import text
from sqlalchemy.exc import DataError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql import func

from geoalchemy2 import Geometry
from geoalchemy2 import Raster
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import from_shape
from geoalchemy2.shape import to_shape

from . import format_wkt
from . import get_postgis_version
from . import select
from . import skip_case_insensitivity
from . import skip_pg12_sa1217
from . import skip_postgis1

SQLA_LT_2 = parse_version(SA_VERSION) <= parse_version("1.999")


class TestInsertionCore():

    def test_insert(self, conn, Lake, setup_tables):
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
        wkt = conn.execute(row[1].ST_AsText()).scalar()
        assert format_wkt(wkt) == 'LINESTRING(0 0,1 1)'
        srid = conn.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

        row = rows[1]
        assert isinstance(row[1], WKBElement)
        wkt = conn.execute(row[1].ST_AsText()).scalar()
        assert format_wkt(wkt) == 'LINESTRING(0 0,2 2)'
        srid = conn.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

        row = rows[2]
        assert isinstance(row[1], WKBElement)
        wkt = conn.execute(row[1].ST_AsText()).scalar()
        assert format_wkt(wkt) == 'LINESTRING(0 0,2 2)'
        srid = conn.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

        row = rows[3]
        assert isinstance(row[1], WKBElement)
        wkt = conn.execute(row[1].ST_AsText()).scalar()
        assert format_wkt(wkt) == 'LINESTRING(0 0,3 3)'
        srid = conn.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

    def test_insert_geom_poi(self, conn, Poi, setup_tables):
        conn.execute(Poi.__table__.insert(), [
            {'geom': 'SRID=4326;POINT(1 1)'},
            {'geom': WKTElement('POINT(1 1)', srid=4326)},
            {'geom': WKTElement('SRID=4326;POINT(1 1)', extended=True)},
            {'geom': from_shape(Point(1, 1), srid=4326)},
            {'geom': from_shape(Point(1, 1), srid=4326, extended=True)}
        ])

        results = conn.execute(Poi.__table__.select())
        rows = results.fetchall()

        for row in rows:
            assert isinstance(row[1], WKBElement)
            wkt = conn.execute(row[1].ST_AsText()).scalar()
            assert format_wkt(wkt) == 'POINT(1 1)'
            srid = conn.execute(row[1].ST_SRID()).scalar()
            assert srid == 4326
            assert row[1] == from_shape(Point(1, 1), srid=4326, extended=True)


class TestSelectBindParam():

    @pytest.fixture
    def setup_one_lake(self, conn, Lake, setup_tables):
        conn.execute(Lake.__table__.insert(), {'geom': 'SRID=4326;LINESTRING(0 0,1 1)'})

    def test_select_bindparam(self, conn, Lake, setup_one_lake):
        s = Lake.__table__.select().where(Lake.__table__.c.geom == bindparam('geom'))
        params = {"geom": "SRID=4326;LINESTRING(0 0,1 1)"}
        if SQLA_LT_2:
            results = conn.execute(s, **params)
        else:
            results = conn.execute(s, params)
        rows = results.fetchall()

        row = rows[0]
        assert isinstance(row[1], WKBElement)
        wkt = conn.execute(row[1].ST_AsText()).scalar()
        assert format_wkt(wkt) == 'LINESTRING(0 0,1 1)'
        srid = conn.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

    def test_select_bindparam_WKBElement(self, conn, Lake, setup_one_lake):
        s = Lake.__table__.select().where(Lake.__table__.c.geom == bindparam('geom'))
        wkbelement = from_shape(LineString([[0, 0], [1, 1]]), srid=4326)
        params = {"geom": wkbelement}
        if SQLA_LT_2:
            results = conn.execute(s, **params)
        else:
            results = conn.execute(s, params)
        rows = results.fetchall()

        row = rows[0]
        assert isinstance(row[1], WKBElement)
        wkt = conn.execute(row[1].ST_AsText()).scalar()
        assert format_wkt(wkt) == 'LINESTRING(0 0,1 1)'
        srid = conn.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

    def test_select_bindparam_WKBElement_extented(self, conn, Lake, setup_one_lake):
        s = Lake.__table__.select()
        results = conn.execute(s)
        rows = results.fetchall()
        geom = rows[0][1]
        assert isinstance(geom, WKBElement)
        assert geom.extended is True

        s = Lake.__table__.select().where(Lake.__table__.c.geom == bindparam('geom'))
        params = {"geom": geom}
        if SQLA_LT_2:
            results = conn.execute(s, **params)
        else:
            results = conn.execute(s, params)
        rows = results.fetchall()

        row = rows[0]
        assert isinstance(row[1], WKBElement)
        wkt = conn.execute(row[1].ST_AsText()).scalar()
        assert format_wkt(wkt) == 'LINESTRING(0 0,1 1)'
        srid = conn.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326


class TestInsertionORM():

    def test_WKT(self, session, Lake, setup_tables):
        # With PostGIS 1.5:
        # IntegrityError: (IntegrityError) new row for relation "lake" violates
        # check constraint "enforce_srid_geom"
        #
        # With PostGIS 2.0:
        # DataError: (DataError) Geometry SRID (0) does not match column SRID
        # (4326)
        lake = Lake('LINESTRING(0 0,1 1)')
        session.add(lake)

        with pytest.raises((DataError, IntegrityError)):
            session.flush()

    def test_WKTElement(self, session, Lake, setup_tables):
        lake = Lake(WKTElement('LINESTRING(0 0,1 1)', srid=4326))
        session.add(lake)
        session.flush()
        session.expire(lake)
        assert isinstance(lake.geom, WKBElement)
        assert str(lake.geom) == (
            '0102000020e6100000020000000000000000000000000000000000000000000'
            '0000000f03f000000000000f03f'
        )
        wkt = session.execute(lake.geom.ST_AsText()).scalar()
        assert format_wkt(wkt) == 'LINESTRING(0 0,1 1)'
        srid = session.execute(lake.geom.ST_SRID()).scalar()
        assert srid == 4326

    def test_WKBElement(self, session, Lake, setup_tables):
        shape = LineString([[0, 0], [1, 1]])
        lake = Lake(from_shape(shape, srid=4326))
        session.add(lake)
        session.flush()
        session.expire(lake)
        assert isinstance(lake.geom, WKBElement)
        assert str(lake.geom) == (
            '0102000020e6100000020000000000000000000000000000000000000000000'
            '0000000f03f000000000000f03f'
        )
        wkt = session.execute(lake.geom.ST_AsText()).scalar()
        assert format_wkt(wkt) == 'LINESTRING(0 0,1 1)'
        srid = session.execute(lake.geom.ST_SRID()).scalar()
        assert srid == 4326

    def test_transform(self, session, LocalPoint, setup_tables):
        # Create new point instance
        p = LocalPoint()
        p.geom = "SRID=4326;POINT(5 45)"  # Insert 2D geometry into 3D column
        p.managed_geom = "SRID=4326;POINT(5 45)"  # Insert 2D geometry into 3D column

        # Insert point
        session.add(p)
        session.flush()
        session.expire(p)

        # Query the point and check the result
        pt = session.query(LocalPoint).one()
        assert pt.id == 1
        assert pt.geom.srid == 4326
        assert pt.managed_geom.srid == 4326
        pt_wkb = to_shape(pt.geom)
        assert round(pt_wkb.x, 5) == 5
        assert round(pt_wkb.y, 5) == 45
        pt_wkb = to_shape(pt.managed_geom)
        assert round(pt_wkb.x, 5) == 5
        assert round(pt_wkb.y, 5) == 45

        # Check that the data is correct in DB using raw query
        q = text(
            """
            SELECT id, ST_AsText(geom) AS geom, ST_AsText(managed_geom) AS managed_geom
            FROM local_point;
            """
        )
        res_q = session.execute(q).fetchone()
        assert res_q.id == 1
        for i in [res_q.geom, res_q.managed_geom]:
            x, y = re.match(r"POINT\((\d+\.\d*) (\d+\.\d*)\)", i).groups()
            assert round(float(x), 3) == 857581.899
            assert round(float(y), 3) == 6435414.748


class TestUpdateORM():

    def test_WKTElement(self, session, Lake, setup_tables):
        raw_wkt = 'LINESTRING(0 0,1 1)'
        lake = Lake(WKTElement(raw_wkt, srid=4326))
        session.add(lake)

        # Insert in DB
        session.flush()

        # Check what was inserted in DB
        assert isinstance(lake.geom, WKTElement)
        wkt = session.execute(lake.geom.ST_AsText()).scalar()
        assert format_wkt(wkt) == raw_wkt
        srid = session.execute(lake.geom.ST_SRID()).scalar()
        assert srid == 4326

        # Set geometry to None
        lake.geom = None

        # Update in DB
        session.flush()

        # Check what was updated in DB
        assert lake.geom is None
        cols = [Lake.id, Lake.geom]
        assert session.execute(select(cols)).fetchall() == [(1, None)]

        # Reset geometry to initial value
        lake.geom = WKTElement(raw_wkt, srid=4326)

        # Update in DB
        session.flush()

        # Check what was inserted in DB
        assert isinstance(lake.geom, WKTElement)
        wkt = session.execute(lake.geom.ST_AsText()).scalar()
        assert format_wkt(wkt) == raw_wkt
        srid = session.execute(lake.geom.ST_SRID()).scalar()
        assert srid == 4326

    def test_WKBElement(self, session, Lake, setup_tables):
        shape = LineString([[0, 0], [1, 1]])
        initial_value = from_shape(shape, srid=4326)
        lake = Lake(initial_value)
        session.add(lake)

        # Insert in DB
        session.flush()

        # Check what was inserted in DB
        assert isinstance(lake.geom, WKBElement)
        wkt = session.execute(lake.geom.ST_AsText()).scalar()
        assert format_wkt(wkt) == 'LINESTRING(0 0,1 1)'
        srid = session.execute(lake.geom.ST_SRID()).scalar()
        assert srid == 4326

        # Set geometry to None
        lake.geom = None

        # Update in DB
        session.flush()

        # Check what was updated in DB
        assert lake.geom is None
        cols = [Lake.id, Lake.geom]
        assert session.execute(select(cols)).fetchall() == [(1, None)]

        # Reset geometry to initial value
        lake.geom = initial_value

        # Insert in DB
        session.flush()

        # Check what was inserted in DB
        assert isinstance(lake.geom, WKBElement)
        wkt = session.execute(lake.geom.ST_AsText()).scalar()
        assert format_wkt(wkt) == 'LINESTRING(0 0,1 1)'
        srid = session.execute(lake.geom.ST_SRID()).scalar()
        assert srid == 4326

        session.refresh(lake)
        assert to_shape(lake.geom) == to_shape(initial_value)

    def test_other_type_fail(self, session, Lake, setup_tables):
        shape = LineString([[0, 0], [1, 1]])
        lake = Lake(from_shape(shape, srid=4326))
        session.add(lake)

        # Insert in DB
        session.flush()

        # Set geometry to 1, which is of wrong type
        lake.geom = 1

        # Update in DB
        if session.bind.dialect.name != "sqlite":
            with pytest.raises(ProgrammingError):
                # Call __eq__() operator of _SpatialElement with 'other' argument equal to 1
                # so the lake instance is detected as different and is thus updated but with
                # an invalid geometry.
                session.flush()
        else:
            # SQLite silently set the geom attribute to NULL
            session.flush()
            session.refresh(lake)
            assert lake.geom is None


class TestCallFunction():
    @pytest.fixture
    def setup_one_lake(self, session, Lake, setup_tables):
        lake = Lake(WKTElement('LINESTRING(0 0,1 1)', srid=4326))
        session.add(lake)
        session.flush()
        session.expire(lake)
        return lake.id

    @pytest.fixture
    def setup_one_poi(self, session, Poi, setup_tables):
        p = Poi('POINT(5 45)')
        session.add(p)
        session.flush()
        session.expire(p)
        return p.id

    def test_ST_GeometryType(self, session, Lake, setup_one_lake):
        lake_id = setup_one_lake

        if session.bind.dialect.name == "postgresql":
            expected_geometry_type = 'ST_LineString'
        else:
            expected_geometry_type = 'LINESTRING'

        s = select([func.ST_GeometryType(Lake.__table__.c.geom)])
        r1 = session.execute(s).scalar()
        assert r1 == expected_geometry_type

        lake = session.query(Lake).get(lake_id)
        r2 = session.execute(lake.geom.ST_GeometryType()).scalar()
        assert r2 == expected_geometry_type

        r3 = session.query(Lake.geom.ST_GeometryType()).scalar()
        assert r3 == expected_geometry_type

        r4 = session.query(Lake).filter(
            Lake.geom.ST_GeometryType() == expected_geometry_type).one()
        assert isinstance(r4, Lake)
        assert r4.id == lake_id

    def test_ST_Buffer(self, session, Lake, setup_one_lake):
        lake_id = setup_one_lake

        s = select([func.ST_Buffer(Lake.__table__.c.geom, 2)])
        r1 = session.execute(s).scalar()
        assert isinstance(r1, WKBElement)

        lake = session.query(Lake).get(lake_id)
        assert isinstance(lake.geom, WKBElement)
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

    def test_ST_AsGeoJson(self, session, Lake, setup_one_lake):
        lake_id = setup_one_lake
        lake = session.query(Lake).get(lake_id)

        # Test geometry
        s1 = select([func.ST_AsGeoJSON(Lake.__table__.c.geom)])
        r1 = session.execute(s1).scalar()
        assert loads(r1) == {
            "type": "LineString",
            "coordinates": [[0, 0], [1, 1]]
        }

        # Test geometry ORM
        s1_orm = lake.geom.ST_AsGeoJSON()
        r1_orm = session.execute(s1_orm).scalar()
        assert loads(r1_orm) == {
            "type": "LineString",
            "coordinates": [[0, 0], [1, 1]]
        }

        # Test from WKTElement
        s1_wkt = WKTElement("LINESTRING(0 0,1 1)", srid=4326, extended=False).ST_AsGeoJSON()
        r1_wkt = session.execute(s1_wkt).scalar()
        assert loads(r1_wkt) == {
            "type": "LineString",
            "coordinates": [[0, 0], [1, 1]]
        }

        # Test from extended WKTElement
        s1_ewkt = WKTElement("SRID=4326;LINESTRING(0 0,1 1)", extended=True).ST_AsGeoJSON()
        r1_ewkt = session.execute(s1_ewkt).scalar()
        assert loads(r1_ewkt) == {
            "type": "LineString",
            "coordinates": [[0, 0], [1, 1]]
        }

        # Test with function inside
        s1_func = select([
            func.ST_AsGeoJSON(func.ST_Translate(Lake.__table__.c.geom, 0.0, 0.0, 0.0))
        ])
        r1_func = session.execute(s1_func).scalar()
        assert loads(r1_func) == {
            "type": "LineString",
            "coordinates": [[0, 0], [1, 1]]
        }

    @skip_case_insensitivity()
    def test_comparator_case_insensitivity(self, session, Lake, setup_one_lake):
        lake_id = setup_one_lake

        s = select([func.ST_Buffer(Lake.__table__.c.geom, 2)])
        r1 = session.execute(s).scalar()
        assert isinstance(r1, WKBElement)

        lake = session.query(Lake).get(lake_id)
        assert isinstance(lake.geom, WKBElement)

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

    def test_unknown_function_column(self, session, Lake, setup_one_lake):
        s = select([func.ST_UnknownFunction(Lake.__table__.c.geom, 2)])
        exc = ProgrammingError if session.bind.dialect.name == "postgresql" else OperationalError
        with pytest.raises(exc, match="ST_UnknownFunction"):
            session.execute(s)

    def test_unknown_function_element(self, session, Lake, setup_one_lake):
        lake_id = setup_one_lake
        lake = session.query(Lake).get(lake_id)

        s = select([func.ST_UnknownFunction(lake.geom, 2)])
        exc = ProgrammingError if session.bind.dialect.name == "postgresql" else OperationalError
        with pytest.raises(exc):
            # TODO: here the query fails because of a
            # "(psycopg2.ProgrammingError) can't adapt type 'WKBElement'"
            # It would be better if it could fail because of a "UndefinedFunction" error
            session.execute(s)

    def test_unknown_function_element_ORM(self, session, Lake, setup_one_lake):
        lake_id = setup_one_lake
        lake = session.query(Lake).get(lake_id)

        with pytest.raises(AttributeError):
            select([lake.geom.ST_UnknownFunction(2)])


class TestShapely():
    def test_to_shape(self, session, Lake, setup_tables):
        if session.bind.dialect.name == "sqlite":
            data_type = str
        else:
            data_type = memoryview

        lake = Lake(WKTElement('LINESTRING(0 0,1 1)', srid=4326))
        session.add(lake)
        session.flush()
        session.expire(lake)
        lake = session.query(Lake).one()
        assert isinstance(lake.geom, WKBElement)
        assert isinstance(lake.geom.data, data_type)
        assert lake.geom.srid == 4326
        s = to_shape(lake.geom)
        assert isinstance(s, LineString)
        assert s.wkt == 'LINESTRING (0 0, 1 1)'
        lake = Lake(lake.geom)
        session.add(lake)
        session.flush()
        session.expire(lake)
        assert isinstance(lake.geom, WKBElement)
        assert isinstance(lake.geom.data, data_type)
        assert lake.geom.srid == 4326


class TestContraint():

    @pytest.fixture
    def ConstrainedLake(self, base):
        class ConstrainedLake(base):
            __tablename__ = 'contrained_lake'
            __table_args__ = (
                CheckConstraint(
                    '(geom is null and a_str is null) = (checked_str is null)', 'check_geom_sk'
                ),
            )
            id = Column(Integer, primary_key=True)
            a_str = Column(String, nullable=True)
            checked_str = Column(String, nullable=True)
            geom = Column(Geometry(geometry_type='LINESTRING', srid=4326, management=False))

            def __init__(self, geom):
                self.geom = geom

        return ConstrainedLake

    def test_insert(self, conn, ConstrainedLake, setup_tables):
        # Insert geometries
        conn.execute(ConstrainedLake.__table__.insert(), [
            {'a_str': None, 'geom': 'SRID=4326;LINESTRING(0 0,1 1)', 'checked_str': 'test'},
            {'a_str': 'test', 'geom': None, 'checked_str': 'test'},
            {'a_str': None, 'geom': None, 'checked_str': None},
        ])

        # Fail when trying to insert null geometry
        with pytest.raises(IntegrityError):
            conn.execute(ConstrainedLake.__table__.insert(), [
                {'a_str': None, 'geom': None, 'checked_str': 'should fail'},
            ])


class TestReflection():

    def test_reflection(self, conn, Lake, setup_tables, schema):
        skip_pg12_sa1217(conn)
        t = Table(
            'lake',
            MetaData(),
            schema=schema,
            autoload_with=conn)
        type_ = t.c.geom.type
        assert isinstance(type_, Geometry)
        if get_postgis_version(conn).startswith('1.') or conn.dialect.name == "sqlite":
            assert type_.geometry_type == 'GEOMETRY'
            assert type_.srid == -1
        else:
            assert type_.geometry_type == 'LINESTRING'
            assert type_.srid == 4326

    def test_raster_reflection(self, conn, Ocean, setup_tables):
        skip_pg12_sa1217(conn)
        skip_postgis1(conn)
        t = Table('ocean', MetaData(), autoload_with=conn)
        type_ = t.c.rast.type
        assert isinstance(type_, Raster)
