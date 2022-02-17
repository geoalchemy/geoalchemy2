from json import loads
import os
import pytest
import re

try:
    from psycopg2cffi import compat
except ImportError:
    pass
else:
    compat.register()
    del compat

from pkg_resources import parse_version
from sqlalchemy import create_engine
from sqlalchemy import Table, MetaData, Column, Integer, String, bindparam, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import DataError, IntegrityError, InternalError, ProgrammingError
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import type_coerce
from sqlalchemy.types import TypeDecorator
from sqlalchemy import __version__ as SA_VERSION

from geoalchemy2 import Geometry, Geography, Raster
from geoalchemy2.elements import WKTElement, WKBElement, RasterElement
from geoalchemy2.shape import from_shape
from geoalchemy2.exc import ArgumentError
from shapely.geometry import LineString, Point

from . import skip_postgis1, skip_postgis2, skip_case_insensitivity, skip_pg12_sa1217, select

SQLA_LT_2 = parse_version(SA_VERSION) <= parse_version("1.999")
if SQLA_LT_2:
    from sqlalchemy.engine.reflection import Inspector
    get_inspector = Inspector.from_engine
else:
    from sqlalchemy import inspect as get_inspector

# tmp_engine = create_engine(
#     os.environ.get('PYTEST_DB_URL', 'postgresql://gis:gis@localhost/gis'), echo=False)
# metadata = MetaData()
# arg_metadata = MetaData()
# Base = declarative_base(metadata=metadata)


# class Lake(Base):
#     __tablename__ = 'lake'
#     __table_args__ = {'schema': 'gis'}
#     id = Column(Integer, primary_key=True)
#     geom = Column(Geometry(geometry_type='LINESTRING', srid=4326))

#     def __init__(self, geom):
#         self.geom = geom


# class Poi(Base):
#     __tablename__ = 'poi'
#     __table_args__ = {'schema': 'gis'}
#     id = Column(Integer, primary_key=True)
#     geom = Column(Geometry(geometry_type='POINT', srid=4326))
#     geog = Column(Geography(geometry_type='POINT', srid=4326))

#     def __init__(self, geog):
#         self.geog = geog


# class Summit(Base):
#     __tablename__ = 'summit'
#     __table_args__ = {'schema': 'gis'}
#     id = Column(Integer, primary_key=True)
#     geom = Column(Geometry(
#         geometry_type='POINT', srid=4326, management=True))

#     def __init__(self, geom):
#         self.geom = geom


# class ThreeDGeometry(TypeDecorator):
#     """This class is used to insert a ST_Force3D() in each insert."""
#     impl = Geometry

#     def bind_expression(self, bindvalue):
#         return func.ST_Force3D(self.impl.bind_expression(bindvalue))


# class PointZ(Base):
#     __tablename__ = "point_z"
#     id = Column(Integer, primary_key=True)
#     three_d_geom = Column(ThreeDGeometry(srid=4326, geometry_type="POINTZ", dimension=3))


# class IndexTestWithSchema(Base):
#     __tablename__ = 'indextestwithschema'
#     __table_args__ = {'schema': 'gis'}
#     id = Column(Integer, primary_key=True)
#     geom1 = Column(Geometry(geometry_type='POINT', srid=4326))
#     geom2 = Column(Geometry(geometry_type='POINT', srid=4326, management=True))


# class IndexTestWithNDIndex(Base):
#     __tablename__ = 'index_test_with_nd_index'
#     __table_args__ = {'schema': 'gis'}
#     id = Column(Integer, primary_key=True)
#     geom1 = Column(Geometry(geometry_type='POINTZ', dimension=3, use_N_D_index=True))


# class IndexTestWithoutSchema(Base):
#     __tablename__ = 'indextestwithoutschema'
#     id = Column(Integer, primary_key=True)
#     geom1 = Column(Geometry(geometry_type='POINT', srid=4326))
#     geom2 = Column(Geometry(geometry_type='POINT', srid=4326, management=True))


# session = sessionmaker(bind=tmp_engine)()

# postgis_version = session.execute(func.postgis_lib_version()).scalar()
# postgres_major_version = re.match(r"([0-9]*)\.([0-9]*).*", session.execute(
#     text("""SELECT current_setting('server_version');""")).scalar()).group(1)

# if postgis_version.startswith('1.'):
#     # With PostGIS 1.x the AddGeometryColumn and DropGeometryColumn
#     # management functions should be used.
#     Lake.__table__.c.geom.type.management = True
# else:
#     # parameter use_typmod for AddGeometryColumn was added in PostGIS 2.0
#     Summit.__table__.c.geom.type.use_typmod = False

#     # The raster type is only available on PostGIS 2.0 and above
#     class Ocean(Base):
#         __tablename__ = 'ocean'
#         __table_args__ = {'schema': 'public'}
#         id = Column(Integer, primary_key=True)
#         rast = Column(Raster)

#         def __init__(self, rast):
#             self.rast = rast


@pytest.fixture
def setup_tables(conn, metadata):
    metadata.drop_all(conn, checkfirst=True)
    metadata.create_all(conn)
    yield
    conn.rollback()
    metadata.drop_all(conn, checkfirst=True)


class TestIndex():

    def test_index_with_schema(self, conn, IndexTestWithSchema, setup_tables):
        inspector = get_inspector(conn)
        indices = inspector.get_indexes(IndexTestWithSchema.__tablename__, schema='gis')
        assert len(indices) == 2
        assert not indices[0].get('unique')
        assert indices[0].get('column_names')[0] in (u'geom1', u'geom2')
        assert not indices[1].get('unique')
        assert indices[1].get('column_names')[0] in (u'geom1', u'geom2')

    def test_n_d_index(self, conn, IndexTestWithNDIndex, setup_tables):
        sql = text("""SELECT
                    tablename,
                    indexname,
                    indexdef
                FROM
                    pg_indexes
                WHERE
                    tablename = 'index_test_with_nd_index'
                ORDER BY
                    tablename,
                    indexname""")
        r = conn.execute(sql)
        results = r.fetchall()

        for index in results:
            if 'geom1' in index[1]:
                nd_index = index[2]

        index_type = nd_index.split("USING ", 1)[1]
        assert index_type == 'gist (geom1 gist_geometry_ops_nd)'

        inspector = get_inspector(conn)

        indices = inspector.get_indexes(IndexTestWithNDIndex.__tablename__)
        assert len(indices) == 1
        assert not indices[0].get('unique')
        assert indices[0].get('column_names')[0] in (u'geom1')

    def test_n_d_index_argument_error(self):
        BaseArgTest = declarative_base(metadata=MetaData())

        with pytest.raises(ArgumentError) as excinfo:
            class NDIndexArgErrorSchema(BaseArgTest):
                __tablename__ = 'nd_index_error_arg'
                __table_args__ = {'schema': 'gis'}
                id = Column(Integer, primary_key=True)
                geom1 = Column(Geometry(geometry_type='POINTZ',
                                        dimension=3,
                                        spatial_index=False,
                                        use_N_D_index=True,
                                        management=False))

        assert "Arg Error(use_N_D_index): spatial_index must be True" == excinfo.value.args[0]

    def test_index_without_schema(self, conn, IndexTestWithoutSchema, setup_tables):
        inspector = get_inspector(conn)
        indices = inspector.get_indexes(IndexTestWithoutSchema.__tablename__)
        assert len(indices) == 2
        assert not indices[0].get('unique')
        assert indices[0].get('column_names')[0] in (u'geom1', u'geom2')
        assert not indices[1].get('unique')
        assert indices[1].get('column_names')[0] in (u'geom1', u'geom2')

    def test_type_decorator_index(self, conn, PointZ, setup_tables):
        inspector = get_inspector(conn)
        indices = inspector.get_indexes(PointZ.__tablename__)
        assert len(indices) == 1
        assert not indices[0].get('unique')
        assert indices[0].get('column_names') == ['three_d_geom']

    def test_all_indexes(self, conn):
        BaseArgTest = declarative_base(metadata=MetaData())

        class TableWithIndexes(BaseArgTest):
            __tablename__ = 'table_with_indexes'
            __table_args__ = {'schema': 'gis'}
            id = Column(Integer, primary_key=True)
            # Test indexes on Geometry columns.
            geom_not_managed_no_index = Column(
                Geometry(
                    geometry_type='POINT',
                    spatial_index=False,
                    management=False,
                )
            )
            geom_not_managed_index = Column(
                Geometry(
                    geometry_type='POINT',
                    spatial_index=True,
                    management=False,
                )
            )
            geom_managed_no_index = Column(
                Geometry(
                    geometry_type='POINT',
                    spatial_index=False,
                    management=True,
                )
            )
            geom_managed_index = Column(
                Geometry(
                    geometry_type='POINT',
                    spatial_index=True,
                    management=True,
                )
            )
            # Test indexes on Geometry columns with ND index.
            geom_not_managed_nd_index = Column(
                Geometry(
                    geometry_type='POINTZ',
                    dimension=3,
                    spatial_index=True,
                    use_N_D_index=True,
                    management=False,
                )
            )
            geom_managed_nd_index = Column(
                Geometry(
                    geometry_type='POINTZ',
                    dimension=3,
                    spatial_index=True,
                    use_N_D_index=True,
                    management=True,
                )
            )
            # Test indexes on Geography columns.
            geog_not_managed_no_index = Column(
                Geography(
                    geometry_type='POINT',
                    spatial_index=False,
                    management=False,
                )
            )
            geog_not_managed_index = Column(
                Geography(
                    geometry_type='POINT',
                    spatial_index=True,
                    management=False,
                )
            )
            geog_managed_no_index = Column(
                Geography(
                    geometry_type='POINT',
                    spatial_index=False,
                    management=True,
                )
            )
            geog_managed_index = Column(
                Geography(
                    geometry_type='POINT',
                    spatial_index=True,
                    management=True,
                )
            )
            # Test indexes on Raster columns.
            # Note: managed Raster columns are not tested because Raster columns can't be managed.
            rast_not_managed_no_index = Column(
                Raster(
                    spatial_index=False,
                )
            )
            rast_not_managed_index = Column(
                Raster(
                    spatial_index=True,
                )
            )

        TableWithIndexes.__table__.create(conn)

        index_query = text(
            """SELECT indexname, indexdef
            FROM pg_indexes
            WHERE
                schemaname = 'gis'
                AND tablename = 'table_with_indexes';"""
        )
        indices = sorted(conn.execute(index_query).fetchall())

        expected_indices = [
            (
                "idx_table_with_indexes_geog_managed_index",
                """CREATE INDEX idx_table_with_indexes_geog_managed_index
                ON gis.table_with_indexes
                USING gist (geog_managed_index)"""
            ),
            (
                "idx_table_with_indexes_geog_not_managed_index",
                """CREATE INDEX idx_table_with_indexes_geog_not_managed_index
                ON gis.table_with_indexes
                USING gist (geog_not_managed_index)"""
            ),
            (
                "idx_table_with_indexes_geom_managed_index",
                """CREATE INDEX idx_table_with_indexes_geom_managed_index
                ON gis.table_with_indexes
                USING gist (geom_managed_index)"""
            ),
            (
                "idx_table_with_indexes_geom_managed_nd_index",
                """CREATE INDEX idx_table_with_indexes_geom_managed_nd_index
                ON gis.table_with_indexes
                USING gist (geom_managed_nd_index gist_geometry_ops_nd)"""
            ),
            (
                "idx_table_with_indexes_geom_not_managed_index",
                """CREATE INDEX idx_table_with_indexes_geom_not_managed_index
                ON gis.table_with_indexes
                USING gist (geom_not_managed_index)"""
            ),
            (
                "idx_table_with_indexes_geom_not_managed_nd_index",
                """CREATE INDEX idx_table_with_indexes_geom_not_managed_nd_index
                ON gis.table_with_indexes
                USING gist (geom_not_managed_nd_index gist_geometry_ops_nd)"""
            ),
            (
                "idx_table_with_indexes_rast_not_managed_index",
                """CREATE INDEX idx_table_with_indexes_rast_not_managed_index
                ON gis.table_with_indexes
                USING gist (st_convexhull(rast_not_managed_index))"""
            ),
            (
                "table_with_indexes_pkey",
                """CREATE UNIQUE INDEX table_with_indexes_pkey
                ON gis.table_with_indexes
                USING btree (id)"""
            ),
        ]

        assert len(indices) == 8

        for idx, expected_idx in zip(indices, expected_indices):
            assert idx[0] == expected_idx[0]
            assert idx[1] == re.sub("\n *", " ", expected_idx[1])


class TestTypMod():

    def test_SummitConstraints(self, conn, Summit, setup_tables):
        """Make sure the geometry column of table Summit is created with
        `use_typmod=False` (explicit constraints are created).
         """
        skip_pg12_sa1217(conn)
        inspector = get_inspector(conn)
        constraints = inspector.get_check_constraints(
            Summit.__tablename__, schema='gis')
        assert len(constraints) == 3

        constraint_names = {c['name'] for c in constraints}
        assert 'enforce_srid_geom' in constraint_names
        assert 'enforce_dims_geom' in constraint_names
        assert 'enforce_geotype_geom' in constraint_names


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
        assert wkt == 'LINESTRING(0 0,1 1)'
        srid = conn.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

        row = rows[1]
        assert isinstance(row[1], WKBElement)
        wkt = conn.execute(row[1].ST_AsText()).scalar()
        assert wkt == 'LINESTRING(0 0,2 2)'
        srid = conn.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

        row = rows[2]
        assert isinstance(row[1], WKBElement)
        wkt = conn.execute(row[1].ST_AsText()).scalar()
        assert wkt == 'LINESTRING(0 0,2 2)'
        srid = conn.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

        row = rows[3]
        assert isinstance(row[1], WKBElement)
        wkt = conn.execute(row[1].ST_AsText()).scalar()
        assert wkt == 'LINESTRING(0 0,3 3)'
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
            assert wkt == 'POINT(1 1)'
            srid = conn.execute(row[1].ST_SRID()).scalar()
            assert srid == 4326
            assert row[1] == from_shape(Point(1, 1), srid=4326, extended=True)

    def test_insert_geog_poi(self, conn, Poi, setup_tables):
        conn.execute(Poi.__table__.insert(), [
            {'geog': 'SRID=4326;POINT(1 1)'},
            {'geog': WKTElement('POINT(1 1)', srid=4326)},
            {'geog': WKTElement('SRID=4326;POINT(1 1)', extended=True)},
            {'geog': from_shape(Point(1, 1), srid=4326)}
        ])

        results = conn.execute(Poi.__table__.select())
        rows = results.fetchall()

        for row in rows:
            assert isinstance(row[2], WKBElement)
            wkt = conn.execute(row[2].ST_AsText()).scalar()
            assert wkt == 'POINT(1 1)'
            srid = conn.execute(row[2].ST_SRID()).scalar()
            assert srid == 4326
            assert row[2] == from_shape(Point(1, 1), srid=4326)


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
        assert wkt == 'LINESTRING(0 0,1 1)'
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
        assert wkt == 'LINESTRING(0 0,1 1)'
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
        assert wkt == 'LINESTRING(0 0,1 1)'
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
        wkt = session.execute(lake.geom.ST_AsText()).scalar()
        assert wkt == 'LINESTRING(0 0,1 1)'
        srid = session.execute(lake.geom.ST_SRID()).scalar()
        assert srid == 4326

    def test_WKBElement(self, session, Lake, setup_tables):
        shape = LineString([[0, 0], [1, 1]])
        lake = Lake(from_shape(shape, srid=4326))
        session.add(lake)
        session.flush()
        session.expire(lake)
        assert isinstance(lake.geom, WKBElement)
        wkt = session.execute(lake.geom.ST_AsText()).scalar()
        assert wkt == 'LINESTRING(0 0,1 1)'
        srid = session.execute(lake.geom.ST_SRID()).scalar()
        assert srid == 4326

    # @skip_postgis1(postgis_version)
    def test_Raster(self, session, Ocean, setup_tables):
        skip_postgis1(session)
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
        assert wkt == raw_wkt
        srid = session.execute(lake.geom.ST_SRID()).scalar()
        assert srid == 4326

        # Set geometry to None
        lake.geom = None

        # Update in DB
        session.flush()

        # Check what was updated in DB
        assert lake.geom is None
        cols = [Lake.id, Lake.geom]
        if SQLA_LT_2:
            assert session.execute(select(cols)).fetchall() == [(1, None)]
        else:
            assert session.execute(select(*cols)).fetchall() == [(1, None)]

        # Reset geometry to initial value
        lake.geom = WKTElement(raw_wkt, srid=4326)

        # Update in DB
        session.flush()

        # Check what was inserted in DB
        assert isinstance(lake.geom, WKTElement)
        wkt = session.execute(lake.geom.ST_AsText()).scalar()
        assert wkt == raw_wkt
        srid = session.execute(lake.geom.ST_SRID()).scalar()
        assert srid == 4326

    def test_WKBElement(self, session, Lake, setup_tables):
        shape = LineString([[0, 0], [1, 1]])
        lake = Lake(from_shape(shape, srid=4326))
        session.add(lake)

        # Insert in DB
        session.flush()

        # Check what was inserted in DB
        assert isinstance(lake.geom, WKBElement)
        wkt = session.execute(lake.geom.ST_AsText()).scalar()
        assert wkt == 'LINESTRING(0 0,1 1)'
        srid = session.execute(lake.geom.ST_SRID()).scalar()
        assert srid == 4326

        # Set geometry to None
        lake.geom = None

        # Update in DB
        session.flush()

        # Check what was updated in DB
        assert lake.geom is None
        cols = [Lake.id, Lake.geom]
        if SQLA_LT_2:
            assert session.execute(select(cols)).fetchall() == [(1, None)]
        else:
            assert session.execute(select(*cols)).fetchall() == [(1, None)]

        # Reset geometry to initial value
        lake.geom = from_shape(shape, srid=4326)

        # Insert in DB
        session.flush()

        # Check what was inserted in DB
        assert isinstance(lake.geom, WKBElement)
        wkt = session.execute(lake.geom.ST_AsText()).scalar()
        assert wkt == 'LINESTRING(0 0,1 1)'
        srid = session.execute(lake.geom.ST_SRID()).scalar()
        assert srid == 4326

    def test_other_type_fail(self, session, Lake, setup_tables):
        shape = LineString([[0, 0], [1, 1]])
        lake = Lake(from_shape(shape, srid=4326))
        session.add(lake)

        # Insert in DB
        session.flush()

        # Set geometry to 1, which is of wrong type
        lake.geom = 1

        # Update in DB
        with pytest.raises(ProgrammingError):
            # Call __eq__() operator of _SpatialElement with 'other' argument equal to 1
            session.flush()

    # @skip_postgis1(postgis_version)
    def test_Raster(self, session, Ocean, setup_tables):
        skip_postgis1(session)
        polygon = WKTElement('POLYGON((0 0,1 1,0 1,0 0))', srid=4326)
        o = Ocean(polygon.ST_AsRaster(5, 5))
        session.add(o)
        session.flush()
        session.expire(o)

        assert isinstance(o.rast, RasterElement)

        rast_data = (
            '01000001009A9999999999C93F9A9999999999C9BF0000000000000000000000000000F03'
            'F00000000000000000000000000000000E610000005000500440001010101010101010100'
            '010101000001010000000100000000'
        )

        assert o.rast.data == rast_data

        assert session.execute(
            select([Ocean.rast.ST_Height(), Ocean.rast.ST_Width()])
        ).fetchall() == [(5, 5)]

        # Set rast to None
        o.rast = None

        # Insert in DB
        session.flush()
        session.expire(o)

        # Check what was updated in DB
        assert o.rast is None
        cols = [Ocean.id, Ocean.rast]
        if SQLA_LT_2:
            assert session.execute(select(cols)).fetchall() == [(1, None)]
        else:
            assert session.execute(select(*cols)).fetchall() == [(1, None)]

        # Reset rast to initial value
        o.rast = RasterElement(rast_data)

        # Insert in DB
        session.flush()
        session.expire(o)

        # Check what was updated in DB
        assert o.rast.data == rast_data

        assert session.execute(
            select([Ocean.rast.ST_Height(), Ocean.rast.ST_Width()])
        ).fetchall() == [(5, 5)]


class TestPickle():

    @pytest.fixture
    def setup_one_lake(self, session, Lake, setup_tables):
        lake = Lake(WKTElement('LINESTRING(0 0,1 1)', srid=4326))
        session.add(lake)
        session.flush()
        session.expire(lake)
        return lake.id

    def test_pickle_unpickle(self, session, Lake, setup_one_lake):
        import pickle

        lake_id = setup_one_lake

        lake = session.query(Lake).get(lake_id)
        assert isinstance(lake.geom, WKBElement)
        data_desc = str(lake.geom)

        pickled = pickle.dumps(lake)
        unpickled = pickle.loads(pickled)
        assert unpickled.geom.srid == 4326
        assert str(unpickled.geom) == data_desc
        assert unpickled.geom.extended is True


class TestCallFunction():

    # def setup(self):
    #     metadata.drop_all(checkfirst=True)
    #     metadata.create_all()

    # def teardown(self):
    #     session.rollback()
    #     session.expunge_all()
    #     metadata.drop_all()

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

    def test_ST_Dump(self, session, Lake, setup_one_lake):
        lake_id = setup_one_lake
        lake = session.query(Lake).get(lake_id)
        assert isinstance(lake.geom, WKBElement)

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

    def test_ST_DumpPoints(self, session, Lake, setup_one_lake):
        lake_id = setup_one_lake
        lake = session.query(Lake).get(lake_id)
        assert isinstance(lake.geom, WKBElement)

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

    def test_ST_Buffer_Mixed_SRID(self, session, Lake, setup_one_lake):
        with pytest.raises(InternalError):
            session.query(Lake).filter(
                func.ST_Within('POINT(0 0)',
                               Lake.geom.ST_Buffer(2))).one()

    def test_ST_Distance_type_coerce(self, session, Poi, setup_one_poi):
        poi_id = setup_one_poi
        poi = session.query(Poi) \
            .filter(Poi.geog.ST_Distance(
                type_coerce('POINT(5 45)', Geography)) < 1000).one()
        assert poi.id == poi_id

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

        # Test with function inside
        s1_func = select([func.ST_AsGeoJSON(func.ST_MakeValid(Lake.__table__.c.geom))])
        r1_func = session.execute(s1_func).scalar()
        assert loads(r1_func) == {
            "type": "LineString",
            "coordinates": [[0, 0], [1, 1]]
        }

    # @skip_postgis1(postgis_version)
    # @skip_postgis2(postgis_version)
    def test_ST_AsGeoJson_feature(self, session, Lake, setup_one_lake):
        skip_postgis1(session)
        skip_postgis2(session)
        # Test feature
        s2 = select([func.ST_AsGeoJSON(Lake, 'geom')])
        r2 = session.execute(s2).scalar()
        assert loads(r2) == {
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [[0, 0], [1, 1]]
            },
            "properties": {"id": 1}
        }

        # Test feature with subquery
        ss3 = select([Lake, bindparam('dummy_val', 10).label('dummy_attr')]).alias()
        s3 = select([func.ST_AsGeoJSON(ss3, 'geom')])
        r3 = session.execute(s3).scalar()
        assert loads(r3) == {
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [[0, 0], [1, 1]]
            },
            "properties": {"dummy_attr": 10, "id": 1}
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
        with pytest.raises(ProgrammingError, match="ST_UnknownFunction"):
            session.execute(s)

    def test_unknown_function_element(self, session, Lake, setup_one_lake):
        lake_id = setup_one_lake
        lake = session.query(Lake).get(lake_id)

        s = select([func.ST_UnknownFunction(lake.geom, 2)])
        with pytest.raises(ProgrammingError):
            # TODO: here the query fails because of a
            # "(psycopg2.ProgrammingError) can't adapt type 'WKBElement'"
            # It would be better if it could fail because of a "UndefinedFunction" error
            session.execute(s)

    def test_unknown_function_element_ORM(self, session, Lake, setup_one_lake):
        lake_id = setup_one_lake
        lake = session.query(Lake).get(lake_id)

        with pytest.raises(AttributeError):
            select([lake.geom.ST_UnknownFunction(2)])


class TestReflection():

    # @skip_pg12_sa1217(postgres_major_version)
    def test_reflection(self, conn, Lake, setup_tables):
        skip_pg12_sa1217(conn)
        t = Table(
            'lake',
            MetaData(),
            schema='gis',
            autoload_with=conn)
        type_ = t.c.geom.type
        assert isinstance(type_, Geometry)
        if postgis_version.startswith('1.'):
            assert type_.geometry_type == 'GEOMETRY'
            assert type_.srid == -1
        else:
            assert type_.geometry_type == 'LINESTRING'
            assert type_.srid == 4326

    # @skip_postgis1(postgis_version)
    # @skip_pg12_sa1217(postgres_major_version)
    def test_raster_reflection(self, conn, Ocean, setup_tables):
        skip_pg12_sa1217(conn)
        skip_postgis1(conn)
        t = Table('ocean', MetaData(), autoload_with=conn)
        type_ = t.c.rast.type
        assert isinstance(type_, Raster)


class TestSTAsGeoJson():
    InternalBase = declarative_base()

    class TblWSpacesAndDots(InternalBase):
        """
        Dummy class to test names with dots and spaces.
        No metadata is attached so the dialect is default SQL, not postgresql.
        """
        __tablename__ = "this is.an AWFUL.name"
        __table_args__ = {'schema': 'another AWFUL.name for.schema'}

        id = Column(Integer, primary_key=True)
        geom = Column(String)

    @staticmethod
    def _assert_stmt(stmt, expected):
        strstmt = str(stmt)
        strstmt = strstmt.replace("\n", "")
        assert strstmt == expected

    def test_one(self, Lake):
        stmt = select([func.ST_AsGeoJSON(Lake.__table__.c.geom)])

        self._assert_stmt(
            stmt, 'SELECT ST_AsGeoJSON(gis.lake.geom) AS "ST_AsGeoJSON_1" FROM gis.lake'
        )

    def test_two(self, Lake):
        stmt = select([func.ST_AsGeoJSON(Lake, "geom")])
        self._assert_stmt(
            stmt,
            'SELECT ST_AsGeoJSON(lake, %(ST_AsGeoJSON_2)s) AS '
            '"ST_AsGeoJSON_1" FROM gis.lake',
        )

    # @skip_postgis1(postgis_version)
    # @skip_postgis2(postgis_version)
    def test_three(self, Lake):
        sq = select([Lake, bindparam("dummy_val", 10).label("dummy_attr")]).alias()
        stmt = select([func.ST_AsGeoJSON(sq, "geom")])
        self._assert_stmt(
            stmt,
            'SELECT ST_AsGeoJSON(anon_1, %(ST_AsGeoJSON_2)s) AS "ST_AsGeoJSON_1" '
            "FROM (SELECT gis.lake.id AS id, gis.lake.geom AS geom, %(dummy_val)s AS "
            "dummy_attr FROM gis.lake) AS anon_1",
        )

    # @skip_postgis1(postgis_version)
    # @skip_postgis2(postgis_version)
    def test_four(self):
        stmt = select([func.ST_AsGeoJSON(TestSTAsGeoJson.TblWSpacesAndDots, "geom")])
        self._assert_stmt(
            stmt,
            'SELECT ST_AsGeoJSON("this is.an AWFUL.name", :ST_AsGeoJSON_2) '
            'AS "ST_AsGeoJSON_1" FROM "another AWFUL.name for.schema".'
            '"this is.an AWFUL.name"',
        )

    # @skip_postgis1(postgis_version)
    # @skip_postgis2(postgis_version)
    def test_five(self):
        stmt = select([func.ST_AsGeoJSON(TestSTAsGeoJson.TblWSpacesAndDots, "geom", 3)])
        self._assert_stmt(
            stmt,
            'SELECT ST_AsGeoJSON("this is.an AWFUL.name", '
            ':ST_AsGeoJSON_2, :ST_AsGeoJSON_3) '
            'AS "ST_AsGeoJSON_1" FROM "another AWFUL.name for.schema".'
            '"this is.an AWFUL.name"',
        )

    # @skip_postgis1(postgis_version)
    def test_nested_funcs(self):
        stmt = select([func.ST_AsGeoJSON(func.ST_MakeValid(func.ST_MakePoint(1, 2)))])
        self._assert_stmt(
            stmt,
            'SELECT '
            'ST_AsGeoJSON(ST_MakeValid('
            'ST_MakePoint(:ST_MakePoint_1, :ST_MakePoint_2)'
            ')) AS "ST_AsGeoJSON_1"',
        )

    # @skip_postgis1(postgis_version)
    def test_unknown_func(self):
        stmt = select([
            func.ST_AsGeoJSON(func.ST_UnknownFunction(func.ST_MakePoint(1, 2)))
        ])
        self._assert_stmt(
            stmt,
            'SELECT '
            'ST_AsGeoJSON(ST_UnknownFunction('
            'ST_MakePoint(:ST_MakePoint_1, :ST_MakePoint_2)'
            ')) AS "ST_AsGeoJSON_1"',
        )


class TestSTSummaryStatsAgg():

    def test_st_summary_stats_agg(self, session, Ocean, setup_tables):

        # Create a new raster
        polygon = WKTElement('POLYGON((0 0,1 1,0 1,0 0))', srid=4326)
        o = Ocean(polygon.ST_AsRaster(5, 6))
        session.add(o)
        session.flush()

        # Define the query to compute stats
        stats_agg = select([
            func.ST_SummaryStatsAgg(Ocean.__table__.c.rast, 1, True, 1).label("stats")
        ])
        stats_agg_alias = stats_agg.alias("stats_agg")

        # Use these stats
        query = select([
            stats_agg_alias.c.stats.count.label("count"),
            stats_agg_alias.c.stats.sum.label("sum"),
            stats_agg_alias.c.stats.stddev.label("stddev"),
            stats_agg_alias.c.stats.min.label("min"),
            stats_agg_alias.c.stats.max.label("max")
        ])

        # Check the query
        assert str(query) == (
            "SELECT "
            "(stats_agg.stats).count AS count, "
            "(stats_agg.stats).sum AS sum, "
            "(stats_agg.stats).stddev AS stddev, "
            "(stats_agg.stats).min AS min, "
            "(stats_agg.stats).max AS max \n"
            "FROM ("
            "SELECT "
            "ST_SummaryStatsAgg("
            "public.ocean.rast, "
            "%(ST_SummaryStatsAgg_1)s, %(ST_SummaryStatsAgg_2)s, %(ST_SummaryStatsAgg_3)s"
            ") AS stats \n"
            "FROM public.ocean) AS stats_agg"
        )

        # Execute the query
        res = session.execute(query).fetchall()

        # Check the result
        assert res == [(15, 15.0, 0.0, 1.0, 1.0)]
