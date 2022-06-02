import platform

import pytest
from shapely.geometry import LineString
from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import func

from geoalchemy2 import Geometry
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import from_shape

from . import select
from . import test_only_with_dialects

if platform.python_implementation().lower() == 'pypy':
    pytest.skip('skip SpatiaLite tests on PyPy', allow_module_level=True)


class TestIndex():
    @pytest.fixture
    def TableWithIndexes(self, base):
        class TableWithIndexes(base):
            __tablename__ = 'table_with_indexes'
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

        return TableWithIndexes

    @staticmethod
    def check_spatial_idx(bind, idx_name):
        tables = bind.execute(
            text("SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%';")
        ).fetchall()
        if idx_name in [i[0] for i in tables]:
            return True
        return False

    def test_index(self, conn, Lake, setup_tables):
        assert self.check_spatial_idx(conn, 'idx_lake_geom')

    def test_type_decorator_index(self, conn, LocalPoint, setup_tables):
        assert self.check_spatial_idx(conn, 'idx_local_point_geom')
        assert self.check_spatial_idx(conn, 'idx_local_point_managed_geom')

    def test_all_indexes(self, conn, TableWithIndexes, setup_tables):
        expected_indices = [
            'idx_table_with_indexes_geom_managed_index',
            'idx_table_with_indexes_geom_not_managed_index',
        ]
        for expected_idx in expected_indices:
            assert self.check_spatial_idx(conn, expected_idx)

        TableWithIndexes.__table__.drop(bind=conn)

        indexes_after_drop = conn.execute(text("""SELECT * FROM "geometry_columns";""")).fetchall()
        tables_after_drop = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%';")
        ).fetchall()

        assert indexes_after_drop == []
        assert [table for table in tables_after_drop if 'table_with_indexes' in table.name] == []


class TestInsertionORM():
    pass


class TestUpdateORM():
    pass


class TestCallFunction():

    def test_ST_Buffer(self, session):
        """Test the specific SQLite signature with the `quadrantsegments` parameter."""
        s = select([
            func.St_AsText(func.ST_Buffer(WKTElement('LINESTRING(0 0,1 0)', srid=4326), 2, 1))
        ])
        r1 = session.execute(s).scalar()
        assert r1 == 'POLYGON((1 2, 3 0, 1 -2, 0 -2, -2 0, 0 2, 1 2))'

        s = select([
            func.St_AsText(func.ST_Buffer(WKTElement('LINESTRING(0 0,1 0)', srid=4326), 2, 2))
        ])
        r1 = session.execute(s).scalar()
        assert r1 == (
            'POLYGON((1 2, 2.414214 1.414214, 3 0, 2.414214 -1.414214, 1 -2, 0 -2, '
            '-1.414214 -1.414214, -2 0, -1.414214 1.414214, 0 2, 1 2))'
        )


class TestShapely():
    pass


class TestNullable():
    @pytest.fixture
    def NotNullableLake(self, base):
        class NotNullableLake(base):
            __tablename__ = 'NotNullablelake'
            id = Column(Integer, primary_key=True)
            geom = Column(Geometry(geometry_type='LINESTRING', srid=4326,
                                   management=True, nullable=False))

            def __init__(self, geom):
                self.geom = geom

        return NotNullableLake

    def test_insert(self, conn, NotNullableLake, setup_tables):
        # Insert geometries
        conn.execute(NotNullableLake.__table__.insert(), [
            {'geom': 'SRID=4326;LINESTRING(0 0,1 1)'},
            {'geom': WKTElement('LINESTRING(0 0,2 2)', srid=4326)},
            {'geom': from_shape(LineString([[0, 0], [3, 3]]), srid=4326)}
        ])

        # Fail when trying to insert null geometry
        with pytest.raises(IntegrityError):
            conn.execute(NotNullableLake.__table__.insert(), [
                {'geom': None}
            ])


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

    @pytest.fixture
    def setup_reflection_tables(self, reflection_tables_metadata, conn):
        reflection_tables_metadata.drop_all(conn, checkfirst=True)
        reflection_tables_metadata.create_all(conn)

    @test_only_with_dialects("sqlite-spatialite3")
    def test_reflection_spatialite_lt_4(self, conn, setup_reflection_tables):
        t = Table(
            'lake',
            MetaData(),
            autoload_with=conn)

        type_ = t.c.geom.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == 'LINESTRING'
        assert type_.srid == 4326
        assert type_.dimension == 2

        type_ = t.c.geom_no_idx.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == 'LINESTRING'
        assert type_.srid == 4326
        assert type_.dimension == 2

        type_ = t.c.geom_z.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == 'LINESTRINGZ'
        assert type_.srid == 4326
        assert type_.dimension == 3

        type_ = t.c.geom_m.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == 'LINESTRINGM'
        assert type_.srid == 4326
        assert type_.dimension == 3

        type_ = t.c.geom_zm.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == 'LINESTRINGZM'
        assert type_.srid == 4326
        assert type_.dimension == 4

        # Drop the table
        t.drop(bind=conn)

        # Query to check the tables
        query_tables = text(
            """SELECT
                name
            FROM
                sqlite_master
            WHERE
                type ='table' AND
                name NOT LIKE 'sqlite_%'
            ORDER BY tbl_name;"""
        )

        # Query to check the indices
        query_indexes = text(
            """SELECT * FROM geometry_columns ORDER BY f_table_name, f_geometry_column;"""
        )

        # Check the indices
        geom_cols = conn.execute(query_indexes).fetchall()
        assert geom_cols == []

        # Check the tables
        all_tables = [i[0] for i in conn.execute(query_tables).fetchall()]
        assert all_tables == [
            'SpatialIndex',
            'geometry_columns',
            'geometry_columns_auth',
            'layer_statistics',
            'spatial_ref_sys',
            'spatialite_history',
            'views_geometry_columns',
            'views_layer_statistics',
            'virts_geometry_columns',
            'virts_layer_statistics',
        ]

        # Recreate the table to check that the reflected properties are correct
        t.create(bind=conn)

        # Check the actual properties
        geom_cols = conn.execute(query_indexes).fetchall()
        assert geom_cols == [
            ('lake', 'geom', 'LINESTRING', 'XY', 4326, 1),
            ('lake', 'geom_m', 'LINESTRING', 'XYM', 4326, 1),
            ('lake', 'geom_no_idx', 'LINESTRING', 'XY', 4326, 0),
            ('lake', 'geom_z', 'LINESTRING', 'XYZ', 4326, 1),
            ('lake', 'geom_zm', 'LINESTRING', 'XYZM', 4326, 1),
        ]

        all_tables = [i[0] for i in conn.execute(query_tables).fetchall()]
        assert all_tables == [
            'SpatialIndex',
            'geometry_columns',
            'geometry_columns_auth',
            'idx_lake_geom',
            'idx_lake_geom_m',
            'idx_lake_geom_m_node',
            'idx_lake_geom_m_parent',
            'idx_lake_geom_m_rowid',
            'idx_lake_geom_node',
            'idx_lake_geom_parent',
            'idx_lake_geom_rowid',
            'idx_lake_geom_z',
            'idx_lake_geom_z_node',
            'idx_lake_geom_z_parent',
            'idx_lake_geom_z_rowid',
            'idx_lake_geom_zm',
            'idx_lake_geom_zm_node',
            'idx_lake_geom_zm_parent',
            'idx_lake_geom_zm_rowid',
            'lake',
            'layer_statistics',
            'spatial_ref_sys',
            'spatialite_history',
            'views_geometry_columns',
            'views_layer_statistics',
            'virts_geometry_columns',
            'virts_layer_statistics',
        ]

    @test_only_with_dialects("sqlite-spatialite4")
    def test_reflection_spatialite_ge_4(self, conn, setup_reflection_tables):
        t = Table(
            'lake',
            MetaData(),
            autoload_with=conn)

        type_ = t.c.geom.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == 'LINESTRING'
        assert type_.srid == 4326
        assert type_.dimension == 2

        type_ = t.c.geom_no_idx.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == 'LINESTRING'
        assert type_.srid == 4326
        assert type_.dimension == 2

        type_ = t.c.geom_z.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == 'LINESTRINGZ'
        assert type_.srid == 4326
        assert type_.dimension == 3

        type_ = t.c.geom_m.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == 'LINESTRINGM'
        assert type_.srid == 4326
        assert type_.dimension == 3

        type_ = t.c.geom_zm.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == 'LINESTRINGZM'
        assert type_.srid == 4326
        assert type_.dimension == 4

        # Drop the table
        t.drop(bind=conn)

        # Query to check the tables
        query_tables = text(
            """SELECT
                name
            FROM
                sqlite_master
            WHERE
                type ='table' AND
                name NOT LIKE 'sqlite_%'
            ORDER BY tbl_name;"""
        )

        # Query to check the indices
        query_indexes = text(
            """SELECT * FROM geometry_columns ORDER BY f_table_name, f_geometry_column;"""
        )

        # Check the indices
        geom_cols = conn.execute(query_indexes).fetchall()
        assert geom_cols == []

        # Check the tables
        all_tables = [i[0] for i in conn.execute(query_tables).fetchall()]
        assert all_tables == [
            'ElementaryGeometries',
            'SpatialIndex',
            'geometry_columns',
            'geometry_columns_auth',
            'geometry_columns_field_infos',
            'geometry_columns_statistics',
            'geometry_columns_time',
            'spatial_ref_sys',
            'spatial_ref_sys_aux',
            'spatialite_history',
            'sql_statements_log',
            'views_geometry_columns',
            'views_geometry_columns_auth',
            'views_geometry_columns_field_infos',
            'views_geometry_columns_statistics',
            'virts_geometry_columns',
            'virts_geometry_columns_auth',
            'virts_geometry_columns_field_infos',
            'virts_geometry_columns_statistics',
        ]

        # Recreate the table to check that the reflected properties are correct
        t.create(bind=conn)

        # Check the actual properties
        geom_cols = conn.execute(query_indexes).fetchall()
        assert geom_cols == [
            ('lake', 'geom', 2, 2, 4326, 1),
            ('lake', 'geom_m', 2002, 3, 4326, 1),
            ('lake', 'geom_no_idx', 2, 2, 4326, 0),
            ('lake', 'geom_z', 1002, 3, 4326, 1),
            ('lake', 'geom_zm', 3002, 4, 4326, 1),
        ]

        all_tables = [i[0] for i in conn.execute(query_tables).fetchall()]
        assert all_tables == [
            'ElementaryGeometries',
            'SpatialIndex',
            'geometry_columns',
            'geometry_columns_auth',
            'geometry_columns_field_infos',
            'geometry_columns_statistics',
            'geometry_columns_time',
            'idx_lake_geom',
            'idx_lake_geom_m',
            'idx_lake_geom_m_node',
            'idx_lake_geom_m_parent',
            'idx_lake_geom_m_rowid',
            'idx_lake_geom_node',
            'idx_lake_geom_parent',
            'idx_lake_geom_rowid',
            'idx_lake_geom_z',
            'idx_lake_geom_z_node',
            'idx_lake_geom_z_parent',
            'idx_lake_geom_z_rowid',
            'idx_lake_geom_zm',
            'idx_lake_geom_zm_node',
            'idx_lake_geom_zm_parent',
            'idx_lake_geom_zm_rowid',
            'lake',
            'spatial_ref_sys',
            'spatial_ref_sys_aux',
            'spatialite_history',
            'sql_statements_log',
            'views_geometry_columns',
            'views_geometry_columns_auth',
            'views_geometry_columns_field_infos',
            'views_geometry_columns_statistics',
            'virts_geometry_columns',
            'virts_geometry_columns_auth',
            'virts_geometry_columns_field_infos',
            'virts_geometry_columns_statistics',
        ]
