import platform

import pytest
from shapely.geometry import LineString
from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import func

from geoalchemy2 import Geometry
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import from_shape

from . import select

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
