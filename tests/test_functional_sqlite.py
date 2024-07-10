import re

import pytest
from shapely.geometry import GeometryCollection
from shapely.geometry import LineString
from shapely.geometry import Point
from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import func

from geoalchemy2 import Geometry
from geoalchemy2 import load_spatialite
from geoalchemy2.admin.dialects.geopackage import create_spatial_ref_sys_view
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import from_shape
from geoalchemy2.shape import to_shape

from . import format_wkt
from . import select
from . import skip_case_insensitivity
from . import skip_pypy
from . import test_only_with_dialects
from .schema_fixtures import TransformedGeometry


class TestAdmin:
    def test_create_drop_tables(
        self,
        conn,
        metadata,
        Lake,
        Summit,
        Ocean,
        PointZ,
    ):
        metadata.drop_all(conn, checkfirst=True)
        metadata.create_all(conn)
        metadata.drop_all(conn, checkfirst=True)

    @pytest.mark.parametrize("nullable", [True, False])
    @pytest.mark.parametrize("level", ["col", "type"])
    def test_nullable(self, conn, metadata, setup_tables, dialect_name, nullable, level):
        # Define the table
        col = Column(
            "geom",
            Geometry(
                geometry_type=None,
                srid=4326,
                spatial_index=False,
                nullable=nullable if level == "type" else True,
            ),
            nullable=nullable if level == "col" else True,
        )
        t = Table(
            "nullable_geom_type",
            metadata,
            Column("id", Integer, primary_key=True),
            col,
        )

        # Create the table
        t.create(bind=conn)

        elements = []
        if nullable:
            elements.append({"geom": None})
        else:
            elements.append({"geom": "SRID=4326;LINESTRING(0 0,1 1)"})

        conn.execute(t.insert(), elements)

        if not nullable:
            with pytest.raises((IntegrityError, OperationalError)):
                with conn.begin_nested():
                    conn.execute(t.insert(), [{"geom": None}])

        conn.execute(t.insert(), [{"geom": "SRID=4326;LINESTRING(0 0,1 1)"}])

        results = conn.execute(t.select())
        rows = results.fetchall()

        assert len(rows) == 2

        # Drop the table
        t.drop(bind=conn)

    def test_no_geom_type(self, conn):
        with pytest.warns(UserWarning, match="srid not enforced when geometry_type is None"):
            # Define the table
            t = Table(
                "no_geom_type",
                MetaData(),
                Column("id", Integer, primary_key=True),
                Column("geom", Geometry(geometry_type=None, srid=4326)),
            )

            # Create the table
            t.create(bind=conn)

            # Drop the table
            t.drop(bind=conn)

    def test_explicit_schema(self, conn):
        # Define the table
        t = Table(
            "a_table",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("geom", Geometry()),
            schema="gis",
        )

        # Create the table
        t.create(bind=conn)

        # Check that the table was properly created
        res = conn.execute(text("PRAGMA main.table_info(a_table)")).fetchall()
        assert res == [
            (0, "id", "INTEGER", 1, None, 1),
            (1, "geom", "GEOMETRY", 0, None, 0),
        ]

        # Drop the table
        t.drop(bind=conn)


class TestIndex:
    @pytest.fixture
    def TableWithIndexes(self, base):
        class TableWithIndexes(base):
            __tablename__ = "table_with_indexes"
            id = Column(Integer, primary_key=True)
            # Test indexes on Geometry columns.
            geom_not_managed_no_index = Column(
                Geometry(
                    geometry_type="POINT",
                    spatial_index=False,
                )
            )
            geom_not_managed_index = Column(
                Geometry(
                    geometry_type="POINT",
                    spatial_index=True,
                )
            )
            geom_managed_no_index = Column(
                Geometry(
                    geometry_type="POINT",
                    spatial_index=False,
                )
            )
            geom_managed_index = Column(
                Geometry(
                    geometry_type="POINT",
                    spatial_index=True,
                )
            )

        return TableWithIndexes

    @staticmethod
    def check_spatial_idx(bind, idx_name):
        tables = bind.execute(
            text(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%';"
            )
        ).fetchall()
        if idx_name in [i[0] for i in tables]:
            return True
        return False

    @test_only_with_dialects("sqlite-spatialite3", "sqlite-spatialite4")
    def test_index(self, conn, Lake, setup_tables):
        assert self.check_spatial_idx(conn, "idx_lake_geom")

    @test_only_with_dialects("sqlite-spatialite3", "sqlite-spatialite4")
    def test_type_decorator_index(self, conn, LocalPoint, setup_tables):
        assert self.check_spatial_idx(conn, "idx_local_point_geom")
        assert self.check_spatial_idx(conn, "idx_local_point_managed_geom")

    @test_only_with_dialects("sqlite-spatialite3", "sqlite-spatialite4")
    def test_all_indexes(self, conn, TableWithIndexes, setup_tables):
        expected_indices = [
            "idx_table_with_indexes_geom_managed_index",
            "idx_table_with_indexes_geom_not_managed_index",
        ]
        for expected_idx in expected_indices:
            assert self.check_spatial_idx(conn, expected_idx)

        TableWithIndexes.__table__.drop(bind=conn)

        indexes_after_drop = conn.execute(text("""SELECT * FROM "geometry_columns";""")).fetchall()
        tables_after_drop = conn.execute(
            text(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%';"
            )
        ).fetchall()

        assert indexes_after_drop == []
        assert [table for table in tables_after_drop if "table_with_indexes" in table.name] == []


class TestMiscellaneous:
    @test_only_with_dialects("sqlite-spatialite3", "sqlite-spatialite4")
    @pytest.mark.parametrize(
        [
            "transaction",
            "init_mode",
            "journal_mode",
        ],
        [
            pytest.param(False, "WGS84", None),
            pytest.param(False, "WGS84", "OFF"),
            pytest.param(False, "EMPTY", None),
            pytest.param(False, "EMPTY", "OFF"),
            pytest.param(True, None, None),
            pytest.param(True, None, "OFF"),
            pytest.param(True, "WGS84", None),
            pytest.param(True, "WGS84", "OFF"),
            pytest.param(True, "EMPTY", None),
            pytest.param(True, "EMPTY", "OFF"),
        ],
    )
    def test_load_spatialite(
        self, tmpdir, _engine_echo, check_spatialite, transaction, init_mode, journal_mode
    ):
        if journal_mode == "OFF":
            skip_pypy("The journal mode can not be OFF with PyPy.")

        # Create empty DB
        tmp_db = tmpdir / "test_spatial_db.sqlite"
        db_url = f"sqlite:///{tmp_db}"
        engine = create_engine(
            db_url, echo=_engine_echo, execution_options={"schema_translate_map": {"gis": None}}
        )
        conn = engine.connect()

        assert not conn.execute(text("PRAGMA main.table_info('geometry_columns')")).fetchall()
        assert not conn.execute(text("PRAGMA main.table_info('spatial_ref_sys')")).fetchall()
        assert conn.execute(text("PRAGMA journal_mode")).fetchone()[0].upper() == "DELETE"

        load_spatialite(
            conn.connection.dbapi_connection,
            transaction=transaction,
            init_mode=init_mode,
            journal_mode=journal_mode,
        )

        assert conn.execute(text("SELECT CheckSpatialMetaData();")).scalar() == 3
        assert conn.execute(text("PRAGMA main.table_info('geometry_columns')")).fetchall()
        assert conn.execute(text("PRAGMA main.table_info('spatial_ref_sys')")).fetchall()

        assert conn.execute(text("PRAGMA journal_mode")).fetchone()[0].upper() == "DELETE"

        # Check that spatial_ref_sys table was properly populated
        nb_srid = conn.execute(text("""SELECT COUNT(*) FROM spatial_ref_sys;""")).scalar()
        if init_mode is None:
            assert nb_srid > 1000
        elif init_mode == "WGS84":
            assert nb_srid == 129
        elif init_mode == "EMPTY":
            assert nb_srid == 0

        # Check that the journal mode is properly reset even when an error is returned by the
        # InitSpatialMetaData() function
        assert conn.execute(text("PRAGMA journal_mode")).fetchone()[0].upper() == "DELETE"

        load_spatialite(
            conn.connection.dbapi_connection,
            transaction=transaction,
            init_mode=init_mode,
            journal_mode=journal_mode,
        )

        assert conn.execute(text("PRAGMA journal_mode")).fetchone()[0].upper() == "DELETE"

    @test_only_with_dialects("sqlite-spatialite3", "sqlite-spatialite4")
    def test_load_spatialite_unknown_transaction(self, conn):
        with pytest.raises(ValueError, match=r"The 'transaction' argument must be True or False\."):
            load_spatialite(conn.connection.dbapi_connection, transaction="UNKNOWN MODE")

    @test_only_with_dialects("sqlite-spatialite3", "sqlite-spatialite4")
    def test_load_spatialite_unknown_init_type(self, conn):
        with pytest.raises(
            ValueError, match=r"The 'init_mode' argument must be one of \['WGS84', 'EMPTY'\]\."
        ):
            load_spatialite(conn.connection.dbapi_connection, init_mode="UNKNOWN TYPE")

    @test_only_with_dialects("sqlite-spatialite3", "sqlite-spatialite4")
    def test_load_spatialite_unknown_journal_mode(self, conn):
        with pytest.raises(
            ValueError,
            match=(
                r"The 'journal_mode' argument must be one of "
                r"\['DELETE', 'TRUNCATE', 'PERSIST', 'MEMORY', 'WAL', 'OFF'\]\."
            ),
        ):
            load_spatialite(conn.connection.dbapi_connection, journal_mode="UNKNOWN MODE")

    @test_only_with_dialects("sqlite-spatialite3", "sqlite-spatialite4")
    def test_load_spatialite_no_env_variable(self, monkeypatch, conn):
        monkeypatch.delenv("SPATIALITE_LIBRARY_PATH")
        with pytest.raises(RuntimeError):
            load_spatialite(conn.connection.dbapi_connection)


class TestInsertionCore:
    @pytest.fixture
    def GeomObject(self, base):
        class GeomObject(base):
            __tablename__ = "any_geom_object"
            id = Column(Integer, primary_key=True)
            geom = Column(Geometry(srid=4326))

        return GeomObject

    def test_insert_unparsable_WKT(self, conn, GeomObject, setup_tables, dialect_name):
        with pytest.warns(
            UserWarning,
            match=(
                "The given WKT could not be parsed by GeoAlchemy2, this could lead to undefined "
                "behavior"
            ),
        ):
            conn.execute(
                GeomObject.__table__.insert(),
                [
                    {"geom": "SRID=4326;GeometryCollection(POINT (-1 1),LINESTRING (2 2, 3 3))"},
                ],
            )

        results = conn.execute(GeomObject.__table__.select())
        rows = results.fetchall()

        for row in rows:
            assert isinstance(row[1], WKBElement)
            wkt = conn.execute(row[1].ST_AsText()).scalar()
            assert format_wkt(wkt) == "GEOMETRYCOLLECTION(POINT(-1 1),LINESTRING(2 2,3 3))"
            srid = conn.execute(row[1].ST_SRID()).scalar()
            assert srid == 4326
            if dialect_name == "mysql":
                extended = None
            else:
                extended = True
            assert row[1] == from_shape(
                GeometryCollection([Point(-1, 1), LineString([[2, 2], [3, 3]])]),
                srid=4326,
                extended=extended,
            )


class TestInsertionORM:
    @pytest.fixture
    def LocalPoint(self, base):
        class LocalPoint(base):
            __tablename__ = "local_point"
            id = Column(Integer, primary_key=True)
            geom = Column(TransformedGeometry(db_srid=2154, app_srid=4326, geometry_type="POINT"))

        return LocalPoint

    def test_transform(self, session, conn, LocalPoint, setup_tables, dialect_name):
        if dialect_name == "geopackage":
            # For GeoPackage we have to create the 'spatial_ref_sys' table to be able to use
            # the ST_Transform function. It can be created using InitSpatialMetaData() but it also
            # creates the 'geometry_columns' table, which is useless. So here we create the table
            # manually with only the required SRS IDs.
            create_spatial_ref_sys_view(conn)

        # Create new point instance
        p = LocalPoint()
        p.geom = "SRID=4326;POINT(5 45)"  # Insert geometry with wrong SRID

        # Insert point
        session.add(p)
        session.flush()
        session.expire(p)

        # Query the point and check the result
        pt = session.query(LocalPoint).one()
        assert pt.id == 1
        assert pt.geom.srid == 4326
        pt_wkb = to_shape(pt.geom)
        assert round(pt_wkb.x, 5) == 5
        assert round(pt_wkb.y, 5) == 45

        # Check that the data is correct in DB using raw query
        q = text(
            """
            SELECT id, ST_AsText(geom) AS geom
            FROM local_point;
            """
        )
        res_q = session.execute(q).fetchone()
        assert res_q.id == 1
        for i in [res_q.geom]:
            x, y = re.match(r"POINT\((\d+\.\d*) (\d+\.\d*)\)", i).groups()
            assert round(float(x), 3) == 857581.899
            assert round(float(y), 3) == 6435414.748


class TestUpdateORM:
    pass


class TestCallFunction:
    def test_ST_Buffer(self, session):
        """Test the specific SQLite signature with the `quadrantsegments` parameter."""
        s = select(
            [func.St_AsText(func.ST_Buffer(WKTElement("LINESTRING(0 0,1 0)", srid=4326), 2, 1))]
        )
        r1 = session.execute(s).scalar()
        assert r1 == "POLYGON((1 2, 3 0, 1 -2, 0 -2, -2 0, 0 2, 1 2))"

        s = select(
            [func.St_AsText(func.ST_Buffer(WKTElement("LINESTRING(0 0,1 0)", srid=4326), 2, 2))]
        )
        r1 = session.execute(s).scalar()
        assert r1 == (
            "POLYGON((1 2, 2.414214 1.414214, 3 0, 2.414214 -1.414214, 1 -2, 0 -2, "
            "-1.414214 -1.414214, -2 0, -1.414214 1.414214, 0 2, 1 2))"
        )

    @pytest.fixture
    def setup_one_lake(self, session, Lake, setup_tables):
        lake = Lake(WKTElement("LINESTRING(0 0,1 1)", srid=4326))
        session.add(lake)
        session.flush()
        session.expire(lake)
        return lake.id

    @skip_case_insensitivity()
    def test_comparator_case_insensitivity(self, session, Lake, setup_one_lake):
        lake_id = setup_one_lake

        s = select([func.ST_Buffer(Lake.__table__.c.geom, 1)])
        r1 = session.execute(s).scalar()
        assert isinstance(r1, WKBElement)

        lake = session.query(Lake).get(lake_id)

        r2 = session.execute(lake.geom.ST_Buffer(1)).scalar()
        assert isinstance(r2, WKBElement)

        r3 = session.execute(lake.geom.st_buffer(1)).scalar()
        assert isinstance(r3, WKBElement)

        r4 = session.execute(lake.geom.St_BuFfEr(1)).scalar()
        assert isinstance(r4, WKBElement)

        r5 = session.query(Lake.geom.ST_Buffer(1)).scalar()
        assert isinstance(r5, WKBElement)

        r6 = session.query(Lake.geom.st_buffer(1)).scalar()
        assert isinstance(r6, WKBElement)

        r7 = session.query(Lake.geom.St_BuFfEr(1)).scalar()
        assert isinstance(r7, WKBElement)

        assert r1.data == r2.data == r3.data == r4.data == r5.data == r6.data == r7.data


class TestShapely:
    pass


class TestNullable:
    @pytest.fixture
    def NotNullableLake(self, base):
        class NotNullableLake(base):
            __tablename__ = "NotNullablelake"
            id = Column(Integer, primary_key=True)
            geom = Column(
                Geometry(
                    geometry_type="LINESTRING",
                    srid=4326,
                    nullable=False,
                )
            )

            def __init__(self, geom):
                self.geom = geom

        return NotNullableLake

    def test_insert(self, conn, NotNullableLake, setup_tables):
        # Insert geometries
        conn.execute(
            NotNullableLake.__table__.insert(),
            [
                {"geom": "SRID=4326;LINESTRING(0 0,1 1)"},
                {"geom": WKTElement("LINESTRING(0 0,2 2)", srid=4326)},
                {"geom": from_shape(LineString([[0, 0], [3, 3]]), srid=4326)},
            ],
        )

        # Fail when trying to insert null geometry
        with pytest.raises(IntegrityError):
            conn.execute(NotNullableLake.__table__.insert(), [{"geom": None}])


class TestContraint:
    @pytest.fixture
    def ConstrainedLake(self, base):
        class ConstrainedLake(base):
            __tablename__ = "contrained_lake"
            __table_args__ = (
                CheckConstraint(
                    "(geom is null and a_str is null) = (checked_str is null)",
                    "check_geom_sk",
                ),
            )
            id = Column(Integer, primary_key=True)
            a_str = Column(String, nullable=True)
            checked_str = Column(String, nullable=True)
            geom = Column(Geometry(geometry_type="LINESTRING", srid=4326))

            def __init__(self, geom):
                self.geom = geom

        return ConstrainedLake

    def test_insert(self, conn, ConstrainedLake, setup_tables):
        # Insert geometries
        conn.execute(
            ConstrainedLake.__table__.insert(),
            [
                {
                    "a_str": None,
                    "geom": "SRID=4326;LINESTRING(0 0,1 1)",
                    "checked_str": "test",
                },
                {"a_str": "test", "geom": None, "checked_str": "test"},
                {"a_str": None, "geom": None, "checked_str": None},
            ],
        )

        # Fail when trying to insert null geometry
        with pytest.raises(IntegrityError):
            conn.execute(
                ConstrainedLake.__table__.insert(),
                [
                    {"a_str": None, "geom": None, "checked_str": "should fail"},
                ],
            )


class TestReflection:
    @pytest.fixture
    def setup_reflection_tables(self, reflection_tables_metadata, conn):
        reflection_tables_metadata.drop_all(conn, checkfirst=True)
        reflection_tables_metadata.create_all(conn)

    @test_only_with_dialects("sqlite-spatialite3")
    def test_reflection_spatialite_lt_4(self, conn, setup_reflection_tables):
        t = Table("lake", MetaData(), autoload_with=conn)

        type_ = t.c.geom.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == "LINESTRING"
        assert type_.srid == 4326
        assert type_.dimension == 2

        type_ = t.c.geom_no_idx.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == "LINESTRING"
        assert type_.srid == 4326
        assert type_.dimension == 2

        type_ = t.c.geom_z.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == "LINESTRINGZ"
        assert type_.srid == 4326
        assert type_.dimension == 3

        type_ = t.c.geom_m.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == "LINESTRINGM"
        assert type_.srid == 4326
        assert type_.dimension == 3

        type_ = t.c.geom_zm.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == "LINESTRINGZM"
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
                type = 'table' AND
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
            "SpatialIndex",
            "geometry_columns",
            "geometry_columns_auth",
            "layer_statistics",
            "spatial_ref_sys",
            "spatialite_history",
            "views_geometry_columns",
            "views_layer_statistics",
            "virts_geometry_columns",
            "virts_layer_statistics",
        ]

        # Recreate the table to check that the reflected properties are correct
        t.create(bind=conn)

        # Check the actual properties
        geom_cols = conn.execute(query_indexes).fetchall()
        assert geom_cols == [
            ("lake", "geom", "LINESTRING", "XY", 4326, 1),
            ("lake", "geom_m", "LINESTRING", "XYM", 4326, 1),
            ("lake", "geom_no_idx", "LINESTRING", "XY", 4326, 0),
            ("lake", "geom_z", "LINESTRING", "XYZ", 4326, 1),
            ("lake", "geom_zm", "LINESTRING", "XYZM", 4326, 1),
        ]

        all_tables = [i[0] for i in conn.execute(query_tables).fetchall()]
        assert all_tables == [
            "SpatialIndex",
            "geometry_columns",
            "geometry_columns_auth",
            "idx_lake_geom",
            "idx_lake_geom_m",
            "idx_lake_geom_m_node",
            "idx_lake_geom_m_parent",
            "idx_lake_geom_m_rowid",
            "idx_lake_geom_node",
            "idx_lake_geom_parent",
            "idx_lake_geom_rowid",
            "idx_lake_geom_z",
            "idx_lake_geom_z_node",
            "idx_lake_geom_z_parent",
            "idx_lake_geom_z_rowid",
            "idx_lake_geom_zm",
            "idx_lake_geom_zm_node",
            "idx_lake_geom_zm_parent",
            "idx_lake_geom_zm_rowid",
            "lake",
            "layer_statistics",
            "spatial_ref_sys",
            "spatialite_history",
            "views_geometry_columns",
            "views_layer_statistics",
            "virts_geometry_columns",
            "virts_layer_statistics",
        ]

    @test_only_with_dialects("sqlite-spatialite4")
    def test_reflection_spatialite_ge_4(self, conn, setup_reflection_tables):
        t = Table("lake", MetaData(), autoload_with=conn)

        type_ = t.c.geom.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == "LINESTRING"
        assert type_.srid == 4326
        assert type_.dimension == 2

        type_ = t.c.geom_no_idx.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == "LINESTRING"
        assert type_.srid == 4326
        assert type_.dimension == 2

        type_ = t.c.geom_z.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == "LINESTRINGZ"
        assert type_.srid == 4326
        assert type_.dimension == 3

        type_ = t.c.geom_m.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == "LINESTRINGM"
        assert type_.srid == 4326
        assert type_.dimension == 3

        type_ = t.c.geom_zm.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == "LINESTRINGZM"
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
                type = 'table' AND
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
            "ElementaryGeometries",
            "SpatialIndex",
            "geometry_columns",
            "geometry_columns_auth",
            "geometry_columns_field_infos",
            "geometry_columns_statistics",
            "geometry_columns_time",
            "spatial_ref_sys",
            "spatial_ref_sys_aux",
            "spatialite_history",
            "sql_statements_log",
            "views_geometry_columns",
            "views_geometry_columns_auth",
            "views_geometry_columns_field_infos",
            "views_geometry_columns_statistics",
            "virts_geometry_columns",
            "virts_geometry_columns_auth",
            "virts_geometry_columns_field_infos",
            "virts_geometry_columns_statistics",
        ]

        # Recreate the table to check that the reflected properties are correct
        t.create(bind=conn)

        # Check the actual properties
        geom_cols = conn.execute(query_indexes).fetchall()
        assert geom_cols == [
            ("lake", "geom", 2, 2, 4326, 1),
            ("lake", "geom_m", 2002, 3, 4326, 1),
            ("lake", "geom_no_idx", 2, 2, 4326, 0),
            ("lake", "geom_z", 1002, 3, 4326, 1),
            ("lake", "geom_zm", 3002, 4, 4326, 1),
        ]

        all_tables = [i[0] for i in conn.execute(query_tables).fetchall()]
        assert all_tables == [
            "ElementaryGeometries",
            "SpatialIndex",
            "geometry_columns",
            "geometry_columns_auth",
            "geometry_columns_field_infos",
            "geometry_columns_statistics",
            "geometry_columns_time",
            "idx_lake_geom",
            "idx_lake_geom_m",
            "idx_lake_geom_m_node",
            "idx_lake_geom_m_parent",
            "idx_lake_geom_m_rowid",
            "idx_lake_geom_node",
            "idx_lake_geom_parent",
            "idx_lake_geom_rowid",
            "idx_lake_geom_z",
            "idx_lake_geom_z_node",
            "idx_lake_geom_z_parent",
            "idx_lake_geom_z_rowid",
            "idx_lake_geom_zm",
            "idx_lake_geom_zm_node",
            "idx_lake_geom_zm_parent",
            "idx_lake_geom_zm_rowid",
            "lake",
            "spatial_ref_sys",
            "spatial_ref_sys_aux",
            "spatialite_history",
            "sql_statements_log",
            "views_geometry_columns",
            "views_geometry_columns_auth",
            "views_geometry_columns_field_infos",
            "views_geometry_columns_statistics",
            "virts_geometry_columns",
            "virts_geometry_columns_auth",
            "virts_geometry_columns_field_infos",
            "virts_geometry_columns_statistics",
        ]

    @pytest.fixture
    def reflection_tables_metadata_multi(self, base):
        class Lake(base):
            __tablename__ = "lake"
            id = Column(Integer, primary_key=True)
            geom = Column(Geometry(geometry_type="LINESTRING", srid=4326))

        class LakeNoIdx(base):
            __tablename__ = "lake_no_idx"
            id = Column(Integer, primary_key=True)
            geom = Column(Geometry(geometry_type="LINESTRING", srid=4326, spatial_index=False))

        class LakeZ(base):
            __tablename__ = "lake_z"
            id = Column(Integer, primary_key=True)
            geom = Column(Geometry(geometry_type="LINESTRINGZ", srid=4326, dimension=3))

        class LakeM(base):
            __tablename__ = "lake_m"
            id = Column(Integer, primary_key=True)
            geom = Column(Geometry(geometry_type="LINESTRINGM", srid=4326, dimension=3))

        class LakeZM(base):
            __tablename__ = "lake_zm"
            id = Column(Integer, primary_key=True)
            geom = Column(Geometry(geometry_type="LINESTRINGZM", srid=4326, dimension=4))

        yield

    @pytest.fixture
    def setup_reflection_multiple_tables(self, reflection_tables_metadata_multi, metadata, conn):
        metadata.drop_all(conn, checkfirst=True)
        metadata.create_all(conn)

    @test_only_with_dialects("sqlite-spatialite3", "geopackage")
    def test_reflection_mutliple_tables(self, conn, setup_reflection_multiple_tables, dialect_name):
        reflected_metadata = MetaData()
        t_lake = Table("lake", reflected_metadata, autoload_with=conn)
        t_lake_no_idx = Table("lake_no_idx", reflected_metadata, autoload_with=conn)
        t_lake_z = Table("lake_z", reflected_metadata, autoload_with=conn)
        t_lake_m = Table("lake_m", reflected_metadata, autoload_with=conn)
        t_lake_zm = Table("lake_zm", reflected_metadata, autoload_with=conn)

        type_ = t_lake.c.geom.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == "LINESTRING"
        assert type_.srid == 4326
        assert type_.dimension == 2

        type_ = t_lake_no_idx.c.geom.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == "LINESTRING"
        assert type_.srid == 4326
        assert type_.dimension == 2

        type_ = t_lake_z.c.geom.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == "LINESTRINGZ"
        assert type_.srid == 4326
        assert type_.dimension == 3

        type_ = t_lake_m.c.geom.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == "LINESTRINGM"
        assert type_.srid == 4326
        assert type_.dimension == 3

        type_ = t_lake_zm.c.geom.type
        assert isinstance(type_, Geometry)
        assert type_.geometry_type == "LINESTRINGZM"
        assert type_.srid == 4326
        assert type_.dimension == 4

        # Drop the tables
        reflected_metadata.drop_all(conn, checkfirst=True)

        # Query to check the tables
        query_tables = text(
            """SELECT
                name
            FROM
                sqlite_master
            WHERE
                type = 'table' AND
                name LIKE 'lake%'
            ORDER BY tbl_name;"""
        )

        if dialect_name == "geopackage":
            # Query to check the indices
            query_indexes = text(
                """SELECT
                    A.table_name,
                    A.column_name,
                    A.geometry_type_name,
                    A.z,
                    A.m,
                    A.srs_id,
                    IFNULL(B.has_index, 0) AS has_index
                FROM gpkg_geometry_columns
                AS A
                LEFT JOIN (
                    SELECT table_name, column_name, COUNT(*) AS has_index
                    FROM gpkg_extensions
                    WHERE extension_name = 'gpkg_rtree_index'
                    GROUP BY table_name, column_name
                ) AS B
                ON A.table_name = B.table_name AND A.column_name = B.column_name
                ORDER BY A.table_name;
            """
            )
        else:
            # Query to check the indices
            query_indexes = text(
                """SELECT * FROM geometry_columns ORDER BY f_table_name, f_geometry_column;"""
            )

        # Check the indices
        geom_cols = conn.execute(query_indexes).fetchall()
        assert geom_cols == []

        # Check the tables
        all_tables = [i[0] for i in conn.execute(query_tables).fetchall()]
        assert all_tables == []

        # Recreate the table to check that the reflected properties are correct
        reflected_metadata.create_all(conn)

        # Check the actual properties
        geom_cols = conn.execute(query_indexes).fetchall()

        if dialect_name == "geopackage":
            assert geom_cols == [
                ("lake", "geom", "LINESTRING", 0, 0, 4326, 1),
                ("lake_m", "geom", "LINESTRINGM", 0, 1, 4326, 1),
                ("lake_no_idx", "geom", "LINESTRING", 0, 0, 4326, 0),
                ("lake_z", "geom", "LINESTRINGZ", 1, 0, 4326, 1),
                ("lake_zm", "geom", "LINESTRINGZM", 1, 1, 4326, 1),
            ]
        else:
            assert geom_cols == [
                ("lake", "geom", "LINESTRING", "XY", 4326, 1),
                ("lake_m", "geom", "LINESTRING", "XYM", 4326, 1),
                ("lake_no_idx", "geom", "LINESTRING", "XY", 4326, 0),
                ("lake_z", "geom", "LINESTRING", "XYZ", 4326, 1),
                ("lake_zm", "geom", "LINESTRING", "XYZM", 4326, 1),
            ]

        all_tables = [i[0] for i in conn.execute(query_tables).fetchall()]
        assert all_tables == [
            "lake",
            "lake_m",
            "lake_no_idx",
            "lake_z",
            "lake_zm",
        ]
