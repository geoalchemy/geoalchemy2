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

from packaging.version import parse as parse_version
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
from sqlalchemy.exc import SAWarning
from sqlalchemy.sql import func
from sqlalchemy.testing.assertions import ComparesTables

import geoalchemy2.admin.dialects
from geoalchemy2 import Geography
from geoalchemy2 import Geometry
from geoalchemy2 import Raster
from geoalchemy2.admin.dialects.geopackage import (
    _get_spatialite_attrs as _get_spatialite_attrs_gpkg,
)
from geoalchemy2.admin.dialects.sqlite import _get_spatialite_attrs as _get_spatialite_attrs_sqlite
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import from_shape
from geoalchemy2.shape import to_shape

from . import check_indexes
from . import format_wkt
from . import get_postgis_major_version
from . import select
from . import skip_case_insensitivity
from . import skip_pg12_sa1217
from . import skip_postgis1
from . import test_only_with_dialects

SQLA_LT_2 = parse_version(SA_VERSION) <= parse_version("1.999")


class TestAdmin:
    @test_only_with_dialects("postgresql", "sqlite-spatialite3", "sqlite-spatialite4")
    def test_create_drop_tables(
        self,
        conn,
        metadata,
        Lake,
        Poi,
        Summit,
        Ocean,
        PointZ,
        LocalPoint,
        IndexTestWithSchema,
        IndexTestWithNDIndex,
        IndexTestWithoutSchema,
    ):
        metadata.drop_all(conn, checkfirst=True)
        metadata.create_all(conn)
        metadata.drop_all(conn, checkfirst=True)

    @test_only_with_dialects("postgresql", "mysql", "sqlite-spatialite3", "sqlite-spatialite4")
    def test_nullable(self, conn, metadata, setup_tables, dialect_name):
        # Define the table
        t = Table(
            "nullable_geom_type",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "geom_not_nullable",
                Geometry(geometry_type=None, srid=4326, spatial_index=False, nullable=False),
            ),
            Column(
                "geom_nullable",
                Geometry(geometry_type=None, srid=4326, spatial_index=False, nullable=True),
            ),
            Column(
                "geom_col_not_nullable",
                Geometry(geometry_type=None, srid=4326, spatial_index=False),
                nullable=False,
            ),
            Column(
                "geom_col_nullable",
                Geometry(geometry_type=None, srid=4326, spatial_index=False),
                nullable=True,
            ),
        )

        # Create the table
        t.create(bind=conn)

        conn.execute(
            t.insert(),
            [
                {
                    "geom_not_nullable": "SRID=4326;LINESTRING(0 0,1 1)",
                    "geom_nullable": "SRID=4326;LINESTRING(0 0,1 1)",
                    "geom_col_not_nullable": "SRID=4326;LINESTRING(0 0,1 1)",
                    "geom_col_nullable": "SRID=4326;LINESTRING(0 0,1 1)",
                },
                {
                    "geom_not_nullable": "SRID=4326;LINESTRING(0 0,1 1)",
                    "geom_nullable": None,
                    "geom_col_not_nullable": "SRID=4326;LINESTRING(0 0,1 1)",
                    "geom_col_nullable": None,
                },
            ],
        )

        with pytest.raises((IntegrityError, OperationalError)):
            with conn.begin_nested():
                conn.execute(
                    t.insert(),
                    [
                        {
                            "geom_not_nullable": None,
                            "geom_nullable": None,
                            "geom_col_not_nullable": "SRID=4326;LINESTRING(0 0,1 1)",
                            "geom_col_nullable": None,
                        },
                    ],
                )

        with pytest.raises((IntegrityError, OperationalError)):
            with conn.begin_nested():
                conn.execute(
                    t.insert(),
                    [
                        {
                            "geom_not_nullable": "SRID=4326;LINESTRING(0 0,1 1)",
                            "geom_nullable": None,
                            "geom_col_not_nullable": None,
                            "geom_col_nullable": None,
                        },
                    ],
                )

        results = conn.execute(t.select())
        rows = results.fetchall()

        assert len(rows) == 2

        # Drop the table
        t.drop(bind=conn)

    @test_only_with_dialects("postgresql", "mysql")
    def test_no_geom_type(self, conn):
        with pytest.warns(UserWarning, match="srid not enforced when geometry_type is None"):
            # Define the table
            t = Table(
                "no_geom_type",
                MetaData(),
                Column("id", Integer, primary_key=True),
                Column("geom", Geometry(geometry_type=None)),
                Column("geom_with_srid", Geometry(geometry_type=None, srid=4326)),
                Column("geog", Geography(geometry_type=None)),
                Column("geog_with_srid", Geography(geometry_type=None, srid=4326)),
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

        # Drop the table
        t.drop(bind=conn)

    @test_only_with_dialects("postgresql")
    def test_common_dialect(self, conn, monkeypatch, metadata, Lake):
        monkeypatch.setattr(conn.dialect, "name", "UNKNOWN DIALECT")

        marks = []

        def before_create(table, bind, **kw):
            marks.append("before_create")
            return

        def after_create(table, bind, **kw):
            marks.append("after_create")
            return

        def before_drop(table, bind, **kw):
            marks.append("before_drop")
            return

        def after_drop(table, bind, **kw):
            marks.append("after_drop")
            return

        monkeypatch.setattr(geoalchemy2.admin.dialects.common, "before_create", value=before_create)
        monkeypatch.setattr(geoalchemy2.admin.dialects.common, "after_create", value=after_create)
        monkeypatch.setattr(geoalchemy2.admin.dialects.common, "before_drop", value=before_drop)
        monkeypatch.setattr(geoalchemy2.admin.dialects.common, "after_drop", value=after_drop)

        metadata.drop_all(conn, checkfirst=True)
        metadata.create_all(conn)
        metadata.drop_all(conn, checkfirst=True)

        assert marks == ["before_create", "after_create", "before_drop", "after_drop"]


class TestInsertionCore:
    def test_insert(self, conn, Lake, setup_tables):
        # Issue inserts using DBAPI's executemany() method. This tests the
        # Geometry type's bind_processor and bind_expression functions.
        conn.execute(
            Lake.__table__.insert(),
            [
                {"geom": "SRID=4326;LINESTRING(0 0,1 1)"},
                {"geom": WKTElement("LINESTRING(0 0,2 2)", srid=4326)},
                {"geom": WKTElement("SRID=4326;LINESTRING(0 0,2 2)", extended=True)},
                {"geom": from_shape(LineString([[0, 0], [3, 3]]), srid=4326)},
            ],
        )

        results = conn.execute(Lake.__table__.select())
        rows = results.fetchall()

        row = rows[0]
        assert isinstance(row[1], WKBElement)
        wkt = conn.execute(from_shape(LineString([[0, 0], [3, 3]]), srid=4326).ST_AsText()).scalar()
        wkt = conn.execute(row[1].ST_AsText()).scalar()
        assert format_wkt(wkt) == "LINESTRING(0 0,1 1)"
        srid = conn.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

        row = rows[1]
        assert isinstance(row[1], WKBElement)
        wkt = conn.execute(row[1].ST_AsText()).scalar()
        assert format_wkt(wkt) == "LINESTRING(0 0,2 2)"
        srid = conn.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

        row = rows[2]
        assert isinstance(row[1], WKBElement)
        wkt = conn.execute(row[1].ST_AsText()).scalar()
        assert format_wkt(wkt) == "LINESTRING(0 0,2 2)"
        srid = conn.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

        row = rows[3]
        assert isinstance(row[1], WKBElement)
        wkt = conn.execute(row[1].ST_AsText()).scalar()
        assert format_wkt(wkt) == "LINESTRING(0 0,3 3)"
        srid = conn.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

    @pytest.mark.parametrize(
        "geom_type,wkt",
        [
            pytest.param("POINT", "(1 2)", id="Point"),
            pytest.param("POINTZ", "(1 2 3)", id="Point Z"),
            pytest.param("POINTM", "(1 2 3)", id="Point M"),
            pytest.param("POINTZM", "(1 2 3 4)", id="Point ZM"),
            pytest.param("LINESTRING", "(1 2, 3 4)", id="LineString"),
            pytest.param("LINESTRINGZ", "(1 2 3, 4 5 6)", id="LineString Z"),
            pytest.param("LINESTRINGM", "(1 2 3, 4 5 6)", id="LineString M"),
            pytest.param("LINESTRINGZM", "(1 2 3 4, 5 6 7 8)", id="LineString ZM"),
            pytest.param("POLYGON", "((1 2, 3 4, 5 6, 1 2))", id="Polygon"),
            pytest.param("POLYGONZ", "((1 2 3, 4 5 6, 7 8 9, 1 2 3))", id="Polygon Z"),
            pytest.param("POLYGONM", "((1 2 3, 4 5 6, 7 8 9, 1 2 3))", id="Polygon M"),
            pytest.param(
                "POLYGONZM", "((1 2 3 4,  5 6 7 8, 9 10 11 12, 1 2 3 4))", id="Polygon ZM"
            ),
            pytest.param("MULTIPOINT", "(1 2, 3 4)", id="Multi Point"),
            pytest.param("MULTIPOINTZ", "(1 2 3, 4 5 6)", id="Multi Point Z"),
            pytest.param("MULTIPOINTM", "(1 2 3, 4 5 6)", id="Multi Point M"),
            pytest.param("MULTIPOINTZM", "(1 2 3 4, 5 6 7 8)", id="Multi Point ZM"),
            pytest.param("MULTILINESTRING", "((1 2, 3 4), (10 20, 30 40))", id="Multi LineString"),
            pytest.param(
                "MULTILINESTRINGZ",
                "((1 2 3, 4 5 6), (10 20 30, 40 50 60))",
                id="Multi LineString Z",
            ),
            pytest.param(
                "MULTILINESTRINGM",
                "((1 2 3, 4 5 6), (10 20 30, 40 50 60))",
                id="Multi LineString M",
            ),
            pytest.param(
                "MULTILINESTRINGZM",
                "((1 2 3 4, 5 6 7 8), (10 20 30 40, 50 60 70 80))",
                id="Multi LineString ZM",
            ),
            pytest.param(
                "MULTIPOLYGON",
                "(((1 2, 3 4, 5 6, 1 2)), ((10 20, 30 40, 50 60, 10 20)))",
                id="Multi Polygon",
            ),
            pytest.param(
                "MULTIPOLYGONZ",
                "(((1 2 3, 4 5 6, 7 8 9, 1 2 3)), ((10 20 30, 40 50 60, 70 80 90, 10 20 30)))",
                id="Multi Polygon Z",
            ),
            pytest.param(
                "MULTIPOLYGONM",
                "(((1 2 3, 4 5 6, 7 8 9, 1 2 3)), ((10 20 30, 40 50 60, 70 80 90, 10 20 30)))",
                id="Multi Polygon M",
            ),
            pytest.param(
                "MULTIPOLYGONZM",
                "(((1 2 3 4, 5 6 7 8, 9 10 11 12, 1 2 3 4)),"
                " ((10 20 30 40, 50 60 70 80, 90 100 100 120, 10 20 30 40)))",
                id="Multi Polygon ZM",
            ),
        ],
    )
    @pytest.mark.parametrize(
        "use_floating_point",
        [
            pytest.param(True, id="Use floating point"),
            pytest.param(False, id="Do not use floating point"),
        ],
    )
    def test_insert_all_geom_types(
        self, dialect_name, base, conn, metadata, geom_type, wkt, use_floating_point
    ):
        """Test insertion and selection of all geometry types."""
        ndims = 2
        if "Z" in geom_type[-2:]:
            ndims += 1
        if geom_type.endswith("M"):
            ndims += 1
            has_m = True
        else:
            has_m = False

        if ndims > 2 and dialect_name in ["mysql", "mariadb"]:
            # Explicitly skip MySQL dialect to show that it can only work with 2D geometries
            pytest.xfail(reason="MySQL only supports 2D geometry types")

        class GeomTypeTable(base):
            __tablename__ = "test_geom_types"
            id = Column(Integer, primary_key=True)
            geom = Column(Geometry(srid=4326, geometry_type=geom_type, dimension=ndims))

        metadata.drop_all(bind=conn, checkfirst=True)
        metadata.create_all(bind=conn)

        if use_floating_point:
            wkt = wkt.replace("1 2", "1.5 2.5")

        inserted_wkt = f"{geom_type}{wkt}"

        inserted_elements = [
            {"geom": inserted_wkt},
            {"geom": f"SRID=4326;{inserted_wkt}"},
            {"geom": WKTElement(inserted_wkt, srid=4326)},
            {"geom": WKTElement(f"SRID=4326;{inserted_wkt}")},
        ]
        if dialect_name not in ["postgresql", "sqlite"] or not has_m:
            # Use the DB to generate the corresponding raw WKB
            raw_wkb = conn.execute(
                text("SELECT ST_AsBinary(ST_GeomFromText('{}', 4326))".format(inserted_wkt))
            ).scalar()

            wkb_elem = WKBElement(raw_wkb, srid=4326)

            # Currently Shapely does not support geometry types with M dimension
            inserted_elements.append({"geom": wkb_elem})
            inserted_elements.append({"geom": wkb_elem.as_ewkb()})

        # Insert the elements
        conn.execute(
            GeomTypeTable.__table__.insert(),
            inserted_elements,
        )

        # Select the elements
        query = select(
            [
                GeomTypeTable.__table__.c.id,
                GeomTypeTable.__table__.c.geom.ST_AsText(),
                GeomTypeTable.__table__.c.geom.ST_SRID(),
            ],
        )
        results = conn.execute(query)
        rows = results.all()

        # Check that the selected elements are the same as the inputs
        for row_id, row, srid in rows:
            checked_wkt = row.upper().replace(" ", "")
            expected_wkt = inserted_wkt.upper().replace(" ", "")
            if "MULTIPOINT" in geom_type:
                # Some dialects return MULTIPOINT geometries with nested parenthesis and others
                # do not so we remove them before checking the results
                checked_wkt = re.sub(r"\(([0-9\.]+)\)", "\\1", checked_wkt)
            if row_id >= 5 and dialect_name in ["geopackage"] and has_m:
                # Currently Shapely does not support geometry types with M dimension
                assert checked_wkt != expected_wkt
            else:
                assert checked_wkt == expected_wkt
            assert srid == 4326

    @test_only_with_dialects("postgresql", "sqlite")
    def test_insert_geom_poi(self, conn, Poi, setup_tables):
        conn.execute(
            Poi.__table__.insert(),
            [
                {"geom": "SRID=4326;POINT(1 1)"},
                {"geom": WKTElement("POINT(1 1)", srid=4326)},
                {"geom": WKTElement("SRID=4326;POINT(1 1)", extended=True)},
                {"geom": from_shape(Point(1, 1), srid=4326)},
                {"geom": from_shape(Point(1, 1), srid=4326, extended=True)},
            ],
        )

        results = conn.execute(Poi.__table__.select())
        rows = results.fetchall()

        for row in rows:
            assert isinstance(row[1], WKBElement)
            wkt = conn.execute(row[1].ST_AsText()).scalar()
            assert format_wkt(wkt) == "POINT(1 1)"
            srid = conn.execute(row[1].ST_SRID()).scalar()
            assert srid == 4326
            assert row[1] == from_shape(Point(1, 1), srid=4326, extended=True)

    def test_insert_negative_coords(self, conn, Poi, setup_tables, dialect_name):
        conn.execute(
            Poi.__table__.insert(),
            [
                {"geom": "SRID=4326;POINT(-1 1)"},
                {"geom": WKTElement("POINT(-1 1)", srid=4326)},
                {"geom": WKTElement("SRID=4326;POINT(-1 1)", extended=True)},
                {"geom": from_shape(Point(-1, 1), srid=4326)},
                {"geom": from_shape(Point(-1, 1), srid=4326, extended=True)},
            ],
        )

        results = conn.execute(Poi.__table__.select())
        rows = results.fetchall()

        for row in rows:
            assert isinstance(row[1], WKBElement)
            wkt = conn.execute(row[1].ST_AsText()).scalar()
            assert format_wkt(wkt) == "POINT(-1 1)"
            srid = conn.execute(row[1].ST_SRID()).scalar()
            assert srid == 4326
            extended = dialect_name not in ["mysql", "mariadb"]
            assert row[1] == from_shape(Point(-1, 1), srid=4326, extended=extended)


class TestSelectBindParam:
    @pytest.fixture
    def setup_one_lake(self, conn, Lake, setup_tables):
        conn.execute(Lake.__table__.insert(), {"geom": "SRID=4326;LINESTRING(0 0,1 1)"})

    def test_select_bindparam(self, conn, Lake, setup_one_lake):
        s = Lake.__table__.select().where(Lake.__table__.c.geom == bindparam("geom"))
        params = {"geom": "SRID=4326;LINESTRING(0 0,1 1)"}
        if SQLA_LT_2:
            results = conn.execute(s, **params)
        else:
            results = conn.execute(s, params)
        rows = results.fetchall()

        row = rows[0]
        assert isinstance(row[1], WKBElement)
        wkt = conn.execute(row[1].ST_AsText()).scalar()
        assert format_wkt(wkt) == "LINESTRING(0 0,1 1)"
        srid = conn.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

    def test_select_bindparam_WKBElement(self, conn, Lake, setup_one_lake):
        s = Lake.__table__.select().where(Lake.__table__.c.geom == bindparam("geom"))
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
        assert format_wkt(wkt) == "LINESTRING(0 0,1 1)"
        srid = conn.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326

    def test_select_bindparam_WKBElement_extented(self, conn, Lake, setup_one_lake, dialect_name):
        s = Lake.__table__.select()
        results = conn.execute(s)
        rows = results.fetchall()
        geom = rows[0][1]
        assert isinstance(geom, WKBElement)
        if dialect_name in ["mysql", "mariadb"]:
            assert geom.extended is False
        else:
            assert geom.extended is True

        s = Lake.__table__.select().where(Lake.__table__.c.geom == bindparam("geom"))
        params = {"geom": geom}
        if SQLA_LT_2:
            results = conn.execute(s, **params)
        else:
            results = conn.execute(s, params)
        rows = results.fetchall()

        row = rows[0]
        assert isinstance(row[1], WKBElement)
        wkt = conn.execute(row[1].ST_AsText()).scalar()
        assert format_wkt(wkt) == "LINESTRING(0 0,1 1)"
        srid = conn.execute(row[1].ST_SRID()).scalar()
        assert srid == 4326


class TestInsertionORM:
    def test_WKT(self, session, Lake, setup_tables, dialect_name, postgis_version):
        # With PostGIS 1.5:
        # IntegrityError: (IntegrityError) new row for relation "lake" violates
        # check constraint "enforce_srid_geom"
        #
        # With PostGIS 2.0:
        # DataError: (DataError) Geometry SRID (0) does not match column SRID
        # (4326)
        #
        # With PostGIS 3.0:
        # The SRID is taken from the Column definition so no error is reported
        lake = Lake("LINESTRING(0 0,1 1)")
        session.add(lake)

        if dialect_name == "postgresql" and postgis_version < 3:
            with pytest.raises((DataError, IntegrityError)):
                session.flush()
        else:
            session.flush()

    def test_WKTElement(self, session, Lake, setup_tables, dialect_name):
        lake = Lake(WKTElement("LINESTRING(0 0,1 1)", srid=4326))
        session.add(lake)
        session.flush()
        session.expire(lake)
        assert isinstance(lake.geom, WKBElement)
        if dialect_name in ["mysql", "mariadb"]:
            # Not extended case
            assert str(lake.geom) == (
                "0102000000020000000000000000000000000000000000000000000"
                "0000000f03f000000000000f03f"
            )
        else:
            assert str(lake.geom) == (
                "0102000020e6100000020000000000000000000000000000000000000000000"
                "0000000f03f000000000000f03f"
            )
        wkt = session.execute(lake.geom.ST_AsText()).scalar()
        assert format_wkt(wkt) == "LINESTRING(0 0,1 1)"
        srid = session.execute(lake.geom.ST_SRID()).scalar()
        assert srid == 4326

    def test_WKBElement(self, session, Lake, setup_tables, dialect_name):
        shape = LineString([[0, 0], [1, 1]])
        lake = Lake(from_shape(shape, srid=4326))
        session.add(lake)
        session.flush()
        session.expire(lake)
        assert isinstance(lake.geom, WKBElement)
        if dialect_name in ["mysql", "mariadb"]:
            # Not extended case
            assert str(lake.geom) == (
                "0102000000020000000000000000000000000000000000000000000"
                "0000000f03f000000000000f03f"
            )
        else:
            assert str(lake.geom) == (
                "0102000020e6100000020000000000000000000000000000000000000000000"
                "0000000f03f000000000000f03f"
            )
        wkt = session.execute(lake.geom.ST_AsText()).scalar()
        assert format_wkt(wkt) == "LINESTRING(0 0,1 1)"
        srid = session.execute(lake.geom.ST_SRID()).scalar()
        assert srid == 4326

    @test_only_with_dialects("postgresql", "mysql", "sqlite-spatialite3", "sqlite-spatialite4")
    def test_transform(self, session, LocalPoint, setup_tables):
        if session.bind.dialect.name == "mysql":
            # Explicitly skip MySQL dialect to show that there is an issue
            pytest.skip(
                reason=(
                    "The SRID is not properly retrieved so an exception is raised. TODO: This "
                    "should be fixed later"
                )
            )
        # Create new point instance
        p = LocalPoint()
        p.geom = "SRID=4326;POINT(5 45)"  # Insert geometry with wrong SRID
        p.managed_geom = "SRID=4326;POINT(5 45)"  # Insert geometry with wrong SRID

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


class TestUpdateORM:
    def test_WKTElement(self, session, Lake, setup_tables, dialect_name):
        raw_wkt = "LINESTRING(0 0,1 1)"
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

        if dialect_name not in ["mysql", "mariadb"]:
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

    def test_WKBElement(self, session, Lake, setup_tables, dialect_name):
        shape = LineString([[0, 0], [1, 1]])
        initial_value = from_shape(shape, srid=4326)
        lake = Lake(initial_value)
        session.add(lake)

        # Insert in DB
        session.flush()

        # Check what was inserted in DB
        assert isinstance(lake.geom, WKBElement)
        wkt_query = lake.geom.ST_AsText()
        wkt = session.execute(wkt_query).scalar()
        assert format_wkt(wkt) == "LINESTRING(0 0,1 1)"
        srid = session.execute(lake.geom.ST_SRID()).scalar()
        assert srid == 4326

        if dialect_name not in ["mysql", "mariadb"]:
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
        assert format_wkt(wkt) == "LINESTRING(0 0,1 1)"
        srid = session.execute(lake.geom.ST_SRID()).scalar()
        assert srid == 4326

        session.refresh(lake)
        assert to_shape(lake.geom) == to_shape(initial_value)

    def test_other_type_fail(self, session, Lake, setup_tables, dialect_name):
        shape = LineString([[0, 0], [1, 1]])
        lake = Lake(from_shape(shape, srid=4326))
        session.add(lake)

        # Insert in DB
        session.flush()

        # Set geometry to 1, which is of wrong type
        lake.geom = 1

        # Update in DB
        if dialect_name == "postgresql":
            with pytest.raises(ProgrammingError):
                # Call __eq__() operator of _SpatialElement with 'other' argument equal to 1
                # so the lake instance is detected as different and is thus updated but with
                # an invalid geometry.
                session.flush()
        elif dialect_name in ["sqlite", "geopackage"]:
            # SQLite silently set the geom attribute to NULL
            session.flush()
            session.refresh(lake)
            assert lake.geom is None
        elif dialect_name in ["mysql", "mariadb"]:
            with pytest.raises(OperationalError):
                session.flush()
        else:
            raise ValueError(f"Unexpected dialect: {dialect_name}")


class TestCallFunction:
    @pytest.fixture
    def setup_one_lake(self, session, Lake, setup_tables):
        lake = Lake(WKTElement("LINESTRING(0 0,1 1)", srid=4326))
        session.add(lake)
        session.flush()
        session.expire(lake)
        return lake.id

    @pytest.fixture
    def setup_one_poi(self, session, Poi, setup_tables):
        p = Poi("POINT(5 45)")
        session.add(p)
        session.flush()
        session.expire(p)
        return p.id

    def test_ST_GeometryType(self, session, Lake, setup_one_lake, dialect_name):
        lake_id = setup_one_lake

        if dialect_name == "postgresql":
            expected_geometry_type = "ST_LineString"
        else:
            expected_geometry_type = "LINESTRING"

        s = select([func.ST_GeometryType(Lake.__table__.c.geom)])
        r1 = session.execute(s).scalar()
        assert r1 == expected_geometry_type

        lake = session.query(Lake).get(lake_id)
        r2 = session.execute(lake.geom.ST_GeometryType()).scalar()
        assert r2 == expected_geometry_type

        r3 = session.query(Lake.geom.ST_GeometryType()).scalar()
        assert r3 == expected_geometry_type

        r4 = session.query(Lake).filter(Lake.geom.ST_GeometryType() == expected_geometry_type).one()
        assert isinstance(r4, Lake)
        assert r4.id == lake_id

    @test_only_with_dialects("postgresql", "sqlite")
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

        r4 = (
            session.query(Lake)
            .filter(func.ST_Within(WKTElement("POINT(0 0)", srid=4326), Lake.geom.ST_Buffer(2)))
            .one()
        )
        assert isinstance(r4, Lake)
        assert r4.id == lake_id

    @test_only_with_dialects("postgresql", "sqlite")
    def test_ST_AsGeoJson(self, session, Lake, setup_one_lake):
        lake_id = setup_one_lake
        lake = session.query(Lake).get(lake_id)

        # Test geometry
        s1 = select([func.ST_AsGeoJSON(Lake.__table__.c.geom)])
        r1 = session.execute(s1).scalar()
        assert loads(r1) == {"type": "LineString", "coordinates": [[0, 0], [1, 1]]}

        # Test geometry ORM
        s1_orm = lake.geom.ST_AsGeoJSON()
        r1_orm = session.execute(s1_orm).scalar()
        assert loads(r1_orm) == {"type": "LineString", "coordinates": [[0, 0], [1, 1]]}

        # Test from WKTElement
        s1_wkt = WKTElement("LINESTRING(0 0,1 1)", srid=4326, extended=False).ST_AsGeoJSON()
        r1_wkt = session.execute(s1_wkt).scalar()
        assert loads(r1_wkt) == {"type": "LineString", "coordinates": [[0, 0], [1, 1]]}

        # Test from extended WKTElement
        s1_ewkt = WKTElement("SRID=4326;LINESTRING(0 0,1 1)", extended=True).ST_AsGeoJSON()
        r1_ewkt = session.execute(s1_ewkt).scalar()
        assert loads(r1_ewkt) == {"type": "LineString", "coordinates": [[0, 0], [1, 1]]}

        # Test with function inside
        s1_func = select(
            [func.ST_AsGeoJSON(func.ST_Translate(Lake.__table__.c.geom, 0.0, 0.0, 0.0))]
        )
        r1_func = session.execute(s1_func).scalar()
        assert loads(r1_func) == {"type": "LineString", "coordinates": [[0, 0], [1, 1]]}

    @skip_case_insensitivity()
    @test_only_with_dialects("postgresql", "mysql", "sqlite-spatialite3", "sqlite-spatialite4")
    def test_comparator_case_insensitivity(self, session, Lake, setup_one_lake):
        lake_id = setup_one_lake

        s = select([func.ST_Transform(Lake.__table__.c.geom, 2154)])
        r1 = session.execute(s).scalar()
        assert isinstance(r1, WKBElement)

        lake = session.query(Lake).get(lake_id)

        r2 = session.execute(lake.geom.ST_Transform(2154)).scalar()
        assert isinstance(r2, WKBElement)

        r3 = session.execute(lake.geom.st_transform(2154)).scalar()
        assert isinstance(r3, WKBElement)

        r4 = session.execute(lake.geom.St_TrAnSfOrM(2154)).scalar()
        assert isinstance(r4, WKBElement)

        r5 = session.query(Lake.geom.ST_Transform(2154)).scalar()
        assert isinstance(r5, WKBElement)

        r6 = session.query(Lake.geom.st_transform(2154)).scalar()
        assert isinstance(r6, WKBElement)

        r7 = session.query(Lake.geom.St_TrAnSfOrM(2154)).scalar()
        assert isinstance(r7, WKBElement)

        assert r1.data == r2.data == r3.data == r4.data == r5.data == r6.data == r7.data

    def test_unknown_function_column(self, session, Lake, setup_one_lake, dialect_name):
        s = select([func.ST_UnknownFunction(Lake.__table__.c.geom, 2)])
        exc = ProgrammingError if dialect_name == "postgresql" else OperationalError
        with pytest.raises(exc, match="ST_UnknownFunction"):
            session.execute(s)

    def test_unknown_function_element(self, session, Lake, setup_one_lake, dialect_name):
        lake_id = setup_one_lake
        lake = session.query(Lake).get(lake_id)

        s = select([func.ST_UnknownFunction(lake.geom, 2)])
        exc = ProgrammingError if dialect_name == "postgresql" else OperationalError
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


class TestShapely:
    def test_to_shape(self, session, Lake, setup_tables, dialect_name):
        if dialect_name in ["sqlite", "geopackage"]:
            data_type = str
        elif dialect_name in ["mysql", "mariadb"]:
            data_type = bytes
        else:
            data_type = memoryview

        lake = Lake(WKTElement("LINESTRING(0 0,1 1)", srid=4326))
        session.add(lake)
        session.flush()
        session.expire(lake)
        lake = session.query(Lake).one()
        assert isinstance(lake.geom, WKBElement)
        assert isinstance(lake.geom.data, data_type)
        assert lake.geom.srid == 4326
        s = to_shape(lake.geom)
        assert isinstance(s, LineString)
        assert s.wkt == "LINESTRING (0 0, 1 1)"
        lake = Lake(lake.geom)
        session.add(lake)
        session.flush()
        session.expire(lake)
        assert isinstance(lake.geom, WKBElement)
        assert isinstance(lake.geom.data, data_type)
        assert lake.geom.srid == 4326


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

    @test_only_with_dialects("postgresql", "sqlite-spatialite3", "sqlite-spatialite4")
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

    @test_only_with_dialects("postgresql", "sqlite")
    def test_reflection(self, conn, setup_reflection_tables, dialect_name):
        skip_pg12_sa1217(conn)
        t = Table(
            "lake",
            MetaData(),
            autoload_with=conn,
        )

        if dialect_name == "postgresql":
            # Check index query with explicit schema
            t_with_schema = Table("lake", MetaData(), autoload_with=conn, schema="gis")
            assert sorted([col.name for col in t.columns]) == sorted(
                [col.name for col in t_with_schema.columns]
            )
            assert sorted([idx.name for idx in t.indexes]) == sorted(
                [idx.name for idx in t_with_schema.indexes]
            )

        if get_postgis_major_version(conn) == 1:
            type_ = t.c.geom.type
            assert isinstance(type_, Geometry)
            assert type_.geometry_type == "GEOMETRY"
            assert type_.srid == -1
        else:
            type_ = t.c.geom.type
            assert isinstance(type_, Geometry)
            assert type_.geometry_type == "LINESTRING"
            assert type_.srid == 4326
            assert type_.dimension == 2

            if dialect_name != "geopackage":
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

        # Check the indexes
        check_indexes(
            conn,
            dialect_name,
            {
                "postgresql": [],
                "sqlite": [],
                "geopackage": [],
            },
            table_name=t.name,
        )

        # Recreate the table to check that the reflected properties are correct
        t.create(bind=conn)

        # Check the indexes
        if dialect_name in ["sqlite", "geopackage"]:
            if dialect_name == "geopackage":
                col_attributes = _get_spatialite_attrs_gpkg(conn, t.name, "geom")
            else:
                col_attributes = _get_spatialite_attrs_sqlite(conn, t.name, "geom")
            if isinstance(col_attributes[0], int):
                sqlite_indexes = [
                    ("lake", "geom", 2, 2, 4326, 1),
                    ("lake", "geom_m", 2002, 3, 4326, 1),
                    ("lake", "geom_no_idx", 2, 2, 4326, 0),
                    ("lake", "geom_z", 1002, 3, 4326, 1),
                    ("lake", "geom_zm", 3002, 4, 4326, 1),
                ]
            else:
                sqlite_indexes = [
                    ("lake", "geom", "LINESTRING", "XY", 4326, 1),
                    ("lake", "geom_m", "LINESTRING", "XYM", 4326, 1),
                    ("lake", "geom_no_idx", "LINESTRING", "XY", 4326, 0),
                    ("lake", "geom_z", "LINESTRING", "XYZ", 4326, 1),
                    ("lake", "geom_zm", "LINESTRING", "XYZM", 4326, 1),
                ]
        else:
            sqlite_indexes = []
        check_indexes(
            conn,
            dialect_name,
            {
                "postgresql": [
                    (
                        "idx_lake_geom",
                        "CREATE INDEX idx_lake_geom ON gis.lake USING gist (geom)",
                    ),
                    (
                        "idx_lake_geom_m",
                        "CREATE INDEX idx_lake_geom_m ON gis.lake USING gist (geom_m)",
                    ),
                    (
                        "idx_lake_geom_z",
                        "CREATE INDEX idx_lake_geom_z ON gis.lake USING gist (geom_z)",
                    ),
                    (
                        "idx_lake_geom_zm",
                        "CREATE INDEX idx_lake_geom_zm ON gis.lake USING gist (geom_zm)",
                    ),
                    (
                        "lake_pkey",
                        "CREATE UNIQUE INDEX lake_pkey ON gis.lake USING btree (id)",
                    ),
                ],
                "sqlite": sqlite_indexes,
                "geopackage": [("lake", "geom", "gpkg_rtree_index")],
            },
            table_name=t.name,
        )

    @test_only_with_dialects("postgresql", "sqlite")
    def test_raster_reflection(self, conn, Ocean, setup_tables):
        skip_pg12_sa1217(conn)
        skip_postgis1(conn)
        if SQLA_LT_2:
            with pytest.warns(SAWarning):
                t = Table("ocean", MetaData(), autoload_with=conn)
        else:
            t = Table("ocean", MetaData(), autoload_with=conn)
        type_ = t.c.rast.type
        assert isinstance(type_, Raster)

    @test_only_with_dialects("sqlite")
    def test_sqlite_reflection_with_discarded_col(self, conn, Lake, setup_tables, dialect_name):
        """Test that a discarded geometry column is not properly reflected with SQLite."""
        if dialect_name == "geopackage":
            conn.execute(text("""DELETE FROM "gpkg_geometry_columns" WHERE table_name = 'lake';"""))
        else:
            conn.execute(text("""DELETE FROM "geometry_columns" WHERE f_table_name = 'lake';"""))
        t = Table(
            "lake",
            MetaData(),
            autoload_with=conn,
        )

        # In this case the reflected type is generic with default values
        assert t.c.geom.type.geometry_type == "GEOMETRY"
        assert t.c.geom.type.dimension == 2
        assert t.c.geom.type.extended
        assert t.c.geom.type.nullable
        assert t.c.geom.type.spatial_index
        assert t.c.geom.type.srid == -1

    @pytest.fixture
    def ocean_view(self, conn, Ocean):
        conn.execute(text("CREATE VIEW test_view AS SELECT * FROM ocean;"))
        yield Ocean
        conn.execute(text("DROP VIEW test_view;"))

    @test_only_with_dialects("postgresql", "sqlite")
    def test_view_reflection(self, conn, Ocean, setup_tables, ocean_view):
        """Test reflection of a view.

        Note: the reflected `Table` object has spatial indexes attached. It would be nice to detect
        when a view is reflected to not attach any spatial index.
        """
        skip_pg12_sa1217(conn)
        skip_postgis1(conn)
        t = Table("test_view", MetaData(), autoload_with=conn)
        type_ = t.c.rast.type
        assert isinstance(type_, Raster)


class TestToMetadata(ComparesTables):
    def test_to_metadata(self, Lake):
        new_meta = MetaData()
        new_Lake = Lake.__table__.to_metadata(new_meta)

        self.assert_tables_equal(Lake.__table__, new_Lake)

        # Check that the spatial index was not duplicated
        assert len(new_Lake.indexes) == 1


class TestAsBinaryWKT:
    def test_create_insert(self, conn, dialect_name):
        class GeometryWkt(Geometry):
            """Geometry type that uses WKT strings."""

            from_text = "ST_GeomFromEWKT"
            as_binary = "ST_AsText"
            ElementType = WKTElement

        dialects_with_srid = ["geopackage", "mysql", "mariadb"]

        # Define the table
        cols = [
            Column("id", Integer, primary_key=True),
        ]
        cols.append(Column("geom_with_srid", GeometryWkt(geometry_type="LINESTRING", srid=4326)))
        if dialect_name not in dialects_with_srid:
            cols.append(Column("geom", GeometryWkt(geometry_type="LINESTRING")))
        t = Table("use_wkt", MetaData(), *cols)

        # Create the table
        t.drop(bind=conn, checkfirst=True)
        t.create(bind=conn)

        # Test element insertion
        inserted_values = [
            {"geom_with_srid": v}
            for v in [
                "SRID=4326;LINESTRING(0 0,1 1)",
                WKTElement("LINESTRING(0 0,2 2)", srid=4326),
                WKTElement("SRID=4326;LINESTRING(0 0,3 3)", extended=True),
                from_shape(LineString([[0, 0], [4, 4]]), srid=4326),
            ]
        ]
        if dialect_name not in dialects_with_srid:
            for i, v in zip(
                inserted_values,
                [
                    "LINESTRING(0 0,1 1)",
                    WKTElement("LINESTRING(0 0,2 2)"),
                    WKTElement("SRID=-1;LINESTRING(0 0,3 3)", extended=True),
                    from_shape(LineString([[0, 0], [4, 4]])),
                ],
            ):
                i["geom"] = v

        conn.execute(t.insert(), inserted_values)

        results = conn.execute(t.select())
        rows = results.fetchall()

        for row_num, row in enumerate(rows):
            for num, element in enumerate(row[1:]):
                assert isinstance(element, WKTElement)
                wkt = conn.execute(element.ST_AsText()).scalar()
                assert format_wkt(wkt) == f"LINESTRING(0 0,{row_num + 1} {row_num + 1})"
                srid = conn.execute(element.ST_SRID()).scalar()
                if num == 1:
                    assert srid == 0 if dialect_name != "sqlite" else -1
                else:
                    assert srid == 4326

        # Drop the table
        t.drop(bind=conn)
