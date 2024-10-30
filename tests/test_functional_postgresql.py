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
from shapely.geometry import Point
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import __version__ as SA_VERSION
from sqlalchemy import bindparam
from sqlalchemy import text
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.exc import InternalError
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import type_coerce

from geoalchemy2 import Geography
from geoalchemy2 import Geometry
from geoalchemy2 import Raster
from geoalchemy2.elements import RasterElement
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.exc import ArgumentError
from geoalchemy2.shape import from_shape

from . import select
from . import skip_pg12_sa1217
from . import skip_postgis1
from . import skip_postgis2

SQLA_LT_2 = parse_version(SA_VERSION) <= parse_version("1.4")
if SQLA_LT_2:
    from sqlalchemy.engine.reflection import Inspector

    get_inspector = Inspector.from_engine
else:
    from sqlalchemy import inspect as get_inspector  # type: ignore


class TestIndex:
    def test_index_with_schema(self, conn, IndexTestWithSchema, setup_tables):
        inspector = get_inspector(conn)
        indices = inspector.get_indexes(IndexTestWithSchema.__tablename__, schema="gis")
        assert len(indices) == 2
        assert not indices[0].get("unique")
        assert indices[0].get("column_names")[0] in ("geom1", "geom2")
        assert not indices[1].get("unique")
        assert indices[1].get("column_names")[0] in ("geom1", "geom2")

    def test_n_d_index(self, conn, IndexTestWithNDIndex, setup_tables):
        sql = text(
            """SELECT
                    tablename,
                    indexname,
                    indexdef
                FROM
                    pg_indexes
                WHERE
                    tablename = 'index_test_with_nd_index'
                ORDER BY
                    tablename,
                    indexname"""
        )
        r = conn.execute(sql)
        results = r.fetchall()

        for index in results:
            if "geom1" in index[1]:
                nd_index = index[2]

        index_type = nd_index.split("USING ", 1)[1]
        assert index_type == "gist (geom1 gist_geometry_ops_nd)"

        inspector = get_inspector(conn)

        indices = inspector.get_indexes(IndexTestWithNDIndex.__tablename__)
        assert len(indices) == 1
        assert not indices[0].get("unique")
        assert indices[0].get("column_names")[0] in ("geom1")

    def test_n_d_index_argument_error(self):
        BaseArgTest = declarative_base(metadata=MetaData())

        with pytest.raises(ArgumentError) as excinfo:

            class NDIndexArgErrorSchema(BaseArgTest):
                __tablename__ = "nd_index_error_arg"
                __table_args__ = {"schema": "gis"}
                id = Column(Integer, primary_key=True)
                geom1 = Column(
                    Geometry(
                        geometry_type="POINTZ",
                        dimension=3,
                        spatial_index=False,
                        use_N_D_index=True,
                    )
                )

        assert "Arg Error(use_N_D_index): spatial_index must be True" == excinfo.value.args[0]

    def test_index_without_schema(self, conn, IndexTestWithoutSchema, setup_tables):
        inspector = get_inspector(conn)
        indices = inspector.get_indexes(IndexTestWithoutSchema.__tablename__)
        assert len(indices) == 2
        assert not indices[0].get("unique")
        assert indices[0].get("column_names")[0] in ("geom1", "geom2")
        assert not indices[1].get("unique")
        assert indices[1].get("column_names")[0] in ("geom1", "geom2")

    def test_type_decorator_index(self, conn, PointZ, setup_tables):
        inspector = get_inspector(conn)
        indices = inspector.get_indexes(PointZ.__tablename__)
        assert len(indices) == 1
        assert not indices[0].get("unique")
        assert indices[0].get("column_names") == ["three_d_geom"]

    def test_all_indexes(self, conn):
        BaseArgTest = declarative_base(metadata=MetaData())

        class TableWithIndexes(BaseArgTest):
            __tablename__ = "table_with_indexes"
            __table_args__ = {"schema": "gis"}
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
            # Test indexes on Geometry columns with ND index.
            geom_not_managed_nd_index = Column(
                Geometry(
                    geometry_type="POINTZ",
                    dimension=3,
                    spatial_index=True,
                    use_N_D_index=True,
                )
            )
            geom_managed_nd_index = Column(
                Geometry(
                    geometry_type="POINTZ",
                    dimension=3,
                    spatial_index=True,
                    use_N_D_index=True,
                )
            )
            # Test indexes on Geography columns.
            geog_not_managed_no_index = Column(
                Geography(
                    geometry_type="POINT",
                    spatial_index=False,
                )
            )
            geog_not_managed_index = Column(
                Geography(
                    geometry_type="POINT",
                    spatial_index=True,
                )
            )
            geog_managed_no_index = Column(
                Geography(
                    geometry_type="POINT",
                    spatial_index=False,
                )
            )
            geog_managed_index = Column(
                Geography(
                    geometry_type="POINT",
                    spatial_index=True,
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
                USING gist (geog_managed_index)""",
            ),
            (
                "idx_table_with_indexes_geog_not_managed_index",
                """CREATE INDEX idx_table_with_indexes_geog_not_managed_index
                ON gis.table_with_indexes
                USING gist (geog_not_managed_index)""",
            ),
            (
                "idx_table_with_indexes_geom_managed_index",
                """CREATE INDEX idx_table_with_indexes_geom_managed_index
                ON gis.table_with_indexes
                USING gist (geom_managed_index)""",
            ),
            (
                "idx_table_with_indexes_geom_managed_nd_index",
                """CREATE INDEX idx_table_with_indexes_geom_managed_nd_index
                ON gis.table_with_indexes
                USING gist (geom_managed_nd_index gist_geometry_ops_nd)""",
            ),
            (
                "idx_table_with_indexes_geom_not_managed_index",
                """CREATE INDEX idx_table_with_indexes_geom_not_managed_index
                ON gis.table_with_indexes
                USING gist (geom_not_managed_index)""",
            ),
            (
                "idx_table_with_indexes_geom_not_managed_nd_index",
                """CREATE INDEX idx_table_with_indexes_geom_not_managed_nd_index
                ON gis.table_with_indexes
                USING gist (geom_not_managed_nd_index gist_geometry_ops_nd)""",
            ),
            (
                "idx_table_with_indexes_rast_not_managed_index",
                """CREATE INDEX idx_table_with_indexes_rast_not_managed_index
                ON gis.table_with_indexes
                USING gist (st_convexhull(rast_not_managed_index))""",
            ),
            (
                "table_with_indexes_pkey",
                """CREATE UNIQUE INDEX table_with_indexes_pkey
                ON gis.table_with_indexes
                USING btree (id)""",
            ),
        ]

        assert len(indices) == 8

        for idx, expected_idx in zip(indices, expected_indices):
            assert idx[0] == expected_idx[0]
            assert idx[1] == re.sub("\n *", " ", expected_idx[1])


class TestInsertionCore:
    def test_insert_geog_poi(self, conn, Poi, setup_tables):
        conn.execute(
            Poi.__table__.insert(),
            [
                {"geog": "SRID=4326;POINT(1 1)"},
                {"geog": WKTElement("POINT(1 1)", srid=4326)},
                {"geog": WKTElement("SRID=4326;POINT(1 1)", extended=True)},
                {"geog": from_shape(Point(1, 1), srid=4326)},
            ],
        )

        results = conn.execute(Poi.__table__.select())
        rows = results.fetchall()

        for row in rows:
            assert isinstance(row[2], WKBElement)
            wkt = conn.execute(row[2].ST_AsText()).scalar()
            assert wkt == "POINT(1 1)"
            srid = conn.execute(row[2].ST_SRID()).scalar()
            assert srid == 4326
            assert row[2] == from_shape(Point(1, 1), srid=4326)


class TestInsertionORM:
    def test_Raster(self, session, Ocean, setup_tables):
        skip_postgis1(session)
        polygon = WKTElement("POLYGON((0 0,1 1,0 1,0 0))", srid=4326)
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
        top_left_point = WKTElement("Point(0 1)", srid=4326)
        top_left = session.execute(o.rast.ST_Value(top_left_point)).scalar()
        assert top_left == 1

        # The bottom right corner has NODATA
        bottom_right_point = WKTElement("Point(1 0)", srid=4326)
        bottom_right = session.execute(o.rast.ST_Value(bottom_right_point)).scalar()
        assert bottom_right is None


class TestUpdateORM:
    def test_Raster(self, session, Ocean, setup_tables):
        skip_postgis1(session)
        polygon = WKTElement("POLYGON((0 0,1 1,0 1,0 0))", srid=4326)
        o = Ocean(polygon.ST_AsRaster(5, 5))
        session.add(o)
        session.flush()
        session.expire(o)

        assert isinstance(o.rast, RasterElement)

        rast_data = (
            "01000001009A9999999999C93F9A9999999999C9BF0000000000000000000000000000F03"
            "F00000000000000000000000000000000E610000005000500440001010101010101010100"
            "010101000001010000000100000000"
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
        assert session.execute(select(cols)).fetchall() == [(1, None)]

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


class TestTypMod:
    def test_SummitConstraints(self, conn, Summit, setup_tables):
        """Make sure the geometry column of table Summit is created with
        `use_typmod=False` (explicit constraints are created).
        """
        skip_pg12_sa1217(conn)
        inspector = get_inspector(conn)
        constraints = inspector.get_check_constraints(Summit.__tablename__, schema="gis")
        assert len(constraints) == 3

        constraint_names = {c["name"] for c in constraints}
        assert "enforce_srid_geom" in constraint_names
        assert "enforce_dims_geom" in constraint_names
        assert "enforce_geotype_geom" in constraint_names


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

        q = session.query(dump.path.label("path"), dump.geom.label("geom")).all()
        assert len(q) == 2

        p1 = q[0]
        assert isinstance(p1.path, list)
        assert p1.path == [1]
        assert isinstance(p1.geom, WKBElement)
        p1 = session.execute(func.ST_AsText(p1.geom)).scalar()
        assert p1 == "POINT(0 0)"

        p2 = q[1]
        assert isinstance(p2.path, list)
        assert p2.path == [2]
        assert isinstance(p2.geom, WKBElement)
        p2 = session.execute(func.ST_AsText(p2.geom)).scalar()
        assert p2 == "POINT(1 1)"

    def test_ST_Buffer_Mixed_SRID(self, session, Lake, setup_one_lake):
        with pytest.raises(InternalError):
            session.query(Lake).filter(func.ST_Within("POINT(0 0)", Lake.geom.ST_Buffer(2))).one()

    def test_ST_Distance_type_coerce(self, session, Poi, setup_one_poi):
        poi_id = setup_one_poi
        poi = (
            session.query(Poi)
            .filter(Poi.geog.ST_Distance(type_coerce("POINT(5 45)", Geography)) < 1000)
            .one()
        )
        assert poi.id == poi_id

    def test_ST_AsGeoJson_feature(self, session, Lake, setup_one_lake):
        skip_postgis1(session)
        skip_postgis2(session)
        # Test feature
        s2 = select([func.ST_AsGeoJSON(Lake, "geom")])
        r2 = session.execute(s2).scalar()
        assert loads(r2) == {
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": [[0, 0], [1, 1]]},
            "properties": {"id": 1},
        }

        # Test feature with subquery
        ss3 = select([Lake, bindparam("dummy_val", 10).label("dummy_attr")]).alias()
        s3 = select([func.ST_AsGeoJSON(ss3, "geom")])
        r3 = session.execute(s3).scalar()
        assert loads(r3) == {
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": [[0, 0], [1, 1]]},
            "properties": {"dummy_attr": 10, "id": 1},
        }

    @pytest.mark.parametrize(
        "compared_element,expected_assert",
        [
            pytest.param("LINESTRING(0 1, 1 0)", True, id="intersecting raw string WKT"),
            pytest.param("LINESTRING(99 99, 999 999)", False, id="not intersecting raw string WKT"),
            pytest.param(WKTElement("LINESTRING(0 1, 1 0)"), True, id="intersecting WKTElement"),
            pytest.param(
                WKTElement("LINESTRING(99 99, 999 999)"), False, id="not intersecting WKTElement"
            ),
            pytest.param(
                WKTElement("SRID=2154;LINESTRING(0 1, 1 0)"),
                True,
                id="intersecting extended WKTElement",
            ),
            pytest.param(
                WKTElement("SRID=2154;LINESTRING(99 99, 999 999)"),
                False,
                id="not intersecting extended WKTElement",
            ),
            pytest.param(
                WKBElement(
                    "0102000000020000000000000000000000000000000000F03F000000000000F03F00000000000"
                    "00000"
                ),
                True,
                id="intersecting WKBElement",
            ),
            pytest.param(
                WKBElement(
                    "0102000000020000000000000000C058400000000000C058400000000000388F4000000000003"
                    "88F40"
                ),
                False,
                id="not intersecting WKBElement",
            ),
            pytest.param(
                WKBElement(
                    "01020000206A080000020000000000000000000000000000000000F03F000000000000F03F000"
                    "0000000000000"
                ),
                True,
                id="intersecting extended WKBElement",
            ),
            pytest.param(
                WKBElement(
                    "01020000206A080000020000000000000000C058400000000000C058400000000000388F40000"
                    "0000000388F40"
                ),
                False,
                id="not intersecting extended WKBElement",
            ),
        ],
    )
    def test_comparator(self, session, Lake, setup_one_lake, compared_element, expected_assert):
        query = Lake.__table__.select().where(Lake.__table__.c.geom.intersects(compared_element))
        res = session.execute(query).fetchall()

        assert bool(res) == expected_assert


class TestShapely:
    pass


class TestSTAsGeoJson:
    InternalBase = declarative_base()

    class TblWSpacesAndDots(InternalBase):  # type: ignore
        """
        Dummy class to test names with dots and spaces.
        No metadata is attached so the dialect is default SQL, not postgresql.
        """

        __tablename__ = "this is.an AWFUL.name"
        __table_args__ = {"schema": "another AWFUL.name for.schema"}

        id = Column(Integer, primary_key=True)
        geom = Column(String)

    @staticmethod
    def _assert_stmt(stmt, expected):
        strstmt = str(stmt.compile(dialect=PGDialect_psycopg2()))
        strstmt = strstmt.replace("\n", "")
        assert strstmt == expected

    def test_column(self, Lake):
        stmt = select([func.ST_AsGeoJSON(Lake.__table__.c.geom)])

        self._assert_stmt(
            stmt, 'SELECT ST_AsGeoJSON(gis.lake.geom) AS "ST_AsGeoJSON_1" FROM gis.lake'
        )

    def test_table_col(self, Lake):
        stmt = select([func.ST_AsGeoJSON(Lake, "geom")])
        self._assert_stmt(
            stmt,
            "SELECT ST_AsGeoJSON(lake, %(ST_AsGeoJSON_2)s) AS " '"ST_AsGeoJSON_1" FROM gis.lake',
        )

    def test_subquery(self, Lake):
        sq = select([Lake, bindparam("dummy_val", 10).label("dummy_attr")]).alias()
        stmt = select([func.ST_AsGeoJSON(sq, "geom")])
        self._assert_stmt(
            stmt,
            'SELECT ST_AsGeoJSON(anon_1, %(ST_AsGeoJSON_2)s) AS "ST_AsGeoJSON_1" '
            "FROM (SELECT gis.lake.id AS id, gis.lake.geom AS geom, %(dummy_val)s AS "
            "dummy_attr FROM gis.lake) AS anon_1",
        )

    def test_quotes(self):
        stmt = select([func.ST_AsGeoJSON(TestSTAsGeoJson.TblWSpacesAndDots, "geom")])
        self._assert_stmt(
            stmt,
            'SELECT ST_AsGeoJSON("this is.an AWFUL.name", %(ST_AsGeoJSON_2)s) '
            'AS "ST_AsGeoJSON_1" FROM "another AWFUL.name for.schema".'
            '"this is.an AWFUL.name"',
        )

    def test_quotes_and_param(self):
        stmt = select([func.ST_AsGeoJSON(TestSTAsGeoJson.TblWSpacesAndDots, "geom", 3)])
        self._assert_stmt(
            stmt,
            'SELECT ST_AsGeoJSON("this is.an AWFUL.name", '
            "%(ST_AsGeoJSON_2)s, %(ST_AsGeoJSON_3)s) "
            'AS "ST_AsGeoJSON_1" FROM "another AWFUL.name for.schema".'
            '"this is.an AWFUL.name"',
        )

    def test_nested_funcs(self):
        stmt = select([func.ST_AsGeoJSON(func.ST_MakeValid(func.ST_MakePoint(1, 2)))])
        self._assert_stmt(
            stmt,
            "SELECT "
            "ST_AsGeoJSON(ST_MakeValid("
            "ST_MakePoint(%(ST_MakePoint_1)s, %(ST_MakePoint_2)s)"
            ')) AS "ST_AsGeoJSON_1"',
        )

    def test_unknown_func(self):
        stmt = select([func.ST_AsGeoJSON(func.ST_UnknownFunction(func.ST_MakePoint(1, 2)))])
        self._assert_stmt(
            stmt,
            "SELECT "
            "ST_AsGeoJSON(ST_UnknownFunction("
            "ST_MakePoint(%(ST_MakePoint_1)s, %(ST_MakePoint_2)s)"
            ')) AS "ST_AsGeoJSON_1"',
        )


class TestSTSummaryStatsAgg:
    def test_st_summary_stats_agg(self, session, Ocean, setup_tables):
        # Create a new raster
        polygon = WKTElement("POLYGON((0 0,1 1,0 1,0 0))", srid=4326)
        o = Ocean(polygon.ST_AsRaster(5, 6))
        session.add(o)
        session.flush()

        # Define the query to compute stats
        stats_agg = select(
            [func.ST_SummaryStatsAgg(Ocean.__table__.c.rast, 1, True, 1).label("stats")]
        )
        stats_agg_alias = stats_agg.alias("stats_agg")

        # Use these stats
        query = select(
            [
                stats_agg_alias.c.stats.count.label("count"),
                stats_agg_alias.c.stats.sum.label("sum"),
                stats_agg_alias.c.stats.stddev.label("stddev"),
                stats_agg_alias.c.stats.min.label("min"),
                stats_agg_alias.c.stats.max.label("max"),
            ]
        )

        # Check the query
        assert str(query.compile(dialect=PGDialect_psycopg2())) == (
            "SELECT "
            "(stats_agg.stats).count AS count, "
            "(stats_agg.stats).sum AS sum, "
            "(stats_agg.stats).stddev AS stddev, "
            "(stats_agg.stats).min AS min, "
            "(stats_agg.stats).max AS max \n"
            "FROM ("
            "SELECT "
            "ST_SummaryStatsAgg("
            "ocean.rast, "
            "%(ST_SummaryStatsAgg_1)s, %(ST_SummaryStatsAgg_2)s, %(ST_SummaryStatsAgg_3)s"
            ") AS stats \n"
            "FROM ocean) AS stats_agg"
        )

        # Execute the query
        res = session.execute(query).fetchall()

        # Check the result
        assert res == [(15, 15.0, 0.0, 1.0, 1.0)]
