# Write the benchmarking functions here.
# See "Writing benchmarks" in the asv docs for more information.

import os

import shapely
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base

from geoalchemy2 import Geometry
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import to_shape

DB_URL = os.getenv("BENCHMARK_POSTGRESQL_DB_URL") or "postgresql://gis:gis@localhost/gis"


def create_points(N=50, convert_wkb=False):
    """Create a list of points for benchmarking."""
    points = []
    for i in range(N):
        for j in range(N):
            for k in range(N):
                wkt = f"POINT ZM({i} {j} {k} {i + j + k})"
                points.append(wkt)
    if convert_wkb:
        # Convert WKT to WKB
        points = [shapely.io.to_wkb(to_shape(WKTElement(point)), flavor="iso") for point in points]
    return points


def insert_all_points(conn, table, points):
    """Insert all points into the database."""
    query = table.insert().values(
        [
            {
                "geom": point,
            }
            for point in points
        ]
    )
    return conn.execute(query)


class TimeInsertSuite:
    """Benchmark insertion."""

    params = [2, 10, 50]

    def setup(self, N):
        current_engine = create_engine(DB_URL, plugins=["geoalchemy2"])
        current_engine.update_execution_options(search_path=["gis", "public"])
        self.engine = current_engine
        self.conn = current_engine.connect()
        self.metadata = MetaData()
        self.base = declarative_base(metadata=self.metadata)

        class WktTable(self.base):
            __tablename__ = "wkt_table"
            __table_args__ = {"schema": "gis"}
            id = Column(Integer, primary_key=True)
            geom = Column(Geometry(geometry_type="POINTZM", from_text="ST_GeomFromEWKT", srid=4326))

            def __init__(self, geom):
                self.geom = geom

        self.WktTable = WktTable

        class WkbTable(self.base):
            __tablename__ = "wkb_table"
            __table_args__ = {"schema": "gis"}
            id = Column(Integer, primary_key=True)
            geom = Column(Geometry(geometry_type="POINTZM", from_text="ST_GeomFromWKB", srid=4326))

            def __init__(self, geom):
                self.geom = geom

        self.WkbTable = WkbTable

        # Create the table in the database
        self.metadata.drop_all(self.conn, checkfirst=True)
        self.metadata.create_all(self.conn)

        self.wkt_points = create_points(N, convert_wkb=False)
        self.wkb_points = create_points(N, convert_wkb=True)

    def time_insert_wkt(self, N):
        table = self.WktTable.__table__
        insert_all_points(self.conn, table, self.wkt_points)

    def time_insert_wkb(self, N):
        table = self.WkbTable.__table__
        insert_all_points(self.conn, table, self.wkb_points)

    def teardown(self, N):
        self.metadata.drop_all(self.conn, checkfirst=True)
        self.conn.close()
        self.engine.dispose()
