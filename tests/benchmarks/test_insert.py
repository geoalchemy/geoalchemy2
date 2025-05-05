import pytest
import shapely
from sqlalchemy import Column
from sqlalchemy import Integer

from geoalchemy2 import Geometry
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import to_shape


@pytest.fixture
def WktTable(base, schema):
    class WktTable(base):
        __tablename__ = "wkt_table"
        __table_args__ = {"schema": schema}
        id = Column(Integer, primary_key=True)
        geom = Column(Geometry(geometry_type="POINTZM", from_text="ST_GeomFromEWKT", srid=4326))

        def __init__(self, geom):
            self.geom = geom

    return WktTable


@pytest.fixture
def WkbTable(base, schema):
    class WkbTable(base):
        __tablename__ = "wkb_table"
        __table_args__ = {"schema": schema}
        id = Column(Integer, primary_key=True)
        geom = Column(Geometry(geometry_type="POINTZM", from_text="ST_GeomFromWKB", srid=4326))

        def __init__(self, geom):
            self.geom = geom

    return WkbTable


def create_points(N=50):
    """Create a list of points for benchmarking."""
    points = []
    for i in range(N):
        for j in range(N):
            for k in range(N):
                wkt = f"POINT({i} {j} {k} {i + j + k})"
                points.append(wkt)
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


def _benchmark_insert(conn, table_class, metadata, benchmark, convert_wkb=False, N=50):
    """Benchmark the insert operation."""
    print(f"Benchmarking {table_class.__tablename__} insert operation")

    # Create the points to insert
    points = create_points(N)
    print(f"Number of points to insert: {len(points)}")

    if convert_wkb:
        # Convert WKT to WKB
        points = [shapely.io.to_wkb(to_shape(WKTElement(point)), flavor="iso") for point in points]
        print(f"Converted points to WKB: {len(points)}")

    # Create the table in the database
    metadata.drop_all(conn, checkfirst=True)
    metadata.create_all(conn)
    print(f"Table {table_class.__tablename__} created")

    table = table_class.__table__
    return benchmark.pedantic(insert_all_points, args=(conn, table, points), iterations=1, rounds=1)


def test_insert_wkt(benchmark, WktTable, conn, metadata):
    """Benchmark the insert operation for WKT."""
    N = 10

    _benchmark_insert(conn, WktTable, metadata, benchmark, N=N)

    assert (
        conn.execute(
            WktTable.__table__.select().where(WktTable.__table__.c.geom.is_not(None))
        ).rowcount
        == N * N * N
    )


def test_insert_wkb(benchmark, WkbTable, conn, metadata):
    """Benchmark the insert operation for WKB."""
    N = 10

    _benchmark_insert(conn, WkbTable, metadata, benchmark, convert_wkb=True, N=N)

    assert (
        conn.execute(
            WkbTable.__table__.select().where(WkbTable.__table__.c.geom.is_not(None))
        ).rowcount
        == N * N * N
    )
