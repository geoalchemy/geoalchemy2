import pytest
import shapely
from shapely.wkb import dumps
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy.exc import OperationalError

from geoalchemy2 import Geometry
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import to_shape

from .. import select

ROUNDS = 5


class SuccessfulTest(BaseException):
    """A custom exception used to mark the successful test."""


@pytest.fixture(
    params=[pytest.param(True, id="Default geom type"), pytest.param(False, id="Custom geom type")]
)
def is_default_geom_type(request):
    """Fixture to determine if the test is for raw inputs or not."""
    return request.param


@pytest.fixture(
    params=[pytest.param(True, id="Raw input"), pytest.param(False, id="Not raw input")]
)
def is_raw_input(request):
    """Fixture to determine if the test is for raw inputs or not."""
    return request.param


@pytest.fixture(
    params=[pytest.param(True, id="Extended input"), pytest.param(False, id="Not extended input")]
)
def is_extended_input(request):
    """Fixture to determine if the test is for extended inputs or not."""
    return request.param


@pytest.fixture(
    params=[pytest.param(True, id="Extended output"), pytest.param(False, id="Not extended output")]
)
def is_extended_output(request):
    """Fixture to determine if the test is for extended outputs or not."""
    return request.param


@pytest.fixture(
    params=[
        pytest.param("WKT input"),
        pytest.param("WKB input"),
    ]
)
def input_representation(request):
    """Fixture to determine the representation type of inputs."""
    return request.param


@pytest.fixture(
    params=[
        pytest.param("WKT output"),
        pytest.param("WKB output"),
    ]
)
def output_representation(request):
    """Fixture to determine the representation type of outputs."""
    return request.param


@pytest.fixture
def GeomTable(
    base,
    schema,
    is_extended_input,
    is_extended_output,
    input_representation,
    output_representation,
    is_default_geom_type,
):
    print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    print("GeomTable fixture")
    print("is_extended_input:", is_extended_input)
    print("is_extended_output:", is_extended_output)
    print("input_representation:", input_representation)
    print("output_representation:", output_representation)
    print("is_default_geom_type:", is_default_geom_type)
    print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    if input_representation == "WKB":
        from_text_func = "ST_GeomFromEWKB" if is_extended_input else "ST_GeomFromWKB"
    else:
        from_text_func = "ST_GeomFromEWKT" if is_extended_input else "ST_GeomFromText"
    if output_representation == "WKB":
        to_text_func = "ST_AsEWKB" if is_extended_output else "ST_AsBinary"
        ElementType_cls = WKBElement
    else:
        to_text_func = "ST_AsEWKT" if is_extended_output else "ST_AsText"
        ElementType_cls = WKTElement

    if is_default_geom_type:
        CustomGeometry = Geometry
    else:

        class CustomGeometry(Geometry):
            """Custom Geometry class to handle different input/output representations."""

            name = "geometry"
            from_text = from_text_func
            as_binary = to_text_func
            ElementType = ElementType_cls
            cache_ok = True

    class GeomTable(base):
        __tablename__ = "geom_table"
        __table_args__ = {"schema": schema}
        id = Column(Integer, primary_key=True)
        geom = Column(CustomGeometry(geometry_type="POINT", srid=4326))

        def __init__(self, geom):
            self.geom = geom

    return GeomTable


def create_points(N=50):
    """Create a list of points for benchmarking."""
    points = []
    for i in range(N):
        for j in range(N):
            wkt = f"POINT({i / N} {j / N})"
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


def select_all_points(conn, table):
    """Select all points from the database."""
    query = table.select()
    return conn.execute(query).fetchall()


def insert_and_select_all_points(conn, table, points):
    """Insert all points into the database and select them."""
    insert_all_points(conn, table, points)
    return select_all_points(conn, table)


def _benchmark_setup(
    conn, table_class, metadata, convert_wkb=False, extended=False, raw=False, N=50
):
    """Setup the database for benchmarking."""
    # Create the points to insert
    points = create_points(N)
    print(f"Number of points to insert: {len(points)}")

    if convert_wkb:
        if not extended:
            # Convert WKT to WKB
            points = [
                shapely.io.to_wkb(to_shape(WKTElement(point)), flavor="iso") for point in points
            ]
            print(f"Converted points to WKB: {len(points)}")
        else:
            # Convert WKT to EWKB
            points = [
                dumps(to_shape(WKTElement(point)), flavor="extended", srid=4326) for point in points
            ]
            print(f"Converted points to EWKB: {len(points)}")
        if not raw:
            # Convert WKB string to WKBElement
            points = [WKBElement(point) for point in points]
            print(f"Converted points to WKBElement: {len(points)}")
    else:
        if extended:
            # Convert WKT to EWKT
            points = ["SRID=4326; " + point for point in points]
        if not raw:
            # Convert WKT to WKTElement
            points = [WKTElement(point) for point in points]
            print(f"Converted points to WKTElement: {len(points)}")

    if raw:
        print("Example data:", points[0])
    else:
        print("Example data:", points[0], "=>", points[0].data)

    # Create the table in the database
    metadata.drop_all(conn, checkfirst=True)
    metadata.create_all(conn)
    print(f"Table {table_class.__tablename__} created")

    return points


def _benchmark_insert(
    conn,
    table_class,
    metadata,
    benchmark,
    convert_wkb=False,
    raw_input=False,
    extended_input=False,
    N=50,
    rounds=5,
):
    """Benchmark the insert operation."""
    points = _benchmark_setup(
        conn,
        table_class,
        metadata,
        convert_wkb=convert_wkb,
        raw=raw_input,
        extended=extended_input,
        N=N,
    )

    table = table_class.__table__
    return benchmark.pedantic(
        insert_all_points, args=(conn, table, points), iterations=1, rounds=rounds
    )


def _benchmark_insert_select(
    conn,
    table_class,
    metadata,
    benchmark,
    convert_wkb=False,
    raw_input=False,
    extended_input=False,
    N=50,
    rounds=5,
):
    """Benchmark the insert and select operations."""
    points = _benchmark_setup(
        conn,
        table_class,
        metadata,
        convert_wkb=convert_wkb,
        raw=raw_input,
        extended=extended_input,
        N=N,
    )

    table = table_class.__table__
    return benchmark.pedantic(
        insert_and_select_all_points, args=(conn, table, points), iterations=1, rounds=rounds
    )


@pytest.fixture
def _insert_fail_or_success_type(
    input_representation,
    is_raw_input,
    is_extended_input,
    output_representation,
    is_extended_output,
    is_default_geom_type,
):
    """Fixture to determine if the current test should fail or succeed."""
    return SuccessfulTest


@pytest.mark.parametrize(
    "N",
    [2, 10, 350],
)
# @test_only_with_dialects("postgresql")
def test_insert(
    benchmark,
    GeomTable,
    conn,
    metadata,
    N,
    input_representation,
    is_raw_input,
    is_extended_input,
    _insert_fail_or_success_type,
):
    """Benchmark the insert operation."""
    convert_wkb = input_representation == "WKB"

    with pytest.raises(_insert_fail_or_success_type):
        _benchmark_insert(
            conn,
            GeomTable,
            metadata,
            benchmark,
            convert_wkb=convert_wkb,
            raw_input=is_raw_input,
            extended_input=is_extended_input,
            N=N,
            rounds=ROUNDS,
        )

        assert (
            conn.execute(
                GeomTable.__table__.select().where(GeomTable.__table__.c.geom.is_not(None))
            ).rowcount
            == N * N * ROUNDS
        )

        # Trick to exit the pytest.raises context manager properly when the test is successful
        raise SuccessfulTest("Test was successful and it should be expected.")


@pytest.fixture
def _insert_select_fail_or_success_type(
    dialect_name,
    input_representation,
    is_raw_input,
    is_extended_input,
    output_representation,
    is_extended_output,
    is_default_geom_type,
):
    """Fixture to determine if the current test should fail or succeed."""
    if dialect_name in ["mysql"] and not is_default_geom_type and is_extended_output:
        return OperationalError
    if dialect_name in ["sqlite", "geopackage"] and not is_default_geom_type:
        if not is_extended_output:
            return AssertionError
        else:
            return OperationalError
    if (
        dialect_name in ["postgresql", "sqlite", "geopackage"]
        and is_default_geom_type
        and not is_extended_output
    ):
        return AssertionError
    if dialect_name in ["mysql", "sqlite", "geopackage"] and is_extended_output:
        return AssertionError
    if not is_default_geom_type and is_extended_output:
        return AssertionError
    return SuccessfulTest


def _actual_test_insert_select(
    benchmark,
    GeomTable,
    conn,
    metadata,
    N,
    input_representation,
    is_raw_input,
    is_extended_input,
    output_representation,
    is_extended_output,
):
    """Actual test for insert and select operations."""
    convert_wkb = input_representation == "WKB"
    all_points = _benchmark_insert_select(
        conn,
        GeomTable,
        metadata,
        benchmark,
        convert_wkb=convert_wkb,
        raw_input=is_raw_input,
        extended_input=is_extended_input,
        N=N,
        rounds=ROUNDS,
    )

    assert (
        conn.execute(
            GeomTable.__table__.select().where(GeomTable.__table__.c.geom.is_not(None))
        ).rowcount
        == N * N * ROUNDS
    )
    assert len(all_points) == N * N * ROUNDS

    res = conn.execute(select([GeomTable.__table__.c.geom])).fetchone()
    assert res[0].extended == is_extended_output
    if output_representation == "WKB":
        assert isinstance(res[0], WKBElement)
    elif output_representation == "WKT":
        assert isinstance(res[0], WKTElement)
    assert res[0].srid == 4326


@pytest.mark.parametrize(
    "N",
    [2, 10, 350],
)
# @test_only_with_dialects("postgresql")
def test_insert_select(
    benchmark,
    GeomTable,
    conn,
    metadata,
    N,
    input_representation,
    is_raw_input,
    is_extended_input,
    output_representation,
    is_extended_output,
    _insert_select_fail_or_success_type,
):
    """Benchmark the insert operation."""

    with pytest.raises(_insert_select_fail_or_success_type):
        _actual_test_insert_select(
            benchmark,
            GeomTable,
            conn,
            metadata,
            N,
            input_representation,
            is_raw_input,
            is_extended_input,
            output_representation,
            is_extended_output,
        )

        # Trick to exit the pytest.raises context manager properly when the test is successful
        raise SuccessfulTest("Test was successful and it should be expected.")
