import pytest
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import func

from geoalchemy2 import Geometry
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement

from .. import create_points
from .. import select


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
    input_representation,
    is_extended_input,
    output_representation,
    is_extended_output,
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
    if input_representation == "WKB input":
        from_text_func = "ST_GeomFromEWKB" if is_extended_input else "ST_GeomFromWKB"
    else:
        from_text_func = "ST_GeomFromEWKT" if is_extended_input else "ST_GeomFromText"
    if output_representation == "WKB output":
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


def insert_all_points(conn, table, points):
    """Insert all points into the database."""
    rows = [{"geom": point} for point in points]
    return conn.execute(table.insert(), rows)


def select_all_points(conn, table):
    """Select all points from the database."""
    query = table.select()
    return conn.execute(query).fetchall()


def insert_and_select_all_points(conn, table, points):
    """Insert all points into the database and select them."""
    insert_all_points(conn, table, points)
    return select_all_points(conn, table)


def _reset_benchmark_table(conn, table_class, metadata):
    """Reset the database table for benchmarking."""
    metadata.drop_all(conn, checkfirst=True)
    metadata.create_all(conn)
    print(f"Table {table_class.__tablename__} created")

    return table_class.__table__


def _benchmark_setup(conn, table_class, metadata, points):
    """Setup the database for a benchmark round."""
    table = _reset_benchmark_table(conn, table_class, metadata)
    return (conn, table, points), {}


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
    points = create_points(N, convert_wkb=convert_wkb, raw=raw_input, extended=extended_input)
    return benchmark.pedantic(
        insert_all_points,
        setup=lambda: _benchmark_setup(conn, table_class, metadata, points),
        iterations=1,
        rounds=rounds,
        warmup_rounds=2,
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
    points = create_points(N, convert_wkb=convert_wkb, raw=raw_input, extended=extended_input)
    return benchmark.pedantic(
        insert_and_select_all_points,
        setup=lambda: _benchmark_setup(conn, table_class, metadata, points),
        iterations=1,
        rounds=rounds,
        warmup_rounds=2,
    )


@pytest.fixture
def _insert_fail_or_success_type(
    dialect_name,
    input_representation,
    is_raw_input,
    is_extended_input,
    output_representation,
    is_extended_output,
    is_default_geom_type,
):
    """Fixture to determine if the current test should fail or succeed."""
    if (
        dialect_name == "sqlite"
        and input_representation == "WKB input"
        and is_extended_input
        and not is_default_geom_type
    ):
        return (OperationalError, AssertionError)
    if dialect_name == "geopackage" and input_representation == "WKB input":  # noqa: SIM102
        if is_extended_input and not is_default_geom_type:  # noqa: SIM102
            return AssertionError
    if (
        dialect_name == "mssql"
        and input_representation == "WKB input"
        and not is_extended_input
        and not is_default_geom_type
    ):
        return SQLAlchemyError
    if (
        dialect_name in ["sqlite", "geopackage"]
        and not is_default_geom_type
        and not is_extended_input
    ):
        return AssertionError
    return SuccessfulTest


@pytest.mark.parametrize(
    "N",
    [
        2,
        pytest.param(10, marks=pytest.mark.long_benchmark),
        pytest.param(100, marks=pytest.mark.long_benchmark),
    ],
)
def test_insert(
    insert_select_rounds,
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
    convert_wkb = input_representation == "WKB input"

    try:
        _benchmark_insert(
            conn,
            GeomTable,
            metadata,
            benchmark,
            convert_wkb=convert_wkb,
            raw_input=is_raw_input,
            extended_input=is_extended_input,
            N=N,
            rounds=insert_select_rounds,
        )

        assert (
            conn.execute(
                select([func.count()])
                .select_from(GeomTable.__table__)
                .where(GeomTable.__table__.c.geom.is_not(None))
            ).scalar()
            == N * N
        )

    except SuccessfulTest:
        # Handle the successful test case
        pass
    except _insert_fail_or_success_type:
        # Handle the expected exception
        pytest.xfail(reason=f"Expected exception: {_insert_fail_or_success_type}")


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
    if dialect_name in ["mysql", "mariadb"] and not is_default_geom_type and is_extended_output:
        if output_representation == "WKB output":
            return AssertionError
        return OperationalError
    if (
        dialect_name in ["mysql", "mariadb", "postgresql", "sqlite"]
        and input_representation == "WKB input"
        and is_raw_input
        and is_default_geom_type
    ):
        return (SQLAlchemyError, AssertionError)
    if (
        dialect_name == "sqlite"
        and input_representation == "WKB input"
        and is_extended_input
        and not is_default_geom_type
    ):
        return (OperationalError, AssertionError)
    if (
        dialect_name == "geopackage"
        and not is_default_geom_type
        and is_extended_output
        and output_representation == "WKB output"
    ):
        return AssertionError
    if (
        dialect_name == "mssql"
        and input_representation == "WKB input"
        and not is_extended_input
        and not is_default_geom_type
    ):
        return SQLAlchemyError
    if dialect_name in ["sqlite", "geopackage"] and not is_default_geom_type:
        if not is_extended_output:
            return AssertionError
        else:
            return (OperationalError, AssertionError)
    if (
        dialect_name in ["postgresql", "sqlite", "geopackage"]
        and is_default_geom_type
        and not is_extended_output
    ):
        return AssertionError
    if is_default_geom_type and output_representation == "WKT output":
        return AssertionError
    if dialect_name in ["mysql", "sqlite", "geopackage"] and is_extended_output:
        return AssertionError
    if dialect_name in ["mariadb"] and is_extended_output:
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
    is_default_geom_type,
    insert_select_rounds,
):
    """Actual test for insert and select operations."""
    convert_wkb = input_representation == "WKB input"
    all_points = _benchmark_insert_select(
        conn,
        GeomTable,
        metadata,
        benchmark,
        convert_wkb=convert_wkb,
        raw_input=is_raw_input,
        extended_input=is_extended_input,
        N=N,
        rounds=insert_select_rounds,
    )

    assert (
        len(
            conn.execute(
                GeomTable.__table__.select().where(GeomTable.__table__.c.geom.is_not(None))
            ).fetchall()
        )
        == N * N
    )
    assert len(all_points) == N * N

    res = conn.execute(select([GeomTable.__table__.c.geom])).fetchone()
    expected_extended_output = is_extended_output
    if conn.dialect.name == "mssql" and is_default_geom_type:
        expected_extended_output = True
    assert res[0].extended == expected_extended_output
    if output_representation == "WKB output":
        assert isinstance(res[0], WKBElement)
    elif output_representation == "WKT output":
        assert isinstance(res[0], WKTElement)
    assert res[0].srid == 4326


@pytest.mark.parametrize(
    "N",
    [
        2,
        pytest.param(10, marks=pytest.mark.long_benchmark),
        pytest.param(100, marks=pytest.mark.long_benchmark),
    ],
)
def test_insert_select(
    insert_select_rounds,
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
    is_default_geom_type,
    _insert_select_fail_or_success_type,
):
    """Benchmark the insert operation."""
    try:
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
            is_default_geom_type,
            insert_select_rounds,
        )
    except SuccessfulTest:
        # Handle the successful test case
        pass
    except _insert_select_fail_or_success_type:
        # Handle the expected exception
        pytest.xfail(reason=f"Expected exception: {_insert_select_fail_or_success_type}")
