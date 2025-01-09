import os
from pathlib import Path

import pytest
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.dialects.mysql.base import MySQLDialect
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

from geoalchemy2.alembic_helpers import _monkey_patch_get_indexes_for_mysql
from geoalchemy2.alembic_helpers import _monkey_patch_get_indexes_for_sqlite

from . import copy_and_connect_sqlite_db
from . import get_postgis_major_version
from . import get_postgres_major_version
from .schema_fixtures import *  # noqa


def pytest_addoption(parser):
    parser.addoption(
        "--postgresql_dburl",
        action="store",
        help="PostgreSQL DB URL used for tests (`postgresql://user:password@host:port/dbname`).",
    )
    parser.addoption(
        "--sqlite_spatialite3_dburl",
        action="store",
        help="SQLite DB URL used for tests with SpatiaLite3 (`sqlite:///path_to_db_file`).",
    )
    parser.addoption(
        "--sqlite_spatialite4_dburl",
        action="store",
        help="SQLite DB URL used for tests with SpatiaLite4 (`sqlite:///path_to_db_file`).",
    )
    parser.addoption(
        "--sqlite_geopackage_dburl",
        action="store",
        help="SQLite DB URL used for tests with GeoPackage (`gpkg:///path_to_db_file.gpkg`).",
    )
    parser.addoption(
        "--mysql_dburl",
        action="store",
        help="MySQL DB URL used for tests (`mysql://user:password@host:port/dbname`).",
    )
    parser.addoption(
        "--mariadb_dburl",
        action="store",
        help="MariaDB DB URL used for tests (`mariadb://user:password@host:port/dbname`).",
    )
    parser.addoption(
        "--engine-echo",
        action="store_true",
        default=False,
        help="If set to True, all statements of the engine are logged.",
    )
    parser.addoption(
        "--require-all-dialects",
        action="store_true",
        default=False,
        help="If set to True, all dialects muts be properly executed.",
    )


def pytest_generate_tests(metafunc):
    if "db_url" in metafunc.fixturenames:
        sqlite_dialects = ["sqlite-spatialite3", "sqlite-spatialite4", "geopackage"]
        dialects = None

        if metafunc.module.__name__ == "tests.test_functional_postgresql":
            dialects = ["postgresql"]
        elif metafunc.module.__name__ == "tests.test_functional_sqlite":
            dialects = sqlite_dialects
        elif metafunc.module.__name__ == "tests.test_functional_mysql":
            dialects = ["mysql", "mariadb"]
        elif metafunc.module.__name__ == "tests.test_functional_geopackage":
            dialects = ["geopackage"]

        if getattr(metafunc.function, "tested_dialects", False):
            dialects = metafunc.function.tested_dialects
        elif getattr(metafunc.cls, "tested_dialects", False):
            dialects = metafunc.cls.tested_dialects

        if dialects is None:
            dialects = ["mysql", "mariadb", "postgresql"] + sqlite_dialects

        if "sqlite" in dialects:
            # Order dialects
            dialects = [i for i in dialects if i != "sqlite"] + sqlite_dialects

        metafunc.parametrize("db_url", dialects, indirect=True)


@pytest.fixture(scope="session")
def db_url_postgresql(request):
    return (
        request.config.getoption("--postgresql_dburl")
        or os.getenv("PYTEST_POSTGRESQL_DB_URL")
        or "postgresql://gis:gis@localhost/gis"
    )


@pytest.fixture(scope="session")
def db_url_mysql(request, tmpdir_factory):
    return (
        request.config.getoption("--mysql_dburl")
        or os.getenv("PYTEST_MYSQL_DB_URL")
        or "mysql://gis:gis@localhost/gis"
    )


@pytest.fixture(scope="session")
def db_url_mariadb(request, tmpdir_factory):
    return (
        request.config.getoption("--mariadb_dburl")
        or os.getenv("PYTEST_MARIADB_DB_URL")
        or "mariadb://gis:gis@localhost/gis"
    )


@pytest.fixture(scope="session")
def db_url_sqlite_spatialite3(request, tmpdir_factory):
    return (
        request.config.getoption("--sqlite_spatialite3_dburl")
        or os.getenv("PYTEST_SPATIALITE3_DB_URL")
        or f"sqlite:///{Path(__file__).parent / 'data' / 'spatialite_lt_4.sqlite'}"
    )


@pytest.fixture(scope="session")
def db_url_sqlite_spatialite4(request, tmpdir_factory):
    return (
        request.config.getoption("--sqlite_spatialite4_dburl")
        or os.getenv("PYTEST_SPATIALITE4_DB_URL")
        or f"sqlite:///{Path(__file__).parent / 'data' / 'spatialite_ge_4.sqlite'}"
    )


@pytest.fixture(scope="session")
def db_url_geopackage(request, tmpdir_factory):
    return (
        request.config.getoption("--sqlite_geopackage_dburl")
        or os.getenv("PYTEST_GEOPACKAGE_DB_URL")
        or f"gpkg:///{Path(__file__).parent / 'data' / 'spatialite_geopackage.gpkg'}"
    )


@pytest.fixture(scope="session")
def db_url(
    request,
    db_url_postgresql,
    db_url_sqlite_spatialite3,
    db_url_sqlite_spatialite4,
    db_url_geopackage,
    db_url_mysql,
    db_url_mariadb,
):
    if request.param == "postgresql":
        return db_url_postgresql
    if request.param == "mysql":
        return db_url_mysql
    if request.param == "mariadb":
        return db_url_mariadb
    if request.param == "sqlite-spatialite3":
        return db_url_sqlite_spatialite3
    if request.param == "sqlite-spatialite4":
        return db_url_sqlite_spatialite4
    if request.param == "geopackage":
        return db_url_geopackage
    return None


@pytest.fixture(scope="session")
def _engine_echo(request):
    _engine_echo = request.config.getoption("--engine-echo")
    return _engine_echo


@pytest.fixture(scope="session")
def _require_all_dialects(request):
    _require_all_dialects = request.config.getoption("--require-all-dialects")
    return _require_all_dialects


@pytest.fixture
def engine(tmpdir, db_url, _engine_echo, _require_all_dialects):
    """Provide an engine to test database."""
    try:
        if db_url.startswith("sqlite:///"):
            # Copy the input SQLite DB to a temporary file and return an engine to it
            input_url = str(db_url)[10:]
            output_file = "test_spatial_db.sqlite"
            current_engine = copy_and_connect_sqlite_db(
                input_url, tmpdir / output_file, _engine_echo, "sqlite"
            )
        elif db_url.startswith("gpkg:///"):
            # Copy the input GeoPackage to a temporary file and return an engine to it
            input_url = str(db_url)[8:]
            output_file = "test_spatial_db.gpkg"
            current_engine = copy_and_connect_sqlite_db(
                input_url, tmpdir / output_file, _engine_echo, "gpkg"
            )
        else:
            # For other dialects the engine is directly returned
            current_engine = create_engine(db_url, echo=_engine_echo, plugins=["geoalchemy2"])
            current_engine.update_execution_options(search_path=["gis", "public"])
    except Exception as exc:
        msg = f"Could not create engine for this URL: {db_url}\nThe exception was: {exc}"
        if _require_all_dialects:
            pytest.fail("All dialects are required. " + msg)
        else:
            pytest.skip(reason=msg)

    # Disambiguate MySQL and MariaDB
    if current_engine.dialect.name in ["mysql", "mariadb"]:
        try:
            with current_engine.begin() as connection:
                mysql_type = (
                    "MariaDB"
                    if "mariadb" in connection.execute(text("SELECT VERSION();")).scalar().lower()
                    else "MySQL"
                )
            if current_engine.dialect.name != mysql_type.lower():
                msg = f"Can not execute {mysql_type} queries on {db_url}"
                if _require_all_dialects:
                    pytest.fail("All dialects are required. " + msg)
                else:
                    pytest.skip(reason=msg)
        except InvalidRequestError:
            msg = f"Can not execute MariaDB queries on {db_url}"
            if _require_all_dialects:
                pytest.fail("All dialects are required. " + msg)
            else:
                pytest.skip(reason=msg)

    yield current_engine
    current_engine.dispose()


@pytest.fixture
def check_spatialite():
    if "SPATIALITE_LIBRARY_PATH" not in os.environ:
        pytest.skip("SPATIALITE_LIBRARY_PATH is not defined, skip SpatiaLite tests")


@pytest.fixture
def dialect_name(engine):
    return engine.dialect.name


@pytest.fixture
def conn(engine):
    """Provide a connection to test database."""
    with engine.connect() as connection:
        trans = connection.begin()
        yield connection
        trans.rollback()


@pytest.fixture
def session(engine, conn):
    Session = sessionmaker(bind=conn)
    with Session(bind=conn) as session:
        yield session


@pytest.fixture
def schema(engine):
    if engine.dialect.name == "postgresql":
        return "gis"
    else:
        return None


@pytest.fixture
def metadata():
    return MetaData()


@pytest.fixture()
def base(metadata):
    return declarative_base(metadata=metadata)


@pytest.fixture
def postgis_version(conn):
    return get_postgis_major_version(conn)


@pytest.fixture
def postgres_major_version(conn):
    return get_postgres_major_version(conn)


@pytest.fixture(autouse=True)
def reset_alembic_monkeypatch():
    """Disable Alembic monkeypatching by default."""
    try:
        normal_behavior_sqlite = SQLiteDialect._get_indexes_normal_behavior
        SQLiteDialect.get_indexes = normal_behavior_sqlite
        SQLiteDialect._get_indexes_normal_behavior = normal_behavior_sqlite

        normal_behavior_mysql = MySQLDialect._get_indexes_normal_behavior
        MySQLDialect.get_indexes = normal_behavior_mysql
        MySQLDialect._get_indexes_normal_behavior = normal_behavior_mysql
    except AttributeError:
        pass


@pytest.fixture()
def use_alembic_monkeypatch():
    """Enable Alembic monkeypatching ."""
    _monkey_patch_get_indexes_for_sqlite()
    _monkey_patch_get_indexes_for_mysql()


@pytest.fixture
def setup_tables(conn, metadata):
    metadata.drop_all(conn, checkfirst=True)
    metadata.create_all(conn)
    yield
