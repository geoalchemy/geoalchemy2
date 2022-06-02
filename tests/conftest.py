import os
from pathlib import Path

import pytest
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from . import copy_and_connect_sqlite_db
from . import get_postgis_version
from . import get_postgres_major_version
from .schema_fixtures import *  # noqa


def pytest_addoption(parser):
    parser.addoption(
        '--postgresql_dburl',
        action='store',
        help='PostgreSQL DB URL used for tests (`postgresql://user:password@host:port/dbname`).',
    )
    parser.addoption(
        '--sqlite_spatialite3_dburl',
        action='store',
        help='SQLite DB URL used for tests with SpatiaLite3 (`sqlite:///path_to_db_file`).',
    )
    parser.addoption(
        '--sqlite_spatialite4_dburl',
        action='store',
        help='SQLite DB URL used for tests with SpatiaLite4 (`sqlite:///path_to_db_file`).',
    )
    parser.addoption(
        '--engine-echo',
        action='store_true',
        default=False,
        help='If set to True, all statements of the engine are logged.',
    )


def pytest_generate_tests(metafunc):
    if "db_url" in metafunc.fixturenames:
        sqlite_dialects = ["sqlite-spatialite3", "sqlite-spatialite4"]
        dialects = None

        if metafunc.module.__name__ == "tests.test_functional_postgresql":
            dialects = ["postgresql"]
        elif metafunc.module.__name__ == "tests.test_functional_sqlite":
            dialects = sqlite_dialects

        if getattr(metafunc.function, "tested_dialects", False):
            dialects = metafunc.function.tested_dialects
        elif getattr(metafunc.cls, "tested_dialects", False):
            dialects = metafunc.cls.tested_dialects

        if dialects is None:
            dialects = ["postgresql", "sqlite-spatialite3", "sqlite-spatialite4"]

        if "sqlite" in dialects:
            dialects = [i for i in dialects if i != "sqlite"] + sqlite_dialects

        metafunc.parametrize("db_url", dialects, indirect=True)


@pytest.fixture(scope='session')
def db_url_postgresql(request):
    return (
        request.config.getoption('--postgresql_dburl')
        or os.getenv('PYTEST_POSTGRESQL_DB_URL')
        or 'postgresql://gis:gis@localhost/gis'
    )


@pytest.fixture(scope='session')
def db_url_sqlite(request, tmpdir_factory):
    return (
        request.config.getoption('--sqlite_spatialite4_dburl')
        or os.getenv('PYTEST_SQLITE_DB_URL')
        # or f"sqlite:///{tmpdir_factory.getbasetemp() / 'spatialdb'}"
        or "sqlite-auto"
    )


@pytest.fixture(scope='session')
def db_url_sqlite_spatialite3(request, tmpdir_factory):
    return (
        request.config.getoption('--sqlite_spatialite3_dburl')
        or os.getenv('PYTEST_SPATIALITE3_DB_URL')
        or f"sqlite:///{Path(__file__).parent / 'data' / 'spatialite_lt_4.sqlite'}"
    )


@pytest.fixture(scope='session')
def db_url_sqlite_spatialite4(request, tmpdir_factory):
    return (
        request.config.getoption('--sqlite_spatialite4_dburl')
        or os.getenv('PYTEST_SPATIALITE4_DB_URL')
        or f"sqlite:///{Path(__file__).parent / 'data' / 'spatialite_ge_4.sqlite'}"
    )


@pytest.fixture(scope='session')
def db_url(request, db_url_postgresql, db_url_sqlite_spatialite3, db_url_sqlite_spatialite4):
    if request.param == "postgresql":
        return db_url_postgresql
    elif request.param == "sqlite-spatialite3":
        return db_url_sqlite_spatialite3
    elif request.param == "sqlite-spatialite4":
        return db_url_sqlite_spatialite4
    return None


@pytest.fixture(scope='session')
def _engine_echo(request):
    _engine_echo = request.config.getoption('--engine-echo')
    return _engine_echo


@pytest.fixture
def engine(tmpdir, db_url, _engine_echo):
    """Provide an engine to test database."""
    if db_url.startswith("sqlite:///"):
        # Copy the input SQLite DB to a temporary file and return an engine to it
        input_url = str(db_url)[10:]
        return copy_and_connect_sqlite_db(
            input_url,
            tmpdir / "test_spatial_db",
            _engine_echo
        )

    # For other dialects the engine is directly returned
    engine = create_engine(db_url, echo=_engine_echo)
    engine._spatialite_version = None
    return engine


@pytest.fixture
def session(engine):
    session = sessionmaker(bind=engine)()
    if engine.dialect.name == "sqlite":
        session.execute(text('SELECT InitSpatialMetaData()'))
    yield session
    session.rollback()


@pytest.fixture
def conn(session):
    """Provide a connection to test database."""
    conn = session.connection()
    yield conn


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
    return get_postgis_version(conn)


@pytest.fixture
def postgres_major_version(conn):
    return get_postgres_major_version(conn)


@pytest.fixture
def setup_tables(session, metadata):
    conn = session.connection()
    metadata.drop_all(conn, checkfirst=True)
    metadata.create_all(conn)
    yield
