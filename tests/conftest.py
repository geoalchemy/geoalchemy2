import os

import pytest
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.event import listen
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

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
        '--sqlite_dburl',
        action='store',
        help='SQLite DB URL used for tests (`sqlite:///path_to_db_file`).',
    )
    parser.addoption(
        '--engine-echo',
        action='store_true',
        default=False,
        help='If set to True, all statements of the engine are logged.',
    )


def pytest_generate_tests(metafunc):
    if "db_url" in metafunc.fixturenames:
        if metafunc.module.__name__ == "tests.test_functional_postgresql":
            dialects = ["postgresql"]
        elif metafunc.module.__name__ == "tests.test_functional_sqlite":
            dialects = ["sqlite"]
        elif getattr(metafunc.function, "tested_dialects", False):
            dialects = metafunc.function.tested_dialects
        elif getattr(metafunc.cls, "tested_dialects", False):
            dialects = metafunc.cls.tested_dialects
        else:
            dialects = ["postgresql", "sqlite"]
        metafunc.parametrize("db_url", dialects, indirect=True)


@pytest.fixture(scope='session')
def db_url_postgresql(request):
    return (
        request.config.getoption('--postgresql_dburl')
        or os.getenv('PYTEST_POSTGRESQL_DB_URL')
        or 'postgresql://gis:gis@localhost/gis'
    )


@pytest.fixture(scope='session')
def db_url_sqlite(request):
    return (
        request.config.getoption('--sqlite_dburl')
        or os.getenv('PYTEST_SQLITE_DB_URL')
        or 'sqlite:///spatialdb'
    )


@pytest.fixture(scope='session')
def db_url(request, db_url_postgresql, db_url_sqlite):
    if request.param == "postgresql":
        return db_url_postgresql
    elif request.param == "sqlite":
        return db_url_sqlite
    return None


@pytest.fixture(scope='session')
def _engine_echo(request):
    _engine_echo = request.config.getoption('--engine-echo')
    return _engine_echo


def load_spatialite(dbapi_conn, connection_record):
    """Load SpatiaLite extension in SQLite DB."""
    dbapi_conn.enable_load_extension(True)
    dbapi_conn.load_extension(os.environ['SPATIALITE_LIBRARY_PATH'])
    dbapi_conn.enable_load_extension(False)


@pytest.fixture
def engine(db_url, _engine_echo):
    """Provide an engine to test database."""
    engine = create_engine(db_url, echo=_engine_echo)
    if engine.dialect.name == "sqlite":
        if 'SPATIALITE_LIBRARY_PATH' not in os.environ:
            pytest.skip('SPATIALITE_LIBRARY_PATH is not defined, skip SpatiaLite tests')
        listen(engine, 'connect', load_spatialite)
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
