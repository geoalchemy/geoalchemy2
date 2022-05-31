import os
import re
import shutil

import pytest
from packaging import version
from pkg_resources import parse_version
from sqlalchemy import __version__ as SA_VERSION
from sqlalchemy import create_engine
from sqlalchemy import select as raw_select
from sqlalchemy import text
from sqlalchemy.event import listen
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import func

from geoalchemy2 import load_spatialite


class test_only_with_dialects:
    def __init__(self, *dialects):
        self.tested_dialects = dialects

    def __call__(self, test_obj):
        test_obj.tested_dialects = self.tested_dialects
        return test_obj


def get_postgis_version(bind):
    try:
        return bind.execute(func.postgis_lib_version()).scalar()
    except OperationalError:
        return "0"


def get_postgres_major_version(bind):
    try:
        return re.match(
            r"([0-9]*)\.([0-9]*).*",
            bind.execute(
                text("""SELECT current_setting('server_version');""")
            ).scalar()
        ).group(1)
    except OperationalError:
        return "0"


def skip_postgis1(bind):
    if get_postgis_version(bind).startswith('1.'):
        pytest.skip("requires PostGIS != 1")


def skip_postgis2(bind):
    if get_postgis_version(bind).startswith('2.'):
        pytest.skip("requires PostGIS != 2")


def skip_postgis3(bind):
    if get_postgis_version(bind).startswith('3.'):
        pytest.skip("requires PostGIS != 3")


def skip_case_insensitivity():
    return pytest.mark.skipif(
        parse_version(SA_VERSION) < parse_version("1.3.4"),
        reason='Case-insensitivity is only available for sqlalchemy>=1.3.4')


def skip_pg12_sa1217(bind):
    if (
            parse_version(SA_VERSION) < parse_version("1.2.17")
            and int(get_postgres_major_version(bind)) >= 12
    ):
        pytest.skip("Reflection for PostgreSQL-12 is only supported by sqlalchemy>=1.2.17")


def select(args):
    if version.parse(SA_VERSION) < version.parse("1.4"):
        return raw_select(args)
    else:
        return raw_select(*args)


def format_wkt(wkt):
    return wkt.replace(", ", ",")


def copy_and_connect_sqlite_db(input_db, tmp_db, engine_echo):
    if 'SPATIALITE_LIBRARY_PATH' not in os.environ:
        pytest.skip('SPATIALITE_LIBRARY_PATH is not defined, skip SpatiaLite tests')

    shutil.copyfile(input_db, tmp_db)

    db_url = f"sqlite:///{tmp_db}"
    engine = create_engine(db_url, echo=engine_echo)
    listen(engine, 'connect', load_spatialite)

    if input_db.endswith("spatialite_lt_4.sqlite"):
        engine._spatialite_version = 3
    elif input_db.endswith("spatialite_ge_4.sqlite"):
        engine._spatialite_version = 4
    else:
        engine._spatialite_version = None

    return engine


def check_indexes(conn, expected, table_name):
    if conn.dialect.name == "postgresql":
        # Query to check the indexes
        index_query = text(
            """SELECT indexname, indexdef
            FROM pg_indexes
            WHERE
                tablename = '{}'
            ORDER BY indexname;""".format(table_name)
        )
        indexes = conn.execute(index_query).fetchall()

        expected = [
            (i[0], re.sub("\n *", " ", i[1]))
            for i in expected["postgresql"]
        ]

        assert indexes == expected

    elif conn.dialect.name == "sqlite":
        # Query to check the indexes
        index_query = text(
            """SELECT *
            FROM geometry_columns
            WHERE f_table_name = '{}'
            ORDER BY f_table_name, f_geometry_column;""".format(table_name)
        )

        indexes = conn.execute(index_query).fetchall()
        assert indexes == expected["sqlite"]
