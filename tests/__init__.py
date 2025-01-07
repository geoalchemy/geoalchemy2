import os
import platform
import re
import shutil
import sys

import pytest
from packaging.version import parse as parse_version
from sqlalchemy import __version__ as SA_VERSION
from sqlalchemy import create_engine
from sqlalchemy import select as raw_select
from sqlalchemy import text
from sqlalchemy.event import listen
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import func

from geoalchemy2 import load_spatialite
from geoalchemy2 import load_spatialite_gpkg


class test_only_with_dialects:
    def __init__(self, *dialects):
        self.tested_dialects = dialects

    def __call__(self, test_obj):
        test_obj.tested_dialects = self.tested_dialects
        return test_obj


def get_postgis_major_version(bind):
    try:
        return parse_version(bind.execute(func.postgis_lib_version()).scalar()).major
    except OperationalError:
        return parse_version("0").major


def get_postgres_major_version(bind):
    try:
        return re.match(
            r"([0-9]*)\.([0-9]*).*",
            bind.execute(text("""SELECT current_setting('server_version');""")).scalar(),
        ).group(1)
    except OperationalError:
        return "0"


def skip_postgis1(bind):
    if get_postgis_major_version(bind) == 1:
        pytest.skip("requires PostGIS != 1")


def skip_postgis2(bind):
    if get_postgis_major_version(bind) == 2:
        pytest.skip("requires PostGIS != 2")


def skip_postgis3(bind):
    if get_postgis_major_version(bind) == 3:
        pytest.skip("requires PostGIS != 3")


def skip_case_insensitivity():
    return pytest.mark.skipif(
        parse_version(SA_VERSION) < parse_version("1.3.4"),
        reason="Case-insensitivity is only available for sqlalchemy>=1.3.4",
    )


def skip_pg12_sa1217(bind):
    if (
        parse_version(SA_VERSION) < parse_version("1.2.17")
        and int(get_postgres_major_version(bind)) >= 12
    ):
        pytest.skip("Reflection for PostgreSQL-12 is only supported by sqlalchemy>=1.2.17")


def skip_pypy(msg=None):
    if platform.python_implementation() == "PyPy":
        pytest.skip(msg if msg is not None else "Incompatible with PyPy")


def select(args):
    if parse_version(SA_VERSION) < parse_version("1.4"):
        return raw_select(args)
    else:
        return raw_select(*args)


def format_wkt(wkt):
    return wkt.replace(", ", ",")


def copy_and_connect_sqlite_db(input_db, tmp_db, engine_echo, dialect):
    if "SPATIALITE_LIBRARY_PATH" not in os.environ:
        pytest.skip("SPATIALITE_LIBRARY_PATH is not defined, skip SpatiaLite tests")

    shutil.copyfile(input_db, tmp_db)

    print("INPUT DB:", input_db)
    print("TEST DB:", tmp_db)

    db_url = f"{dialect}:///{tmp_db}"
    engine = create_engine(
        db_url,
        echo=engine_echo,
        execution_options={"schema_translate_map": {"gis": None}},
        plugins=["geoalchemy2"],
    )

    if dialect == "gpkg":
        listen(engine, "connect", load_spatialite_gpkg)
    else:
        listen(engine, "connect", load_spatialite)

    with engine.begin() as connection:
        print(
            "SPATIALITE VERSION:",
            connection.execute(text("SELECT spatialite_version();")).fetchone()[0],
        )
        print(
            "GEOS VERSION:",
            connection.execute(text("SELECT geos_version();")).fetchone()[0],
        )
        if sys.version_info.minor > 7:
            print(
                "PROJ VERSION:",
                connection.execute(text("SELECT proj_version();")).fetchone()[0],
            )
            print(
                "PROJ DB PATH:",
                connection.execute(text("SELECT PROJ_GetDatabasePath();")).fetchone()[0],
            )

    return engine


def check_indexes(conn, dialect_name, expected, table_name):
    """Check that actual indexes are equal to the expected ones."""
    index_query = {
        "postgresql": text(
            """SELECT indexname, indexdef
            FROM pg_indexes
            WHERE
                tablename = '{}'
            ORDER BY indexname;""".format(
                table_name
            )
        ),
        "sqlite": text(
            """SELECT *
            FROM geometry_columns
            WHERE f_table_name = '{}'
            ORDER BY f_table_name, f_geometry_column;""".format(
                table_name
            )
        ),
        "geopackage": text(
            """SELECT table_name, column_name, extension_name
            FROM gpkg_extensions
            WHERE table_name = '{}' and extension_name = 'gpkg_rtree_index'
            """.format(
                table_name
            )
        ),
    }

    # Query to check the indexes
    actual_indexes = conn.execute(index_query[dialect_name]).fetchall()

    expected_indexes = expected[dialect_name]
    if dialect_name == "postgresql":
        expected_indexes = [(i[0], re.sub("\n *", " ", i[1])) for i in expected_indexes]

    try:
        assert actual_indexes == expected_indexes
    except AssertionError as exc:
        print("###############################################")

        print("Expected indexes:")
        for i in expected_indexes:
            print(i)

        print("Actual indexes:")
        for i in actual_indexes:
            print(i)

        print("###############################################")

        raise exc
