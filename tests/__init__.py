import re
from pkg_resources import parse_version

import pytest
from packaging import version
from sqlalchemy import __version__ as SA_VERSION
from sqlalchemy import select as raw_select
from sqlalchemy import text
from sqlalchemy.sql import func
from sqlalchemy.exc import OperationalError


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
