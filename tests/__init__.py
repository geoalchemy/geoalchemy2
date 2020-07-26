from pkg_resources import parse_version
import pytest
from sqlalchemy import __version__ as SA_VERSION


def skip_postgis1(postgis_version):
    return pytest.mark.skipif(
        postgis_version.startswith('1.'),
        reason="requires PostGIS != 1",
    )


def skip_postgis2(postgis_version):
    return pytest.mark.skipif(
        postgis_version.startswith('2.'),
        reason="requires PostGIS != 2",
    )


def skip_postgis3(postgis_version):
    return pytest.mark.skipif(
        postgis_version.startswith('3.'),
        reason="requires PostGIS != 3",
    )


def skip_case_insensitivity():
    return pytest.mark.skipif(
        parse_version(SA_VERSION) < parse_version("1.3.4"),
        reason='Case-insensitivity is only available for sqlalchemy>=1.3.4')


def skip_pg12_sa1217(postgres_major_version):
    return pytest.mark.skipif(
        (
            parse_version(SA_VERSION) < parse_version("1.2.17")
            and int(postgres_major_version) >= 12
        ),
        reason='Reflection for PostgreSQL-12 is only supported by sqlalchemy>=1.2.17')
