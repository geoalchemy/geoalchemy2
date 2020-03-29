import pytest


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
