from nose.tools import eq_

import geoalchemy2  # NOQA


def test_geometry_type():
    from sqlalchemy.sql import func
    eq_(str(func.geo.geometry_type(1)), 'ST_GeometryType(:param_1)')


def test_Buffer():
    from sqlalchemy.sql import func
    eq_(str(func.geo.buffer(1)), 'ST_Buffer(:param_1)')
