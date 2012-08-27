from nose.tools import eq_

import geoalchemy2  # NOQA


def test_GeometryType():
    from sqlalchemy.sql import func
    eq_(str(func.ST_GeometryType(1)), 'ST_GeometryType(:param_1)')


def test_Buffer():
    from sqlalchemy.sql import func
    eq_(str(func.ST_Buffer(1)), 'ST_Buffer(:param_1)')
