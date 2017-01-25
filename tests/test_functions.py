import re

from sqlalchemy.sql import func

#
# Importing geoalchemy2 actually registers the GeoAlchemy generic
# functions in SQLAlchemy's function registry.
#

import geoalchemy2.functions  # NOQA


def eq_sql(a, b):
    a = re.sub(r'[\n\t]', '', str(a))
    assert a == b


def _test_simple_func(name):
    eq_sql(getattr(func, name)(1).select(),
           'SELECT %(name)s(:%(name)s_2) AS "%(name)s_1"' %
           dict(name=name))


def _test_geometry_returning_func(name):
    eq_sql(getattr(func, name)(1).select(),
           'SELECT ST_AsEWKB(%(name)s(:%(name)s_2)) AS "%(name)s_1"' %
           dict(name=name))


def test_ST_Envelope():
    _test_geometry_returning_func('ST_Envelope')


def test_ST_GeometryN():
    _test_simple_func('ST_GeometryN')


def test_ST_GeometryType():
    _test_simple_func('ST_GeometryType')


def test_ST_IsValid():
    _test_simple_func('ST_IsValid')


def test_ST_NPoints():
    _test_simple_func('ST_NPoints')


def test_ST_X():
    _test_simple_func('ST_X')


def test_ST_Y():
    _test_simple_func('ST_Y')


def test_ST_Z():
    _test_simple_func('ST_Z')


def test_ST_AsBinary():
    _test_simple_func('ST_AsBinary')


def test_ST_AsEWKB():
    _test_simple_func('ST_AsEWKB')


def test_ST_AsTWKB():
    _test_simple_func('ST_AsTWKB')


def test_ST_AsGeoJSON():
    _test_simple_func('ST_AsGeoJSON')


def test_ST_AsGML():
    _test_simple_func('ST_AsGML')


def test_ST_AsKML():
    _test_simple_func('ST_AsKML')


def test_ST_AsSVG():
    _test_simple_func('ST_AsSVG')


def test_ST_AsText():
    _test_simple_func('ST_AsText')


def test_ST_AsEWKT():
    _test_simple_func('ST_AsEWKT')


def test_ST_Area():
    _test_simple_func('ST_Area')


def test_ST_Centroid():
    _test_geometry_returning_func('ST_Centroid')


def test_ST_Contains():
    _test_simple_func('ST_Contains')


def test_ST_ContainsProperly():
    _test_simple_func('ST_ContainsProperly')


def test_ST_Covers():
    _test_simple_func('ST_Covers')


def test_ST_CoveredBy():
    _test_simple_func('ST_CoveredBy')


def test_ST_Crosses():
    _test_simple_func('ST_Crosses')


def test_ST_Disjoint():
    _test_simple_func('ST_Disjoint')


def test_ST_Distance():
    _test_simple_func('ST_Distance')


def test_ST_Distance_Sphere():
    _test_simple_func('ST_Distance_Sphere')


def test_ST_DFullyWithin():
    _test_simple_func('ST_DFullyWithin')


def test_ST_DWithin():
    _test_simple_func('ST_DWithin')


def test_ST_Equals():
    _test_simple_func('ST_Equals')


def test_ST_Intersects():
    _test_simple_func('ST_Intersects')


def test_ST_Length():
    _test_simple_func('ST_Length')


def test_ST_OrderingEquals():
    _test_simple_func('ST_OrderingEquals')


def test_ST_Overlaps():
    _test_simple_func('ST_Overlaps')


def test_ST_Perimeter():
    _test_simple_func('ST_Perimeter')


def test_ST_Relate():
    _test_simple_func('ST_Relate')


def test_ST_Touches():
    _test_simple_func('ST_Touches')


def test_ST_Within():
    _test_simple_func('ST_Within')


def test_ST_Buffer():
    _test_geometry_returning_func('ST_Buffer')


def test_ST_Difference():
    _test_geometry_returning_func('ST_Difference')


def test_ST_Intersection():
    _test_geometry_returning_func('ST_Intersection')


def test_ST_Union():
    _test_geometry_returning_func('ST_Union')


def test_ST_Simplify():
    _test_geometry_returning_func('ST_Simplify')
