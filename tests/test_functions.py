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


def _test_geography_returning_func(name):
    eq_sql(getattr(func, name)(1).select(),
           'SELECT ST_AsBinary(%(name)s(:%(name)s_2)) AS "%(name)s_1"' %
           dict(name=name))


def test_ST_Envelope():
    _test_geometry_returning_func('ST_Envelope')


def test_ST_GeometryN():
    _test_geometry_returning_func('ST_GeometryN')


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


def test_ST_Azimuth():
    _test_simple_func('ST_Azimuth')


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


def test_ST_LineLocatePoint():
    _test_simple_func('ST_LineLocatePoint')


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


def test_ST_AddPoint():
    _test_geometry_returning_func('ST_AddPoint')


def test_ST_Affine():
    _test_geometry_returning_func('ST_Affine')


def test_ST_AsRaster():
    _test_simple_func('ST_AsRaster')


def test_ST_Boundary():
    _test_geometry_returning_func('ST_Boundary')


def test_ST_BoundingDiagonal():
    _test_geometry_returning_func('ST_BoundingDiagonal')


def test_ST_CollectionExtract():
    _test_geometry_returning_func('ST_CollectionExtract')


def test_ST_CollectionHomogenize():
    _test_geometry_returning_func('ST_CollectionHomogenize')


def test_ST_Dump():
    _test_simple_func('ST_Dump')


def test_ST_DumpPoints():
    _test_simple_func('ST_DumpPoints')


def test_ST_EndPoint():
    _test_geometry_returning_func('ST_EndPoint')


def test_ST_ExteriorRing():
    _test_geometry_returning_func('ST_ExteriorRing')


def test_ST_Force2D():
    _test_geometry_returning_func('ST_Force2D')


def test_ST_Force3D():
    _test_geometry_returning_func('ST_Force3D')


def test_ST_Force3DM():
    _test_geometry_returning_func('ST_Force3DM')


def test_ST_Force3DZ():
    _test_geometry_returning_func('ST_Force3DZ')


def test_ST_Force4D():
    _test_geometry_returning_func('ST_Force4D')


def test_ST_ForceCollection():
    _test_geometry_returning_func('ST_ForceCollection')


def test_ST_ForceCurve():
    _test_geometry_returning_func('ST_ForceCurve')


def test_ST_ForcePolygonCCW():
    _test_geometry_returning_func('ST_ForcePolygonCCW')


def test_ST_ForcePolygonCW():
    _test_geometry_returning_func('ST_ForcePolygonCW')


def test_ST_ForceRHR():
    _test_geometry_returning_func('ST_ForceRHR')


def test_ST_ForceSFS():
    _test_geometry_returning_func('ST_ForceSFS')


def test_ST_GeogFromText():
    _test_geography_returning_func('ST_GeogFromText')


def test_ST_GeomFromEWKB():
    _test_geometry_returning_func('ST_GeomFromEWKB')


def test_ST_GeomFromEWKT():
    _test_geometry_returning_func('ST_GeomFromEWKT')


def test_ST_GeomFromText():
    _test_geometry_returning_func('ST_GeomFromText')


def test_ST_Height():
    _test_simple_func('ST_Height')


def test_ST_InteriorRingN():
    _test_geometry_returning_func('ST_InteriorRingN')


def test_ST_LineMerge():
    _test_geometry_returning_func('ST_LineMerge')


def test_ST_LineSubstring():
    _test_geometry_returning_func('ST_LineSubstring')


def test_ST_M():
    _test_geometry_returning_func('ST_M')


def test_ST_Multi():
    _test_geometry_returning_func('ST_Multi')


def test_ST_Normalize():
    _test_geometry_returning_func('ST_Normalize')


def test_ST_PatchN():
    _test_geometry_returning_func('ST_PatchN')


def test_ST_PointN():
    _test_geometry_returning_func('ST_PointN')


def test_ST_Points():
    _test_geometry_returning_func('ST_Points')


def test_ST_Project():
    _test_geography_returning_func('ST_Project')


def test_ST_QuantizeCoordinates():
    _test_geometry_returning_func('ST_QuantizeCoordinates')


def test_ST_RemovePoint():
    _test_geometry_returning_func('ST_RemovePoint')


def test_ST_Reverse():
    _test_geometry_returning_func('ST_Reverse')


def test_ST_Rotate():
    _test_geometry_returning_func('ST_Rotate')


def test_ST_RotateX():
    _test_geometry_returning_func('ST_RotateX')


def test_ST_RotateY():
    _test_geometry_returning_func('ST_RotateY')


def test_ST_RotateZ():
    _test_geometry_returning_func('ST_RotateZ')


def test_ST_SRID():
    _test_simple_func('ST_SRID')


def test_ST_Scale():
    _test_geometry_returning_func('ST_Scale')


def test_ST_Segmentize():
    _test_geometry_returning_func('ST_Segmentize')


def test_ST_SetPoint():
    _test_geometry_returning_func('ST_SetPoint')


def test_ST_SetSRID():
    _test_geometry_returning_func('ST_SetSRID')


def test_ST_Snap():
    _test_geometry_returning_func('ST_Snap')


def test_ST_SnapToGrid():
    _test_geometry_returning_func('ST_SnapToGrid')


def test_ST_StartPoint():
    _test_geometry_returning_func('ST_StartPoint')


def test_ST_TransScale():
    _test_geometry_returning_func('ST_TransScale')


def test_ST_Transform():
    _test_geometry_returning_func('ST_Transform')


def test_ST_Translate():
    _test_geometry_returning_func('ST_Translate')


def test_ST_Value():
    _test_simple_func('ST_Value')


def test_ST_Width():
    _test_simple_func('ST_Width')
