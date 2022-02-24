"""
Disable wrapping in select
==========================

If the application wants to build queries with GeoAlchemy 2 and gets them as strings,
the wrapping of geometry columns with a `ST_AsEWKB()` function might be annoying. In
this case it is possible to disable this wrapping.
This example uses SQLAlchemy ORM queries.
"""
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import func
from sqlalchemy.ext.declarative import declarative_base

from geoalchemy2 import Geometry

# Tests imports
from tests import select

Base = declarative_base()


class RawGeometry(Geometry):
    """This class is used to remove the 'ST_AsEWKB()'' function from select queries"""

    def column_expression(self, col):
        return col


class Point(Base):
    __tablename__ = "point"
    id = Column(Integer, primary_key=True)
    geom = Column(Geometry(srid=4326, geometry_type="POINT"))
    raw_geom = Column(
        RawGeometry(srid=4326, geometry_type="POINT"))


def test_no_wrapping():
    # Select all columns
    select_query = select([Point])

    # Check that the 'geom' column is wrapped by 'ST_AsEWKB()' and that the column
    # 'raw_geom' is not.
    assert str(select_query) == (
        "SELECT point.id, ST_AsEWKB(point.geom) AS geom, point.raw_geom \n"
        "FROM point"
    )


def test_func_no_wrapping():
    # Select query with function
    select_query = select([
        func.ST_Buffer(Point.geom),  # with wrapping (default behavior)
        func.ST_Buffer(Point.geom, type_=Geometry),  # with wrapping
        func.ST_Buffer(Point.geom, type_=RawGeometry)  # without wrapping
    ])

    # Check the query
    assert str(select_query) == (
        "SELECT "
        "ST_AsEWKB(ST_Buffer(point.geom)) AS \"ST_Buffer_1\", "
        "ST_AsEWKB(ST_Buffer(point.geom)) AS \"ST_Buffer_2\", "
        "ST_Buffer(point.geom) AS \"ST_Buffer_3\" \n"
        "FROM point"
    )
