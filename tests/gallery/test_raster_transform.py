"""
Reproject a Raster using ST_Transform
=====================================

The `ST_Transform()` function (and a few others like `ST_SnapToGrid()`) can be used on
both `Geometry` and `Raster` types. In `GeoAlchemy2`, this function is only defined for
`Geometry` as it can not be defined for several types at the same time. Thus using this
function on `Raster` requires minor tweaking.

This example uses both SQLAlchemy core and ORM queries.
"""
from sqlalchemy import Column
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy.orm import Query
from sqlalchemy.ext.declarative import declarative_base

from geoalchemy2 import Geometry
from geoalchemy2 import Raster


metadata = MetaData()
Base = declarative_base(metadata=metadata)

table = Table(
    "raster_table",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("geom", Geometry("POLYGON", 4326)),
    Column("rast", Raster(srid=4326)),
)


class RasterTable(Base):
    __tablename__ = 'raster_table_orm'
    id = Column(Integer, primary_key=True)
    geom = Column(Geometry("POLYGON", 4326))
    rast = Column(Raster(srid=4326))

    def __init__(self, rast):
        self.rast = rast


def test_transform_core():
    # Define the transform query for both the geometry and the raster in a naive way
    wrong_query = select([
        func.ST_Transform(table.c.geom, 2154),
        func.ST_Transform(table.c.rast, 2154)
    ])

    # Check the query
    assert str(wrong_query) == (
        "SELECT "
        "ST_AsEWKB("
        "ST_Transform(raster_table.geom, :ST_Transform_2)) AS \"ST_Transform_1\", "
        "ST_AsEWKB("  # <= Note that the raster is processed as a Geometry here
        "ST_Transform(raster_table.rast, :ST_Transform_4)) AS \"ST_Transform_3\" \n"
        "FROM raster_table"
    )

    # Define the transform query for both the geometry and the raster in the correct way
    correct_query = select([
        func.ST_Transform(table.c.geom, 2154),
        func.ST_Transform(table.c.rast, 2154, type_=Raster)
    ])

    # Check the query
    assert str(correct_query) == (
        "SELECT "
        "ST_AsEWKB("
        "ST_Transform(raster_table.geom, :ST_Transform_2)) AS \"ST_Transform_1\", "
        "raster("  # <= This time the raster is correctly processed as a Raster
        "ST_Transform(raster_table.rast, :ST_Transform_4)) AS \"ST_Transform_3\" \n"
        "FROM raster_table"
    )


def test_transform_ORM():
    # Define the transform query for both the geometry and the raster in a naive way
    wrong_query = Query([
        RasterTable.geom.ST_Transform(2154),
        RasterTable.rast.ST_Transform(2154)
    ])

    # Check the query
    assert str(wrong_query) == (
        "SELECT "
        "ST_AsEWKB("
        "ST_Transform(raster_table_orm.geom, :ST_Transform_2)) AS \"ST_Transform_1\", "
        "ST_AsEWKB("  # <= Note that the raster is processed as a Geometry here
        "ST_Transform(raster_table_orm.rast, :ST_Transform_4)) AS \"ST_Transform_3\" \n"
        "FROM raster_table_orm"
    )

    # Define the transform query for both the geometry and the raster in the correct way
    correct_query = Query([
        RasterTable.geom.ST_Transform(2154),
        RasterTable.rast.ST_Transform(2154, type_=Raster)
    ])

    # Check the query
    assert str(correct_query) == (
        "SELECT "
        "ST_AsEWKB("
        "ST_Transform(raster_table_orm.geom, :ST_Transform_2)) AS \"ST_Transform_1\", "
        "raster("  # <= This time the raster is correctly processed as a Raster
        "ST_Transform(raster_table_orm.rast, :ST_Transform_4)) AS \"ST_Transform_3\" \n"
        "FROM raster_table_orm"
    )
