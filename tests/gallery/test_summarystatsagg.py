"""
Use CompositeType
=================

Some functions return composite types. This example shows how to deal with this
kind of functions.
"""
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import Float
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from geoalchemy2 import Raster, WKTElement
from geoalchemy2.functions import GenericFunction
from geoalchemy2.types import CompositeType


class SummaryStats(CompositeType):
    """Define the composite type returned by the function ST_SummaryStatsAgg"""
    typemap = {
        'count': Integer,
        'sum': Float,
        'mean': Float,
        'stddev': Float,
        'min': Float,
        'max': Float,
    }


class ST_SummaryStatsAgg(GenericFunction):
    type = SummaryStats


engine = create_engine('postgresql://gis:gis@localhost/gis', echo=True)
metadata = MetaData(engine)
Base = declarative_base(metadata=metadata)
session = sessionmaker(bind=engine)()


class Ocean(Base):
    __tablename__ = 'ocean'
    __table_args__ = {'schema': 'public'}
    id = Column(Integer, primary_key=True)
    rast = Column(Raster)

    def __init__(self, rast):
        self.rast = rast


class TestSTSummaryStatsAgg():

    def setup(self):
        metadata.drop_all(checkfirst=True)
        metadata.create_all()

    def teardown(self):
        session.rollback()
        metadata.drop_all()

    def test_st_summary_stats_agg(self):

        # Create a new raster
        polygon = WKTElement('POLYGON((0 0,1 1,0 1,0 0))', srid=4326)
        o = Ocean(polygon.ST_AsRaster(5, 6))
        session.add(o)
        session.flush()

        # Define the query to compute stats
        stats_agg = select([
            func.ST_SummaryStatsAgg(Ocean.__table__.c.rast, 1, True, 1).label("stats")
        ])
        stats_agg_alias = stats_agg.alias("stats_agg")

        # Use these stats
        query = select([
            stats_agg_alias.c.stats.count.label("count"),
            stats_agg_alias.c.stats.sum.label("sum"),
            stats_agg_alias.c.stats.stddev.label("stddev"),
            stats_agg_alias.c.stats.min.label("min"),
            stats_agg_alias.c.stats.max.label("max")
        ])

        # Check the query
        assert str(query) == (
            "SELECT "
            "(stats_agg.stats).count AS count, "
            "(stats_agg.stats).sum AS sum, "
            "(stats_agg.stats).stddev AS stddev, "
            "(stats_agg.stats).min AS min, "
            "(stats_agg.stats).max AS max \n"
            "FROM ("
            "SELECT "
            "ST_SummaryStatsAgg("
            "public.ocean.rast, "
            "%(ST_SummaryStatsAgg_1)s, %(ST_SummaryStatsAgg_2)s, %(ST_SummaryStatsAgg_3)s"
            ") AS stats \n"
            "FROM public.ocean) AS stats_agg"
        )

        # Execute the query
        res = session.execute(query).fetchall()

        # Check the result
        assert res == [(15, 15.0, 0.0, 1.0, 1.0)]
