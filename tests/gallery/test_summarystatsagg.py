"""
Use CompositeType
=================

Some functions return composite types. This example shows how to deal with this
kind of functions.
"""
import pytest
from pkg_resources import parse_version

from sqlalchemy import __version__ as SA_VERSION
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from geoalchemy2 import Raster, WKTElement
from geoalchemy2.functions import GenericFunction
from geoalchemy2.types import CompositeType


class SummaryStatsCustomType(CompositeType):
    """Define the composite type returned by the function ST_SummaryStatsAgg."""
    typemap = {
        'count': Integer,
        'sum': Float,
        'mean': Float,
        'stddev': Float,
        'min': Float,
        'max': Float,
    }

    cache_ok = True


class ST_SummaryStatsAgg(GenericFunction):
    type = SummaryStatsCustomType
    # Set a specific identifier to not override the actual ST_SummaryStatsAgg function
    identifier = "ST_SummaryStatsAgg_custom"

    inherit_cache = True


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

    @pytest.mark.skipif(
        parse_version(SA_VERSION) < parse_version("1.4"),
        reason="requires SQLAlchely>1.4",
    )
    def test_st_summary_stats_agg(self):

        # Create a new raster
        polygon = WKTElement('POLYGON((0 0,1 1,0 1,0 0))', srid=4326)
        o = Ocean(polygon.ST_AsRaster(5, 6))
        session.add(o)
        session.flush()

        # Define the query to compute stats
        stats_agg = select(
            Ocean.rast.ST_SummaryStatsAgg_custom(1, True, 1).label("stats")
        )
        stats_agg_alias = stats_agg.alias("stats_agg")

        # Use these stats
        query = select(
            stats_agg_alias.c.stats.count.label("count"),
            stats_agg_alias.c.stats.sum.label("sum"),
            stats_agg_alias.c.stats.mean.label("mean"),
            stats_agg_alias.c.stats.stddev.label("stddev"),
            stats_agg_alias.c.stats.min.label("min"),
            stats_agg_alias.c.stats.max.label("max")
        )

        # Check the query
        assert str(query) == (
            "SELECT "
            "(stats_agg.stats).count AS count, "
            "(stats_agg.stats).sum AS sum, "
            "(stats_agg.stats).mean AS mean, "
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
        assert res == [(15, 15.0, 1.0, 0.0, 1.0, 1.0)]
