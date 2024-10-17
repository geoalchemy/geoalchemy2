"""Declare tables used in tests."""

import pytest
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.types import TypeDecorator

from geoalchemy2 import Geography
from geoalchemy2 import Geometry
from geoalchemy2 import Raster


@pytest.fixture
def Lake(base, postgis_version, schema):
    class Lake(base):
        __tablename__ = "lake"
        __table_args__ = {"schema": schema}
        id = Column(Integer, primary_key=True)
        geom = Column(Geometry(geometry_type="LINESTRING", srid=4326))

        def __init__(self, geom):
            self.geom = geom

    return Lake


@pytest.fixture
def Poi(base, schema, dialect_name):
    class Poi(base):
        __tablename__ = "poi"
        __table_args__ = {"schema": schema}
        id = Column(Integer, primary_key=True)
        geom = Column(Geometry(geometry_type="POINT", srid=4326))
        geog = (
            Column(Geography(geometry_type="POINT", srid=4326))
            if dialect_name == "postgresql"
            else None
        )

        def __init__(self, geog):
            self.geog = geog

    return Poi


@pytest.fixture
def Summit(base, postgis_version, schema):
    with_use_typemod = postgis_version == 1

    class Summit(base):
        __tablename__ = "summit"
        __table_args__ = {"schema": schema}
        id = Column(Integer, primary_key=True)
        geom = Column(
            Geometry(
                geometry_type="POINT",
                srid=4326,
                use_typmod=with_use_typemod,
            )
        )

        def __init__(self, geom):
            self.geom = geom

    return Summit


@pytest.fixture
def Ocean(base, postgis_version):
    # The raster type is only available on PostGIS 2.0 and above
    if postgis_version == 1:
        pytest.skip("The raster type is only available on PostGIS 2.0 and above")

    class Ocean(base):
        __tablename__ = "ocean"
        id = Column(Integer, primary_key=True)
        rast = Column(Raster)

        def __init__(self, rast):
            self.rast = rast

    return Ocean


class ThreeDGeometry(TypeDecorator):
    """This class is used to insert a ST_Force3D() in each insert."""

    impl = Geometry

    def bind_expression(self, bindvalue):
        return func.ST_Force3D(self.impl.bind_expression(bindvalue))


@pytest.fixture
def PointZ(base):
    class PointZ(base):
        __tablename__ = "point_z"
        id = Column(Integer, primary_key=True)
        three_d_geom = Column(ThreeDGeometry(srid=4326, geometry_type="POINTZ", dimension=3))

    return PointZ


class TransformedGeometry(TypeDecorator):
    """This class is used to insert a ST_Transform() in each insert or select."""

    impl = Geometry

    def __init__(self, db_srid, app_srid, **kwargs):
        kwargs["srid"] = db_srid
        self.impl = self.__class__.impl(**kwargs)
        self.app_srid = app_srid
        self.db_srid = db_srid

    def column_expression(self, col):
        """The column_expression() method is overridden to ensure that the
        SRID of the resulting WKBElement is correct"""
        return getattr(func, self.impl.as_binary)(
            func.ST_Transform(col, self.app_srid),
            type_=self.__class__.impl(srid=self.app_srid),
            # srid could also be -1 so that the SRID is deduced from the
            # WKB data
        )

    def bind_expression(self, bindvalue):
        return func.ST_Transform(self.impl.bind_expression(bindvalue), self.db_srid)


@pytest.fixture
def LocalPoint(base):
    class LocalPoint(base):
        __tablename__ = "local_point"
        id = Column(Integer, primary_key=True)
        geom = Column(TransformedGeometry(db_srid=2154, app_srid=4326, geometry_type="POINT"))
        managed_geom = Column(
            TransformedGeometry(db_srid=2154, app_srid=4326, geometry_type="POINT")
        )

    return LocalPoint


@pytest.fixture
def IndexTestWithSchema(base, schema):
    class IndexTestWithSchema(base):
        __tablename__ = "indextestwithschema"
        __table_args__ = {"schema": schema} if schema else {}
        id = Column(Integer, primary_key=True)
        geom1 = Column(Geometry(geometry_type="POINT", srid=4326))
        geom2 = Column(Geometry(geometry_type="POINT", srid=4326))

    return IndexTestWithSchema


@pytest.fixture
def IndexTestWithNDIndex(base, schema):
    class IndexTestWithNDIndex(base):
        __tablename__ = "index_test_with_nd_index"
        __table_args__ = {"schema": schema}
        id = Column(Integer, primary_key=True)
        geom1 = Column(Geometry(geometry_type="POINTZ", dimension=3, use_N_D_index=True))

    return IndexTestWithNDIndex


@pytest.fixture
def IndexTestWithoutSchema(base):
    class IndexTestWithoutSchema(base):
        __tablename__ = "indextestwithoutschema"
        id = Column(Integer, primary_key=True)
        geom1 = Column(Geometry(geometry_type="POINT", srid=4326))
        geom2 = Column(Geometry(geometry_type="POINT", srid=4326))

    return IndexTestWithoutSchema


@pytest.fixture
def reflection_tables_metadata(dialect_name):
    metadata = MetaData()
    base = declarative_base(metadata=metadata)

    class Lake(base):
        __tablename__ = "lake"
        id = Column(Integer, primary_key=True)
        geom = Column(Geometry(geometry_type="LINESTRING", srid=4326))
        if dialect_name != "geopackage":
            geom_no_idx = Column(
                Geometry(geometry_type="LINESTRING", srid=4326, spatial_index=False)
            )
            if dialect_name not in ["mysql", "mariadb"]:
                geom_z = Column(Geometry(geometry_type="LINESTRINGZ", srid=4326, dimension=3))
                geom_m = Column(Geometry(geometry_type="LINESTRINGM", srid=4326, dimension=3))
                geom_zm = Column(Geometry(geometry_type="LINESTRINGZM", srid=4326, dimension=4))

    return metadata
