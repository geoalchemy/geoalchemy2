import pytest
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy.orm import declarative_base

from geoalchemy2 import Geometry
from geoalchemy2 import WKBElement
from geoalchemy2 import WKTElement

metadata = MetaData()
Base = declarative_base(metadata=metadata)


class PickledLake(Base):  # type: ignore
    __tablename__ = "pickled_lake"
    id = Column(Integer, primary_key=True)
    geom = Column(Geometry(geometry_type="LINESTRING", srid=4326))

    def __init__(self, geom):
        self.geom = geom


class TestPickle:
    @pytest.fixture
    def setup_one_lake(self, session):
        conn = session.bind
        metadata.drop_all(conn, checkfirst=True)
        metadata.create_all(conn)

        lake = PickledLake(WKTElement("LINESTRING(0 0,1 1)", srid=4326))
        session.add(lake)
        session.flush()
        session.expire(lake)

        yield lake.id

        session.rollback()
        metadata.drop_all(session.bind, checkfirst=True)

    def test_pickle_unpickle(self, session, setup_one_lake, dialect_name):
        import pickle

        lake_id = setup_one_lake

        lake = session.get(PickledLake, lake_id)
        assert isinstance(lake.geom, WKBElement)
        data_desc = str(lake.geom)

        pickled = pickle.dumps(lake)
        unpickled = pickle.loads(pickled)
        assert unpickled.geom.srid == 4326
        assert str(unpickled.geom) == data_desc
        if dialect_name in ["mysql", "mariadb"]:
            assert unpickled.geom.extended is False
        else:
            assert unpickled.geom.extended is True
