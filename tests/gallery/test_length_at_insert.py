"""
Compute length on insert
========================

It is possible to insert a geometry and ask PostgreSQL to compute its length at the same
time.
This example uses SQLAlchemy core queries.
"""
from sqlalchemy import bindparam
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import Float
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Table

from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape


engine = create_engine('postgresql://gis:gis@localhost/gis', echo=True)
metadata = MetaData(engine)

table = Table(
    "inserts",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("geom", Geometry("LINESTRING", 4326)),
    Column("distance", Float),
)


class TestLengthAtInsert():

    def setup(self):
        self.conn = engine.connect()
        metadata.drop_all(checkfirst=True)
        metadata.create_all()

    def teardown(self):
        self.conn.close()
        metadata.drop_all()

    def test_query(self):
        conn = self.conn

        # Define geometries to insert
        values = [
            {"ewkt": "SRID=4326;LINESTRING(0 0, 1 0)"},
            {"ewkt": "SRID=4326;LINESTRING(0 0, 0 1)"}
        ]

        # Define the query to compute distance (without spheroid)
        distance = func.ST_Length(func.ST_GeomFromText(bindparam("ewkt")), False)

        i = table.insert()
        i = i.values(geom=bindparam("ewkt"), distance=distance)

        # Execute the query with values as parameters
        conn.execute(i, values)

        # Check the result
        q = select([table])
        res = conn.execute(q).fetchall()

        # Check results
        assert len(res) == 2

        r1 = res[0]
        assert r1[0] == 1
        assert r1[1].srid == 4326
        assert to_shape(r1[1]).wkt == "LINESTRING (0 0, 1 0)"
        assert round(r1[2]) == 111195

        r2 = res[1]
        assert r2[0] == 2
        assert r2[1].srid == 4326
        assert to_shape(r2[1]).wkt == "LINESTRING (0 0, 0 1)"
        assert round(r2[2]) == 111195
