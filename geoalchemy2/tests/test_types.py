import unittest
import re

from nose.tools import eq_


def eq_sql(a, b, msg=None):
    a = re.sub(r'[\n\t]', '', str(a))
    eq_(a, b, msg)


class TestGeometry(unittest.TestCase):

    def _create_table(self):
        from sqlalchemy import Table, MetaData, Column
        from geoalchemy2.types import Geometry
        table = Table('table', MetaData(), Column('geom', Geometry))
        return table

    def test_column_expression(self):
        from sqlalchemy.sql import select
        table = self._create_table()
        s = select([table.c.geom])
        eq_sql(s, 'SELECT ST_AsBinary("table".geom) AS geom_1 FROM "table"')

    def test_bind_expression(self):
        from sqlalchemy.sql import select
        table = self._create_table()
        s = select(['foo']).where(table.c.geom == 'POINT(1 2)')
        eq_sql(s, 'SELECT foo FROM "table" WHERE ' \
                  '"table".geom = ST_GeomFromText(:geom_1)')
