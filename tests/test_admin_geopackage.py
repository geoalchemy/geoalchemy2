from unittest.mock import MagicMock

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect

from geoalchemy2.admin.dialects import geopackage


def test_after_create_registers_non_spatial_table_in_gpkg_contents():
    table = Table("non_spatial_table", MetaData(), Column("id", Integer, primary_key=True))
    table.info["_after_create_indexes"] = []
    bind = MagicMock()
    bind.dialect = sqlite_dialect()

    geopackage.after_create(table, bind)

    bind.execute.assert_called_once()
    stmt = bind.execute.call_args.args[0]
    assert "INSERT INTO gpkg_contents" in str(stmt)
    assert "'attributes'" in str(stmt)
    assert stmt.compile().params["table_name"] == "non_spatial_table"


def test_before_drop_unregisters_non_spatial_table_from_gpkg_contents():
    table = Table("non_spatial_table", MetaData(), Column("id", Integer, primary_key=True))
    bind = MagicMock()
    bind.dialect = sqlite_dialect()

    geopackage.before_drop(table, bind)

    bind.execute.assert_called_once()
    stmt = bind.execute.call_args.args[0]
    assert "DELETE FROM gpkg_contents" in str(stmt)
    assert stmt.compile().params["table_name"] == "non_spatial_table"
