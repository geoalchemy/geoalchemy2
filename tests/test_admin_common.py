from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table

from geoalchemy2.admin.dialects import common


class _RecordingColumnCollection:
    def __init__(self):
        self.columns = []

    def add(self, column):
        self.columns.append(column)


class _LegacyColumnCollection(_RecordingColumnCollection):
    pass


class _WriteableColumnCollection(_RecordingColumnCollection):
    pass


def _test_table():
    table = Table("lake", MetaData(), Column("id", Integer), Column("depth", Integer))
    return table, table.c.id


def test_update_table_for_dispatch_prefers_writeable_column_collection(monkeypatch):
    table, regular_col = _test_table()
    original_columns = table.columns

    monkeypatch.setattr(common.expression, "ColumnCollection", _LegacyColumnCollection)
    monkeypatch.setattr(
        common.expression, "WriteableColumnCollection", _WriteableColumnCollection, raising=False
    )

    common._update_table_for_dispatch(table, [regular_col])

    assert table.info["_saved_columns"] is original_columns
    assert isinstance(table.columns, _WriteableColumnCollection)
    assert table.columns.columns == [regular_col]


def test_update_table_for_dispatch_falls_back_to_column_collection(monkeypatch):
    table, regular_col = _test_table()
    original_columns = table.columns

    monkeypatch.setattr(common.expression, "ColumnCollection", _LegacyColumnCollection)
    monkeypatch.delattr(common.expression, "WriteableColumnCollection", raising=False)

    common._update_table_for_dispatch(table, [regular_col])

    assert table.info["_saved_columns"] is original_columns
    assert isinstance(table.columns, _LegacyColumnCollection)
    assert table.columns.columns == [regular_col]
