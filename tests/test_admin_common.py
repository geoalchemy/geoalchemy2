import importlib

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


def test_column_collection_class_prefers_writeable_column_collection(monkeypatch):
    try:
        with monkeypatch.context() as m:
            m.setattr(common.expression, "ColumnCollection", _LegacyColumnCollection)
            m.setattr(
                common.expression,
                "WriteableColumnCollection",
                _WriteableColumnCollection,
                raising=False,
            )
            importlib.reload(common)

            assert common._COLUMN_COLLECTION_CLASS is _WriteableColumnCollection
    finally:
        importlib.reload(common)


def test_column_collection_class_falls_back_to_column_collection(monkeypatch):
    try:
        with monkeypatch.context() as m:
            m.setattr(common.expression, "ColumnCollection", _LegacyColumnCollection)
            m.delattr(common.expression, "WriteableColumnCollection", raising=False)
            importlib.reload(common)

            assert common._COLUMN_COLLECTION_CLASS is _LegacyColumnCollection
    finally:
        importlib.reload(common)


def test_update_table_for_dispatch_uses_selected_column_collection(monkeypatch):
    table, regular_col = _test_table()
    original_columns = table.columns

    monkeypatch.setattr(common, "_COLUMN_COLLECTION_CLASS", _WriteableColumnCollection)

    common._update_table_for_dispatch(table, [regular_col])

    assert table.info["_saved_columns"] is original_columns
    assert isinstance(table.columns, _WriteableColumnCollection)
    assert table.columns.columns == [regular_col]
