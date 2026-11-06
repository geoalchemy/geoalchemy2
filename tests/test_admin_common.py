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


def test_spatial_index_name_resolution_flag_off_default_convention():
    from geoalchemy2 import Geometry

    table = Table("lake", MetaData(), Column("geom", Geometry(spatial_index=True)))
    idx = list(table.indexes)[0]
    assert idx.name == "idx_lake_geom"
    assert idx._column_flag is True
    assert idx.info.get("geoalchemy2_spatial_index") is True


def test_spatial_index_name_resolution_flag_on_default_convention():
    from geoalchemy2 import Geometry

    table = Table(
        "lake",
        MetaData(),
        Column("geom", Geometry(spatial_index=True, follow_naming_convention=True)),
    )
    idx = list(table.indexes)[0]
    assert idx.name == "ix_lake_geom"
    assert idx._column_flag is True
    assert idx.info.get("geoalchemy2_spatial_index") is True


def test_spatial_index_name_resolution_flag_on_default_convention_schema():
    from geoalchemy2 import Geometry

    table = Table(
        "lake",
        MetaData(),
        Column("geom", Geometry(spatial_index=True, follow_naming_convention=True)),
        schema="gis",
    )
    idx = list(table.indexes)[0]
    assert idx.name == "ix_gis_lake_geom"
    assert idx._column_flag is True
    assert idx.info.get("geoalchemy2_spatial_index") is True


def test_spatial_index_name_resolution_flag_on_custom_ix_convention():
    from geoalchemy2 import Geometry

    metadata = MetaData(naming_convention={"ix": "my_idx_%(table_name)s_%(column_0_name)s"})
    table = Table(
        "lake",
        metadata,
        Column("geom", Geometry(spatial_index=True, follow_naming_convention=True)),
    )
    idx = list(table.indexes)[0]
    assert idx.name == "my_idx_lake_geom"
    assert idx._column_flag is True
    assert idx.info.get("geoalchemy2_spatial_index") is True


def test_spatial_index_name_resolution_flag_on_index_class_convention():
    from sqlalchemy import Index

    from geoalchemy2 import Geometry

    metadata = MetaData(naming_convention={Index: "my_idx_%(table_name)s_%(column_0_name)s"})
    table = Table(
        "lake",
        metadata,
        Column("geom", Geometry(spatial_index=True, follow_naming_convention=True)),
    )
    idx = list(table.indexes)[0]
    assert idx.name == "my_idx_lake_geom"
    assert idx._column_flag is True
    assert idx.info.get("geoalchemy2_spatial_index") is True


def test_spatial_index_name_resolution_flag_on_convention_without_ix_falls_back():
    from geoalchemy2 import Geometry

    metadata = MetaData(naming_convention={"uq": "uq_%(table_name)s_%(column_0_name)s"})
    table = Table(
        "lake",
        metadata,
        Column("geom", Geometry(spatial_index=True, follow_naming_convention=True)),
    )
    idx = list(table.indexes)[0]
    assert idx.name == "idx_lake_geom"
    assert idx._column_flag is True
    assert idx.info.get("geoalchemy2_spatial_index") is True


def test_spatial_index_name_resolution_raster_flag_on():
    from geoalchemy2 import Raster

    table = Table(
        "ocean",
        MetaData(),
        Column("rast", Raster(spatial_index=True, follow_naming_convention=True)),
    )
    idx = list(table.indexes)[0]
    assert idx.name == "ix_ocean_rast"
    assert idx._column_flag is True
    assert idx.info.get("geoalchemy2_spatial_index") is True


def test_type_repr_includes_follow_naming_convention():
    from geoalchemy2 import Geography
    from geoalchemy2 import Geometry
    from geoalchemy2 import Raster

    geom = Geometry("POINT", 4326, follow_naming_convention=True)
    assert "follow_naming_convention=True" in repr(geom)

    geog = Geography("POINT", 4326, follow_naming_convention=True)
    assert "follow_naming_convention=True" in repr(geog)

    rast = Raster(follow_naming_convention=True)
    assert "follow_naming_convention=True" in repr(rast)


def test_mysql_after_create_retains_generated_spatial_index_for_multiple_creates(monkeypatch):
    from sqlalchemy.dialects import mysql

    from geoalchemy2 import Geometry
    from geoalchemy2.admin.dialects import mysql as mysql_admin

    table = Table("lake", MetaData(), Column("geom", Geometry(spatial_index=True)))
    executed = []
    bind = type(
        "Bind",
        (),
        {"dialect": mysql.dialect(), "execute": lambda self, q, *a, **k: executed.append(str(q))},
    )()

    mysql_admin.before_create(table, bind)
    mysql_admin.after_create(table, bind)

    # Generated index is kept in table.info, NOT table.indexes
    assert len([i for i in table.indexes if table.c.geom in i.columns.values()]) == 0
    assert "geom" in table.info.get("_generated_spatial_indexes", {})
    assert len(executed) == 1

    # Second cycle preserves the index and executes again without error
    mysql_admin.before_create(table, bind)
    mysql_admin.after_create(table, bind)
    assert len([i for i in table.indexes if table.c.geom in i.columns.values()]) == 0
    assert len(executed) == 2


def test_mysql_after_create_ddl_flag_off_vs_flag_on(monkeypatch):
    from sqlalchemy.dialects import mysql

    from geoalchemy2 import Geometry
    from geoalchemy2.admin.dialects import mysql as mysql_admin

    # Flag off: raw unnamed statement
    table_off = Table("lake", MetaData(), Column("geom", Geometry(spatial_index=True)))
    executed = []
    bind_off = type(
        "Bind",
        (),
        {"dialect": mysql.dialect(), "execute": lambda self, q, *a, **k: executed.append(str(q))},
    )()

    mysql_admin.before_create(table_off, bind_off)
    mysql_admin.after_create(table_off, bind_off)
    assert executed == ["ALTER TABLE lake ADD SPATIAL INDEX(geom);"]

    # Flag on: formatted statement with format_table, format_index, quoted col
    executed.clear()
    table_on = Table(
        "lake",
        MetaData(),
        Column("geom", Geometry(spatial_index=True, follow_naming_convention=True)),
    )
    bind_on = type(
        "Bind",
        (),
        {"dialect": mysql.dialect(), "execute": lambda self, q, *a, **k: executed.append(str(q))},
    )()

    mysql_admin.before_create(table_on, bind_on)
    mysql_admin.after_create(table_on, bind_on)
    assert executed == ["ALTER TABLE lake ADD SPATIAL INDEX ix_lake_geom(geom);"]

    # Flag on with reserved words: identifier_preparer quotes them
    executed.clear()
    table_reserved = Table(
        "order",
        MetaData(),
        Column("select", Geometry(spatial_index=True, follow_naming_convention=True)),
    )
    mysql_admin.before_create(table_reserved, bind_on)
    mysql_admin.after_create(table_reserved, bind_on)
    assert executed == ["ALTER TABLE `order` ADD SPATIAL INDEX ix_order_select(`select`);"]


def test_sqlite_after_create_retains_generated_spatial_index_for_multiple_creates(monkeypatch):
    from sqlalchemy.dialects import sqlite

    from geoalchemy2 import Geometry
    from geoalchemy2.admin.dialects import sqlite as sqlite_admin

    table = Table("lake", MetaData(), Column("geom", Geometry(spatial_index=True)))
    bind = type("Bind", (), {"dialect": sqlite.dialect(), "execute": lambda *a, **k: None})()
    created = []
    monkeypatch.setattr(sqlite_admin, "create_spatial_index", lambda *a, **k: created.append(a))

    sqlite_admin.before_create(table, bind)
    sqlite_admin.after_create(table, bind)

    assert len([i for i in table.indexes if table.c.geom in i.columns.values()]) == 0
    assert "geom" in table.info.get("_generated_spatial_indexes", {})
    assert len(created) == 1

    # Second cycle
    sqlite_admin.before_create(table, bind)
    sqlite_admin.after_create(table, bind)
    assert len([i for i in table.indexes if table.c.geom in i.columns.values()]) == 0
    assert len(created) == 2


def test_postgresql_managed_path_retains_generated_spatial_index(monkeypatch):
    from sqlalchemy.dialects import postgresql

    from geoalchemy2 import Geometry
    from geoalchemy2.admin.dialects import postgresql as postgresql_admin

    table = Table(
        "lake",
        MetaData(),
        Column("geom", Geometry(spatial_index=True, use_typmod=False)),
    )
    bind = type("Bind", (), {"dialect": postgresql.dialect(), "execute": lambda *a, **k: None})()
    created = []
    monkeypatch.setattr(postgresql_admin, "create_spatial_index", lambda *a, **k: created.append(a))

    postgresql_admin.before_create(table, bind)
    postgresql_admin.after_create(table, bind)

    assert len([i for i in table.indexes if table.c.geom in i.columns.values()]) == 0
    assert "geom" in table.info.get("_generated_spatial_indexes", {})
    assert len(created) == 1

    # Second cycle
    postgresql_admin.before_create(table, bind)
    postgresql_admin.after_create(table, bind)
    assert len([i for i in table.indexes if table.c.geom in i.columns.values()]) == 0
    assert len(created) == 2
