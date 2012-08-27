from .types import (  # NOQA
    WKTElement,
    WKBElement,
    Geometry,
    LineString
    )

from .sql.functions import (  # NOQA
    Buffer
    )

from sqlalchemy import Table, event
from sqlalchemy.sql import select, func, expression


def _setup_ddl_events():
    @event.listens_for(Table, "before_create")
    def before_create(target, connection, **kw):
        dispatch("before-create", target, connection)

    @event.listens_for(Table, "after_create")
    def after_create(target, connection, **kw):
        dispatch("after-create", target, connection)

    @event.listens_for(Table, "before_drop")
    def before_drop(target, connection, **kw):
        dispatch("before-drop", target, connection)

    @event.listens_for(Table, "after_drop")
    def after_drop(target, connection, **kw):
        dispatch("after-drop", target, connection)

    def dispatch(event, table, bind):
        if event in ('before-create', 'before-drop'):
            regular_cols = [c for c in table.c if not
                                    isinstance(c.type, Geometry)]
            gis_cols = set(table.c).difference(regular_cols)
            table.info["_saved_columns"] = table.c

            # temporarily patch a set of columns not including the
            # Geometry columns
            table.columns = expression.ColumnCollection(*regular_cols)

            if event == 'before-drop':
                for c in gis_cols:
                    stmt = select([
                        func.DropGeometryColumn('public', table.name, c.name)])
                    stmt = stmt.execution_options(autocommit=True)
                    bind.execute(stmt)

        elif event == 'after-create':
            table.columns = table.info.pop('_saved_columns')
            for c in table.c:
                if isinstance(c.type, Geometry):
                    stmt = select([
                        func.AddGeometryColumn(
                            table.name, c.name,
                            c.type.srid,
                            c.type.name,
                            c.type.dimension)])
                    stmt = stmt.execution_options(autocommit=True)
                    bind.execute(stmt)

        elif event == 'after-drop':
            table.columns = table.info.pop('_saved_columns')
_setup_ddl_events()
