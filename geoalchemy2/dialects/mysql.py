

def before_create(table, bind, **kw):
    # Remove the spatial indexes from the table metadata because they should not be
    # created during the table.create() step since the associated columns do not exist
    # at this time.
    table.info["_after_create_indexes"] = []
    current_indexes = set(table.indexes)
    for idx in current_indexes:
        for col in table.info["_saved_columns"]:
            if (
                _check_spatial_type(col.type, Geometry, bind.dialect)
                and check_management(col, bind.dialect)
            ) and col in idx.columns.values():
                table.indexes.remove(idx)
                if (
                    idx.name != _spatial_idx_name(table.name, col.name)
                    or not getattr(col.type, "spatial_index", False)
                ):
                    table.info["_after_create_indexes"].append(idx)
    if bind.dialect.name == 'sqlite':
        for col in gis_cols:
            # Add dummy columns with GEOMETRY type
            col._actual_type = col.type
            col.type = _DummyGeometry()
            col.nullable = col._actual_type.nullable
        table.columns = table.info["_saved_columns"]



def after_create(table, bind, **kw):
            # Restore original column list including managed Geometry columns
            table.columns = table.info.pop('_saved_columns')

            for col in table.columns:
                # Add the managed Geometry columns with AddGeometryColumn()
                if (
                    _check_spatial_type(col.type, Geometry, bind.dialect)
                    and check_management(col, bind.dialect)
                ):
                    dimension = col.type.dimension
                    # Add geometry columns for MySQL
                    spec = '%s' % col.type.geometry_type
                    if not col.type.nullable:
                        spec += ' NOT NULL'
                    if col.type.srid > 0:
                        spec += ' SRID %d' % col.type.srid
                    sql = "ALTER TABLE {} ADD COLUMN {} {}".format(table.name, col.name, spec)
                    stmt = text(sql)
                    stmt = stmt.execution_options(autocommit=True)
                    bind.execute(stmt)
                    create_func = None

                # Add spatial indices for the Geometry and Geography columns
                if (
                    _check_spatial_type(col.type, (Geometry, Geography), bind.dialect)
                    and col.type.spatial_index is True
                ):
                    # index_name = 'idx_{}_{}'.format(table.name, col.name)
                    sql = "ALTER TABLE {} ADD SPATIAL INDEX({});".format(table.name,
                                                                         col.name)
                    q = text(sql)
                    bind.execute(q)

    return



def before_drop(table, bind, **kw):
    return



def after_drop(table, bind, **kw):
    # Restore original column list including managed Geometry columns
    table.columns = table.info.pop('_saved_columns')

