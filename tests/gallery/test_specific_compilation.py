"""
Function translation for specific dialect
=========================================

Some functions have different names depending on the dialect. But sometimes one function in one
dialect can be mapped to several other functions in another dialect, depending on the arguments
passed. For example, the `ST_Buffer` function in PostgreSQL can translate into 2 functions in
SQLite:

1. if the buffer is two-sided (symmetric), the PostgreSQL function::

    ST_Buffer(the_table.geom, 10)

  should become in SQLite::

    Buffer(the_table.geom, 10)

2. if the buffer is one-sided, the PostgreSQL function::

    ST_Buffer(the_table.geom, 10, 'side=right')

  should become in SQLite::

    SingleSidedBuffer(the_table.geom, 10, 0)

This case is much more complicated than just mapping a function name and we show here how to deal
with it.

This example uses SQLAlchemy core queries.
"""
from sqlalchemy import MetaData
from sqlalchemy import func
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import BindParameter

from geoalchemy2 import WKTElement
from geoalchemy2 import functions

# Tests imports
from tests import format_wkt
from tests import select

metadata = MetaData()
Base = declarative_base(metadata=metadata)


def _compile_buffer_default(element, compiler, **kw):
    """Compile the element in the default case (no specific dialect).

    This function should not be needed for SQLAlchemy >= 1.1.
    """
    return '{}({})'.format('ST_Buffer', compiler.process(element.clauses, **kw))


def _compile_buffer_sqlite(element, compiler, **kw):
    """Compile the element for the SQLite dialect."""
    # Get the side parameters
    compiled = compiler.process(element.clauses, **kw)
    side_params = [
        i for i in element.clauses
        if isinstance(i, BindParameter) and 'side' in str(i.value)
    ]

    if side_params:
        side_param = side_params[0]
        if 'right' in side_param.value:
            # If the given side is 'right', we translate the value into 0 and switch to the sided
            # function
            side_param.value = 0
            element.identifier = 'SingleSidedBuffer'
        elif 'left' in side_param.value:
            # If the given side is 'left', we translate the value into 1 and switch to the sided
            # function
            side_param.value = 1
            element.identifier = 'SingleSidedBuffer'

    if element.identifier == 'ST_Buffer':
        # If the identifier is still the default ST_Buffer we switch to the SpatiaLite function
        element.identifier = 'Buffer'

    # If there is no side parameter or if the side value is 'both', we use the default function
    return '{}({})'.format(element.identifier, compiled)


# Register the specific compilation rules
compiles(functions.ST_Buffer)(_compile_buffer_default)
compiles(functions.ST_Buffer, 'sqlite')(_compile_buffer_sqlite)


def test_specific_compilation(conn):
    # Build a query with a sided buffer
    query = select([
        func.ST_AsText(
            func.ST_Buffer(WKTElement('LINESTRING(0 0, 1 0)', srid=4326), 1, 'side=left')
        )
    ])

    # Check the compiled query: the sided buffer should appear only in the SQLite query
    compiled_query = str(query.compile(dialect=conn.dialect))
    if conn.dialect.name == 'sqlite':
        assert 'SingleSidedBuffer' in compiled_query
        assert 'ST_Buffer' not in compiled_query
    else:
        assert 'SingleSidedBuffer' not in compiled_query
        assert 'ST_Buffer' in compiled_query

    # Check the actual result of the query
    res = conn.execute(query).scalar()
    assert format_wkt(res) == 'POLYGON((1 0,0 0,0 1,1 1,1 0))'

    # Build a query with symmetric buffer to check nothing was broken
    query = select([
        func.ST_AsText(
            func.ST_Buffer(WKTElement('LINESTRING(0 0, 1 0)', srid=4326), 1)
        )
    ])

    # Check the compiled query: the sided buffer should never appear in the query
    compiled_query = str(query.compile(dialect=conn.dialect))
    assert 'SingleSidedBuffer' not in compiled_query
    if conn.dialect.name == 'sqlite':
        assert 'ST_Buffer' not in compiled_query
        assert 'Buffer' in compiled_query
    else:
        assert 'ST_Buffer' in compiled_query

    # Check the actual result of the query
    res = conn.execute(query).scalar()
    assert format_wkt(res) != 'POLYGON((1 0,0 0,0 1,1 1,1 0))'
    assert format_wkt(res).startswith('POLYGON((1 1,1')
