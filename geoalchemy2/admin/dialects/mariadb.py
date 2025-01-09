"""This module defines specific functions for MariaDB dialect."""

from sqlalchemy.ext.compiler import compiles

from geoalchemy2 import functions
from geoalchemy2.admin.dialects.mysql import after_create  # noqa
from geoalchemy2.admin.dialects.mysql import after_drop  # noqa
from geoalchemy2.admin.dialects.mysql import before_create  # noqa
from geoalchemy2.admin.dialects.mysql import before_drop  # noqa
from geoalchemy2.admin.dialects.mysql import reflect_geometry_column  # noqa
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.shape import to_shape


def _cast(param):
    if isinstance(param, memoryview):
        param = param.tobytes()
    if isinstance(param, bytes):
        data_element = WKBElement(param)
        param = to_shape(data_element).wkt.encode("utf-8")
    return param


def before_cursor_execute(
    conn, cursor, statement, parameters, context, executemany, convert=True
):  # noqa: D417
    """Event handler to cast the parameters properly.

    Args:
        convert (bool): Trigger the conversion.
    """
    if convert:
        if isinstance(parameters, (tuple, list)):
            parameters = tuple(_cast(x) for x in parameters)
        elif isinstance(parameters, dict):
            for k in parameters:
                parameters[k] = _cast(parameters[k])

    return statement, parameters


_MARIADB_FUNCTIONS = {
    "ST_AsEWKB": "ST_AsBinary",
}


def _compiles_mariadb(cls, fn):
    def _compile_mariadb(element, compiler, **kw):
        return "{}({})".format(fn, compiler.process(element.clauses, **kw))

    compiles(getattr(functions, cls), "mariadb")(_compile_mariadb)


def register_mariadb_mapping(mapping):
    """Register compilation mappings for the given functions.

    Args:
        mapping: Should have the following form::

                {
                    "function_name_1": "mariadb_function_name_1",
                    "function_name_2": "mariadb_function_name_2",
                    ...
                }
    """
    for cls, fn in mapping.items():
        _compiles_mariadb(cls, fn)


register_mariadb_mapping(_MARIADB_FUNCTIONS)


def _compile_GeomFromText_MariaDB(element, compiler, **kw):
    element.identifier = "ST_GeomFromText"
    compiled = compiler.process(element.clauses, **kw)
    try:
        clauses = list(element.clauses)
        data_element = WKTElement(clauses[0].value)
        srid = max(0, data_element.srid)
        if srid <= 0:
            srid = max(0, element.type.srid)
        if len(clauses) > 1 and srid > 0:
            clauses[1].value = srid
    except Exception:
        srid = max(0, element.type.srid)

    if srid > 0:
        res = "{}({}, {})".format(element.identifier, compiled, srid)
    else:
        res = "{}({})".format(element.identifier, compiled)
    return res


def _compile_GeomFromWKB_MariaDB(element, compiler, **kw):
    element.identifier = "ST_GeomFromText"

    try:
        clauses = list(element.clauses)
        data_element = WKBElement(clauses[0].value)
        srid = max(0, data_element.srid)
        if srid <= 0:
            srid = max(0, element.type.srid)
        if len(clauses) > 1 and srid > 0:
            clauses[1].value = srid
    except Exception:
        srid = max(0, element.type.srid)
    compiled = compiler.process(element.clauses, **kw)

    if srid > 0:
        res = "{}({}, {})".format(element.identifier, compiled, srid)
    else:
        res = "{}({})".format(element.identifier, compiled)
    return res


@compiles(functions.ST_GeomFromText, "mariadb")  # type: ignore
def _MariaDB_ST_GeomFromText(element, compiler, **kw):
    return _compile_GeomFromText_MariaDB(element, compiler, **kw)


@compiles(functions.ST_GeomFromEWKT, "mariadb")  # type: ignore
def _MariaDB_ST_GeomFromEWKT(element, compiler, **kw):
    return _compile_GeomFromText_MariaDB(element, compiler, **kw)


@compiles(functions.ST_GeomFromWKB, "mariadb")  # type: ignore
def _MariaDB_ST_GeomFromWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_MariaDB(element, compiler, **kw)


@compiles(functions.ST_GeomFromEWKB, "mariadb")  # type: ignore
def _MariaDB_ST_GeomFromEWKB(element, compiler, **kw):
    return _compile_GeomFromWKB_MariaDB(element, compiler, **kw)
