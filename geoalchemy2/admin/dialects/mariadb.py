"""This module defines specific functions for MariaDB dialect."""

from sqlalchemy.ext.compiler import compiles

from geoalchemy2 import functions
from geoalchemy2.admin.dialects.common import compile_bin_literal
from geoalchemy2.admin.dialects.common import unwrap_wkb_constructor_clauses
from geoalchemy2.admin.dialects.mysql import _coerce_ewkb_bind_clause_to_wkb
from geoalchemy2.admin.dialects.mysql import _coerce_ewkb_clause_to_wkb
from geoalchemy2.admin.dialects.mysql import _coerce_known_ewkb_clause_to_wkb
from geoalchemy2.admin.dialects.mysql import _compile_srid_arg
from geoalchemy2.admin.dialects.mysql import _dynamic_ewkb_default_srid
from geoalchemy2.admin.dialects.mysql import _dynamic_ewkb_split_disabled
from geoalchemy2.admin.dialects.mysql import _dynamic_inferred_srid_bind_clauses
from geoalchemy2.admin.dialects.mysql import _ewkb_processor_fixed_srid
from geoalchemy2.admin.dialects.mysql import _get_mysql_dynamic_ewkb_bind_clauses
from geoalchemy2.admin.dialects.mysql import _has_effective_srid_argument
from geoalchemy2.admin.dialects.mysql import _is_bindparam_clause
from geoalchemy2.admin.dialects.mysql import _runtime_ewkb_processor_context
from geoalchemy2.admin.dialects.mysql import after_create  # noqa
from geoalchemy2.admin.dialects.mysql import after_drop  # noqa
from geoalchemy2.admin.dialects.mysql import before_create  # noqa
from geoalchemy2.admin.dialects.mysql import before_drop  # noqa
from geoalchemy2.admin.dialects.mysql import before_execute  # noqa
from geoalchemy2.admin.dialects.mysql import reflect_geometry_column  # noqa
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement


def _cast(param):
    if isinstance(param, memoryview):
        param = param.tobytes()
    if isinstance(param, bytearray):
        param = bytes(param)
    if isinstance(param, bytes):
        param = WKBElement(param)
    if isinstance(param, WKBElement):
        param = param.as_wkb().desc
    return param


def before_cursor_execute(conn, cursor, statement, parameters, context, executemany, convert=True):  # noqa: D417
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
        return f"{fn}({compiler.process(element.clauses, **kw)})"

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
    identifier = "ST_GeomFromText"
    compiled = compiler.process(element.clauses, **kw)
    try:
        clauses = list(element.clauses)
        data_element = WKTElement(clauses[0].value)
        srid = data_element.srid
        if srid <= 0:
            srid = element.type.srid
    except Exception:
        srid = element.type.srid

    res = f"{identifier}({compiled}, {srid})" if srid > 0 else f"{identifier}({compiled})"
    return res


def _compile_GeomFromWKB_MariaDB(element, compiler, *, coerce_ewkb=False, **kw):
    identifier = "ST_GeomFromWKB"
    # Store the SRID
    clauses = list(element.clauses)
    if kw.get("literal_binds", False):
        clauses, _ = unwrap_wkb_constructor_clauses(clauses)

    inferred_srid = None
    dynamic_srid_clause = None
    if coerce_ewkb:
        original_wkb_clause = clauses[0]
        split_disabled = _dynamic_ewkb_split_disabled(compiler) or kw.get("literal_binds", False)
        should_process_at_runtime = (
            _is_bindparam_clause(original_wkb_clause)
            and not kw.get("literal_binds", False)
            and not split_disabled
            and (
                getattr(original_wkb_clause, "value", None) is None
                or getattr(original_wkb_clause, "callable", None) is not None
            )
        )
        if should_process_at_runtime:
            has_srid_argument = _has_effective_srid_argument(clauses)
            dynamic_default_srid = _dynamic_ewkb_default_srid(element, clauses)
            if dynamic_default_srid is not None:
                wkb_value_clause, dynamic_srid_clause = _get_mysql_dynamic_ewkb_bind_clauses(
                    original_wkb_clause,
                    compiler,
                    default_srid=dynamic_default_srid,
                    as_hex=True,
                )
            else:
                fixed_srid = None
                if not has_srid_argument:
                    fixed_srid = _ewkb_processor_fixed_srid(element, clauses)
                processor_context = _runtime_ewkb_processor_context(
                    compiler,
                    original_wkb_clause,
                    fixed_srid=fixed_srid,
                    has_srid_argument=has_srid_argument,
                )
                wkb_value_clause = _coerce_ewkb_bind_clause_to_wkb(
                    original_wkb_clause,
                    as_hex=True,
                    fixed_srid=fixed_srid,
                    has_srid_argument=has_srid_argument,
                    processor_context=processor_context,
                )
        else:
            _, inferred_srid = _coerce_ewkb_clause_to_wkb(original_wkb_clause, as_hex=True)
            dynamic_wkb_clause, dynamic_srid_clause = _dynamic_inferred_srid_bind_clauses(
                element,
                compiler,
                original_wkb_clause,
                clauses,
                inferred_srid=inferred_srid,
                as_hex=True,
                split_disabled=split_disabled,
            )
            if dynamic_wkb_clause is not None:
                wkb_value_clause = dynamic_wkb_clause
            else:
                wkb_value_clause, inferred_srid = _coerce_known_ewkb_clause_to_wkb(
                    element,
                    compiler,
                    original_wkb_clause,
                    clauses,
                    as_hex=True,
                    preserve_user_bind=not kw.get("literal_binds", False),
                )
    else:
        wkb_value_clause = clauses[0]

    wkb_clause = wkb_value_clause
    if kw.get("literal_binds", False):
        wkb_clause = compile_bin_literal(wkb_value_clause)
    prefix = "unhex("
    suffix = ")"

    compiled = compiler.process(wkb_clause, **kw)
    compiled_srid = _compile_srid_arg(
        element,
        clauses,
        inferred_srid,
        compiler,
        srid_clause=dynamic_srid_clause,
        **kw,
    )

    if compiled_srid is not None:
        return f"{identifier}({prefix}{compiled}{suffix}, {compiled_srid})"
    else:
        return f"{identifier}({prefix}{compiled}{suffix})"


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
    return _compile_GeomFromWKB_MariaDB(element, compiler, coerce_ewkb=True, **kw)
