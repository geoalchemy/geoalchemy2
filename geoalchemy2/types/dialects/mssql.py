"""This module defines specific functions for MSSQL dialect."""

import re

from sqlalchemy.types import TypeDecorator

from geoalchemy2 import _wkb_wkt
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.elements import _SpatialElement
from geoalchemy2.exc import ArgumentError

_WKT_DIMENSION_SUFFIX = re.compile(
    r"\b(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|"
    r"GEOMETRYCOLLECTION)\s*(ZM|Z|M)(?=\s*(?:\(|EMPTY))",
    re.IGNORECASE | re.DOTALL,
)


def _normalize_wkt_for_mssql(wkt):
    return _WKT_DIMENSION_SUFFIX.sub(r"\1", wkt)


def _split_mssql_st_point_args(spec):
    """Split the two top-level args of GeoAlchemy's MSSQL computed-column ST_POINT.

    MSSQL geometry/geography constructors require an explicit SRID argument, and
    geography::Point expects latitude before longitude. We therefore need the
    two ST_POINT arguments separately instead of forwarding the full inner SQL.

    This helper is intentionally just a block-aware argument splitter, not a SQL
    parser. It finds the comma that separates the two outer ST_POINT arguments
    while treating nested calls, quoted strings, and bracketed identifiers as
    opaque blocks, e.g. COALESCE(latitude, 0) stays one argument.
    """
    spec = spec.strip()
    match = re.match(r"ST_POINT\s*\(", spec, flags=re.IGNORECASE)
    if not match:
        return None

    args_start = match.end()
    depth = 1
    comma_index = None
    quote_end = None
    index = args_start
    while index < len(spec):
        char = spec[index]
        if quote_end is not None:
            if char == quote_end:
                if (
                    quote_end in ("'", '"', "]")
                    and index + 1 < len(spec)
                    and spec[index + 1] == char
                ):
                    index += 2
                    continue
                quote_end = None
            index += 1
            continue

        if char in ("'", '"'):
            quote_end = char
        elif char == "[":
            quote_end = "]"
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                if spec[index + 1 :].strip() or comma_index is None:
                    return None
                x_expr = spec[args_start:comma_index].strip()
                y_expr = spec[comma_index + 1 : index].strip()
                if not x_expr or not y_expr:
                    return None
                return x_expr, y_expr
            if depth < 0:
                return None
        elif char == "," and depth == 1:
            if comma_index is not None:
                return None
            comma_index = index
        index += 1

    return None


def _coerce_wkb_data(value):
    if isinstance(value, WKBElement):
        value = value.data
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, str):
        return value
    raise TypeError("Unsupported WKB value type")


def _wkb_to_mssql_wkt(value):
    data = _coerce_wkb_data(value)
    try:
        return _normalize_wkt_for_mssql(_wkb_wkt.to_wkt_no_srid(data))
    except ValueError as exc:
        message = str(exc)
        message_lower = message.lower()
        if "unsupported geometry type" in message_lower:
            raise ValueError(f"Unsupported WKB geometry type: {message}") from None
        if "invalid byte order marker" in message_lower:
            raise ValueError(f"Invalid WKB byte order marker: {message}") from None
        raise


def _split_wkb_to_mssql_wkt(value):
    data = _coerce_wkb_data(value)
    try:
        wkt, srid = _wkb_wkt.split_wkb_srid(data)
    except ValueError as exc:
        message = str(exc)
        message_lower = message.lower()
        if "unsupported geometry type" in message_lower:
            raise ValueError(f"Unsupported WKB geometry type: {message}") from None
        if "invalid byte order marker" in message_lower:
            raise ValueError(f"Invalid WKB byte order marker: {message}") from None
        raise
    return _normalize_wkt_for_mssql(wkt), srid


def _to_mssql_wkt(value):
    if isinstance(value, WKTElement):
        return _normalize_wkt_for_mssql(_wkb_wkt.to_wkt_no_srid(value.data))
    return _wkb_to_mssql_wkt(value)


def _validate_wkb_srid(column_srid, has_fixed_srid, srid):
    if has_fixed_srid and srid is not None and srid > 0 and srid != column_srid:
        raise ArgumentError(
            f"The SRID ({srid}) of the supplied value is different "
            f"from the one of the column ({column_srid})"
        )


def _resolve_mssql_spatial_type(spatial_type, dialect):
    if isinstance(spatial_type, TypeDecorator) and dialect is not None:
        return spatial_type.load_dialect_impl(dialect)
    return spatial_type


def bind_processor_process(spatial_type, bindvalue, dialect=None):
    spatial_type = _resolve_mssql_spatial_type(spatial_type, dialect)
    column_srid = spatial_type.srid
    has_fixed_srid = column_srid >= 0
    if isinstance(bindvalue, str):
        wkt_match = WKTElement._REMOVE_SRID.match(bindvalue)
        srid = wkt_match.group(2)
        try:
            if srid is not None:
                srid = int(srid)
        except (ValueError, TypeError):  # pragma: no cover
            raise ArgumentError(
                f"The SRID ({srid}) of the supplied value can not be casted to integer"
            ) from None

        if has_fixed_srid and srid is not None and srid != column_srid:
            raise ArgumentError(
                f"The SRID ({srid}) of the supplied value is different "
                f"from the one of the column ({column_srid})"
            )
        return _normalize_wkt_for_mssql(wkt_match.group(3))

    if (
        isinstance(bindvalue, _SpatialElement)
        and has_fixed_srid
        and bindvalue.srid != -1
        and bindvalue.srid != column_srid
    ):
        raise ArgumentError(
            f"The SRID ({bindvalue.srid}) of the supplied value is different "
            f"from the one of the column ({column_srid})"
        )

    if isinstance(bindvalue, WKTElement):
        bindvalue = bindvalue.as_wkt()
        if bindvalue.srid <= 0:
            bindvalue.srid = spatial_type.srid
        return _normalize_wkt_for_mssql(bindvalue.data)
    elif isinstance(bindvalue, WKBElement):
        return _to_mssql_wkt(bindvalue)
    elif isinstance(bindvalue, (bytes, bytearray, memoryview)):
        wkt, srid = _split_wkb_to_mssql_wkt(bindvalue)
        _validate_wkb_srid(column_srid, has_fixed_srid, srid)
        return wkt
    return bindvalue
