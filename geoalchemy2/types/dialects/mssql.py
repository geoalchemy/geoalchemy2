"""This module defines specific functions for MSSQL dialect."""

import re

from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.elements import _SpatialElement
from geoalchemy2.exc import ArgumentError
from geoalchemy2.shape import to_shape


_WKT_DIMENSION_SUFFIX = re.compile(
    r"^([A-Z]+?)\s*(ZM|Z|M)(\s*\(.*)$",
    re.IGNORECASE | re.DOTALL,
)


def _normalize_wkt_for_mssql(wkt):
    return _WKT_DIMENSION_SUFFIX.sub(r"\1\3", wkt)


def _to_mssql_wkt(value):
    return _normalize_wkt_for_mssql(to_shape(value).wkt)


def bind_processor_process(spatial_type, bindvalue):
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

        if srid is not None and srid != spatial_type.srid:
            raise ArgumentError(
                f"The SRID ({srid}) of the supplied value is different "
                f"from the one of the column ({spatial_type.srid})"
            )
        return _normalize_wkt_for_mssql(wkt_match.group(3))

    if (
        isinstance(bindvalue, _SpatialElement)
        and bindvalue.srid != -1
        and bindvalue.srid != spatial_type.srid
    ):
        raise ArgumentError(
            f"The SRID ({bindvalue.srid}) of the supplied value is different "
            f"from the one of the column ({spatial_type.srid})"
        )

    if isinstance(bindvalue, WKTElement):
        bindvalue = bindvalue.as_wkt()
        if bindvalue.srid <= 0:
            bindvalue.srid = spatial_type.srid
        return _normalize_wkt_for_mssql(bindvalue.data)
    elif isinstance(bindvalue, WKBElement):
        return _to_mssql_wkt(bindvalue)
    elif isinstance(bindvalue, (bytes, bytearray, memoryview)):
        return _to_mssql_wkt(WKBElement(bindvalue))
    return bindvalue
