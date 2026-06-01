"""This module defines specific functions for MySQL dialect."""

from geoalchemy2 import _wkb_wkt
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.elements import _SpatialElement
from geoalchemy2.exc import ArgumentError
from geoalchemy2.types.dialects.common import as_binary_wkb
from geoalchemy2.types.dialects.common import is_wkb_constructor
from geoalchemy2.types.dialects.common import validate_wkb_srid


def bind_processor_process(spatial_type, bindvalue):
    if isinstance(bindvalue, str):
        if is_wkb_constructor(spatial_type):
            return as_binary_wkb(bindvalue, strip_srid=True, column_srid=spatial_type.srid)

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
        return wkt_match.group(3)

    if (
        isinstance(bindvalue, _SpatialElement)
        and bindvalue.srid > 0
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
        return bindvalue
    elif isinstance(bindvalue, WKBElement):
        if is_wkb_constructor(spatial_type):
            return as_binary_wkb(bindvalue, strip_srid=True, column_srid=spatial_type.srid)
        else:
            return _wkb_wkt.to_wkt_no_srid(bindvalue.data)
    elif isinstance(bindvalue, (bytes, bytearray, memoryview)):
        if is_wkb_constructor(spatial_type):
            return as_binary_wkb(bindvalue, strip_srid=True, column_srid=spatial_type.srid)
        wkt, srid = _wkb_wkt.split_wkb_srid(bindvalue)
        validate_wkb_srid(spatial_type.srid, srid)
        return wkt
    return bindvalue
