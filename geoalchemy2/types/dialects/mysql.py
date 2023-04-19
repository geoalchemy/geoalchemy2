"""This module defines specific functions for MySQL dialect."""
from geoalchemy2.elements import WKBElement
from geoalchemy2.elements import WKTElement
from geoalchemy2.elements import _SpatialElement
from geoalchemy2.exc import ArgumentError
from geoalchemy2.shape import to_shape


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
            )

        if srid is not None and srid != spatial_type.srid:
            raise ArgumentError(
                f"The SRID ({srid}) of the supplied value is different "
                f"from the one of the column ({spatial_type.srid})"
            )
        return wkt_match.group(3)

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
        if bindvalue.srid == -1:
            bindvalue.srid = spatial_type.srid
        return bindvalue
    elif isinstance(bindvalue, WKBElement):
        if "wkb" not in spatial_type.from_text.lower():
            # With MySQL we use Shapely to convert the WKBElement to an EWKT string
            return to_shape(bindvalue).wkt
        return bindvalue
