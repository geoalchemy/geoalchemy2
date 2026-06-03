"""This module defines specific functions for GeoPackage dialect."""

from geoalchemy2.elements import WKBElement
from geoalchemy2.types.dialects.common import as_binary_wkb
from geoalchemy2.types.dialects.common import as_ewkb_hex
from geoalchemy2.types.dialects.common import is_ewkb_constructor
from geoalchemy2.types.dialects.common import is_wkb_constructor
from geoalchemy2.types.dialects.sqlite import (
    bind_processor_process as sqlite_bind_processor_process,
)

__all__ = ["bind_processor_process"]


def bind_processor_process(spatial_type, bindvalue):
    if is_wkb_constructor(spatial_type) and isinstance(
        bindvalue, (WKBElement, bytes, bytearray, memoryview, str)
    ):
        if is_ewkb_constructor(spatial_type):
            return as_ewkb_hex(bindvalue, column_srid=spatial_type.srid)
        return as_binary_wkb(bindvalue)
    return sqlite_bind_processor_process(spatial_type, bindvalue)
