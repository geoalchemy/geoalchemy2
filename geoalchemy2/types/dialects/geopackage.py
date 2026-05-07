"""This module defines specific functions for GeoPackage dialect."""

from geoalchemy2.elements import WKBElement
from geoalchemy2.types.dialects.sqlite import (
    bind_processor_process as sqlite_bind_processor_process,
)

__all__ = ["bind_processor_process"]


def _is_wkb_constructor(spatial_type):
    return "wkb" in (getattr(spatial_type, "from_text", "") or "").lower()


def _as_binary_wkb(bindvalue):
    if isinstance(bindvalue, WKBElement):
        bindvalue = bindvalue.data
    if isinstance(bindvalue, memoryview):
        return bindvalue.tobytes()
    if isinstance(bindvalue, str):
        return WKBElement._data_from_desc(bindvalue)
    return bytes(bindvalue)


def bind_processor_process(spatial_type, bindvalue):
    if _is_wkb_constructor(spatial_type) and isinstance(
        bindvalue, (WKBElement, bytes, memoryview, str)
    ):
        return _as_binary_wkb(bindvalue)
    return sqlite_bind_processor_process(spatial_type, bindvalue)
