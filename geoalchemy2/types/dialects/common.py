"""This module defines functions used by several dialects."""

from geoalchemy2.elements import WKBElement
from geoalchemy2.exc import ArgumentError


def is_wkb_constructor(spatial_type):
    return "wkb" in (getattr(spatial_type, "from_text", "") or "").lower()


def as_binary_wkb(bindvalue, *, strip_srid=False):
    if strip_srid:
        wkb_element = bindvalue if isinstance(bindvalue, WKBElement) else WKBElement(bindvalue)
        bindvalue = wkb_element.as_wkb().data
    elif isinstance(bindvalue, WKBElement):
        bindvalue = bindvalue.data
    if isinstance(bindvalue, memoryview):
        return bindvalue.tobytes()
    if isinstance(bindvalue, str):
        return WKBElement._data_from_desc(bindvalue)
    return bytes(bindvalue)


def as_wkb_hex(bindvalue, *, strip_srid=True):
    if strip_srid:
        wkb_element = bindvalue if isinstance(bindvalue, WKBElement) else WKBElement(bindvalue)
        return wkb_element.as_wkb().desc
    if isinstance(bindvalue, WKBElement):
        bindvalue = bindvalue.data
    if isinstance(bindvalue, memoryview):
        bindvalue = bindvalue.tobytes()
    if isinstance(bindvalue, (bytes, bytearray)):
        return bytes(bindvalue).hex()
    return bindvalue.lower()


def validate_wkb_srid(column_srid, srid, *, has_fixed_srid=True):
    if has_fixed_srid and srid is not None and srid != column_srid:
        raise ArgumentError(
            f"The SRID ({srid}) of the supplied value is different "
            f"from the one of the column ({column_srid})"
        )


def bind_processor_process(spatial_type, bindvalue):
    return bindvalue  # pragma: no cover
