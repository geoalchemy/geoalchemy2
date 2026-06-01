"""This module defines functions used by several dialects."""

from geoalchemy2 import _wkb_wkt
from geoalchemy2.elements import WKBElement
from geoalchemy2.exc import ArgumentError


def is_wkb_constructor(spatial_type):
    return "wkb" in (getattr(spatial_type, "from_text", "") or "").lower()


def is_ewkb_constructor(spatial_type):
    return "ewkb" in (getattr(spatial_type, "from_text", "") or "").lower()


def _validate_wkb_bindvalue_srid(bindvalue, column_srid):
    if column_srid is None or column_srid <= 0:
        return

    if isinstance(bindvalue, bytearray):
        bindvalue = bytes(bindvalue)

    srids = []
    if isinstance(bindvalue, WKBElement):
        if bindvalue.srid > 0:
            srids.append(bindvalue.srid)
        bindvalue = bindvalue.data

    if isinstance(bindvalue, (bytes, bytearray, memoryview, str)):
        srid = _wkb_wkt.wkb_srid(bindvalue)
        if srid is not None and srid > 0:
            srids.append(srid)

    for srid in srids:
        validate_wkb_srid(column_srid, srid)


def as_binary_wkb(bindvalue, *, strip_srid=False, column_srid=None):
    if strip_srid:
        _validate_wkb_bindvalue_srid(bindvalue, column_srid)
        if isinstance(bindvalue, WKBElement):
            bindvalue = bindvalue.data
        if isinstance(bindvalue, bytearray):
            bindvalue = bytes(bindvalue)
        return _wkb_wkt.to_wkb_no_srid(bindvalue)
    elif isinstance(bindvalue, WKBElement):
        bindvalue = bindvalue.data
    if isinstance(bindvalue, memoryview):
        return bindvalue.tobytes()
    if isinstance(bindvalue, str):
        return WKBElement._data_from_desc(bindvalue)
    return bytes(bindvalue)


def as_wkb_hex(bindvalue, *, strip_srid=True, column_srid=None):
    if strip_srid:
        _validate_wkb_bindvalue_srid(bindvalue, column_srid)
        if isinstance(bindvalue, WKBElement):
            bindvalue = bindvalue.data
        if isinstance(bindvalue, bytearray):
            bindvalue = bytes(bindvalue)
        return _wkb_wkt.to_hex_wkb_no_srid(bindvalue).lower()
    if isinstance(bindvalue, WKBElement):
        bindvalue = bindvalue.data
    if isinstance(bindvalue, memoryview):
        bindvalue = bindvalue.tobytes()
    if isinstance(bindvalue, (bytes, bytearray)):
        return bytes(bindvalue).hex()
    return bindvalue.lower()


def validate_wkb_srid(column_srid, srid, *, has_fixed_srid=True):
    if (
        has_fixed_srid
        and column_srid is not None
        and column_srid > 0
        and srid is not None
        and srid > 0
        and srid != column_srid
    ):
        raise ArgumentError(
            f"The SRID ({srid}) of the supplied value is different "
            f"from the one of the column ({column_srid})"
        )


def bind_processor_process(spatial_type, bindvalue):
    return bindvalue  # pragma: no cover
