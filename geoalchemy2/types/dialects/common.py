"""This module defines functions used by several dialects."""

from geoalchemy2 import _wkb_wkt
from geoalchemy2._wkb_wkt import is_known_srid
from geoalchemy2.elements import WKBElement
from geoalchemy2.exc import ArgumentError


def is_wkb_constructor(spatial_type):
    return "wkb" in (getattr(spatial_type, "from_text", "") or "").lower()


def is_ewkb_constructor(spatial_type):
    return "ewkb" in (getattr(spatial_type, "from_text", "") or "").lower()


def _validate_wkb_bindvalue_srid(bindvalue, column_srid):
    if not is_known_srid(column_srid):
        return

    if isinstance(bindvalue, bytearray):
        bindvalue = bytes(bindvalue)

    srids = []
    if isinstance(bindvalue, WKBElement):
        if is_known_srid(bindvalue.srid):
            srids.append(bindvalue.srid)
        bindvalue = bindvalue.data

    if isinstance(bindvalue, (bytes, bytearray, memoryview, str)):
        srid = _wkb_wkt.wkb_srid(bindvalue)
        if is_known_srid(srid):
            srids.append(srid)

    for srid in srids:
        validate_wkb_srid(column_srid, srid)


def as_binary_wkb(bindvalue, *, strip_srid=False, column_srid=None):
    if bindvalue is None:
        return None
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


def as_binary_ewkb(bindvalue, *, column_srid=None):
    if bindvalue is None:
        return None

    element_srid = None
    if isinstance(bindvalue, WKBElement):
        if is_known_srid(bindvalue.srid):
            element_srid = bindvalue.srid
        bindvalue = bindvalue.data
    if isinstance(bindvalue, bytearray):
        bindvalue = bytes(bindvalue)

    embedded_srid = None
    if isinstance(bindvalue, (bytes, bytearray, memoryview, str)):
        embedded_srid = _wkb_wkt.wkb_srid(bindvalue)
    if isinstance(bindvalue, str):
        bindvalue = WKBElement._data_from_desc(bindvalue)

    if is_known_srid(element_srid):
        validate_wkb_srid(column_srid, element_srid)
    elif is_known_srid(embedded_srid):
        validate_wkb_srid(column_srid, embedded_srid)

    if is_known_srid(element_srid):
        return _wkb_wkt.to_ewkb_header(bindvalue, element_srid)

    if is_known_srid(embedded_srid):
        return as_binary_wkb(bindvalue)

    if is_known_srid(column_srid):
        return _wkb_wkt.to_ewkb_header(bindvalue, column_srid)

    return as_binary_wkb(bindvalue)


def as_ewkb_hex(bindvalue, *, column_srid=None):
    ewkb = as_binary_ewkb(bindvalue, column_srid=column_srid)
    if ewkb is None:
        return None
    return ewkb.hex()


def as_wkb_hex(bindvalue, *, column_srid=None):
    _validate_wkb_bindvalue_srid(bindvalue, column_srid)
    if isinstance(bindvalue, WKBElement):
        bindvalue = bindvalue.data
    if isinstance(bindvalue, bytearray):
        bindvalue = bytes(bindvalue)
    return _wkb_wkt.to_hex_wkb_no_srid(bindvalue).lower()


def validate_wkb_srid(column_srid, srid, *, has_fixed_srid=True):
    if (
        has_fixed_srid
        and column_srid is not None
        and is_known_srid(column_srid)
        and is_known_srid(srid)
        and srid != column_srid
    ):
        raise ArgumentError(
            f"The SRID ({srid}) of the supplied value is different "
            f"from the one of the column ({column_srid})"
        )


def bind_processor_process(spatial_type, bindvalue):
    return bindvalue  # pragma: no cover
