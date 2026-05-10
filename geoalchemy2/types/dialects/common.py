"""This module defines functions used by several dialects."""

from wkb_wkt_converter import to_wkt


def _wkbelement_to_wkt(bindvalue):
    """Convert a WKBElement to a plain WKT string (no SRID prefix)."""
    return to_wkt(bindvalue.data, srid=False)


def bind_processor_process(spatial_type, bindvalue):
    return bindvalue  # pragma: no cover
