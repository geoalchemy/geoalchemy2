"""This module defines functions used by several dialects."""

from wkb_wkt_converter import text_to_wkt
from wkb_wkt_converter import wkb_to_wkt_split_srid


def _wkbelement_to_wkt(bindvalue):
    """Convert a WKBElement to a plain WKT string (no SRID prefix)."""
    data = bindvalue.data
    if isinstance(data, str):
        return text_to_wkt(data, srid=False)
    data = data.tobytes() if isinstance(data, memoryview) else bytes(data)
    wkt, _ = wkb_to_wkt_split_srid(data)
    return wkt


def bind_processor_process(spatial_type, bindvalue):
    return bindvalue  # pragma: no cover
