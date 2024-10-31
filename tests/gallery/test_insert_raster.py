"""
Insert Raster
=============

The `RasterElement` objects store the Raster data in WKB form. This WKB format is usually fetched
from the database but when the data comes from another source it can be hard to format it as as a
WKB. This example shows a method to convert input data into a WKB in order to insert it.
This example uses SQLAlchemy ORM queries.

.. warning::
    The PixelType values are not always properly translated by the
    `Rasterio <https://rasterio.readthedocs.io/en/stable/index.html>`_ library, so exporting a
    raster and re-importing it using this method will properly import the values but might not
    keep the same internal types.
"""

import struct
from sys import byteorder

import numpy as np
import pytest
import rasterio
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import text
from sqlalchemy.orm import declarative_base

from geoalchemy2 import Raster
from geoalchemy2 import RasterElement

# Tests imports
from tests import test_only_with_dialects

metadata = MetaData()
Base = declarative_base(metadata=metadata)


class Ocean(Base):  # type: ignore
    __tablename__ = "ocean"
    id = Column(Integer, primary_key=True)
    rast = Column(Raster)

    def __init__(self, rast):
        self.rast = rast


_DTYPE = {
    "?": [0, "?", 1],
    "u1": [2, "B", 1],
    "i1": [3, "b", 1],
    "B": [4, "B", 1],
    "i2": [5, "h", 2],
    "u2": [6, "H", 2],
    "i4": [7, "i", 4],
    "u4": [8, "I", 4],
    "f4": [10, "f", 4],
    "f8": [11, "d", 8],
}


def write_wkb_raster(dataset):
    """Creates a WKB raster from the given raster file with rasterio.
    :dataset: Rasterio dataset
    :returns: binary: Binary raster in WKB format

    This function was imported from
    https://github.com/nathancahill/wkb-raster/blob/master/wkb_raster.py
    and slightly adapted.
    """

    # Define format, see https://docs.python.org/3/library/struct.html
    format_string = "bHHddddddIHH"

    if byteorder == "big":
        endian = ">"
        endian_byte = 0
    elif byteorder == "little":
        endian = "<"
        endian_byte = 1

    # Write the raster header data.
    header = bytes()

    transform = dataset.transform.to_gdal()

    version = 0
    nBands = int(dataset.count)
    scaleX = transform[1]
    scaleY = transform[5]
    ipX = transform[0]
    ipY = transform[3]
    skewX = 0
    skewY = 0
    srid = int(dataset.crs.to_string().split("EPSG:")[1])
    width = int(dataset.meta.get("width"))
    height = int(dataset.meta.get("height"))

    if width > 65535 or height > 65535:
        raise ValueError("PostGIS does not support rasters with width or height greater than 65535")

    fmt = f"{endian}{format_string}"

    header = struct.pack(
        fmt,
        endian_byte,
        version,
        nBands,
        scaleX,
        scaleY,
        ipX,
        ipY,
        skewX,
        skewY,
        srid,
        width,
        height,
    )

    bands = []

    # Create band header data

    # not used - always False
    isOffline = False
    hasNodataValue = False

    if "nodata" in dataset.meta:
        hasNodataValue = True

    # not used - always False
    isNodataValue = False

    # unset
    reserved = False

    # # Based on the pixel type, determine the struct format, byte size and
    # # numpy dtype
    rasterio_dtype = dataset.meta.get("dtype")
    dt_short = np.dtype(rasterio_dtype).str[1:]
    pixtype, nodata_fmt, _ = _DTYPE[dt_short]

    # format binary -> :b
    binary_str = f"{isOffline:b}{hasNodataValue:b}{isNodataValue:b}{reserved:b}{pixtype:b}"
    # convert to int
    binary_decimal = int(binary_str, 2)

    # pack to 1 byte
    # 4 bits for ifOffline, hasNodataValue, isNodataValue, reserved
    # 4 bit for pixtype
    # -> 8 bit = 1 byte
    band_header = struct.pack("<b", binary_decimal)

    # Write the nodata value
    nodata = struct.pack(nodata_fmt, int(dataset.meta.get("nodata") or 0))

    for i in range(1, nBands + 1):
        band_array = dataset.read(i)

        # # Write the pixel values: width * height * size

        # numpy tobytes() method instead of packing with struct.pack()
        band_binary = band_array.reshape(width * height).tobytes()

        bands.append(band_header + nodata + band_binary)

    # join all bands
    allbands = bytes()
    for b in bands:
        allbands += b

    wkb = header + allbands

    return wkb


@test_only_with_dialects("postgresql")
class TestInsertRaster:
    @pytest.fixture(
        params=[
            "1BB",
            "2BUI",
            "4BUI",
            "8BSI",
            "8BUI",
            "16BSI",
            "16BUI",
            "32BSI",
            "32BUI",
            "32BF",
            "64BF",
        ]
    )
    def input_img(self, conn, tmpdir, request):
        """Create a TIFF image that will be imported as Raster."""
        pixel_type = request.param
        conn.execute(text("SET postgis.gdal_enabled_drivers = 'ENABLE_ALL';"))
        data = conn.execute(
            text(
                """SELECT
                    ST_AsTIFF(
                        ST_AsRaster(
                            ST_GeomFromText('POLYGON((0 0,1 1,0 1,0 0))'),
                            5,
                            6,
                            '{}'
                        ),
                        'GTiff'
                    );""".format(
                    pixel_type
                )
            )
        ).scalar()
        filename = tmpdir / "image.tiff"
        with open(filename, "wb") as f:
            f.write(data.tobytes())
        return filename, pixel_type

    def test_insert_raster(self, session, conn, input_img):
        """Insert a TIFF image into a raster column."""
        filename, pixel_type = input_img
        metadata.drop_all(conn, checkfirst=True)
        metadata.create_all(conn)

        # Load the image and transform it into a WKB
        with rasterio.open(str(filename), "r+") as dataset:
            dataset.crs = rasterio.crs.CRS.from_epsg(4326)
            expected_values = dataset.read()[0]
            raw_wkb = write_wkb_raster(dataset)

        # Insert a new raster element
        polygon_raster = RasterElement(raw_wkb)
        o = Ocean(polygon_raster)
        session.add(o)
        session.flush()

        # Check inserted values
        new_values = conn.execute(text("SELECT ST_DumpValues(rast, 1, false) FROM ocean;")).scalar()
        np.testing.assert_array_equal(
            np.array(new_values, dtype=expected_values.dtype), expected_values
        )
