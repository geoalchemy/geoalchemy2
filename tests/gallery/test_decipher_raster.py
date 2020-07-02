"""
Decipher Raster
===============

The `RasterElement` objects store the Raster data in WKB form. When using rasters it is
usually better to convert them into TIFF, PNG, JPEG or whatever. Nevertheless, it is
possible to decipher the WKB to get a 2D list of values.
This example uses SQLAlchemy ORM queries.
"""
import binascii
import struct

import pytest
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from geoalchemy2 import Raster, WKTElement


engine = create_engine('postgresql://gis:gis@localhost/gis', echo=False)
metadata = MetaData(engine)
Base = declarative_base(metadata=metadata)

session = sessionmaker(bind=engine)()


class Ocean(Base):
    __tablename__ = 'ocean'
    id = Column(Integer, primary_key=True)
    rast = Column(Raster)

    def __init__(self, rast):
        self.rast = rast


def _format_e(endianess, struct_format):
    return _ENDIANESS[endianess] + struct_format


def wkbHeader(raw):
    # Function to decipher the WKB header
    # See http://trac.osgeo.org/postgis/browser/trunk/raster/doc/RFC2-WellKnownBinaryFormat

    header = {}

    header['endianess'] = struct.unpack('b', raw[0:1])[0]

    e = header['endianess']
    header['version'] = struct.unpack(_format_e(e, 'H'), raw[1:3])[0]
    header['nbands'] = struct.unpack(_format_e(e, 'H'), raw[3:5])[0]
    header['scaleX'] = struct.unpack(_format_e(e, 'd'), raw[5:13])[0]
    header['scaleY'] = struct.unpack(_format_e(e, 'd'), raw[13:21])[0]
    header['ipX'] = struct.unpack(_format_e(e, 'd'), raw[21:29])[0]
    header['ipY'] = struct.unpack(_format_e(e, 'd'), raw[29:37])[0]
    header['skewX'] = struct.unpack(_format_e(e, 'd'), raw[37:45])[0]
    header['skewY'] = struct.unpack(_format_e(e, 'd'), raw[45:53])[0]
    header['srid'] = struct.unpack(_format_e(e, 'i'), raw[53:57])[0]
    header['width'] = struct.unpack(_format_e(e, 'H'), raw[57:59])[0]
    header['height'] = struct.unpack(_format_e(e, 'H'), raw[59:61])[0]

    return header


def read_band(data, offset, pixtype, height, width, endianess=1):
    ptype, _, psize = _PTYPE[pixtype]
    pix_data = data[offset + 1: offset + 1 + width * height * psize]
    band = [
        [
            struct.unpack(_format_e(endianess, ptype), pix_data[
                (i * width + j) * psize: (i * width + j + 1) * psize
            ])[0]
            for j in range(width)
        ]
        for i in range(height)
    ]
    return band


def read_band_numpy(data, offset, pixtype, height, width, endianess=1):
    import numpy as np  # noqa
    _, dtype, psize = _PTYPE[pixtype]
    dt = np.dtype(dtype)
    dt = dt.newbyteorder(_ENDIANESS[endianess])
    band = np.frombuffer(data, dtype=dtype,
                         count=height * width, offset=offset + 1)
    band = (np.reshape(band, ((height, width))))
    return band


_PTYPE = {
    0: ['?', '?', 1],
    1: ['B', 'B', 1],
    2: ['B', 'B', 1],
    3: ['b', 'b', 1],
    4: ['B', 'B', 1],
    5: ['h', 'i2', 2],
    6: ['H', 'u2', 2],
    7: ['i', 'i4', 4],
    8: ['I', 'u4', 4],
    10: ['f', 'f4', 4],
    11: ['d', 'f8', 8],
}

_ENDIANESS = {
    0: '>',
    1: '<',
}


def wkbImage(raster_data, use_numpy=False):
    """Function to decipher the WKB raster data"""

    # Get binary data
    raw = binascii.unhexlify(raster_data)

    # Read header
    h = wkbHeader(bytes(raw))
    e = h["endianess"]

    img = []  # array to store image bands
    offset = 61  # header raw length in bytes
    band_size = h['width'] * h['height']  # number of pixels in each band

    for i in range(h['nbands']):
        # Determine pixtype for this band
        pixtype = struct.unpack(_format_e(e, 'b'), raw[offset: offset + 1])[0] - 64

        # Read data with either pure Python or Numpy
        if use_numpy:
            band = read_band_numpy(
                raw, offset, pixtype, h['height'], h['width'])
        else:
            band = read_band(
                raw, offset, pixtype, h['height'], h['width'])

        # Store the result
        img.append(band)
        offset = offset + 2 + band_size

    return img


class TestDecipherRaster():

    def setup(self):
        metadata.drop_all(checkfirst=True)
        metadata.create_all()

    def teardown(self):
        session.rollback()
        metadata.drop_all()

    @pytest.mark.parametrize("pixel_type", [
        '1BB',
        '2BUI',
        '4BUI',
        '8BSI',
        '8BUI',
        '16BSI',
        '16BUI',
        '32BSI',
        '32BUI',
        '32BF',
        '64BF'
    ])
    def test_decipher_raster(self, pixel_type):
        """Create a raster and decipher it"""

        # Create a new raster
        polygon = WKTElement('POLYGON((0 0,1 1,0 1,0 0))', srid=4326)
        o = Ocean(polygon.ST_AsRaster(5, 6, pixel_type))
        session.add(o)
        session.flush()

        # Decipher data from each raster
        image = wkbImage(o.rast.data)

        # Define expected result
        expected = [
            [0, 1, 1, 1, 1],
            [1, 1, 1, 1, 1],
            [0, 1, 1, 1, 0],
            [0, 1, 1, 0, 0],
            [0, 1, 0, 0, 0],
            [0, 0, 0, 0, 0]
        ]

        # Check results
        band = image[0]
        assert band == expected
