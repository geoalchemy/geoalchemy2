""" WKB Serialization and Deserialization

TODO/MEMO/CONSIDER:
    * Consider the factor of mutability and eventually change the named tuples
      to lightweight/dictless classes (__slots__). Eventually it would not even
      be necessary if you keep the aggregations in the named tuples as
      type->list (is mutable).
    * Serialization (see point of mutability, since there is no point in
      serializing something you deserialized to something inmutable).

Beware WKB dumper has not been tested for proper type frame unpacking (if there
is)

---------------
See:
http://publib.boulder.ibm.com/infocenter/db2luw/v8/index.jsp?topic=/com.ibm.db2
.udb.doc/opt/rsbp4121.htm
"""
#TODO: Giszmo May 25, 2013: This class by far does *NOT* implement the full
# WKB standard. Read the details about the standard here:
#
#       http://www.opengeospatial.org/standards/sfa
#       pages 62 - 72
#
# The code is proof of work (and eventually works for my project).
# Missing from the standard is all the 3D and 4D stuff as well as Triangles,
# Curves and Surfaces.
# Following the standard would most likely allow to strip tons of redundant
# code. (Sorry, version 1 of this file is based on reverse engeneering and
# Wikipedia only)
#TODO: Giszmo May 25, 2013: Endianness is read correctly but can not be changed
# yet when writing. The machine's endianness is taken.
import sys
import struct
from collections import namedtuple

""" ===========================================================================
    WKB Types
===============================================================================
"""
#TODO: Maybe type assertions to make the usage of the named tuples more robust.
# [0] - Geometry
_Geometry = namedtuple('Geometry', 'wkb_id, geometry')
def Geometry(geometry):
    return _Geometry(0, geometry)

# [1] - Point
_Point = namedtuple('Point', 'wkb_id, x, y')
def Point(x, y):
    return _Point(1, x, y)

# [2] - LineString
_LineString = namedtuple('LineString', 'wkb_id, points')
def LineString(points):
    return _LineString(2, points)

# [3] - Polygon
_Polygon = namedtuple('Polygon', 'wkb_id, rings')
def Polygon(rings):
    return _Polygon(3, rings)

# [4] - MultiPoint
_MultiPoint = namedtuple('MultiPoint', 'wkb_id, points')
def MultiPoint(points):
    return _MultiPoint(4, points)

# [5] - MultiLineString
_MultiLineString = namedtuple('MultiLineString', 'wkb_id, linestrings')
def MultiLineString(linestrings):
    return _MultiLineString(5, linestrings)

# [6] - MultiPolygon
_MultiPolygon = namedtuple('MultiPolygon', 'wkb_id, polygons')
def MultiPolygon(polygons):
    return _MultiPolygon(6, polygons)

# [7] - GeometryCollection
_GeometryCollection = namedtuple('GeometryCollection', 'wkb_id, geometries')
def GeometryCollection(geometries):
    return _GeometryCollection(7, geometries)

# [8] - CircularString
_CircularString = namedtuple('CircularString', 'wkb_id, ')
def CircularString():
    raise NotImplementedError
    return _CircularString(8)

# [9] - CompoundCurve
_CompoundCurve = namedtuple('CompoundCurve', 'wkb_id, ')
def CompoundCurve():
    raise NotImplementedError
    return _CompoundCurve(9)

# [10] - CurvePolygon
_CurvePolygon = namedtuple('CurvePolygon', 'wkb_id, ')
def CurvePolygon():
    raise NotImplementedError
    return _CurvePolygon(10)

# [11] - MultiCurve
_MultiCurve = namedtuple('MultiCurve', 'wkb_id, ')
def MultiCurve():
    raise NotImplementedError
    return _MultiCurve(11)

# [12] - MultiSurface
_MultiSurface = namedtuple('MultiSurface', 'wkb_id, ')
def MultiSurface():
    raise NotImplementedError
    return _MultiSurface(12)

# [13] - Curve
_Curve = namedtuple('Curve', 'wkb_id, ')
def Curve():
    raise NotImplementedError
    return _Curve(13)

# [14] - Surface
_Surface = namedtuple('Surface', 'wkb_id, ')
def Surface():
    raise NotImplementedError
    return _Surface(14)

# [15] - PolyhedralSurface
_PolyhedralSurface = namedtuple('PolyhedralSurface', 'wkb_id, ')
def PolyhedralSurface():
    raise NotImplementedError
    return _PolyhedralSurface(15)

# [16] - TriangularIrregularNetwork
_TriIrregNetwork = namedtuple('TriangularIrregularNetwork', 'wkb_id, ')
def TriIrregNetwork():
    raise NotImplementedError
    return _TriIrregNetwork(16)

# [17] - Triangle
_Triangle = namedtuple('Triangle', 'wkb_id, p1, p2, p3')
def Triangle(p1, p2, p3):
    return _Triangle(17, p1, p2, p3)

""" ===========================================================================
    Helpers
=========================================================================== """
# Buffer Type (helper)
class WKBBuffer(object):
    __slots__ = ('buffer',
                 'bytorder',
                 'bytorder_byte',
                 'offset')

    def __init__(self, buffer=b''):
        self.buffer = buffer
        self.bytorder = "<"
        self.offset = 0

    def unpack(self, typestr):
        # Extract the data
        value_tpl = struct.unpack_from(self.bytorder + typestr,
                                       self.buffer,
                                       self.offset)
        # Update offset
        self.offset += struct.calcsize(typestr)
        # print(value_tpl)
        return value_tpl

    def pack(self, typestr, *args):
        #TODO: Giszmo May 24, 2013: ctypes.create_string_buffer could be faster
        self.buffer += struct.pack(self.bytorder + typestr, *args)

""" ===========================================================================
    WKB Deserialization
=========================================================================== """
def load_wkb(buffer):
    #TODO: Giszmo May 25, 2013: implement handling of memoryview
    if not isinstance(buffer, bytes):
        raise TypeError("Expecting binary data in a string of bytes, got"
                        " instead: {0}".format(buffer.__class__.__name__))
    # Create the helper
    wkb_buff = WKBBuffer(buffer)

    return load_geometry(wkb_buff)

def load_geometry(wkb_buff):
    # Extract header
    # First byte is 0|¬0 regardless of byte order
    wkb_buff.bytorder_byte = wkb_buff.unpack('b')[0]
    # Anything ¬0 is little endian:
    wkb_buff.bytorder = '>' if wkb_buff.bytorder_byte == 0 else '<'

    wkb_type = wkb_buff.unpack('I')[0]
    if not wkb_type in deserializer.keys():
        raise TypeError("Unexpecte wkb_type {0}.".format(wkb_type))
    return deserializer[wkb_type](wkb_buff)

def load_point(wkb_buff):
    x, y = wkb_buff.unpack('dd')
    return Point(x, y)

def load_linestring(wkb_buff):
    num_points = wkb_buff.unpack('I')[0]

    points = []
    for unused in range(num_points):
        points.append(load_point(wkb_buff))
    return LineString(points)

def load_polygon(wkb_buff):
    num_rings = wkb_buff.unpack('I')[0]

    rings = []
    for unused in range(num_rings):
        rings.append(load_linestring(wkb_buff))
    return Polygon(rings)

def load_multi_point(wkb_buff):
    num_points = wkb_buff.unpack('I')[0]

    points = []
    for _ in range(num_points):
        points.append(load_geometry(wkb_buff))
    return MultiPoint(points)

def load_multi_linestring(wkb_buff):
    num_linestrings = wkb_buff.unpack('I')[0]

    linestrings = []
    for _ in range(num_linestrings):
        linestrings.append(load_geometry(wkb_buff))
    return MultiLineString(linestrings)

def load_multi_polygon(wkb_buff):
    num_polys = wkb_buff.unpack('I')[0]

    polys = []
    for _ in range(num_polys):
        polys.append(load_geometry(wkb_buff))
    return MultiPolygon(polys)

def load_geometry_collection(wkb_buff):
    num_geometries = wkb_buff.unpack('I')[0]
    geometries = []
    for _ in range(num_geometries):
        geometries.append(load_geometry(wkb_buff))
    return GeometryCollection(geometries)

def load_circular_string(wkb_buff):
    raise NotImplementedError

def load_compound_curve(wkb_buff):
    raise NotImplementedError

def load_curve_polygon(wkb_buff):
    raise NotImplementedError

def load_multi_curve(wkb_buff):
    raise NotImplementedError

def load_multi_surface(wkb_buff):
    raise NotImplementedError

def load_curve(wkb_buff):
    raise NotImplementedError

def load_surface(wkb_buff):
    raise NotImplementedError

def load_polyhedral_surface(wkb_buff):
    raise NotImplementedError

def load_triangular_irregular_network(wkb_buff):
    raise NotImplementedError

def load_triangle(wkb_buff):
    raise NotImplementedError

# Lookup by enumeration
deserializer = {0:  load_geometry,
                1:  load_point,
                2:  load_linestring,
                3:  load_polygon,
                4:  load_multi_point,
                5:  load_multi_linestring,
                6:  load_multi_polygon,
                7:  load_geometry_collection,
                8:  load_circular_string,
                9:  load_compound_curve,
                10: load_curve_polygon,
                11: load_multi_curve,
                12: load_multi_surface,
                13: load_curve,
                14: load_surface,
                15: load_polyhedral_surface,
                16: load_triangular_irregular_network,
                17: load_triangle}

""" ===========================================================================
    WKB Serialization (TODO)
=========================================================================== """
def dump_wkb(wkb_obj):
    if not isinstance(wkb_obj, tuple(serializer.keys())):
        raise TypeError("Expecting any structure of WKB type objects, got"
                        " instead: {0}".format(wkb_obj.__class__.__name__))
    # Create buffer object
    wkb_buff = WKBBuffer()
    # Determine byte order of this machine (avoid reordering every time we want
    # to pack something)
    if sys.byteorder == 'little':
        wkb_buff.bytorder = '<'
        wkb_buff.bytorder_byte = 1
    else:
        wkb_buff.bytorder = '>'
        wkb_buff.bytorder_byte = 0
    # dump
    dump_geometry(wkb_obj, wkb_buff)
    return wkb_buff.buffer

def dump_geometry(wkb_obj, wkb_buff):
    wkb_buff.pack('b', wkb_buff.bytorder_byte)

    # Get the appropriate packer for the WKB object type, and dispatch packing
    packer = serializer[type(wkb_obj)]
    packer(wkb_obj, wkb_buff)

def dump_point(point, wkb_buff):
    """ type x y """
    wkb_buff.pack('Idd', point.wkb_id, point.x, point.y)

def dump_linestring(linestring, wkb_buff):
    """ type count [ x y ]* """
    wkb_buff.pack('II', linestring.wkb_id, len(linestring.points))
    for point in linestring.points:
        wkb_buff.pack('dd', point.x, point.y)

def dump_polygon(polygon, wkb_buff):
    """ type count [ count [ x y ]* ]* """
    wkb_buff.pack('II', polygon.wkb_id, len(polygon.rings))
    for ring in polygon.rings:
        wkb_buff.pack('I', len(ring.points))
        for point in ring.points:
            wkb_buff.pack('dd', point.x, point.y)

def dump_multi_point(multi_point, wkb_buff):
    wkb_buff.pack('II', multi_point.wkb_id, len(multi_point.points))
    for point in multi_point.points:
        dump_geometry(point, wkb_buff)

def dump_multi_linestring(multi_line, wkb_buff):
    wkb_buff.pack('II', multi_line.wkb_id, len(multi_line.linestrings))
    for linestring in multi_line.linestrings:
        dump_geometry(linestring, wkb_buff)

def dump_multi_polygon(multi_polygon, wkb_buff):
    wkb_buff.pack('II', multi_polygon.wkb_id, len(multi_polygon.polygons))
    for polygon in multi_polygon.polygons:
        dump_geometry(polygon, wkb_buff)

def dump_geometry_collection(geometry_collection, wkb_buff):
    wkb_buff.pack('II', geometry_collection.wkb_id,
                  len(geometry_collection.geometries))
    for geometry in geometry_collection.geometries:
        dump_geometry(geometry, wkb_buff)

def dump_circular_string(wkb_obj, wkb_buff):
    raise NotImplementedError

def dump_compound_curve(wkb_obj, wkb_buff):
    raise NotImplementedError

def dump_curve_polygon(wkb_obj, wkb_buff):
    raise NotImplementedError

def dump_multi_curve(wkb_obj, wkb_buff):
    raise NotImplementedError

def dump_multi_surface(wkb_obj, wkb_buff):
    raise NotImplementedError

def dump_curve(wkb_obj, wkb_buff):
    raise NotImplementedError

def dump_surface(wkb_obj, wkb_buff):
    raise NotImplementedError

def dump_polyhedral_surface(wkb_obj, wkb_buff):
    raise NotImplementedError

def dump_triangular_irregular_network(wkb_obj, wkb_buff):
    raise NotImplementedError

def dump_triangle(wkb_obj, wkb_buff):
    raise NotImplementedError

# Lookup by type
serializer = {_Geometry           : dump_geometry,
              _Point              : dump_point,
              _LineString         : dump_linestring,
              _Polygon            : dump_polygon,
              _MultiPoint         : dump_multi_point,
              _MultiLineString    : dump_multi_linestring,
              _MultiPolygon       : dump_multi_polygon,
              _GeometryCollection : dump_geometry_collection,
              _CircularString     : dump_circular_string,
              _CompoundCurve      : dump_compound_curve,
              _CurvePolygon       : dump_curve_polygon,
              _MultiCurve         : dump_multi_curve,
              _MultiSurface       : dump_multi_surface,
              _Curve              : dump_curve,
              _Surface            : dump_surface,
              _PolyhedralSurface  : dump_polyhedral_surface,
              _TriIrregNetwork    : dump_triangular_irregular_network,
              _Triangle           : dump_triangle}

""" ===========================================================================
    Main
=========================================================================== """
if __name__ == '__main__':
    #TODO: Giszmo May 24, 2013: get proper unit test once embedding in
    # geoalchemy or wherever
    #TODO: Giszmo May 25, 2013: WTH? importing doctest leads to
    # ImportError: cannot import name FunctionType
    # leading to
    # AttributeError: 'module' object has no attribute 'MethodType'
    #import doctest
    #doctest.testfile("test_wkb.txt")
    pass
    import binascii
    data = (b'010700000006000000010400000004000000010100000000000000000024400000000000004440010100000000000000000044400000000000003e4001010000000000000000003440000000000000344001010000000000000000003e400000000000002440010500000002000000010200000003000000000000000000244000000000000024400000000000003440000000000000344000000000000024400000000000004440010200000004000000000000000000444000000000000044400000000000003e400000000000003e40000000000000444000000000000034400000000000003e40000000000000244001010000000000000000001040000000000000184001060000000200000001030000000100000004000000000000000000444000000000000044400000000000003440000000000080464000000000008046400000000000003e40000000000000444000000000000044400103000000020000000600000000000000000034400000000000804140000000000080464000000000000034400000000000003e4000000000000014400000000000002440000000000000244000000000000024400000000000003e4000000000000034400000000000804140040000000000000000003e4000000000000034400000000000003440000000000000394000000000000034400000000000002e400000000000003e4000000000000034400103000000020000000500000000000000008041400000000000002440000000000000244000000000000034400000000000002e40000000000000444000000000008046400000000000804640000000000080414000000000000024400400000000000000000034400000000000003e40000000000080414000000000008041400000000000003e40000000000000344000000000000034400000000000003e40010200000002000000000000000000104000000000000018400000000000001c400000000000002440')
    print(data)
    data = binascii.unhexlify(data)
    poly = load_wkb(data)
    print(poly)
    poly_data = dump_wkb(poly)
    poly_data = binascii.hexlify(poly_data)
    print(poly_data)