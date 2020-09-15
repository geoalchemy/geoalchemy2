"""

This module defines the :class:`GenericFunction` class, which is the base for
the implementation of spatial functions in GeoAlchemy.  This module is also
where actual spatial functions are defined. Spatial functions supported by
GeoAlchemy are defined in this module. See :class:`GenericFunction` to know how
to create new spatial functions.

.. note::

    By convention the names of spatial functions are prefixed by ``ST_``.  This
    is to be consistent with PostGIS', which itself is based on the ``SQL-MM``
    standard.

Functions created by subclassing :class:`GenericFunction` can be called
in several ways:

* By using the ``func`` object, which is the SQLAlchemy standard way of calling
  a function. For example, without the ORM::

      select([func.ST_Area(lake_table.c.geom)])

  and with the ORM::

      Session.query(func.ST_Area(Lake.geom))

* By applying the function to a geometry column. For example, without the
  ORM::

      select([lake_table.c.geom.ST_Area()])

  and with the ORM::

      Session.query(Lake.geom.ST_Area())

* By applying the function to a :class:`geoalchemy2.elements.WKBElement`
  object (:class:`geoalchemy2.elements.WKBElement` is the type into
  which GeoAlchemy converts geometry values read from the database), or
  to a :class:`geoalchemy2.elements.WKTElement` object. For example,
  without the ORM::

      conn.scalar(lake['geom'].ST_Area())

  and with the ORM::

      session.scalar(lake.geom.ST_Area())

Reference
---------

"""
import re

from sqlalchemy import inspect
from sqlalchemy.sql import functions
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.util import with_metaclass

from . import types
from . import elements


class _GenericMeta(functions._GenericMeta):
    """Extend the metaclass mechanism of sqlalchemy to register the functions in
    a specific registry for geoalchemy2"""

    _register = False

    def __init__(cls, clsname, bases, clsdict):
        # Register the function
        elements.function_registry.add(clsname.lower())

        super(_GenericMeta, cls).__init__(clsname, bases, clsdict)


class TableRowElement(ColumnElement):
    def __init__(self, selectable):
        self.selectable = selectable

    @property
    def _from_objects(self):
        return [self.selectable]


class ST_AsGeoJSON(with_metaclass(_GenericMeta, functions.GenericFunction)):
    """Special process for the ST_AsGeoJSON() function to be able to work with its
    feature version introduced in PostGIS 3."""

    name = "ST_AsGeoJSON"

    def __init__(self, *args, **kwargs):
        expr = kwargs.pop('expr', None)
        args = list(args)
        if expr is not None:
            args = [expr] + args
        for idx, element in enumerate(args):
            if isinstance(element, functions.Function):
                continue
            elif isinstance(element, elements.HasFunction):
                if element.extended:
                    func_name = element.geom_from_extended_version
                    func_args = [element.data]
                else:
                    func_name = element.geom_from
                    func_args = [element.data, element.srid]
                args[idx] = getattr(functions.func, func_name)(*func_args)
            else:
                try:
                    insp = inspect(element)
                    if hasattr(insp, "selectable"):
                        args[idx] = TableRowElement(insp.selectable)
                except Exception:
                    continue

        functions.GenericFunction.__init__(self, *args, **kwargs)

    __doc__ = (
        'Return the geometry as a GeoJSON "geometry" object, or the row as a '
        'GeoJSON feature" object (PostGIS 3 only). (Cf GeoJSON specifications RFC '
        '7946). 2D and 3D Geometries are both supported. GeoJSON only support SFS '
        '1.1 geometry types (no curve support for example). '
        'See https://postgis.net/docs/ST_AsGeoJSON.html')


@compiles(TableRowElement)
def _compile_table_row_thing(element, compiler, **kw):
    # In order to get a name as reliably as possible, noting that some
    # SQL compilers don't say "table AS name" and might not have the "AS",
    # table and alias names can have spaces in them, etc., get it from
    # a column instead because that's what we want to be showing here anyway.

    compiled = compiler.process(list(element.selectable.columns)[0], **kw)

    # 1. check for exact name of the selectable is here, use that.
    # This way if it has dots and spaces and anything else in it, we
    # can get it w/ correct quoting
    schema = getattr(element.selectable, "schema", "")
    name = element.selectable.name
    pattern = r"(.?%s.?\.)?(.?%s.?)\." % (schema, name)
    m = re.match(pattern, compiled)
    if m:
        return m.group(2)

    # 2. just split on the dot, assume anonymized name
    return compiled.split(".")[0]


class GenericFunction(with_metaclass(_GenericMeta, functions.GenericFunction)):
    """
    The base class for GeoAlchemy functions.

    This class inherits from ``sqlalchemy.sql.functions.GenericFunction``, so
    functions defined by subclassing this class can be given a fixed return
    type. For example, functions like :class:`ST_Buffer` and
    :class:`ST_Envelope` have their ``type`` attributes set to
    :class:`geoalchemy2.types.Geometry`.

    This class allows constructs like ``Lake.geom.ST_Buffer(2)``. In that
    case the ``Function`` instance is bound to an expression (``Lake.geom``
    here), and that expression is passed to the function when the function
    is actually called.

    If you need to use a function that GeoAlchemy does not provide you will
    certainly want to subclass this class. For example, if you need the
    ``ST_TransScale`` spatial function, which isn't (currently) natively
    supported by GeoAlchemy, you will write this::

        from geoalchemy2 import Geometry
        from geoalchemy2.functions import GenericFunction

        class ST_TransScale(GenericFunction):
            name = 'ST_TransScale'
            type = Geometry
    """

    # Set _register to False in order not to register this class in
    # sqlalchemy.sql.functions._registry. Only its children will be registered.
    _register = False

    def __init__(self, *args, **kwargs):
        expr = kwargs.pop('expr', None)
        args = list(args)
        if expr is not None:
            args = [expr] + args
        for idx, elem in enumerate(args):
            if isinstance(elem, elements.HasFunction):
                if elem.extended:
                    func_name = elem.geom_from_extended_version
                    func_args = [elem.data]
                else:
                    func_name = elem.geom_from
                    func_args = [elem.data, elem.srid]
                args[idx] = getattr(functions.func, func_name)(*func_args)
        functions.GenericFunction.__init__(self, *args, **kwargs)


# Functions are classified as in the PostGIS doc.
# <http://www.postgis.org/documentation/manual-svn/reference.html>


_FUNCTIONS = [
    #
    # Geometry Constructors
    #

    ('ST_Collect', types.Geometry,
     'Return a specified ST_Geometry value from a collection of other geometries.'),

    ('ST_BdPolyFromText', types.Geometry,
     'Construct a Polygon given an arbitrary collection of closed linestrings'
     'as a MultiLineString Well-Known text representation.'),

    ('ST_BdMPolyFromText', types.Geometry,
     'Construct a MultiPolygon given an arbitrary collection of closed '
     'linestrings as a MultiLineString text representation Well-Known text '
     'representation.'),

    ('ST_Box2dFromGeoHash', types.Geometry,
     'Return a BOX2D from a GeoHash string.'),

    ('ST_GeogFromText', types.Geography,
     'Return a specified geography value from Well-Known Text representation '
     'or extended (WKT).'),

    ('ST_GeographyFromText', types.Geography,
     'Return a specified geography value from Well-Known Text representation '
     'or extended (WKT).'),

    ('ST_GeogFromWKB', types.Geography,
     'Creates a geography instance from a Well-Known Binary geometry '
     'representation (WKB) or extended Well Known Binary (EWKB).'),

    ('ST_GeomFromTWKB', types.Geometry,
     'Creates a geometry instance from a TWKB ("Tiny Well-Known Binary") '
     'geometry representation.'),

    ('ST_GeomCollFromText', types.Geometry,
     'Makes a collection Geometry from collection WKT with the given SRID. If '
     'SRID is not given, it defaults to 0.'),

    ('ST_GeomFromEWKB', types.Geometry,
     'Return a specified ST_Geometry value from Extended Well-Known Binary '
     'representation (EWKB).'),

    ('ST_GeomFromEWKT', types.Geometry,
     'Return a specified ST_Geometry value from Extended Well-Known Text '
     'representation (EWKT).'),

    ('ST_GeometryFromText', types.Geometry,
     'Return a specified ST_Geometry value from Well-Known Text representation'
     ' (WKT). This is an alias name for ST_GeomFromText'),

    ('ST_GeomFromGeoHash', types.Geometry,
     'Return a geometry from a GeoHash string.'),

    ('ST_GeomFromGML', types.Geometry,
     'Takes as input GML representation of geometry and outputs a PostGIS '
     'geometry object'),

    ('ST_GeomFromGeoJSON', types.Geometry,
     'Takes as input a geojson representation of a geometry and outputs a '
     'PostGIS geometry object'),

    ('ST_GeomFromKML', types.Geometry,
     'Takes as input KML representation of geometry and outputs a PostGIS '
     'geometry object'),

    ('ST_GMLToSQL', types.Geometry,
     'Return a specified ST_Geometry value from GML representation. This is an'
     ' alias name for ST_GeomFromGML'),

    ('ST_GeomFromText', types.Geometry,
     'Return a specified ST_Geometry value from Well-Known Text representation'
     ' (WKT).'),

    ('ST_GeomFromWKB', types.Geometry,
     'Creates a geometry instance from a Well-Known Binary geometry '
     'representation (WKB) and optional SRID.'),

    ('ST_LineFromEncodedPolyline', types.Geometry,
     'Creates a LineString from an Encoded Polyline.'),

    ('ST_LineFromMultiPoint', types.Geometry,
     'Creates a LineString from a MultiPoint geometry.'),

    ('ST_LineFromText', types.Geometry,
     'Makes a Geometry from WKT representation with the given SRID. If SRID is'
     ' not given, it defaults to 0.'),

    ('ST_LineFromWKB', types.Geometry,
     'Makes a LINESTRING from WKB with the given SRID'),

    ('ST_LinestringFromWKB', types.Geometry,
     'Makes a geometry from WKB with the given SRID.'),

    ('ST_MakeBox2D', types.Geometry,
     'Creates a BOX2D defined by the given point geometries.'),

    ('ST_3DMakeBox', types.Geometry,
     'Creates a BOX3D defined by the given 3d point geometries.'),

    ('ST_MakeLine', types.Geometry,
     'Creates a Linestring from point, multipoint, or line geometries.'),

    ('ST_MakeEnvelope', types.Geometry,
     'Creates a rectangular Polygon formed from the given minimums and '
     'maximums. Input values must be in SRS specified by the SRID.'),

    ('ST_MakePolygon', types.Geometry,
     'Creates a Polygon formed by the given shell. Input geometries must be '
     'closed LINESTRINGS.'),

    ('ST_MakePoint', types.Geometry,
     'Creates a 2D, 3DZ or 4D point geometry.'),

    ('ST_MakePointM', types.Geometry,
     'Creates a point geometry with an x y and m coordinate.'),

    ('ST_MLineFromText', types.Geometry,
     'Return a specified ST_MultiLineString value from WKT representation.'),

    ('ST_MPointFromText', types.Geometry,
     'Makes a Geometry from WKT with the given SRID. If SRID is not given, it '
     'defaults to 0.'),

    ('ST_MPolyFromText', types.Geometry,
     'Makes a MultiPolygon Geometry from WKT with the given SRID. If SRID is '
     'not given, it defaults to 0.'),

    ('ST_Point', types.Geometry,
     'Returns an ST_Point with the given coordinate values. OGC alias for '
     'ST_MakePoint.'),

    ('ST_PointFromGeoHash', types.Geometry,
     'Return a point from a GeoHash string.'),

    ('ST_PointFromText', types.Geometry,
     'Makes a point Geometry from WKT with the given SRID. If SRID is not '
     'given, it defaults to unknown.'),

    ('ST_PointFromWKB', types.Geometry,
     'Makes a geometry from WKB with the given SRID'),

    ('ST_Polygon', types.Geometry,
     'Returns a polygon built from the specified linestring and SRID.'),

    ('ST_PolygonFromText', types.Geometry,
     'Makes a Geometry from WKT with the given SRID. If SRID is not given, it '
     'defaults to 0.'),

    ('ST_TileEnvelope', types.Geometry,
     'Creates a rectangular Polygon in Web Mercator (SRID:3857) using the XYZ tile '
     'system.'),

    ('ST_WKBToSQL', types.Geometry,
     'Return a specified ST_Geometry value from Well-Known Binary '
     'representation (WKB). This is an alias name for ST_GeomFromWKB that '
     'takes no srid'),

    ('ST_WKTToSQL', types.Geometry,
     'Return a specified ST_Geometry value from Well-Known Text representation'
     ' (WKT). This is an alias name for ST_GeomFromText'),

    #
    # Geometry Accessors
    #

    ('ST_Boundary', types.Geometry,
     'Returns the closure of the combinatorial boundary of this Geometry.'),

    ('ST_BoundingDiagonal', types.Geometry,
     'Returns the diagonal of the supplied geometry\'s bounding box.'),

    ('ST_EndPoint', types.Geometry,
     'Returns the last point of a ``LINESTRING`` or ``CIRCULARLINESTRING`` '
     'geometry as a ``POINT``.'),

    ('ST_Envelope', types.Geometry,
     'Returns a geometry representing the double precision (float8) bounding'
     'box of the supplied geometry.'),

    ('ST_GeometryN', types.Geometry,
     'Return the 1-based Nth geometry if the geometry is a '
     '``GEOMETRYCOLLECTION``, ``(MULTI)POINT``, ``(MULTI)LINESTRING``, '
     '``MULTICURVE`` or ``(MULTI)POLYGON``, ``POLYHEDRALSURFACE`` Otherwise, '
     'return ``None``.'),

    ('ST_GeometryType', None,
     'Return the geometry type of the ``ST_Geometry`` value.'),

    ('ST_InteriorRingN', types.Geometry,
     'Return the Nth interior linestring ring of the polygon geometry. Return '
     '``NULL`` if the geometry is not a polygon or the given N is out of '
     'range.'),

    ('ST_IsValid', None,
     'Returns ``True`` if the ``ST_Geometry`` is well formed.'),

    ('ST_NPoints', None,
     'Return the number of points (vertices) in a geometry.'),

    ('ST_PatchN', types.Geometry,
     'Return the 1-based Nth geometry (face) if the geometry is a '
     '``POLYHEDRALSURFACE``, ``POLYHEDRALSURFACEM``. Otherwise, return '
     '``NULL``.'),

    ('ST_PointN', types.Geometry,
     'Return the Nth point in the first LineString or circular LineString in '
     'the geometry. Negative values are counted backwards from the end of the '
     'LineString. Returns ``NULL`` if there is no linestring in the geometry.'
     ),

    ('ST_Points', types.Geometry,
     'Returns a MultiPoint containing all of the coordinates of a geometry.'),

    ('ST_SRID', None,
     'Returns the spatial reference identifier for the ``ST_Geometry`` as '
     'defined in ``spatial_ref_sys`` table.'),

    ('ST_StartPoint', types.Geometry,
     'Returns the first point of a ``LINESTRING`` geometry as a ``POINT``.'),

    ('ST_X', None,
     'Return the X coordinate of the point, or ``None`` if not available. '
     'Input must be a point.'),

    ('ST_Y', None,
     'Return the Y coordinate of the point, or ``None`` if not available. '
     'Input must be a point.'),

    ('ST_Z', None,
     'Return the Z coordinate of the point, or ``None`` if not available. '
     'Input must be a point.'),

    #
    # Geometry Editors
    #

    ('ST_AddPoint', types.Geometry,
     'Add a point to a LineString.'),

    ('ST_Affine', types.Geometry,
     'Apply a 3d affine transformation to a geometry.'),

    ('ST_CollectionExtract', types.Geometry,
     'Given a (multi)geometry, return a (multi)geometry consisting only of '
     'elements of the specified type.'),

    ('ST_CollectionHomogenize', types.Geometry,
     'Given a geometry collection, return the "simplest" representation of '
     'the contents.'),

    ('ST_ExteriorRing', types.Geometry,
     'Returns a line string representing the exterior ring of the ``POLYGON`` '
     'geometry. Return ``NULL`` if the geometry is not a polygon. Will not '
     'work with ``MULTIPOLYGON``.'),

    ('ST_Force2D', types.Geometry,
     'Force the geometries into a "2-dimensional mode".'),

    ('ST_Force3D', types.Geometry,
     ('Force the geometries into XYZ mode. This is an alias for ``ST_Force3DZ``.',
      'ST_Force_3D')),

    ('ST_Force3DM', types.Geometry,
     ('Force the geometries into XYM mode.', 'ST_Force_3DM')),

    ('ST_Force3DZ', types.Geometry,
     ('Force the geometries into XYZ mode.', 'ST_Force_3DZ')),

    ('ST_Force4D', types.Geometry,
     ('Force the geometries into XYZM mode.', 'ST_Force_4D')),

    ('ST_ForceCollection', types.Geometry,
     ('Convert the geometry into a ``GEOMETRYCOLLECTION``.',
      'ST_Force_Collection')),

    ('ST_ForceCurve', types.Geometry,
     'Upcast a geometry into its curved type, if applicable.'),

    ('ST_ForcePolygonCCW', types.Geometry,
     'Orients all exterior rings counter-clockwise and all interior rings '
     'clockwise.'),

    ('ST_ForcePolygonCW', types.Geometry,
     'Orients all exterior rings clockwise and all interior rings '
     'counter-clockwise.'),

    ('ST_ForceRHR', types.Geometry,
     'Force the orientation of the vertices in a polygon to follow the '
     'Right-Hand-Rule.'),

    ('ST_ForceSFS', types.Geometry,
     'Force the geometries to use SFS 1.1 geometry types only.'),

    ('ST_M', None,
     'Return the M coordinate of the point, or ``NULL`` if not available. '
     'Input must be a point.'),

    ('ST_Multi', types.Geometry,
     'Return the geometry as a ``MULTI*`` geometry.'),

    ('ST_Normalize', types.Geometry,
     'Return the geometry in its canonical form.'),

    ('ST_QuantizeCoordinates', types.Geometry,
     'Sets least significant bits of coordinates to zero.'),

    ('ST_RemovePoint', types.Geometry,
     'Remove point from a linestring.'),

    ('ST_Reverse', types.Geometry,
     'Return the geometry with vertex order reversed.'),

    ('ST_Rotate', types.Geometry,
     'Rotate a geometry rotRadians counter-clockwise about an origin.'),

    ('ST_RotateX', types.Geometry,
     'Rotate a geometry rotRadians about the X axis.'),

    ('ST_RotateY', types.Geometry,
     'Rotate a geometry rotRadians about the Y axis.'),

    ('ST_RotateZ', types.Geometry,
     'Rotate a geometry rotRadians about the Z axis.'),

    ('ST_Scale', types.Geometry,
     'Scale a geometry by given factors.'),

    ('ST_Segmentize', types.Geometry,
     'Return a modified geometry/geography having no segment longer than the '
     'given distance.'),

    ('ST_SetPoint', types.Geometry,
     'Replace point of a linestring with a given point.'),

    ('ST_SetSRID', types.Geometry,
     'Set the SRID on a geometry to a particular integer value.'),

    ('ST_Snap', types.Geometry,
     'Snap segments and vertices of input geometry to vertices of a reference '
     'geometry.'),

    ('ST_SnapToGrid', types.Geometry,
     'Snap all points of the input geometry to a regular grid.'),

    ('ST_SwapOrdinates', types.Geometry,
     'Returns a version of the given geometry with given ordinate values swapped.'),

    ('ST_Transform', types.Geometry,
     'Return a new geometry with its coordinates transformed to the SRID '
     'referenced by the integer parameter.'),

    ('ST_Translate', types.Geometry,
     'Translate a geometry by given offsets.'),

    ('ST_TransScale', types.Geometry,
     'Translate a geometry by given factors and offsets.'),

    #
    # Geometry Outputs
    #

    ('ST_AsBinary', None,
     'Return the Well-Known Binary (WKB) representation of the geometry/'
     'geography without SRID meta data.'),

    ('ST_AsEWKB', None,
     'Return the Well-Known Binary (WKB) representation of the geometry/'
     'geography with SRID meta data.'),

    ('ST_AsTWKB', None,
     'Returns the geometry as TWKB, aka "Tiny Well-Known Binary"'),

    ('ST_AsGML', None, 'Return the geometry as a GML version 2 or 3 element.'),

    ('ST_AsKML', None,
     'Return the geometry as a KML element. Several variants. Default '
     'version=2, default precision=15'),

    ('ST_AsSVG', None,
     'Returns a Geometry in SVG path data given a geometry or geography '
     'object.'),

    ('ST_AsText', None,
     'Return the Well-Known Text (WKT) representation of the geometry/'
     'geography without SRID metadata.'),

    ('ST_AsEWKT', None,
     'Return the Well-Known Text (WKT) representation of the geometry/'
     'geography with SRID metadata.'),

    ('ST_AsMVTGeom', types.Geometry,
     'Transform a geometry into the coordinate space of a Mapbox Vector Tile.'),

    #
    # Spatial Relationships and Measurements
    #

    ('ST_Area', None,
     'Returns the area of the surface if it is a polygon or multi-polygon. '
     'For ``geometry`` type area is in SRID units. For ``geography`` area is '
     'in square meters.'),

    ('ST_Azimuth', None,
     'Returns the angle in radians from the horizontal of the '
     'vector defined by pointA and pointB. Angle is computed clockwise from '
     'down-to-up: on the clock: 12=0; 3=PI/2; 6=PI; 9=3PI/2.'),

    ('ST_Centroid', types.Geometry,
     'Returns the geometric center of a geometry.'),

    ('ST_ClosestPoint', types.Geometry,
     'Returns the 2-dimensional point on g1 that is closest to g2. This is '
     'the first point of the shortest line.'),

    ('ST_3DClosestPoint', types.Geometry,
     'Returns the 3-dimensional point on g1 that is closest to g2. This is '
     'the first point of the 3D shortest line.'),

    ('ST_Contains', None,
     'Returns ``True`` if and only if no points of B lie in the exterior of '
     'A, and at least one point of the interior of B lies in the interior '
     'of A.'),

    ('ST_ContainsProperly', None,
     'Returns ``True`` if B intersects the interior of A but not the boundary '
     '(or exterior). A does not contain properly itself, but does contain '
     'itself.'),

    ('ST_Covers', None,
     'Returns ``True`` if no point in Geometry B is outside Geometry A'),

    ('ST_CoveredBy', None,
     'Returns ``True`` if no point in Geometry/Geography A is outside Geometry'
     '/Geography B'),

    ('ST_Crosses', None,
     'Returns ``True`` if the supplied geometries have some, but not all, '
     'interior points in common.'),

    ('ST_Disjoint', None,
     'Returns ``True`` if the Geometries do not "spatially intersect" - if '
     'they do not share any space together.'),

    ('ST_Distance', None,
     'For geometry type Returns the 2-dimensional cartesian minimum distance '
     '(based on spatial ref) between two geometries in projected units. For '
     'geography type defaults to return spheroidal minimum distance between '
     'two geographies in meters.'),

    ('ST_Distance_Sphere', None,
     'Returns minimum distance in meters between two lon/lat geometries. Uses '
     'a spherical earth and radius of 6370986 meters. Faster than '
     '``ST_Distance_Spheroid``, but less accurate. PostGIS versions '
     'prior to 1.5 only implemented for points.'),

    ('ST_DistanceSphere', None,
     'Returns minimum distance in meters between two lon/lat points. Uses '
     'a spherical earth and radius derived from the spheroid defined by the '
     'SRID. Faster than ST_DistanceSpheroid, but less accurate. PostGIS '
     'Versions prior to 1.5 only implemented for points. Availability: 1.5 - '
     'support for other geometry types besides points was introduced. Prior '
     'versions only work with points. Changed: 2.2.0 In prior versions this '
     'used to be called ST_Distance_Sphere'),

    ('ST_DFullyWithin', None,
     'Returns ``True`` if all of the geometries are within the specified '
     'distance of one another'),

    ('ST_DWithin', None,
     'Returns ``True`` if the geometries are within the specified distance of '
     'one another. For geometry units are in those of spatial reference and '
     'for geography units are in meters and measurement is defaulted to '
     '``use_spheroid=True`` (measure around spheroid), for faster check, '
     '``use_spheroid=False`` to measure along sphere.'),

    ('ST_Equals', None,
     'Returns ``True`` if the given geometries represent the same geometry. '
     'Directionality is ignored.'),

    ('ST_Intersects', None,
     'Returns ``True`` if the Geometries/Geography "spatially intersect in '
     '2D" - (share any portion of space) and ``False`` if they don\'t (they '
     'are Disjoint). For geography -- tolerance is 0.00001 meters (so any '
     'points that close are considered to intersect)'),

    ('ST_Length', None,
     'Returns the 2d length of the geometry if it is a linestring or '
     'multilinestring. geometry are in units of spatial reference and '
     'geography are in meters (default spheroid)'),

    ('ST_LineLocatePoint', None,
     'Returns a float between 0 and 1 representing the location of the '
     'closest point on LineString to the given Point, as a fraction of '
     'total 2d line length.'),

    ('ST_LongestLine', types.Geometry,
     'Returns the 2-dimensional longest line points of two geometries. The function '
     'will only return the first longest line if more than one, that the function '
     'finds. The line returned will always start in g1 and end in g2. The length of '
     'the line this function returns will always be the same as st_maxdistance '
     'returns for g1 and g2.'),

    ('ST_3DLongestLine', types.Geometry,
     'Returns the 3-dimensional longest line between two geometries'),

    ('ST_MinimumClearanceLine', types.Geometry,
     'Returns the two-point LineString spanning a geometry\'s minimum clearance.'),

    ('ST_OrderingEquals', None,
     'Returns ``True`` if the given geometries represent the same geometry '
     'and points are in the same directional order.'),

    ('ST_Overlaps', None,
     'Returns ``True`` if the Geometries share space, are of the same '
     'dimension, but are not completely contained by each other.'),

    ('ST_Perimeter', None,
     'Return the length measurement of the boundary of an ST_Surface or '
     'ST_MultiSurface geometry or geography. (Polygon, Multipolygon). '
     'geometry measurement is in units of spatial reference and geography is '
     'in meters.'),

    ('ST_Project', types.Geography,
     'Returns a ``POINT`` projected from a start point using a distance in '
     'meters and bearing (azimuth) in radians.'),

    ('ST_Relate', None,
     'Returns ``True`` if this Geometry is spatially related to '
     'anotherGeometry, by testing for intersections between the Interior, '
     'Boundary and Exterior of the two geometries as specified by the values '
     'in the intersectionMatrixPattern. If no intersectionMatrixPattern is '
     'passed in, then returns the maximum intersectionMatrixPattern that '
     'relates the 2 geometries.'),

    ('ST_ShortestLine', types.Geometry,
     'Returns the 2-dimensional shortest line between two geometries'),

    ('ST_3DShortestLine', types.Geometry,
     'Returns the 3-dimensional shortest line between two geometries'),

    ('ST_Touches', None,
     'Returns ``True`` if the geometries have at least one point in common, '
     'but their interiors do not intersect.'),

    ('ST_Within', None,
     'Returns ``True`` if the geometry A is completely inside geometry B'),

    #
    # Geometry Processing
    #

    ('ST_Buffer', types.Geometry,
     'For geometry: Returns a geometry that represents all points whose '
     'distance from this Geometry is less than or equal to distance. '
     'Calculations are in the Spatial Reference System of this Geometry.\n\n'
     'For geography: Uses a planar transform wrapper. Introduced in 1.5 '
     'support for different end cap and mitre settings to control shape.'),

    ('ST_BuildArea', types.Geometry,
     'Creates an areal geometry formed by the constituent linework of given '
     'geometry'),

    ('ST_ClipByBox2D', types.Geometry,
     'Returns the portion of a geometry falling within a rectangle.'),

    ('ST_ChaikinSmoothing', types.Geometry,
     'Returns a "smoothed" version of the given geometry using the Chaikin '
     'algorithm'),

    ('ST_ConcaveHull', types.Geometry,
     'The concave hull of a geometry represents a possibly concave geometry that '
     'encloses all geometries within the set. You can think of it as shrink wrapping.'),

    ('ST_ConvexHull', types.Geometry,
     'The convex hull of a geometry represents the minimum convex geometry '
     'that encloses all geometries within the set.'),

    ('ST_CurveToLine', types.Geometry,
     'Converts a CIRCULARSTRING/CURVEPOLYGON/MULTISURFACE to a '
     'LINESTRING/POLYGON/MULTIPOLYGON'),

    ('ST_DelaunayTriangles', types.Geometry,
     'Return a Delaunay triangulation around the given input points.'),

    ('ST_Difference', types.Geometry,
     'Returns a geometry that represents that part of geometry A that does '
     'not intersect with geometry B.'),

    ('ST_Dump', types.GeometryDump,
     'Returns a set of geometry_dump (geom,path) rows, that make up a '
     'geometry g1.'),

    ('ST_DumpPoints', types.GeometryDump,
     'Returns a set of geometry_dump (geom,path) rows of all points that '
     'make up a geometry.'),

    ('ST_FilterByM', types.Geometry,
     'Filters vertex points based on their m-value'),

    ('ST_FlipCoordinates', types.Geometry,
     'Returns a version of the given geometry with X and Y axis flipped. Useful '
     'for people who have built latitude/longitude features and need to fix them.'),

    ('ST_GeneratePoints', types.Geometry,
     'Converts a polygon or multi-polygon into a multi-point composed of randomly '
     'location points within the original areas.'),

    ('ST_GeometricMedian', types.Geometry,
     'Returns the geometric median of a MultiPoint.'),

    ('ST_Intersection', types.Geometry,
     'Returns a geometry that represents the shared portion of geomA and '
     'geomB. The geography implementation does a transform to geometry to do '
     'the intersection and then transform back to WGS84.'),

    ('ST_LineToCurve', types.Geometry,
     'Converts a LINESTRING/POLYGON to a CIRCULARSTRING, CURVEPOLYGON'),

    ('ST_LineMerge', types.Geometry,
     'Returns a (set of) LineString(s) formed by sewing together the '
     'constituent line work of a MULTILINESTRING.'),

    ('ST_LineSubstring', types.Geometry,
     'Return a linestring being a substring of the input one starting and '
     'ending at the given fractions of total 2d length. Second and third '
     'arguments are float8 values between 0 and 1. This only works with '
     'LINESTRINGs. To use with contiguous MULTILINESTRINGs use in '
     'conjunction with ST_LineMerge.'
     ''
     'If \'start\' and \'end\' have the same value this is equivalent '
     'to ST_LineInterpolatePoint.'),

    ('ST_MakeValid', types.Geometry,
     'Attempts to make an invalid geometry valid without losing vertices.'),

    ('ST_MemUnion', types.Geometry,
     'Same as ST_Union, only memory-friendly (uses less memory and more processor '
     'time).'),

    ('ST_MinimumBoundingCircle', types.Geometry,
     'Returns the smallest circle polygon that can fully contain a geometry. '
     'Default uses 48 segments per quarter circle.'),

    ('ST_Node', types.Geometry,
     'Node a set of linestrings.'),

    ('ST_OffsetCurve', types.Geometry,
     'Return an offset line at a given distance and side from an input line. '
     'Useful for computing parallel lines about a center line'),

    ('ST_OrientedEnvelope', types.Geometry,
     'Returns a minimum rotated rectangle enclosing a geometry.'),

    ('ST_PointOnSurface', types.Geometry,
     'Returns a POINT guaranteed to lie on the surface.'),

    ('ST_Polygonize', types.Geometry,
     'Aggregate. Creates a GeometryCollection containing possible polygons formed '
     'from the constituent linework of a set of geometries.'),

    ('ST_RemoveRepeatedPoints', types.Geometry,
     'Returns a version of the given geometry with duplicated points removed.'),

    ('ST_SetEffectiveArea', types.Geometry,
     'Sets the effective area for each vertex, storing the value in the M ordinate. '
     'A simplified geometry can then be generated by filtering on the M ordinate.'),

    ('ST_SharedPaths', types.Geometry,
     'Returns a collection containing paths shared by the two input '
     'linestrings/multilinestrings.'),

    ('ST_ShiftLongitude', types.Geometry,
     'Toggle geometry coordinates between -180..180 and 0..360 ranges.'),

    ('ST_Simplify', types.Geometry,
     'Returns a "simplified" version of the given geometry using the '
     'Douglas-Peucker algorithm.'),

    ('ST_SimplifyPreserveTopology', types.Geometry,
     'Returns a "simplified" version of the given geometry using the '
     'Douglas-Peucker algorithm. Will avoid creating derived geometries (polygons '
     'in particular) that are invalid.'),

    ('ST_SimplifyVW', types.Geometry,
     'Returns a "simplified" version of the given geometry using the '
     'Visvalingam-Whyatt algorithm'),

    ('ST_Split', types.Geometry,
     'Returns a collection of geometries resulting by splitting a geometry.'),

    ('ST_Subdivide', types.Geometry,
     'Returns a set of geometry where no geometry in the set has more than the '
     'specified number of vertices.'),

    ('ST_SymDifference', types.Geometry,
     'Returns a geometry that represents the portions of A and B that do not '
     'intersect. It is called a symmetric difference because ST_SymDifference(A,B) '
     '= ST_SymDifference(B,A).'),

    ('ST_Union', types.Geometry,
     'Returns a geometry that represents the point set union of the '
     'Geometries.'),

    ('ST_UnaryUnion', types.Geometry,
     'Like ST_Union, but working at the geometry component level.'),

    ('ST_VoronoiLines', types.Geometry,
     'Returns the boundaries between the cells of the Voronoi diagram constructed '
     'from the vertices of a geometry.'),

    ('ST_VoronoiPolygons', types.Geometry,
     'Returns the cells of the Voronoi diagram constructed from the vertices '
     'of a geometry.'),

    ('ST_WrapX', types.Geometry,
     'Wrap a geometry around an X value.'),

    #
    # Bounding Box Functions
    #

    ('ST_Expand', types.Geometry,
     'Returns bounding box expanded in all directions from the bounding box of '
     'the input geometry. Uses double-precision'),

    #
    # Linear Referencing
    #
    ('ST_AddMeasure', types.Geometry,
     'Return a derived geometry with measure elements linearly interpolated '
     'between the start and end points.'),

    ('ST_LineInterpolatePoint', types.Geometry,
     'Returns a point interpolated along a line. Second argument is a float8 '
     'between 0 and 1 representing fraction of total length of linestring the point '
     'has to be located.'),

    ('ST_LineInterpolatePoints', types.Geometry,
     'Returns one or more points interpolated along a line.'),

    ('ST_LocateAlong', types.Geometry,
     'Return a derived geometry collection value with elements that match the '
     'specified measure. Polygonal elements are not supported.'),

    ('ST_LocateBetween', types.Geometry,
     'Return a derived geometry collection value with elements that match the '
     'specified range of measures inclusively. Polygonal elements are not supported.'),

    ('ST_LocateBetweenElevations', types.Geometry,
     'Return a derived geometry (collection) value with elements that intersect '
     'the specified range of elevations inclusively. Only 3D, 4D LINESTRINGS '
     'and MULTILINESTRINGS are supported.'),

    ('ST_3DLineInterpolatePoint', types.Geometry,
     'Returns a point interpolated along a line in 3D. Second argument is a float8'
     ' between 0 and 1 representing fraction of total length of linestring the point '
     'has to be located.'),

    #
    # Raster Constructors
    #

    ('ST_AddBand', types.Raster,
     ('Returns a raster with the new band(s) of given type added with given initial '
      'value in the given index location. If no index is specified, the band is added '
      'to the end.', 'RT_ST_AddBand')),

    ('ST_AsRaster', types.Raster,
     ('Converts a PostGIS geometry to a PostGIS raster.', 'RT_ST_AsRaster')),

    #
    # Raster Editors
    #

    ('ST_Resample', types.Raster,
     ('Resample a raster using a specified resampling algorithm, new dimensions, an '
      'arbitrary grid corner and a set of raster georeferencing attributes defined or '
      'borrowed from another raster.', 'RT_ST_Resample')),

    ('ST_Rescale', types.Raster,
     ('Resample a raster by adjusting only its scale (or pixel size). New pixel values '
      'are computed using the NearestNeighbor (english or american spelling), '
      'Bilinear, Cubic, CubicSpline or Lanczos resampling algorithm. Default is '
      'NearestNeighbor.', 'RT_ST_Rescale')),

    ('ST_Reskew', types.Raster,
     ('Resample a raster by adjusting only its skew (or rotation parameters). New '
      'pixel values are computed using the NearestNeighbor (english or american '
      'spelling), Bilinear, Cubic, CubicSpline or Lanczos resampling algorithm. '
      'Default is NearestNeighbor.', 'RT_ST_Reskew')),

    # ST_SnapToGrid already exists for Geometry type so it can not be duplicated
    # ('ST_SnapToGrid', types.Raster,
    #  ('Resample a raster by snapping it to a grid. New pixel values are computed '
    #   'using the NearestNeighbor (english or american spelling), Bilinear, Cubic, '
    #   'CubicSpline or Lanczos resampling algorithm. Default is '
    #   'NearestNeighbor.', 'RT_ST_SnapToGrid')),

    ('ST_Resize', types.Raster,
     ('Resize a raster to a new width/height', 'RT_ST_Resize')),

    #
    # Raster Accessors
    #

    ('ST_Height', None,
     ('Returns the height of the raster in pixels.', 'RT_ST_Height')),

    ('ST_Width', None,
     ('Returns the width of the raster in pixels.', 'RT_ST_Width')),

    #
    # Raster Pixel Accessors and Setters
    #

    ('ST_Value', None,
     ('Returns the value of a given band in a given columnx, rowy pixel or at '
      'a particular geometric point. Band numbers start at 1 and assumed to '
      'be 1 if not specified. If ``exclude_nodata_value`` is set to '
      '``false``, then all pixels include nodata pixels are considered to '
      'intersect and return value. If ``exclude_nodata_value`` is not passed '
      'in then reads it from metadata of raster.', 'RT_ST_Value')),

    #
    # Raster Band Statistics and Analytics
    #

    ('ST_ValueCount', types.Raster,
     ('Returns a set of records containing a pixel band value and count of the number '
      'of pixels in a given band of a raster (or a raster coverage) that have a '
      'given set of values. If no band is specified defaults to band 1. By default '
      'nodata value pixels are not counted. and all other values in the pixel are '
      'output and pixel band values are rounded to the nearest integer.')),

    #
    # DEM (Elevation)
    #

    ('ST_HillShade', types.Raster,
     ('Returns the hypothetical illumination of an elevation raster band using '
      'provided azimuth, altitude, brightness and scale inputs.', 'RT_ST_HillShade')),
]

# Iterate through _FUNCTION and create GenericFunction classes dynamically
for name, type_, doc in _FUNCTIONS:
    attributes = {'name': name}
    docs = []

    if isinstance(doc, tuple):
        docs.append(doc[0])
        docs.append('see http://postgis.net/docs/{0}.html'.format(doc[1]))
    elif doc is not None:
        docs.append(doc)
        docs.append('see http://postgis.net/docs/{0}.html'.format(name))

    if type_ is not None:
        attributes['type'] = type_

        type_str = '{0}.{1}'.format(type_.__module__, type_.__name__)
        docs.append('Return type: :class:`{0}`.'.format(type_str))

    if len(docs) != 0:
        attributes['__doc__'] = '\n\n'.join(docs)

    globals()[name] = type(name, (GenericFunction,), attributes)


#
# Define compiled versions for functions in SpatiaLite whose names don't have
# the ST_ prefix.
#


_SQLITE_FUNCTIONS = {
    "ST_GeomFromEWKT": "GeomFromEWKT",
    "ST_GeomFromEWKB": "GeomFromEWKB",
    "ST_AsBinary": "AsBinary",
    "ST_AsEWKB": "AsEWKB",
    "ST_AsGeoJSON": "AsGeoJSON",
}


# Default handlers are required for SQLAlchemy < 1.1
# See more details in https://github.com/geoalchemy/geoalchemy2/issues/213
def _compiles_default(cls):
    def _compile_default(element, compiler, **kw):
        return "{}({})".format(cls, compiler.process(element.clauses, **kw))
    compiles(globals()[cls])(_compile_default)


def _compiles_sqlite(cls, fn):
    def _compile_sqlite(element, compiler, **kw):
        return "{}({})".format(fn, compiler.process(element.clauses, **kw))
    compiles(globals()[cls], "sqlite")(_compile_sqlite)


for cls, fn in _SQLITE_FUNCTIONS.items():
    _compiles_default(cls)
    _compiles_sqlite(cls, fn)
