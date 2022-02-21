"""Some helpers to use with Alembic migration tool."""
from geoalchemy2 import Geography
from geoalchemy2 import Geometry
from geoalchemy2 import Raster
from geoalchemy2 import _check_spatial_type


def render_item(obj_type, obj, autogen_context):
    """Apply custom rendering for selected items."""
    if obj_type == 'type' and isinstance(obj, (Geometry, Geography, Raster)):
        import_name = obj.__class__.__name__
        autogen_context.imports.add(f"from geoalchemy2 import {import_name}")
        return "%r" % obj

    # default rendering for other objects
    return False


def include_object(obj, name, obj_type, reflected, compare_to):
    """Do not include spatial indexes if they are automatically created by GeoAlchemy2."""
    if obj_type == "index":
        if len(obj.expressions) == 1:
            try:
                col = obj.expressions[0]
                if (
                    _check_spatial_type(col.type, (Geometry, Geography, Raster))
                    and col.type.spatial_index
                ):
                    return False
            except AttributeError:
                pass
    # Never include the spatial_ref_sys table
    if (obj_type == "table" and name == "spatial_ref_sys"):
        return False
    return True
