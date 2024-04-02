"""GeoAlchemy2 package."""

from geoalchemy2 import admin
from geoalchemy2 import elements  # noqa
from geoalchemy2 import exc  # noqa
from geoalchemy2 import functions  # noqa
from geoalchemy2 import shape  # noqa
from geoalchemy2 import types  # noqa
from geoalchemy2.admin.dialects.geopackage import load_spatialite_gpkg  # noqa
from geoalchemy2.admin.dialects.sqlite import load_spatialite  # noqa
from geoalchemy2.elements import CompositeElement  # noqa
from geoalchemy2.elements import RasterElement  # noqa
from geoalchemy2.elements import WKBElement  # noqa
from geoalchemy2.elements import WKTElement  # noqa
from geoalchemy2.exc import ArgumentError  # noqa
from geoalchemy2.types import Geography  # noqa
from geoalchemy2.types import Geometry  # noqa
from geoalchemy2.types import Raster  # noqa

admin.setup_ddl_event_listeners()


# Get version number
__version__ = "UNKNOWN VERSION"

# Attempt to use importlib.metadata first because it's much faster
# though it's only available in Python 3.8+ so we'll need to fall
# back to pkg_resources for Python 3.7 support
try:
    import importlib.metadata
except ImportError:
    try:
        from pkg_resources import DistributionNotFound
        from pkg_resources import get_distribution
    except ImportError:  # pragma: no cover
        pass
    else:
        try:
            __version__ = get_distribution("GeoAlchemy2").version
        except DistributionNotFound:  # pragma: no cover
            pass
else:
    try:
        __version__ = importlib.metadata.version("GeoAlchemy2")
    except importlib.metadata.PackageNotFoundError:  # pragma: no cover
        pass


__all__ = [
    "__version__",
    "ArgumentError",
    "CompositeElement",
    "Geography",
    "Geometry",
    "Raster",
    "RasterElement",
    "WKBElement",
    "WKTElement",
    "admin",
    "elements",
    "exc",
    "load_spatialite",
    "load_spatialite_gpkg",
    "shape",
    "types",
]


def __dir__():
    return __all__
