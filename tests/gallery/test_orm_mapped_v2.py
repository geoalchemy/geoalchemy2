"""
New ORM Declarative Mapping Style
=================================

``SQLAlchemy>=2`` introduced a new way to construct mappings using the
``sqlalchemy.orm.DeclarativeBase`` base class.
This example shows how to use GeoAlchemy2 types in this context.
"""

import pytest
from packaging.version import parse as parse_version
from sqlalchemy import __version__ as SA_VERSION

try:
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column
except ImportError:
    pass

from geoalchemy2 import Geometry
from geoalchemy2 import WKBElement
from geoalchemy2 import shape


def check_wkb(wkb, x, y) -> None:
    pt = shape.to_shape(wkb)
    assert round(pt.x, 5) == x
    assert round(pt.y, 5) == y


@pytest.mark.skipif(
    parse_version(SA_VERSION) < parse_version("2"),
    reason="New ORM mapping is only available for sqlalchemy>=2",
)
def test_ORM_mapping(session, conn, schema) -> None:
    class Base(DeclarativeBase):
        pass

    class Lake(Base):
        __tablename__ = "lake"
        __table_args__ = {"schema": schema}
        id: Mapped[int] = mapped_column(primary_key=True)
        mapped_geom: Mapped[WKBElement] = mapped_column(Geometry(geometry_type="POINT", srid=4326))

    Lake.__table__.drop(conn, checkfirst=True)  # type: ignore[attr-defined]
    Lake.__table__.create(bind=conn)  # type: ignore[attr-defined]

    # Create new point instance
    p = Lake()
    p.mapped_geom = "SRID=4326;POINT(5 45)"  # type: ignore[assignment]

    # Insert point
    session.add(p)
    session.flush()
    session.expire(p)

    # Query the point and check the result
    pt = session.query(Lake).one()
    assert pt.id == 1
    assert pt.mapped_geom.srid == 4326
    check_wkb(pt.mapped_geom, 5, 45)
