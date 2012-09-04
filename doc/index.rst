.. GeoAlchemy2 documentation master file, created by
   sphinx-quickstart on Thu Aug 23 06:38:45 2012.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

GeoAlchemy 2 Documentation
==========================

*Using SQLAlchemy with Spatial Databases.*

GeoAlchemy 2 aims to be simpler than its predecessor, in terms of both usage,
and maintainance. GeoAlchemy 2 uses SQLAlchemy's most recent features, and aims
to work well with, and fully benefit from, what will be SQLAlchemy 1.0.

GeoAlchemy 2 supports PostGIS 2. It also supports the 1.x series of PostGIS for
the moment. Whether we will continue supporting the 1.x series is not decided
yet, and will depend on the level complexity required to support both series.

GeoAlchemy 2 doesn't currently other dialects than PostgreSQL/PostGIS.
Supporting Oracle Locator in the previous series was the main contributor to
complexifying the code. So it is currently not clear whether we want to go
there again. Please contact us you want to add, and maintain, support for other
spatial databases in GeoAlchemy 2.

.. note::

    GeoAlchemy 2 doesn't currently work with any official release of
    SQLAlchemy. GeoAlchemy 2 works with SQLAlchemy's current development
    branch, which is available at http://hg.sqlalchemy.org/sqlalchemy.
    GeoAlchemy 2 will work with the next SQLAlchemy release, namely 0.8.

Changes from previous series
----------------------------

The first series had its own namespace for spatial functions, namely
``geoalchemy.functions``. With GeoAlchemy 2, spatial functions are called like
any other function, using ``func``, which is SQLAlchemy's `standard way
<http://docs.sqlalchemy.org/en/latest/core/expression_api.html#sqlalchemy.sql.expression.func>`_
of calling SQL functions.

GeoAlchemy 2 works with SQLAlchemy's ORM, as well as with SQLAlchemy's *SQL
Expression Language* (a.k.a the SQLAlchemy Core). This is thanks to SQLAlchemy's
new `type-level comparator system
<http://docs.sqlalchemy.org/en/latest/core/types.html?highlight=comparator_factory#types-operators>`_.

GeoAlchemy 2 adds ``to_shape``, ``from_shape`` functions for a better
integration with `Shapely <http://pypi.python.org/pypi/Shapely>`_.

GeoAlchemy 2 supports PostGIS' ``geography`` type. The support is
partial at the moment.


Tutorials
---------

GeoAlchemy works both with SQLAlchemy's *Object Relational Mapping* (ORM) *SQL
Expression Language*. This documentation provides a tutorial for each system.
If you're new to GeoAlchemy start with this.

.. toctree::
   :maxdepth: 1

   orm_tutorial
   core_tutorial

Reference
---------

.. toctree::
   :maxdepth: 1

   types
   elements
   spatial_functions

Development
-----------

The code is available on GitHub: https://github.com/geoalchemy/geoalchemy2.

Contributors:

* Eric Lemoine, Camptocamp SA

Many thanks to Mike Bayer for his guidance and support! He also `fostered
<https://groups.google.com/forum/?fromgroups=#!topic/geoalchemy/k3PmQOB_FX4>`_
the birth of GeoAlchemy 2.


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

