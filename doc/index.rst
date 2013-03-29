.. GeoAlchemy2 documentation master file, created by
   sphinx-quickstart on Thu Aug 23 06:38:45 2012.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

GeoAlchemy 2 Documentation
==========================

.. image:: assets/geoalchemy.png

*Using SQLAlchemy with Spatial Databases.*

GeoAlchemy 2 aims to be simpler than its predecessor, in terms of both usage,
and maintainance.

GeoAlchemy 2 requires SQLAlchemy 0.8; it does not work with SQLAlchemy 0.7 and
lower.

GeoAlchemy 2 supports PostGIS 2.0 and PostGIS 1.5.

GeoAlchemy 2 doesn't currently support other dialects than PostgreSQL/PostGIS.
Supporting Oracle Locator in the previous series was the main contributor to
code complexity. So it is currently not clear whether we want to go there
again. Please contact us you want to add, and maintain, support for other
spatial databases in GeoAlchemy 2.

What's New in GeoAlchemy 2
--------------------------

The first series had its own namespace for spatial functions, namely
``geoalchemy.functions``. With GeoAlchemy 2, spatial functions are called like
any other function, using ``func``, which is SQLAlchemy's `standard way
<http://docs.sqlalchemy.org/en/latest/core/expression_api.html#sqlalchemy.sql.expression.func>`_
of calling SQL functions.

GeoAlchemy 2 works with SQLAlchemy's ORM, as well as with SQLAlchemy's *SQL
Expression Language* (a.k.a the SQLAlchemy Core). This is thanks to SQLAlchemy's
new `type-level comparator system
<http://docs.sqlalchemy.org/en/latest/core/types.html?highlight=comparator_factory#types-operators>`_.

GeoAlchemy 2 supports PostGIS' ``geometry`` type, as well as ``geography``
type.

GeoAlchemy 2 supports `reflection
<http://docs.sqlalchemy.org/en/latest/core/schema.html#metadata-reflection>`_
of geometry and geography columns.

GeoAlchemy 2 adds ``to_shape``, ``from_shape`` functions for a better
integration with `Shapely <http://pypi.python.org/pypi/Shapely>`_.


.. toctree::
   :hidden:

   migrate

See the :ref:`migrate` page for details on how to migrate a GeoAlchemy
application to GeoAlchemy 2.


Tutorials
---------

GeoAlchemy 2 works with both SQLAlchemy's *Object Relational Mapping* (ORM) and
*SQL Expression Language*. This documentation provides a tutorial for each
system. If you're new to GeoAlchemy 2 start with this.

.. toctree::
   :maxdepth: 1

   orm_tutorial
   core_tutorial

Reference Documentation
-----------------------

.. toctree::
   :maxdepth: 1

   types
   elements
   spatial_functions
   spatial_operators
   shape

Development
-----------

The code is available on GitHub: https://github.com/geoalchemy/geoalchemy2.

Contributors:

* Eric Lemoine, Camptocamp SA (https://github.com/elemoine)
* Frédéric Junod, Camptocamp SA (https://github.com/fredj)
* rapto (https://github.com/rapto)
* Tom Payne, Camptocamp SA (https://github.com/twpayne)

Many thanks to Mike Bayer for his guidance and support! He also `fostered
<https://groups.google.com/forum/?fromgroups=#!topic/geoalchemy/k3PmQOB_FX4>`_
the birth of GeoAlchemy 2.


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

