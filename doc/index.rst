GeoAlchemy 2 Documentation
==========================

*Using SQLAlchemy with Spatial Databases.*

GeoAlchemy 2 provides extensions to `SQLAlchemy <http://sqlalchemy.org>`_ for
working with spatial databases.

GeoAlchemy 2 focuses on `PostGIS <http://postgis.net/>`_. PostGIS 1.5,
PostGIS 2 and PostGIS 3 are supported.

SpatiaLite is also supported, but using GeoAlchemy 2 with SpatiaLite requires some specific
configuration on the application side. GeoAlchemy 2 works with SpatiaLite 4.3.0 and higher
(except for alembic helpers that need SpatiaLite >= 5).

GeoAlchemy 2 aims to be simpler than its predecessor, `GeoAlchemy
<https://pypi.python.org/pypi/GeoAlchemy>`_. Simpler to use, and simpler
to maintain.

The current version of this documentation applies to the version |version| of GeoAlchemy 2.


Requirements
------------

GeoAlchemy 2 requires `SQLAlchemy <http://sqlalchemy.org>`_ >= 1.4.

Installation
------------

GeoAlchemy 2 is `available on the Python Package Index
<https://pypi.python.org/pypi/GeoAlchemy2/>`_. So it can be installed
with the standard `pip <http://www.pip-installer.org>`_ or
`easy_install <http://peak.telecommunity.com/DevCenter/EasyInstall>`_
tools.

What's New in GeoAlchemy 2
--------------------------

* GeoAlchemy 2 supports PostGIS' ``geometry`` type, as well as the ``geography``
  and ``raster`` types.
* The first series had its own namespace for spatial functions. With GeoAlchemy
  2, spatial functions are called like any other SQLAlchemy function, using
  ``func``, which is SQLAlchemy's `standard way
  <http://docs.sqlalchemy.org/en/latest/core/expression_api.html#sqlalchemy.sql.expression.func>`_
  of calling SQL functions.
* GeoAlchemy 2 works with SQLAlchemy's ORM, as well as with SQLAlchemy's *SQL
  Expression Language* (a.k.a the SQLAlchemy Core). (This is thanks to
  SQLAlchemy's new `type-level comparator system
  <http://docs.sqlalchemy.org/en/latest/core/types.html?highlight=comparator_factory#types-operators>`_.)
* GeoAlchemy 2 supports `reflection
  <http://docs.sqlalchemy.org/en/latest/core/schema.html#metadata-reflection>`_
  of geometry and geography columns.
* GeoAlchemy 2 adds ``to_shape``, ``from_shape`` functions for a better
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
   spatialite_tutorial


Gallery
---------

.. toctree::
   :hidden:

   gallery/index

The :ref:`gallery` page shows examples of the GeoAlchemy 2's functionalities.


Use with Alembic
----------------

.. toctree::
   :hidden:

   alembic

The GeoAlchemy 2 package is compatible with the migration tool
`Alembic <https://alembic.sqlalchemy.org/en/latest/>`_. The :ref:`alembic_use` page
provides more details on this topic.


Reference Documentation
-----------------------

.. toctree::
   :maxdepth: 1

   types
   elements
   spatial_functions
   spatial_operators
   shape
   alembic_helpers

Development
-----------

The code is available on GitHub: https://github.com/geoalchemy/geoalchemy2.

Contributors:

* Adrien Berchet (https://github.com/adrien-berchet)
* Éric Lemoine (https://github.com/elemoine)
* Dolf Andringa (https://github.com/dolfandringa)
* Frédéric Junod, Camptocamp SA (https://github.com/fredj)
* ijl (https://github.com/ijl)
* Loïc Gasser (https://github.com/loicgasser)
* Marcel Radischat (https://github.com/quiqua)
* rapto (https://github.com/rapto)
* Serge Bouchut (https://github.com/SergeBouchut)
* Tobias Bieniek (https://github.com/Turbo87)
* Tom Payne (https://github.com/twpayne)

Many thanks to Mike Bayer for his guidance and support! He also `fostered
<https://groups.google.com/forum/?fromgroups=#!topic/geoalchemy/k3PmQOB_FX4>`_
the birth of GeoAlchemy 2.


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
