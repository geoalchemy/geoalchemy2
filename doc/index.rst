GeoAlchemy 2 Documentation
==========================

*Using SQLAlchemy with Spatial Databases.*

GeoAlchemy 2 provides extensions to `SQLAlchemy <http://sqlalchemy.org>`_ for
working with spatial databases.

GeoAlchemy 2 focuses on `PostGIS <http://postgis.net/>`_. PostGIS 2 and PostGIS 3 are supported.

GeoAlchemy 2 also supports the following dialects:

* `SpatiaLite <https://www.gaia-gis.it/fossil/libspatialite/home>`_ >= 4.3.0 (except for alembic
  helpers that require SpatiaLite >= 5)
* `MySQL <https://dev.mysql.com/doc/refman/8.0/en/spatial-types.html>`_ >= 8
* `MariaDB <https://mariadb.com/kb/en/gis-features-in-533/>`_ >= 5.3.3 (experimental)
* `GeoPackage <http://www.geopackage.org/spec/>`_

Note that using GeoAlchemy 2 with these dialects may require some specific configuration on the
application side. It also may not be optimal for performance.

GeoAlchemy 2 aims to be simpler than its predecessor, `GeoAlchemy
<https://pypi.python.org/pypi/GeoAlchemy>`_. Simpler to use, and simpler
to maintain.

.. toctree::
   :hidden:

   changelog

The current version of this documentation applies to the version |version| of GeoAlchemy 2. See the
:ref:`changelog` page for details on recent changes.


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
   dialect_specific_features


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

   admin
   plugin
   types
   elements
   spatial_functions
   spatial_operators
   shape
   alembic_helpers

Development
-----------

The code is available on GitHub: https://github.com/geoalchemy/geoalchemy2.

Main authors:

* Adrien Berchet (https://github.com/adrien-berchet)
* Éric Lemoine (https://github.com/elemoine)

Other contributors:

* Caleb Johnson (https://github.com/calebj)
* Dolf Andringa (https://github.com/dolfandringa)
* Frédéric Junod, Camptocamp SA (https://github.com/fredj)
* ijl (https://github.com/ijl)
* Loïc Gasser (https://github.com/loicgasser)
* Marcel Radischat (https://github.com/quiqua)
* Matt Broadway (https://github.com/mbway)
* rapto (https://github.com/rapto)
* Serge Bouchut (https://github.com/SergeBouchut)
* Tobias Bieniek (https://github.com/Turbo87)
* Tom Payne (https://github.com/twpayne)

Many thanks to Mike Bayer for his guidance and support! He also `fostered
<https://groups.google.com/forum/?fromgroups=#!topic/geoalchemy/k3PmQOB_FX4>`_
the birth of GeoAlchemy 2.

Citation
--------

When you use this software, we kindly ask you to cite the following DOI:

.. image:: https://zenodo.org/badge/5638538.svg
  :target: https://zenodo.org/doi/10.5281/zenodo.10808783


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
