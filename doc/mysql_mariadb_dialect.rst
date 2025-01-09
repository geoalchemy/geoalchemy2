.. _mysql_mariadb_dialect:

MySQL / MariaDB Tutorial
========================

GeoAlchemy 2's main target is PostGIS. But GeoAlchemy 2 also supports MySQL and MariaDB.
This tutorial describes how to use GeoAlchemy 2 with these dialects.

.. _mysql_mariadb_connect:

Connect to the DB
-----------------

Just like when using PostGIS connecting to a MySQL or MariaDB database requires an ``Engine``.
An engine for these dialects can be created in two ways. Using the plugin provided by
``GeoAlchemy2`` (see :ref:`plugin` for more details)::

    >>> from sqlalchemy import create_engine
    >>> engine = create_engine(
    ...     "mysql://user:password@host:port/dbname",
    ...     echo=True,
    ...     plugins=["geoalchemy2"]
    ... )

The call to ``create_engine`` creates an engine bound to the database given in the URL. After that
a ``before_cursor_execute`` listener is registered on the engine (see
:func:`geoalchemy2.admin.dialects.mysql.before_cursor_execute` and
:func:`geoalchemy2.admin.dialects.mariadb.before_cursor_execute`). The listener is responsible for
converting the parameters passed to query in the proper format, which is often a necessary operation
for using these dialects, though it depends on the driver used. If the driver does not require such
conversion, it is possible to disable this feature with the URL parameter
``geoalchemy2_before_cursor_execute_mysql_convert`` or
``geoalchemy2_before_cursor_execute_mariadb_convert``, depending on the dialect used.


It is also possible to create a raw engine and attach the listener manually::

    >>> from geoalchemy2.admin.dialects.mysql import before_cursor_execute
    >>> from sqlalchemy import create_engine
    >>> from sqlalchemy.event import listen
    >>>
    >>> engine = create_engine("mysql://user:password@host:port/dbname", echo=True)
    >>> listen(engine, "before_cursor_execute", before_cursor_execute)
