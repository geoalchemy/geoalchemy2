Release
-------

This file provides the steps for releasing a new version of GeoAlchemy 2.

Change the version number in ``setup.py`` and ``docs/conf.py``, then commit
and push.

Make sure Travis is all green: https://travis-ci.org/geoalchemy/geoalchemy2.

Create Git tag and push it::

    $ git tag -a x.y -m 'version x.y'
    $ git push origin x.y

Go to http://readthedocs.org/dashboard/geoalchemy-2/edit/ and set "Default
version" to x.y.

Upload the package to PyPI::

    $ python setup.py sdist upload
