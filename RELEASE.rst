Release
-------

This file provides the steps for releasing a new version of GeoAlchemy 2.

Verify that the version number is correct in ``setup.py`` and ``docs/conf.py``.
If not then change it, then commit and push.

Verify that the tests pass, with 100% coverage::

    $ python setup.py nosetests
    .............................................................................................
    Name                     Stmts   Miss  Cover   Missing
    ------------------------------------------------------
    geoalchemy2                 37      0   100%
    geoalchemy2.comparator      40      0   100%
    geoalchemy2.elements        23      0   100%
    geoalchemy2.functions      100      0   100%
    geoalchemy2.shape           11      0   100%
    geoalchemy2.types           33      0   100%
    ------------------------------------------------------
    TOTAL                      244      0   100%
    ----------------------------------------------------------------------
    Ran 93 tests in 3.141s

    OK

Create Git tag and push it::

    $ git tag -a x.y -m 'version x.y'
    $ git push origin x.y

Go to http://readthedocs.org/dashboard/geoalchemy-2/edit/ and set "Default
version" to x.y.

Upload the package to PyPI::

    $ python setup.py sdist upload
