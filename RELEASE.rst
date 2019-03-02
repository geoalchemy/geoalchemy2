Release
-------

This file provides the steps for releasing a new version of GeoAlchemy 2.

Add a new section to CHANGES.txt, change the version number in ``setup.py`` and
``docs/conf.py``, then create a PR with that. Proceed when the PR is merged.

Make sure Travis is all green: https://travis-ci.org/geoalchemy/geoalchemy2.

Create Git tag and push it::

    $ git tag -a x.y -m 'version x.y'
    $ git push origin x.y

Go to http://readthedocs.org/dashboard/geoalchemy-2/edit/ and set "Default
version" to x.y.

Note that there's no need to manually upload the package to PyPI. This is
done automatically by Travis when the release tag is pushed to GitHub.
