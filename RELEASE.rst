Release
-------

This file provides the steps for releasing a new version of GeoAlchemy 2.

Add a new section to CHANGES.txt, then create a PR with that.
Proceed when the PR is merged.

Make sure the CI is all green: https://github.com/geoalchemy/geoalchemy2/actions

Create a new Release on GitHub. The release tag should be formatted as 'X.Y.Z'.

Go to https://readthedocs.org/projects/geoalchemy-2/builds/ and run the compilation for
the Latest version.

Note that there's no need to manually upload the package to PyPI. This is
done automatically by the CI when the release tag is pushed to GitHub.
