import os
import sys

from geoalchemy2._functions_helpers import _diff_function_catalog

"""
Compares a live PostGIS instance's pg_proc catalog against geoalchemy2's `_FUNCTIONS` and
`_FUNCTION_OVERLOADS` tables (in geoalchemy2/_functions.py), and prints a report: functions
PostGIS has that geoalchemy2 doesn't expose, functions geoalchemy2 tracks that this instance
doesn't have, and polymorphic functions (return type depends on the GIS type of the
arguments) that aren't yet reflected in `_FUNCTION_OVERLOADS`.

This is a read-only report for manual review - it never writes to `_functions.py` itself.
Re-run it whenever a new PostGIS release ships, to catch drift between what PostGIS actually
offers and what this library tracks.

1. Start a disposable PostGIS instance
----------------------------------------

Docker/Podman, matching the PostGIS version you want to check against (see
https://hub.docker.com/r/postgis/postgis/tags for available tags - they combine a Postgres
major version with a PostGIS version, e.g. ``18-3.6``, not a bare PostGIS patch version)::

    docker run -d --name geoalchemy2-postgis-test \\
        -e POSTGRES_PASSWORD=postgres \\
        -p 5432:5432 \\
        postgis/postgis:18-3.6

If ``docker run`` fails with a netavark/bridge-creation error (seen in some rootless
Podman/sandboxed setups - the container is "Created" but never reaches "Up"), add
``--network host`` instead of ``-p 5432:5432``. If even that leaves the port unreachable from
outside the container (e.g. the shell you're running this from doesn't share a network
namespace with the Docker/Podman daemon), you can still validate the report's *logic*, if not
run it live end-to-end, via `docker exec`, without needing psycopg2 connectivity at all::

    docker exec <container> psql -U postgres --csv -c "
        SELECT p.proname, pg_catalog.pg_get_function_result(p.oid),
               pg_catalog.pg_get_function_identity_arguments(p.oid),
               pg_catalog.obj_description(p.oid, 'pg_proc')
        FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE p.proname LIKE 'st\\_%' AND n.nspname = 'public'
        ORDER BY p.proname
    " > pg_functions.csv

then feed the rows through `geoalchemy2._functions_helpers._process_pg_function_rows` and
`_compute_polymorphic_overloads` directly in a Python shell (that's exactly the split those
two functions exist for).

2. Enable the extensions this library cares about
--------------------------------------------------

By default only core PostGIS functions are registered - raster and SFCGAL (3D/solid
geometry) functions won't exist in pg_proc at all until their extensions are created, which
will make this script wrongly report hundreds of real functions as "missing from postgis"::

    docker exec <container> psql -U postgres -c "CREATE EXTENSION postgis_raster;"
    docker exec <container> psql -U postgres -c "CREATE EXTENSION postgis_sfcgal;"

3. Run this script
---------------------

    GEOALCHEMY2_TEST_PG_DSN="dbname=postgres user=postgres password=postgres host=localhost" \\
        python generate_function_overloads.py

The report has four sections: functions PostGIS has that `_FUNCTIONS` doesn't track at all
(candidates to add - see step 2 above before trusting this list, and check
https://postgis.net/docs/ for each candidate's real name casing and description rather than
guessing, since pg_proc only ever returns lowercase names); functions `_FUNCTIONS` tracks
that this instance doesn't have (likely renamed/removed - check before deleting, older
callers may still target an older PostGIS version); new polymorphic-function candidates for
`_FUNCTION_OVERLOADS`; and any existing `_FUNCTION_OVERLOADS` entries that disagree with what
this instance actually reports.
"""

if __name__ == "__main__":
    dsn = os.environ.get("GEOALCHEMY2_TEST_PG_DSN")
    if not dsn:
        print(
            "Set GEOALCHEMY2_TEST_PG_DSN to a psycopg2 connection string for a reachable "
            "PostGIS instance, e.g.:\n"
            '  GEOALCHEMY2_TEST_PG_DSN="dbname=postgres user=postgres password=postgres '
            'host=localhost" python generate_function_overloads.py\n'
            "See this script's module docstring for how to start one and which extensions "
            "to enable first.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(_diff_function_catalog(dsn))
