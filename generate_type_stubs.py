from pathlib import Path

from geoalchemy2._functions_helpers import _generate_stubs

"""
this script is outside the geoalchemy2 package because the 'geoalchemy2.types'
package interferes with the 'types' module in the standard library
"""

script_dir = Path(__file__).resolve().parent


if __name__ == "__main__":
    (script_dir / "geoalchemy2/functions.pyi").write_text(_generate_stubs())
