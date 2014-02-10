"""
Python 2 and 3 compatibility:

    - Py3k `memoryview()` made an alias for Py2k `buffer()`
    - Py3k `bytes()` made an alias for Py2k `str()`
"""

import sys

if sys.version_info[0] == 2:
    buffer = __builtins__['buffer']
    bytes = str

else:
    # Python 2.6 flake8 workaround
    buffer = __builtins__['memoryview']
    bytes = bytes
