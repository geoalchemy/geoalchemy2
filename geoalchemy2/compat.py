"""
Python 2 and 3 compatibility:

    - Py3k `memoryview()` made an alias for Py2k `buffer()`
    - Py3k `bytes()` made an alias for Py2k `str()`
"""

import sys
py3k = sys.version_info[0] == 3


if py3k:
    # Python 2.6 flake8 workaround
    buffer = __builtins__['memoryview']
    bytes = bytes

else:
    buffer = __builtins__['buffer']
    bytes = str
