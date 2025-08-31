"""
Test harness convenience: ensure `src/` is on sys.path so imports like
`import catalog_pii_scanner` work without requiring editable installs.
"""

import os
import sys

here = os.path.dirname(os.path.abspath(__file__))
src = os.path.join(here, "src")
if os.path.isdir(src) and src not in sys.path:
    sys.path.insert(0, src)
