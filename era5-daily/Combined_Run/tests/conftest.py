"""tests/conftest.py."""

import sys
import os

# ensure the project root (one level up) is on the import path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
