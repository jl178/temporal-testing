import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ETL_DIR = os.path.join(REPO_ROOT, "etl")
for path in (REPO_ROOT, ETL_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)
