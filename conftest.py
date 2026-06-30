# Make the non-package app modules importable from tests. simulator/ and ingest/
# are plain script dirs (no __init__), so add them to the path before collection.
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
for d in ("simulator", "ingest"):
    sys.path.insert(0, str(ROOT / d))
