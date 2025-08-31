import os
import sys
from pathlib import Path


def _ensure_src_on_path() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    src = repo_root / "src"
    if src.exists():
        sys.path.insert(0, str(src))


_ensure_src_on_path()

# Force offline mode in CI and local tests to avoid model downloads
os.environ.setdefault("CPS_OFFLINE", "1")
