"""Centralized path resolution for Project_K.

Design goals:
- **No hard-coded absolute paths** anywhere in code.
- Defaults are **repo-relative**, so the project is portable across machines/drives.
- All paths are overrideable via env vars for power users / deployment.

Path 1 (symlink strategy) friendly:
- Default TRAINING_DIR is <repo>/data/training.
- On a host with a large external drive, make that folder a symlink to the drive.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional


def _resolve_path(value: str, *, base: Optional[Path] = None) -> Path:
    """Resolve a user-supplied path.

    - Expands '~'
    - If relative, interprets relative to `base` (or CWD if base is None)
    - Resolves without requiring the path to exist
    """
    p = Path(os.path.expanduser(value))
    if not p.is_absolute():
        p = (base or Path.cwd()) / p
    return p.resolve(strict=False)


def _default_repo_root() -> Path:
    """Default repo root: the directory above ./services.

    This is robust across machines and avoids assuming a specific home subdirectory.
    """
    return Path(__file__).resolve().parents[1]


def resolve_repo_root() -> Path:
    env_root = os.getenv("PROJECT_K_ROOT")
    if env_root:
        return _resolve_path(env_root)
    return _default_repo_root()


REPO_ROOT: Path = resolve_repo_root()

# ---- Data paths ----
ENV_DATA_DIR = os.getenv("PROJECT_K_DATA_DIR")
DATA_DIR: Path = _resolve_path(ENV_DATA_DIR, base=REPO_ROOT) if ENV_DATA_DIR else (REPO_ROOT / "data").resolve(strict=False)

ENV_DB_PATH = os.getenv("PROJECT_K_DB_PATH")
DB_PATH: Path = _resolve_path(ENV_DB_PATH, base=REPO_ROOT) if ENV_DB_PATH else (DATA_DIR / "kalshi.db").resolve(strict=False)

ENV_OBI_DB_PATH = os.getenv("PROJECT_K_OBI_DB_PATH")
OBI_DB_PATH: Path = _resolve_path(ENV_OBI_DB_PATH, base=REPO_ROOT) if ENV_OBI_DB_PATH else DB_PATH

ENV_TRAINING_DIR = os.getenv("PROJECT_K_TRAINING_DIR")
# Default is repo-relative; a symlink at <repo>/data/training can point to an external drive.
TRAINING_DIR: Path = _resolve_path(ENV_TRAINING_DIR, base=REPO_ROOT) if ENV_TRAINING_DIR else (DATA_DIR / "training").resolve(strict=False)

# ---- Artifact paths ----
ENV_ARTIFACTS_DIR = os.getenv("PROJECT_K_ARTIFACTS_DIR")
ARTIFACTS_DIR: Path = _resolve_path(ENV_ARTIFACTS_DIR, base=REPO_ROOT) if ENV_ARTIFACTS_DIR else (REPO_ROOT / "artifacts").resolve(strict=False)
