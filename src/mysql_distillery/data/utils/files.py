"""Filesystem helpers — streaming checksum, safe text write, empty-dir check."""
from __future__ import annotations

import hashlib
from pathlib import Path


def sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    """Streaming SHA256 — safe for multi-GB Parquet files."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


def write_text(path: Path, content: str) -> Path:
    """Write UTF-8 text, creating parent dirs. Returns the path for chaining."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def ensure_empty_dir(path: Path, *, force: bool) -> None:
    """Create `path`; if it already contains files, require --force to proceed."""
    path.mkdir(parents=True, exist_ok=True)
    # Ignore dotfiles from filesystem noise (.DS_Store etc).
    non_empty = any(p for p in path.iterdir() if not p.name.startswith("."))
    if non_empty and not force:
        raise SystemExit(
            f"Output directory {path} is not empty. Pass --force to overwrite."
        )
