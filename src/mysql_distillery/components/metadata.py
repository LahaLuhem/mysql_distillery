"""Write the snapshot manifest — must run AFTER every other component.

Produces:
  - metadata/auto_increment.yaml  : next AUTO_INCREMENT value per table
  - manifest.yaml                 : top-level index with source info + SHA256s
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import yaml

from mysql_distillery.data.dtos import ComponentResult
from mysql_distillery.data.models import ServerConnectionConfig
from mysql_distillery.data.utils.cli import make_component_cli
from mysql_distillery.data.utils.duckdb import get_duckdb_connection
from mysql_distillery.data.utils.files import sha256_file, write_text
from mysql_distillery.data.utils.logging import setup_logger

# Subdirectories produced by other components that we checksum.
_COMPONENT_DIRS = [
    "schema",
    "constraints",
    "data",
    "views",
    "routines",
    "triggers",
    "events",
    "metadata",
]


def _collect_auto_increments(conn, database: str) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT table_name, auto_increment
        FROM src.information_schema.tables
        WHERE table_schema = ?
          AND auto_increment IS NOT NULL
        ORDER BY table_name
        """,
        [database],
    ).fetchall()
    return {name: int(val) for name, val in rows}


def _collect_row_counts(data_dir: Path, duck_conn) -> dict[str, int]:
    """Read row counts from the Parquet files we just wrote (not from MySQL).

    Using the Parquet files as the source of truth means the manifest always
    reflects what was actually extracted.
    """
    counts: dict[str, int] = {}
    if not data_dir.exists():
        return counts
    for path in sorted(data_dir.glob("*.parquet")):
        (n,) = duck_conn.execute(
            "SELECT count(*) FROM read_parquet(?);",
            [path.as_posix()],
        ).fetchone()
        counts[path.stem] = int(n)
    return counts


def _collect_source_info(conn) -> dict[str, str]:
    """Grab server-level context that a restorer needs to reproduce behavior."""
    version = conn.execute("SELECT * FROM mysql_query('src', 'SELECT VERSION()');").fetchone()[0]
    sql_mode = conn.execute(
        "SELECT * FROM mysql_query('src', 'SELECT @@global.sql_mode');"
    ).fetchone()[0]
    charset = conn.execute(
        "SELECT * FROM mysql_query('src', 'SELECT @@global.character_set_server');"
    ).fetchone()[0]
    return {
        "mysql_version": str(version),
        "sql_mode": str(sql_mode),
        "default_charset": str(charset),
    }


def _checksum_tree(out_dir: Path) -> dict[str, dict[str, str]]:
    """Return {component_name: {relative_path: sha256}} for every artifact file."""
    result: dict[str, dict[str, str]] = {}
    for comp in _COMPONENT_DIRS:
        comp_dir = out_dir / comp
        if not comp_dir.exists():
            continue
        entries: dict[str, str] = {}
        for path in sorted(comp_dir.rglob("*")):
            if path.is_file():
                rel = path.relative_to(out_dir).as_posix()
                entries[rel] = sha256_file(path)
        if entries:
            result[comp] = entries
    return result


def run(
    cfg: ServerConnectionConfig,
    out_dir: Path,
    *,
    max_workers: int = 1,
) -> ComponentResult:
    logger = setup_logger("metadata", out_dir)

    with ComponentResult.timed("metadata") as result:
        conn = get_duckdb_connection(cfg)
        try:
            # 1. Auto-increment snapshot.
            auto_inc = _collect_auto_increments(conn, cfg.database)
            auto_inc_path = out_dir / "metadata" / "auto_increment.yaml"
            write_text(auto_inc_path, yaml.safe_dump(auto_inc, sort_keys=True))
            result.artifacts.append(auto_inc_path)
            logger.info(
                "wrote %s (%d tables)",
                auto_inc_path.relative_to(out_dir),
                len(auto_inc),
            )

            # 2. Row counts from the extracted Parquet files.
            row_counts = _collect_row_counts(out_dir / "data", conn)

            # 3. Source info.
            source = _collect_source_info(conn)
            source["host"] = cfg.host  # never include password
            source["database"] = cfg.database

            # 4. Manifest with per-file checksums.
            manifest = {
                "schema_version": 1,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "source": source,
                "row_counts": row_counts,
                "components": _checksum_tree(out_dir),
            }
            manifest_path = out_dir / "manifest.yaml"
            write_text(manifest_path, yaml.safe_dump(manifest, sort_keys=False))
            result.artifacts.append(manifest_path)
            logger.info("wrote %s", manifest_path.relative_to(out_dir))
        finally:
            conn.close()

    return result


if __name__ == "__main__":
    make_component_cli("metadata", run)()
