"""Extract table rows as Parquet, one file per table, in parallel.

Uses DuckDB's MySQL extension to stream rows — no Pandas intermediate, so
multi-GB tables don't blow up memory. Each worker thread opens its own DuckDB
connection (DuckDB connections are not thread-safe).

Column types whose Parquet round-trip deserves restore-time attention (blob,
json, bit, geometry, enum, set, varbinary, binary) are flagged as ``notes`` on
the :class:`ComponentResult` so a reviewer sees them.
"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Final

from mysql_distillery.data.dtos import ComponentResult
from mysql_distillery.data.models import ServerConnectionConfig
from mysql_distillery.data.utils.cli import make_component_cli
from mysql_distillery.data.utils.duckdb import get_duckdb_connection
from mysql_distillery.data.utils.logging import setup_logger
from mysql_distillery.data.utils.mysql import preflight_check

# Column types whose Parquet round-trip needs restore-time attention.
# We surface these as `notes` on the ComponentResult so a reviewer sees them.
_TYPES_TO_WARN: Final[frozenset[str]] = frozenset(
    {"json", "bit", "geometry", "enum", "set", "varbinary", "binary", "blob"}
)


def _list_base_tables(conn, database: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT table_name
        FROM src.information_schema.tables
        WHERE table_schema = ? AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        [database],
    ).fetchall()
    return [r[0] for r in rows]


def _risky_columns(conn, database: str, table: str) -> list[tuple[str, str]]:
    """Return [(column_name, data_type), ...] for types in _TYPES_TO_WARN."""
    rows = conn.execute(
        """
        SELECT column_name, data_type
        FROM src.information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        """,
        [database, table],
    ).fetchall()
    return [(c, t) for c, t in rows if t and t.lower() in _TYPES_TO_WARN]


def _export_one_table(
    cfg: ServerConnectionConfig,
    database: str,
    table: str,
    out_path: Path,
    logger,
    lock: threading.Lock,
) -> tuple[Path, int, list[str]]:
    """Dump `database`.`table` to `out_path` as ZSTD Parquet. Returns (path, row_count, notes)."""
    conn = get_duckdb_connection(cfg)
    notes: list[str] = []
    try:
        risky = _risky_columns(conn, database, table)
        if risky:
            msg = f"{table}: review types on restore — " + ", ".join(
                f"{c} ({t})" for c, t in risky
            )
            notes.append(msg)
            with lock:
                logger.warning(msg)

        # Parameter binding isn't available inside a COPY ... TO literal, so
        # we splice identifiers directly. `database` and `table` come from
        # information_schema, not user input.
        out_path.parent.mkdir(parents=True, exist_ok=True)
        conn.execute(
            f'COPY (SELECT * FROM src."{database}"."{table}") '
            f"TO '{out_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);"
        )

        row_count = conn.execute(
            "SELECT count(*) FROM read_parquet(?);",
            [out_path.as_posix()],
        ).fetchone()[0]

        with lock:
            logger.info("wrote %s (%s rows)", out_path.name, row_count)
        return out_path, int(row_count), notes
    finally:
        conn.close()


def run(
    cfg: ServerConnectionConfig,
    out_dir: Path,
    *,
    max_workers: int = 4,
) -> ComponentResult:
    logger = setup_logger("data", out_dir)
    data_dir = out_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    log_lock = threading.Lock()

    with ComponentResult.timed("data") as result:
        # Preflight: verify the source MySQL is reachable before any work.
        # Fails fast with a human-friendly error instead of discovering the
        # problem per-worker halfway through extraction.
        preflight_check(cfg)
        logger.info("preflight: MySQL %s:%d reachable", cfg.host, cfg.port)

        # Discover tables on the main thread.
        main_conn = get_duckdb_connection(cfg)
        try:
            tables = _list_base_tables(main_conn, cfg.database)
        finally:
            main_conn.close()

        logger.info(
            "Exporting %d tables with max_workers=%d", len(tables), max_workers
        )

        # Parallel per-table export (Parquet for all tables).
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(
                    _export_one_table,
                    cfg,
                    cfg.database,
                    table,
                    data_dir / f"{table}.parquet",
                    logger,
                    log_lock,
                ): table
                for table in tables
            }
            for fut in as_completed(futures):
                table = futures[fut]
                try:
                    path, _, notes = fut.result()
                    result.artifacts.append(path)
                    result.notes.extend(notes)
                except Exception as exc:  # one table failure shouldn't abort the rest
                    msg = f"{table}: {type(exc).__name__}: {exc}"
                    with log_lock:
                        logger.error(msg)
                    result.notes.append("FAILED: " + msg)
                    result.status = "error"
                    result.error = msg

    return result


if __name__ == "__main__":
    make_component_cli("data", run)()
