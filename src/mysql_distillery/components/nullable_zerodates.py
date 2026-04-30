"""Capture source ``0000-00-00`` values on NULLABLE date/datetime/timestamp cols.

Emits ``<db>/metadata/<table>_zerodates.sql`` — pre-baked, prim_key-keyed UPDATEs
that the restore script replays under relaxed sql_mode to rehydrate
zero-dates that DuckDB's MySQL extension would otherwise collapse into
Parquet NULL indistinguishably from genuine source NULLs.

Implementation notes kept at point-of-use in this file:
- Direct pymysql (not DuckDB) — so zero-dates survive the SELECT.
- Detection via ``CAST(col AS CHAR) LIKE '0000-00-00%'`` to side-step
  strict-mode NO_ZERO_DATE (casting to DATETIME implicitly fails) and to
  cover any ``DATETIME(n)`` fractional precision.
- Tables without a PRIMARY KEY are skipped (warning logged).

Why this component is needed at all: DuckDB's MySQL extension collapses
``0000-00-00`` to Parquet NULL on the SELECT side, indistinguishable from
genuine source NULLs — so capture them here as prim_key-keyed UPDATE
replays. The NOT NULL zero-date case is handled differently on the restore
side (hex-TSV ``COALESCE`` path), not by this component.
"""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from mysql_distillery.data.dtos import ComponentResult
from mysql_distillery.data.models import ServerConnectionConfig
from mysql_distillery.data.utils.cli import make_component_cli
from mysql_distillery.data.utils.files import write_text
from mysql_distillery.data.utils.logging import setup_logger
from mysql_distillery.data.utils.mysql import get_pymysql_connection


def _nullable_date_cols_by_table(
    conn, database: str,
) -> dict[str, list[tuple[str, str]]]:
    """``table_name -> [(col_name, data_type), …]`` for NULLABLE date cols.

    Ordered by ORDINAL_POSITION so logs / emitted UPDATEs reflect the source
    column order. Only date / datetime / timestamp are in scope — those are
    the types DuckDB's MySQL extension collapses.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s
              AND DATA_TYPE IN ('date','datetime','timestamp')
              AND IS_NULLABLE='YES'
            ORDER BY TABLE_NAME, ORDINAL_POSITION
            """,
            (database,),
        )
        out: dict[str, list[tuple[str, str]]] = defaultdict(list)
        for tbl, col, dtype in cur.fetchall():
            out[tbl].append((col, dtype))
    return dict(out)


def _primary_key_columns(conn, database: str, table: str) -> list[str]:
    """Primary-key column names in ordinal order; ``[]`` if table has no prim_key."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COLUMN_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA=%s
              AND TABLE_NAME=%s
              AND CONSTRAINT_NAME='PRIMARY'
            ORDER BY ORDINAL_POSITION
            """,
            (database, table),
        )
        return [r[0] for r in cur.fetchall()]


def _zero_literal(data_type: str) -> str:
    """MySQL zero-value literal matching the column's date family."""
    return "0000-00-00" if data_type == "date" else "0000-00-00 00:00:00"


def _build_updates_for_column(
    conn,
    database: str,
    table: str,
    col: str,
    data_type: str,
    prim_key_cols: list[str],
) -> list[str]:
    """One UPDATE per row where ``table.col`` is a zero-date on source.

    Detects via ``CAST(col AS CHAR) LIKE '0000-00-00%'`` — see module docstring
    for the NO_ZERO_DATE rationale. prim_key values are escaped through
    pymysql's own escape path (``conn.escape(value)``) — same code the driver
    uses for parameter binding, so literals round-trip faithfully for every
    supported type.
    """
    zero_lit = _zero_literal(data_type)
    prim_key_select = ", ".join(f"`{c}`" for c in prim_key_cols)
    with conn.cursor() as cur:
        # nosemgrep: bandit.B608 -- identifiers (database/table/col/prim_key_cols) come from information_schema, not user input; SQL parameter binding cannot substitute identifiers.
        cur.execute(
            f"SELECT {prim_key_select} "
            f"FROM `{database}`.`{table}` "
            f"WHERE CAST(`{col}` AS CHAR) LIKE '0000-00-00%'"
        )
        prim_key_rows = cur.fetchall()

    if not prim_key_rows:
        return []

    updates: list[str] = []
    for row in prim_key_rows:
        where_parts = [
            f"`{prim_key_col}` = {conn.escape(val)}"
            for prim_key_col, val in zip(prim_key_cols, row)
        ]
        where = " AND ".join(where_parts)
        updates.append(
            f"UPDATE `{table}` SET `{col}` = '{zero_lit}' WHERE {where};"
        )
    return updates


def run(
    cfg: ServerConnectionConfig,
    out_dir: Path,
    *,
    max_workers: int = 1,
) -> ComponentResult:
    logger = setup_logger("nullable_zerodates", out_dir)
    metadata_dir = out_dir / "metadata"

    with ComponentResult.timed("nullable_zerodates") as result:
        conn = get_pymysql_connection(cfg)
        try:
            by_table = _nullable_date_cols_by_table(conn, cfg.database)
            logger.info(
                "Scanning %d table(s) with nullable date/datetime/timestamp col(s)",
                len(by_table),
            )

            sidecars_written = 0
            total_updates = 0
            skipped_no_prim_key = 0

            for table, cols in by_table.items():
                prim_key_cols = _primary_key_columns(conn, cfg.database, table)
                if not prim_key_cols:
                    col_names = ", ".join(c for c, _ in cols)
                    msg = (
                        f"{table}: no PRIMARY key — skipping nullable "
                        f"zero-date capture for col(s): {col_names}"
                    )
                    logger.warning(msg)
                    result.notes.append(msg)
                    skipped_no_prim_key += 1
                    continue

                table_updates: list[str] = []
                for col, dtype in cols:
                    col_updates = _build_updates_for_column(
                        conn, cfg.database, table, col, dtype, prim_key_cols,
                    )
                    if col_updates:
                        logger.info(
                            "%s.%s: %d nullable zero-date row(s)",
                            table, col, len(col_updates),
                        )
                        table_updates.extend(col_updates)

                if not table_updates:
                    continue

                header = (
                    f"-- Nullable-col zero-date restore for `{table}`.\n"
                    f"-- Generated by mysql_distillery.components.nullable_zerodates.\n"
                    f"-- Apply under relaxed sql_mode (e.g. SET sql_mode = '') —\n"
                    f"-- NO_ZERO_DATE would otherwise reject the 0000-00-00 literal.\n\n"
                )
                path = write_text(
                    metadata_dir / f"{table}_zerodates.sql",
                    header + "\n".join(table_updates) + "\n",
                )
                result.artifacts.append(path)
                sidecars_written += 1
                total_updates += len(table_updates)

            logger.info(
                "wrote %d sidecar(s), %d UPDATE stmt(s) total; "
                "%d table(s) skipped (no PRIMARY key)",
                sidecars_written, total_updates, skipped_no_prim_key,
            )
            if sidecars_written == 0:
                result.notes.append("no nullable zero-dates found")
        finally:
            conn.close()

    return result


if __name__ == "__main__":
    make_component_cli("nullable_zerodates", run)()
