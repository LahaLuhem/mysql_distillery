"""Extract foreign-key constraints as standalone `ALTER TABLE` statements.

Written to a single file `constraints/<db>.sql`, applied by the restorer
AFTER tables and data have loaded.
"""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from mysql_distillery.data.dtos import ComponentResult
from mysql_distillery.data.models import ServerConnectionConfig
from mysql_distillery.data.utils.cli import make_component_cli
from mysql_distillery.data.utils.duckdb import get_duckdb_connection
from mysql_distillery.data.utils.files import write_text
from mysql_distillery.data.utils.logging import setup_logger


def _fetch_foreign_keys(conn, database: str) -> list[dict]:
    """Return one row per (constraint, column) from information_schema.

    Multi-column foreign_keys produce multiple rows; we aggregate in `run`.
    """
    rows = conn.execute(
        """
        SELECT
            kcu.constraint_name,
            kcu.table_name,
            kcu.column_name,
            kcu.ordinal_position,
            kcu.referenced_table_name,
            kcu.referenced_column_name,
            rc.delete_rule,
            rc.update_rule
        FROM src.information_schema.key_column_usage AS kcu
        JOIN src.information_schema.referential_constraints AS rc
            ON  rc.constraint_schema = kcu.constraint_schema
            AND rc.constraint_name   = kcu.constraint_name
        WHERE kcu.constraint_schema = ?
          AND kcu.referenced_table_name IS NOT NULL
        ORDER BY kcu.table_name, kcu.constraint_name, kcu.ordinal_position
        """,
        [database],
    ).fetchall()

    return [
        {
            "constraint_name": r[0],
            "table_name": r[1],
            "column_name": r[2],
            "ordinal_position": r[3],
            "referenced_table_name": r[4],
            "referenced_column_name": r[5],
            "delete_rule": r[6],
            "update_rule": r[7],
        }
        for r in rows
    ]


def _build_alter_statements(rows: list[dict]) -> list[str]:
    """Group multi-column foreign_keys back together and emit one ALTER per constraint."""
    # key = (table, constraint_name)
    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in rows:
        grouped[(row["table_name"], row["constraint_name"])].append(row)

    statements: list[str] = []
    for (table, cname), cols in grouped.items():
        cols.sort(key=lambda r: r["ordinal_position"])
        local = ", ".join(f"`{c['column_name']}`" for c in cols)
        remote = ", ".join(f"`{c['referenced_column_name']}`" for c in cols)
        ref_table = cols[0]["referenced_table_name"]
        delete_rule = cols[0]["delete_rule"]
        update_rule = cols[0]["update_rule"]

        parts = [
            f"ALTER TABLE `{table}`",
            f"  ADD CONSTRAINT `{cname}` FOREIGN KEY ({local})",
            f"  REFERENCES `{ref_table}` ({remote})",
        ]
        if delete_rule and delete_rule != "NO ACTION":
            parts.append(f"  ON DELETE {delete_rule}")
        if update_rule and update_rule != "NO ACTION":
            parts.append(f"  ON UPDATE {update_rule}")
        statements.append("\n".join(parts) + ";")
    return statements


def run(
    cfg: ServerConnectionConfig,
    out_dir: Path,
    *,
    max_workers: int = 1,
) -> ComponentResult:
    logger = setup_logger("constraints", out_dir)

    with ComponentResult.timed("constraints") as result:
        conn = get_duckdb_connection(cfg)
        try:
            rows = _fetch_foreign_keys(conn, cfg.database)
            statements = _build_alter_statements(rows)
            logger.info(
                "Found %d foreign_key column-entries → %d ALTER statements",
                len(rows),
                len(statements),
            )

            path = out_dir / "constraints" / f"{cfg.database}.sql"
            body = "-- Foreign key constraints. Apply AFTER data load.\n\n"
            body += "\n\n".join(statements) + ("\n" if statements else "")
            write_text(path, body)
            result.artifacts.append(path)
            logger.info("wrote %s", path.relative_to(out_dir))
        finally:
            conn.close()

    return result


if __name__ == "__main__":
    make_component_cli("constraints", run)()
