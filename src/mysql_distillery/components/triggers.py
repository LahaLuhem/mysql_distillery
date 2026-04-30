"""Extract triggers, one file per trigger.

Triggers must be applied AFTER data load, otherwise they'll fire on every
inserted row. Each file is DELIMITER-wrapped and DEFINER-stripped.
"""
from __future__ import annotations

from pathlib import Path

from mysql_distillery.data.dtos import ComponentResult
from mysql_distillery.data.models import ServerConnectionConfig
from mysql_distillery.data.utils.cli import make_component_cli
from mysql_distillery.data.utils.ddl import strip_definer
from mysql_distillery.data.utils.duckdb import get_duckdb_connection
from mysql_distillery.data.utils.files import write_text
from mysql_distillery.data.utils.logging import setup_logger
from mysql_distillery.data.utils.mysql import get_pymysql_connection, show_create


def _list_triggers(conn, database: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT trigger_name
        FROM src.information_schema.triggers
        WHERE trigger_schema = ?
        ORDER BY trigger_name
        """,
        [database],
    ).fetchall()
    return [r[0] for r in rows]


def _show_create_trigger(mysql_conn, database: str, name: str) -> str:
    # DuckDB's ``mysql_query()`` can't prepare ``SHOW CREATE TRIGGER``; use pymysql.
    # Columns: (Trigger, sql_mode, SQL Original Statement, character_set_client, ...)
    return show_create(
        mysql_conn,
        f"SHOW CREATE TRIGGER `{database}`.`{name}`",
        ddl_column=2,
    )


def run(
    cfg: ServerConnectionConfig,
    out_dir: Path,
    *,
    max_workers: int = 1,
) -> ComponentResult:
    logger = setup_logger("triggers", out_dir)
    triggers_dir = out_dir / "triggers"
    triggers_dir.mkdir(parents=True, exist_ok=True)

    with ComponentResult.timed("triggers") as result:
        conn = get_duckdb_connection(cfg)
        mysql_conn = get_pymysql_connection(cfg)
        try:
            names = _list_triggers(conn, cfg.database)
            logger.info("Found %d triggers", len(names))

            for name in names:
                ddl = strip_definer(_show_create_trigger(mysql_conn, cfg.database, name))
                body = f"DELIMITER $$\n\n{ddl}$$\n\nDELIMITER ;\n"
                path = write_text(triggers_dir / f"{name}.sql", body)
                result.artifacts.append(path)
                logger.info("wrote %s", path.relative_to(out_dir))
        finally:
            conn.close()
            mysql_conn.close()

    return result


if __name__ == "__main__":
    make_component_cli("triggers", run)()
