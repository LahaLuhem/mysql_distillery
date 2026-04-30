"""Extract stored procedures and functions, one file per routine.

Each file is wrapped in DELIMITER $$ ... $$ so it can be sourced directly,
and has DEFINER stripped so it restores on any user.
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


def _list_routines(conn, database: str) -> list[tuple[str, str]]:
    """Return [(routine_name, routine_type), ...]. routine_type is PROCEDURE or FUNCTION."""
    rows = conn.execute(
        """
        SELECT routine_name, routine_type
        FROM src.information_schema.routines
        WHERE routine_schema = ?
        ORDER BY routine_name
        """,
        [database],
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _show_create_routine(mysql_conn, database: str, name: str, routine_type: str) -> str:
    # DuckDB's ``mysql_query()`` can't prepare ``SHOW CREATE PROCEDURE|FUNCTION``;
    # use pymysql directly. SHOW CREATE PROCEDURE/FUNCTION columns:
    #   (Procedure/Function, sql_mode, Create Procedure/Function, character_set_client, ...)
    kw = "PROCEDURE" if routine_type.upper() == "PROCEDURE" else "FUNCTION"
    return show_create(
        mysql_conn,
        f"SHOW CREATE {kw} `{database}`.`{name}`",
        ddl_column=2,
    )


def _wrap_delimiter(ddl: str) -> str:
    return f"DELIMITER $$\n\n{ddl}$$\n\nDELIMITER ;\n"


def run(
    cfg: ServerConnectionConfig,
    out_dir: Path,
    *,
    max_workers: int = 1,
) -> ComponentResult:
    logger = setup_logger("routines", out_dir)
    routines_dir = out_dir / "routines"
    routines_dir.mkdir(parents=True, exist_ok=True)

    with ComponentResult.timed("routines") as result:
        conn = get_duckdb_connection(cfg)
        mysql_conn = get_pymysql_connection(cfg)
        try:
            names = _list_routines(conn, cfg.database)
            logger.info("Found %d routines", len(names))

            for name, rtype in names:
                raw = _show_create_routine(mysql_conn, cfg.database, name, rtype)
                ddl = strip_definer(raw)
                path = write_text(routines_dir / f"{name}.sql", _wrap_delimiter(ddl))
                result.artifacts.append(path)
                logger.info("wrote %s (%s)", path.relative_to(out_dir), rtype)
        finally:
            conn.close()
            mysql_conn.close()

    return result


if __name__ == "__main__":
    make_component_cli("routines", run)()
