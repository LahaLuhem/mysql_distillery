"""Extract view definitions, one file per view, with DEFINER clauses stripped."""
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


def _list_views(conn, database: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT table_name
        FROM src.information_schema.views
        WHERE table_schema = ?
        ORDER BY table_name
        """,
        [database],
    ).fetchall()
    return [r[0] for r in rows]


def _show_create_view(mysql_conn, database: str, view: str) -> str:
    # DuckDB's ``mysql_query()`` can't prepare ``SHOW CREATE VIEW``; use pymysql.
    # Columns: (View, Create View, character_set_client, collation_connection)
    return show_create(
        mysql_conn,
        f"SHOW CREATE VIEW `{database}`.`{view}`",
        ddl_column=1,
    )


def run(
    cfg: ServerConnectionConfig,
    out_dir: Path,
    *,
    max_workers: int = 1,
) -> ComponentResult:
    logger = setup_logger("views", out_dir)
    views_dir = out_dir / "views"
    views_dir.mkdir(parents=True, exist_ok=True)

    with ComponentResult.timed("views") as result:
        conn = get_duckdb_connection(cfg)
        mysql_conn = get_pymysql_connection(cfg)
        try:
            names = _list_views(conn, cfg.database)
            logger.info("Found %d views", len(names))

            for name in names:
                ddl = strip_definer(_show_create_view(mysql_conn, cfg.database, name))
                path = write_text(views_dir / f"{name}.sql", ddl + ";\n")
                result.artifacts.append(path)
                logger.info("wrote %s", path.relative_to(out_dir))
        finally:
            conn.close()
            mysql_conn.close()

    return result


if __name__ == "__main__":
    make_component_cli("views", run)()
