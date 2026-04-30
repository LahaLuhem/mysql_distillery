"""Extract per-table CREATE TABLE DDL, with foreign keys stripped.

One file per table → `schema/<table>.sql`. foreign_keys are extracted separately by
`components.constraints` so that data loading can happen in any order.
"""
from __future__ import annotations

from pathlib import Path

from mysql_distillery.data.dtos import ComponentResult
from mysql_distillery.data.models import ServerConnectionConfig
from mysql_distillery.data.utils.cli import make_component_cli
from mysql_distillery.data.utils.ddl import strip_foreign_keys
from mysql_distillery.data.utils.duckdb import get_duckdb_connection
from mysql_distillery.data.utils.files import write_text
from mysql_distillery.data.utils.logging import setup_logger
from mysql_distillery.data.utils.mysql import get_pymysql_connection, show_create


def _list_base_tables(conn, database: str) -> list[str]:
    """Return the base tables (excludes views) in `database`, sorted."""
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


def _show_create_table(mysql_conn, database: str, table: str) -> str:
    """Run `SHOW CREATE TABLE` on the source MySQL via a direct pymysql connection.

    DuckDB's ``mysql_query()`` can't prepare ``SHOW CREATE TABLE`` on some
    server versions ("Failed to fetch return types"), so we bypass it here.
    Row shape: ``(Table, Create Table)``.
    """
    return show_create(
        mysql_conn,
        f"SHOW CREATE TABLE `{database}`.`{table}`",
        ddl_column=1,
    )


def run(
    cfg: ServerConnectionConfig,
    out_dir: Path,
    *,
    max_workers: int = 1,  # unused here — schema is tiny, no need to parallelize
) -> ComponentResult:
    """Extract schema DDL for every base table in cfg.database."""
    logger = setup_logger("schema", out_dir)
    schema_dir = out_dir / "schema"
    schema_dir.mkdir(parents=True, exist_ok=True)

    with ComponentResult.timed("schema") as result:
        conn = get_duckdb_connection(cfg)
        mysql_conn = get_pymysql_connection(cfg)
        try:
            tables = _list_base_tables(conn, cfg.database)
            logger.info("Found %d base tables in %s", len(tables), cfg.database)

            for table in tables:
                raw_ddl = _show_create_table(mysql_conn, cfg.database, table)
                cleaned = strip_foreign_keys(raw_ddl)
                path = write_text(schema_dir / f"{table}.sql", cleaned + "\n")
                result.artifacts.append(path)
                logger.info("wrote %s", path.relative_to(out_dir))
        finally:
            conn.close()
            mysql_conn.close()

    return result


if __name__ == "__main__":
    make_component_cli("schema", run)()
