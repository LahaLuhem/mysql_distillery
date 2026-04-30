"""Direct pymysql connection factory — for queries DuckDB's mysql_query() can't prepare.

DuckDB's mysql extension issues ``COM_STMT_PREPARE`` on every ``mysql_query()``
call to discover the result's column types. A handful of MySQL statements —
notably ``SHOW CREATE TABLE|VIEW|TRIGGER|PROCEDURE|FUNCTION|EVENT`` — aren't
preparable on some server versions and surface as::

    Failed to fetch return types for query 'SHOW CREATE TABLE ...'

For those, bypass DuckDB and hit MySQL directly via pymysql. ``information_schema``
SELECTs still go through DuckDB — they prepare fine and keep the nicer API.
"""
from __future__ import annotations

import pymysql
from pymysql.connections import Connection

from mysql_distillery.data.models.server_connection_config import ServerConnectionConfig


def get_pymysql_connection(cfg: ServerConnectionConfig) -> Connection:
    """Open a direct pymysql connection to the source database.

    Each worker thread MUST call this to get its own connection — pymysql
    connections are not thread-safe.
    """
    return pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset="utf8mb4",
        use_unicode=True,
        autocommit=True,
    )


def preflight_check(cfg: ServerConnectionConfig) -> None:
    """Verify the source MySQL is reachable and responding. Fails fast on error.

    Runs before parallel workers spin up so an auth/host/db failure surfaces
    as one human-readable ``RuntimeError`` instead of N identical worker errors.
    """
    try:
        conn = get_pymysql_connection(cfg)
    except pymysql.err.OperationalError as exc:
        raise RuntimeError(
            f"Cannot connect to MySQL at {cfg.host}:{cfg.port} "
            f"as user {cfg.user!r}: {exc}"
        ) from exc

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
    finally:
        conn.close()


def show_create(conn: Connection, statement: str, ddl_column: int) -> str:
    """Run a ``SHOW CREATE ...`` and return the DDL at ``ddl_column`` as ``str``.

    MySQL returns the DDL column as bytes in some configurations (e.g. when
    ``character_set_results`` is binary); decode defensively so callers always
    get text.
    """
    with conn.cursor() as cur:
        cur.execute(statement)
        row = cur.fetchone()
    if not row:
        raise RuntimeError(f"{statement!r} returned no rows")
    value = row[ddl_column]
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8")
    return value
