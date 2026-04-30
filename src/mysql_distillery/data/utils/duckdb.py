"""DuckDB connection factory — installs the MySQL extension and attaches the source DB."""
from __future__ import annotations

import duckdb

from mysql_distillery.data.models.server_connection_config import ServerConnectionConfig


def get_duckdb_connection(cfg: ServerConnectionConfig) -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB, install+load the MySQL extension, attach the DB.

    Each worker thread MUST call this to get its own connection — DuckDB
    connections are not thread-safe.
    """
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL mysql;")
    conn.execute("LOAD mysql;")
    # Disable the TINYINT(1) → BOOLEAN coercion (JDBC-era convention the
    # extension defaults to). Source TINYINT(1) columns may legitimately
    # hold values outside {0, 1} — e.g. -1 as a tri-state flag, or small
    # enums stored as signed bytes. Leaving the default ON collapses all
    # non-zero values to TRUE and loses the original integer, which then
    # round-trips through Parquet as BOOLEAN and corrupts the restore.
    # Must be SET before ATTACH so the catalog describes columns as
    # TINYINT instead of BOOLEAN.
    conn.execute("SET mysql_tinyint1_as_boolean = false;")
    conn.execute(cfg.duckdb_attach_sql("src"))
    return conn
