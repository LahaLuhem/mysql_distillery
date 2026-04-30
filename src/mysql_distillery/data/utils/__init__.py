"""Stateless helpers shared across components.

Grouped by concern — each module is independently importable:
    - logging : per-component file + stdout logger
    - duckdb  : connection factory with the MySQL extension attached
    - mysql   : direct pymysql connection for queries DuckDB can't prepare
    - safety  : --prod guardrail
    - ddl     : regex-based DDL cleanup (foreign_keys, DEFINER)
    - files   : checksums, text writes, empty-dir checks
    - cli     : shared click scaffolding for standalone component CLIs
"""
