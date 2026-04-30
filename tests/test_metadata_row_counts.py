"""Regression for the DuckDB parameter-bound ``read_parquet(?)`` pattern.

A prior f-string version of this SELECT tripped the SAST scanner's B608
check. The replacement relies on DuckDB honoring ``?`` binding inside
``read_parquet(?)`` — same call shape is also used in
``components/data.py::_export_one_table``. If DuckDB ever drops that,
both sites break silently, so we pin the contract here.
"""
from __future__ import annotations

import duckdb

from mysql_distillery.components.metadata import _collect_row_counts


def test_collect_row_counts_reads_every_parquet_file(tmp_path):
    conn = duckdb.connect(":memory:")
    # Build two parquet files with distinct row counts via DuckDB's Python
    # ``write_parquet`` API (no SQL literal — keeps the test itself clear
    # of any B608-style string-SQL pattern).
    conn.sql("SELECT range AS id FROM range(5)").write_parquet(
        (tmp_path / "tbl_one.parquet").as_posix(),
    )
    conn.sql("SELECT range AS id FROM range(42)").write_parquet(
        (tmp_path / "tbl_two.parquet").as_posix(),
    )

    counts = _collect_row_counts(tmp_path, conn)

    assert counts == {"tbl_one": 5, "tbl_two": 42}


def test_collect_row_counts_empty_dir_yields_empty_dict(tmp_path):
    conn = duckdb.connect(":memory:")
    assert _collect_row_counts(tmp_path, conn) == {}


def test_collect_row_counts_missing_dir_yields_empty_dict(tmp_path):
    conn = duckdb.connect(":memory:")
    missing = tmp_path / "does-not-exist"
    assert _collect_row_counts(missing, conn) == {}
