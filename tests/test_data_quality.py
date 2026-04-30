"""Unit tests for data_quality helpers.

The scan functions themselves require a live MySQL and live schema, so
they live under integration-style coverage (see `tests/drift_detector.py`
for the end-to-end harness). These tests cover the pure helpers — the
ones a regression would hit first on any refactor.
"""
from __future__ import annotations

import yaml

from mysql_distillery.components.data_quality import (
    _CharsetMismatch,
    _MissingPrimKey,
    _NullInNotNull,
    _OrphanForeignKey,
    _Report,
    _RowLengthOutlier,
    _ZeroDate,
    _human_bytes,
    _report_to_yaml,
)


def test_human_bytes_boundaries():
    assert _human_bytes(0) == "0 B"
    assert _human_bytes(1023) == "1023 B"
    assert _human_bytes(1024) == "1.0 KiB"
    assert _human_bytes(1024 * 1024) == "1.0 MiB"
    assert _human_bytes(1024 * 1024 * 1024) == "1.0 GiB"
    # Non-round numbers format to one decimal.
    assert _human_bytes(1536) == "1.5 KiB"


def test_total_issues_sums_every_category():
    r = _Report(database="x", scanned_at="t")
    assert r.total_issues() == 0

    r.tables_without_prim_key.append(_MissingPrimKey("a"))
    r.zero_dates_prim_key_not_null.append(_ZeroDate("a", "b", "date", 1))
    r.zero_dates_non_prim_key_not_null.append(_ZeroDate("a", "c", "date", 1))
    r.zero_dates_nullable.append(_ZeroDate("a", "d", "date", 1))
    r.nulls_in_not_null.append(_NullInNotNull("a", "e", 1))
    r.orphaned_foreign_keys.append(_OrphanForeignKey("fk", "a", "b", "p", "q", 1))
    r.row_length_outliers.append(
        _RowLengthOutlier("a", "b", "blob", 100, 10, 10.0),
    )
    r.charset_mismatches.append(_CharsetMismatch("table", "a", None, "x", "y"))

    # One per category — sanity on the sum logic.
    assert r.total_issues() == 8


def test_report_to_yaml_handles_inf_ratio():
    # Single-row tables produce ratio=inf (avg=0 guard in the scanner).
    # ``yaml.safe_dump`` refuses float('inf'); our serialiser coerces.
    r = _Report(database="x", scanned_at="t")
    r.row_length_outliers.append(
        _RowLengthOutlier(
            table="t", column="c", data_type="longblob",
            max_bytes=1_000_000, avg_bytes=0, ratio=float("inf"),
        ),
    )
    text = _report_to_yaml(r)
    # safe_load the output; if our inf coercion broke, it'd throw.
    parsed = yaml.safe_load(text)
    assert parsed["row_length_outliers"][0]["ratio"] == ".inf"


def test_report_to_yaml_round_trips_ordinary_fields():
    r = _Report(database="db1", scanned_at="2026-04-21T00:00:00Z")
    r.tables_without_prim_key.append(_MissingPrimKey("v_serie"))
    r.zero_dates_prim_key_not_null.append(
        _ZeroDate(table="srkrt", column="MUTADT", data_type="datetime", row_count=152),
    )
    parsed = yaml.safe_load(_report_to_yaml(r))
    assert parsed["database"] == "db1"
    assert parsed["tables_without_prim_key"] == [{"table": "v_serie"}]
    assert parsed["zero_dates_prim_key_not_null"][0]["row_count"] == 152
