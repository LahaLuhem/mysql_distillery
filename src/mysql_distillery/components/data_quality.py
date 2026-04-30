"""Data-quality scan — 8 read-only integrity checks on a source DB.

Default off; toggled by the orchestrator's ``--data-quality-report`` /
``--data-quality-only`` flags. Emits ``<out>/<db>/logs/data_quality.yaml``
plus per-category rich tables to stderr (empty categories suppressed).

See ``APPENDIX.md#data-quality-scan`` for the full rationale: why each
class of check exists, the exclusion reasoning on NULL-check date cols,
composite-foreign_key handling, outlier thresholds, and cost expectations.
"""
from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

import yaml
from pymysql.connections import Connection
from rich.console import Console
from rich.table import Table

from mysql_distillery.data.dtos import ComponentResult
from mysql_distillery.data.models import ServerConnectionConfig
from mysql_distillery.data.utils.cli import make_component_cli
from mysql_distillery.data.utils.files import write_text
from mysql_distillery.data.utils.logging import setup_logger
from mysql_distillery.data.utils.mysql import get_pymysql_connection

_VARLEN_TYPES: Final[frozenset[str]] = frozenset({
    "text", "tinytext", "mediumtext", "longtext",
    "blob", "tinyblob", "mediumblob", "longblob",
})
# Both thresholds must trip — see ``APPENDIX.md#data-quality-scan``.
_ROW_LEN_ABS_THRESHOLD_BYTES: Final[int] = 1 * 1024 * 1024
_ROW_LEN_RATIO_THRESHOLD: Final[float] = 10.0

# Dedicated stderr console so the report stays separable from component
# stdout that other callers might consume.
_STDERR: Final[Console] = Console(stderr=True)


# ── Result carriers (keep lightweight — these serialise straight to YAML). ──
@dataclass
class _MissingPrimKey:
    table: str


@dataclass
class _ZeroDate:
    table: str
    column: str
    data_type: str
    row_count: int


@dataclass
class _NullInNotNull:
    table: str
    column: str
    row_count: int


@dataclass
class _OrphanForeignKey:
    constraint: str
    table: str
    column: str
    parent_table: str
    parent_column: str
    orphan_count: int


@dataclass
class _RowLengthOutlier:
    table: str
    column: str
    data_type: str
    max_bytes: int
    avg_bytes: int
    ratio: float


@dataclass
class _CharsetMismatch:
    scope: str                # "table" | "column"
    table: str
    column: str | None        # None for scope=table
    actual: str
    expected: str


@dataclass
class _Report:
    database: str
    scanned_at: str
    tables_without_prim_key: list[_MissingPrimKey] = field(default_factory=list)
    zero_dates_prim_key_not_null: list[_ZeroDate] = field(default_factory=list)
    zero_dates_non_prim_key_not_null: list[_ZeroDate] = field(default_factory=list)
    zero_dates_nullable: list[_ZeroDate] = field(default_factory=list)
    nulls_in_not_null: list[_NullInNotNull] = field(default_factory=list)
    orphaned_foreign_keys: list[_OrphanForeignKey] = field(default_factory=list)
    row_length_outliers: list[_RowLengthOutlier] = field(default_factory=list)
    charset_mismatches: list[_CharsetMismatch] = field(default_factory=list)

    def total_issues(self) -> int:
        return (
            len(self.tables_without_prim_key)
            + len(self.zero_dates_prim_key_not_null)
            + len(self.zero_dates_non_prim_key_not_null)
            + len(self.zero_dates_nullable)
            + len(self.nulls_in_not_null)
            + len(self.orphaned_foreign_keys)
            + len(self.row_length_outliers)
            + len(self.charset_mismatches)
        )


# ── Small fetch helpers — keep call sites terse. ──
def _fetchall(conn: Connection, sql: str, params: tuple = ()) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def _fetchone(conn: Connection, sql: str, params: tuple = ()) -> tuple | None:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def _parallel_scan(
    items: list,
    worker,
    max_workers: int,
):
    """Thread-pool helper: applies ``worker`` to each item, yields non-None
    results. Each worker is expected to open its own pymysql connection
    (``pymysql`` is not thread-safe)."""
    if not items:
        return []
    out: list = []
    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as pool:
        for fut in as_completed([pool.submit(worker, it) for it in items]):
            res = fut.result()
            if res is not None:
                out.append(res)
    return out


# ── Check 1: Tables without PRIMARY KEY. ──
def _scan_missing_prim_keys(conn: Connection, database: str) -> list[_MissingPrimKey]:
    rows = _fetchall(
        conn,
        """
        SELECT t.TABLE_NAME
        FROM information_schema.TABLES t
        WHERE t.TABLE_SCHEMA = %s
          AND t.TABLE_TYPE = 'BASE TABLE'
          AND NOT EXISTS (
              SELECT 1 FROM information_schema.KEY_COLUMN_USAGE k
              WHERE k.TABLE_SCHEMA = t.TABLE_SCHEMA
                AND k.TABLE_NAME   = t.TABLE_NAME
                AND k.CONSTRAINT_NAME = 'PRIMARY'
          )
        ORDER BY t.TABLE_NAME
        """,
        (database,),
    )
    return [_MissingPrimKey(table=r[0]) for r in rows]


# ── Checks 2-4: Zero-dates, partitioned by (is_prim_key, is_nullable). ──
def _date_cols_by_class(
    conn: Connection, database: str,
) -> tuple[
    list[tuple[str, str, str]],   # (table, col, dtype) — prim_key, NOT NULL
    list[tuple[str, str, str]],   # non-prim_key, NOT NULL
    list[tuple[str, str, str]],   # NULLABLE (regardless of prim_key — but prim_key is NOT NULL by def)
]:
    rows = _fetchall(
        conn,
        """
        SELECT c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE,
               (SELECT 1 FROM information_schema.KEY_COLUMN_USAGE k
                WHERE k.TABLE_SCHEMA  = c.TABLE_SCHEMA
                  AND k.TABLE_NAME    = c.TABLE_NAME
                  AND k.COLUMN_NAME   = c.COLUMN_NAME
                  AND k.CONSTRAINT_NAME = 'PRIMARY') AS is_prim_key
        FROM information_schema.COLUMNS c
        WHERE c.TABLE_SCHEMA = %s
          AND c.DATA_TYPE IN ('date','datetime','timestamp')
        ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION
        """,
        (database,),
    )
    prim_key_nn: list[tuple[str, str, str]] = []
    non_prim_key_nn: list[tuple[str, str, str]] = []
    nullable: list[tuple[str, str, str]] = []
    for tbl, col, dtype, is_nullable, is_prim_key in rows:
        entry = (tbl, col, dtype)
        if is_nullable == "YES":
            nullable.append(entry)
        elif is_prim_key == 1:
            prim_key_nn.append(entry)
        else:
            non_prim_key_nn.append(entry)
    return prim_key_nn, non_prim_key_nn, nullable


def _scan_zero_dates(
    cfg: ServerConnectionConfig,
    cols: list[tuple[str, str, str]],
    max_workers: int,
) -> list[_ZeroDate]:
    # ``CAST(col AS CHAR) LIKE '0000-00-00%'`` — same rationale as
    # nullable_zerodates.py: sidesteps NO_ZERO_DATE rejection a naive
    # ``WHERE col='0000-00-00 00:00:00'`` would trip, and the LIKE handles
    # DATETIME(n) fractional precision too.
    def worker(entry: tuple[str, str, str]) -> _ZeroDate | None:
        tbl, col, dtype = entry
        conn = get_pymysql_connection(cfg)
        try:
            row = _fetchone(
                conn,
                f"SELECT COUNT(*) FROM `{cfg.database}`.`{tbl}` "
                f"WHERE CAST(`{col}` AS CHAR) LIKE '0000-00-00%%'",
            )
        finally:
            conn.close()
        n = int(row[0]) if row else 0
        return None if n == 0 else _ZeroDate(tbl, col, dtype, n)

    out: list[_ZeroDate] = _parallel_scan(cols, worker, max_workers)
    out.sort(key=lambda z: (z.table, z.column))
    return out


# ── Check 5: NULLs in NOT NULL cols (non-date types only). ──
# Date types are excluded to avoid duplicating zero-date hits from
# checks 2/3/4 — see ``APPENDIX.md#data-quality-scan``.
def _not_null_cols(conn: Connection, database: str) -> list[tuple[str, str]]:
    rows = _fetchall(
        conn,
        """
        SELECT TABLE_NAME, COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND IS_NULLABLE = 'NO'
          AND DATA_TYPE NOT IN ('date','datetime','timestamp')
          -- Generated / virtual cols derive from others; skip to save
          -- queries. Their NULL-ness is an expression artefact, not
          -- source-data corruption.
          AND (EXTRA IS NULL OR EXTRA NOT LIKE '%%GENERATED%%')
        ORDER BY TABLE_NAME, ORDINAL_POSITION
        """,
        (database,),
    )
    return [(r[0], r[1]) for r in rows]


def _scan_nulls_in_not_null(
    cfg: ServerConnectionConfig,
    cols: list[tuple[str, str]],
    max_workers: int,
) -> list[_NullInNotNull]:
    def worker(entry: tuple[str, str]) -> _NullInNotNull | None:
        tbl, col = entry
        conn = get_pymysql_connection(cfg)
        try:
            row = _fetchone(
                conn,
                f"SELECT COUNT(*) FROM `{cfg.database}`.`{tbl}` "
                f"WHERE `{col}` IS NULL",
            )
        finally:
            conn.close()
        n = int(row[0]) if row else 0
        return None if n == 0 else _NullInNotNull(tbl, col, n)

    out: list[_NullInNotNull] = _parallel_scan(cols, worker, max_workers)
    out.sort(key=lambda n: (n.table, n.column))
    return out


# ── Check 6: Orphaned foreign_key references (single-column foreign_keys). ──
def _foreign_key_defs(
    conn: Connection, database: str, logger,
) -> list[tuple[str, str, str, str, str]]:
    """Single-column foreign_keys only; composite foreign_keys are logged and skipped.

    See ``APPENDIX.md#data-quality-scan`` for why composites are out of scope.
    """
    rows = _fetchall(
        conn,
        """
        SELECT rc.CONSTRAINT_NAME, kcu.TABLE_NAME, kcu.COLUMN_NAME,
               kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME,
               kcu.ORDINAL_POSITION
        FROM information_schema.REFERENTIAL_CONSTRAINTS rc
        JOIN information_schema.KEY_COLUMN_USAGE kcu
          ON kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
         AND kcu.CONSTRAINT_NAME   = rc.CONSTRAINT_NAME
        WHERE rc.CONSTRAINT_SCHEMA = %s
        ORDER BY kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
        """,
        (database,),
    )
    by_constraint: dict[
        tuple[str, str], list[tuple[str, str, str]]
    ] = defaultdict(list)
    for cname, tbl, col, ref_tbl, ref_col, _ord in rows:
        by_constraint[(cname, tbl)].append((col, ref_tbl, ref_col))

    single: list[tuple[str, str, str, str, str]] = []
    for (cname, tbl), parts in by_constraint.items():
        if len(parts) == 1:
            col, ref_tbl, ref_col = parts[0]
            single.append((cname, tbl, col, ref_tbl, ref_col))
        else:
            cols = ", ".join(p[0] for p in parts)
            logger.info(
                "foreign_key %s on %s(%s) is composite — skipping orphan check "
                "(composite foreign_key orphan semantics need a multi-column key)",
                cname, tbl, cols,
            )
    return single


def _scan_orphan_foreign_keys(
    cfg: ServerConnectionConfig,
    foreign_keys: list[tuple[str, str, str, str, str]],
    max_workers: int,
) -> list[_OrphanForeignKey]:
    def worker(foreign_key: tuple[str, str, str, str, str]) -> _OrphanForeignKey | None:
        cname, tbl, col, ref_tbl, ref_col = foreign_key
        conn = get_pymysql_connection(cfg)
        try:
            row = _fetchone(
                conn,
                f"""
                SELECT COUNT(*) FROM `{cfg.database}`.`{tbl}` c
                LEFT JOIN `{cfg.database}`.`{ref_tbl}` p
                  ON c.`{col}` = p.`{ref_col}`
                WHERE c.`{col}` IS NOT NULL AND p.`{ref_col}` IS NULL
                """,
            )
        finally:
            conn.close()
        n = int(row[0]) if row else 0
        return None if n == 0 else _OrphanForeignKey(
            cname, tbl, col, ref_tbl, ref_col, n,
        )

    out: list[_OrphanForeignKey] = _parallel_scan(foreign_keys, worker, max_workers)
    out.sort(key=lambda o: (o.table, o.column))
    return out


# ── Check 7: Row-length outliers on TEXT/BLOB cols. ──
def _varlen_cols(conn: Connection, database: str) -> list[tuple[str, str, str]]:
    # VARCHAR is intentionally NOT in scope — its upper bound is
    # schema-declared, so outliers there are rarely interesting and the
    # scan adds noise for hundreds of cols with tiny contents.
    rows = _fetchall(
        conn,
        """
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND DATA_TYPE IN ('text','tinytext','mediumtext','longtext',
                            'blob','tinyblob','mediumblob','longblob')
        ORDER BY TABLE_NAME, ORDINAL_POSITION
        """,
        (database,),
    )
    return [(r[0], r[1], r[2]) for r in rows]


def _scan_row_length_outliers(
    cfg: ServerConnectionConfig,
    cols: list[tuple[str, str, str]],
    max_workers: int,
) -> list[_RowLengthOutlier]:
    def worker(entry: tuple[str, str, str]) -> _RowLengthOutlier | None:
        tbl, col, dtype = entry
        conn = get_pymysql_connection(cfg)
        try:
            row = _fetchone(
                conn,
                f"SELECT MAX(OCTET_LENGTH(`{col}`)), "
                f"       AVG(OCTET_LENGTH(`{col}`)) "
                f"FROM `{cfg.database}`.`{tbl}`",
            )
        finally:
            conn.close()
        if not row or row[0] is None:
            return None
        max_b = int(row[0])
        if max_b < _ROW_LEN_ABS_THRESHOLD_BYTES:
            return None
        avg_b = float(row[1] or 0.0)
        if avg_b <= 0:
            ratio = float("inf")  # single-row table: max trivially dwarfs avg=0
        else:
            ratio = max_b / avg_b
            if ratio < _ROW_LEN_RATIO_THRESHOLD:
                return None
        return _RowLengthOutlier(tbl, col, dtype, max_b, int(avg_b), ratio)

    out: list[_RowLengthOutlier] = _parallel_scan(cols, worker, max_workers)
    out.sort(key=lambda o: -o.max_bytes)
    return out


# ── Check 8: Charset mismatches. ──
def _scan_charset_mismatches(
    conn: Connection, database: str,
) -> list[_CharsetMismatch]:
    row = _fetchone(
        conn,
        "SELECT DEFAULT_CHARACTER_SET_NAME "
        "FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = %s",
        (database,),
    )
    db_default = row[0] if row else None

    out: list[_CharsetMismatch] = []

    # Table-level: base-table charset (derived from TABLE_COLLATION) that
    # differs from the DB default. If the DB itself has no default we
    # skip the comparison — nothing to be "wrong" against.
    if db_default:
        for tbl, cs in _fetchall(
            conn,
            """
            SELECT t.TABLE_NAME, ccsa.CHARACTER_SET_NAME
            FROM information_schema.TABLES t
            JOIN information_schema.COLLATION_CHARACTER_SET_APPLICABILITY ccsa
              ON ccsa.COLLATION_NAME = t.TABLE_COLLATION
            WHERE t.TABLE_SCHEMA = %s
              AND t.TABLE_TYPE = 'BASE TABLE'
              AND ccsa.CHARACTER_SET_NAME IS NOT NULL
              AND ccsa.CHARACTER_SET_NAME <> %s
            ORDER BY t.TABLE_NAME
            """,
            (database, db_default),
        ):
            out.append(_CharsetMismatch(
                scope="table", table=tbl, column=None,
                actual=cs, expected=db_default,
            ))

    # Column-level: character columns whose CHARACTER_SET_NAME differs
    # from the parent table's effective charset (derived from the table's
    # collation, resolved via COLLATION_CHARACTER_SET_APPLICABILITY).
    for tbl, col, col_cs, tbl_cs in _fetchall(
        conn,
        """
        SELECT c.TABLE_NAME, c.COLUMN_NAME, c.CHARACTER_SET_NAME,
               ccsa.CHARACTER_SET_NAME
        FROM information_schema.COLUMNS c
        JOIN information_schema.TABLES t
          ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
         AND t.TABLE_NAME   = c.TABLE_NAME
        JOIN information_schema.COLLATION_CHARACTER_SET_APPLICABILITY ccsa
          ON ccsa.COLLATION_NAME = t.TABLE_COLLATION
        WHERE c.TABLE_SCHEMA = %s
          AND c.CHARACTER_SET_NAME IS NOT NULL
          AND ccsa.CHARACTER_SET_NAME IS NOT NULL
          AND c.CHARACTER_SET_NAME <> ccsa.CHARACTER_SET_NAME
        ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION
        """,
        (database,),
    ):
        out.append(_CharsetMismatch(
            scope="column", table=tbl, column=col,
            actual=col_cs, expected=tbl_cs,
        ))
    return out


# ── Rendering. ──
def _human_bytes(n: int | float) -> str:
    x = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if x < 1024.0:
            return f"{int(x)} {unit}" if unit == "B" else f"{x:.1f} {unit}"
        x /= 1024.0
    return f"{x:.1f} PiB"


def _render_stderr(report: _Report) -> None:
    """Stderr scream: one rich table per non-empty category. Empty cats
    are suppressed to keep the output scannable."""
    if report.total_issues() == 0:
        _STDERR.print(
            f"\n[green]✓ data-quality scan[/green] on "
            f"[cyan]{report.database}[/cyan]: no issues found across 8 checks."
        )
        return

    _STDERR.print(
        f"\n[bold red]══ data-quality report: {report.database} "
        f"({report.total_issues()} issue(s)) ══[/bold red]"
    )

    if report.tables_without_prim_key:
        t = Table(
            title=f"Missing PRIMARY KEY ({len(report.tables_without_prim_key)} table(s))",
        )
        t.add_column("table")
        for e in report.tables_without_prim_key:
            t.add_row(e.table)
        _STDERR.print(t)

    for label, items, severity in (
        ("Zero-dates in NOT NULL prim_key cols (unrestorable)",
            report.zero_dates_prim_key_not_null, "red"),
        ("Zero-dates in NOT NULL non-prim_key cols (relax/fill/restore)",
            report.zero_dates_non_prim_key_not_null, "yellow"),
        ("Zero-dates in NULLABLE cols (sidecar)",
            report.zero_dates_nullable, "yellow"),
    ):
        if not items:
            continue
        t = Table(
            title=f"[{severity}]{label}[/{severity}] "
                  f"({len(items)} col(s))",
        )
        t.add_column("table")
        t.add_column("column")
        t.add_column("type")
        t.add_column("rows", justify="right")
        for e in items:
            t.add_row(e.table, e.column, e.data_type, f"{e.row_count:,}")
        _STDERR.print(t)

    if report.nulls_in_not_null:
        t = Table(
            title=f"[red]NULLs in NOT NULL cols "
                  f"({len(report.nulls_in_not_null)} col(s))[/red]",
        )
        t.add_column("table")
        t.add_column("column")
        t.add_column("rows", justify="right")
        for e in report.nulls_in_not_null:
            t.add_row(e.table, e.column, f"{e.row_count:,}")
        _STDERR.print(t)

    if report.orphaned_foreign_keys:
        t = Table(
            title=f"[red]Orphaned foreign_key refs "
                  f"({len(report.orphaned_foreign_keys)})[/red]",
        )
        t.add_column("constraint")
        t.add_column("child")
        t.add_column("→ parent")
        t.add_column("orphans", justify="right")
        for e in report.orphaned_foreign_keys:
            t.add_row(
                e.constraint,
                f"{e.table}.{e.column}",
                f"{e.parent_table}.{e.parent_column}",
                f"{e.orphan_count:,}",
            )
        _STDERR.print(t)

    if report.row_length_outliers:
        t = Table(
            title=f"Row-length outliers ({len(report.row_length_outliers)})",
        )
        t.add_column("table")
        t.add_column("column")
        t.add_column("type")
        t.add_column("max", justify="right")
        t.add_column("avg", justify="right")
        t.add_column("max/avg", justify="right")
        for e in report.row_length_outliers:
            ratio_txt = "∞" if e.ratio == float("inf") else f"{e.ratio:.1f}×"
            t.add_row(
                e.table, e.column, e.data_type,
                _human_bytes(e.max_bytes),
                _human_bytes(e.avg_bytes),
                ratio_txt,
            )
        _STDERR.print(t)

    if report.charset_mismatches:
        t = Table(
            title=f"Charset mismatches ({len(report.charset_mismatches)})",
        )
        t.add_column("scope")
        t.add_column("table")
        t.add_column("column")
        t.add_column("actual")
        t.add_column("expected")
        for e in report.charset_mismatches:
            t.add_row(
                e.scope, e.table, e.column or "—", e.actual, e.expected,
            )
        _STDERR.print(t)


# ── YAML serialisation. ──
def _report_to_yaml(report: _Report) -> str:
    # ``float('inf')`` isn't safe for ``yaml.safe_dump``. Coerce into the
    # YAML ``.inf`` literal (as a string — safe_dump will quote it, but
    # the downstream CI consumer can still detect and handle it).
    def _clean(obj):
        if isinstance(obj, dict):
            return {k: _clean(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_clean(v) for v in obj]
        if isinstance(obj, float) and obj == float("inf"):
            return ".inf"
        return obj

    data = _clean(asdict(report))
    return yaml.safe_dump(
        data, default_flow_style=False, sort_keys=False, width=120,
    )


# ── Entry point. ──
def run(
    cfg: ServerConnectionConfig,
    out_dir: Path,
    *,
    max_workers: int = 1,
) -> ComponentResult:
    logger = setup_logger("data_quality", out_dir)
    logs_dir = out_dir / "logs"

    with ComponentResult.timed("data_quality") as result:
        conn = get_pymysql_connection(cfg)
        try:
            report = _Report(
                database=cfg.database,
                scanned_at=datetime.now(timezone.utc).isoformat(),
            )

            logger.info("1/8 tables without PRIMARY KEY")
            report.tables_without_prim_key = _scan_missing_prim_keys(conn, cfg.database)

            logger.info("enumerating date cols")
            prim_key_nn, non_prim_key_nn, nullable_dates = _date_cols_by_class(
                conn, cfg.database,
            )
            logger.info(
                "2-4/8 zero-dates (%d prim_key-NN cols, %d non-prim_key-NN cols, "
                "%d nullable cols, max_workers=%d)",
                len(prim_key_nn), len(non_prim_key_nn), len(nullable_dates), max_workers,
            )
            report.zero_dates_prim_key_not_null = _scan_zero_dates(
                cfg, prim_key_nn, max_workers,
            )
            report.zero_dates_non_prim_key_not_null = _scan_zero_dates(
                cfg, non_prim_key_nn, max_workers,
            )
            report.zero_dates_nullable = _scan_zero_dates(
                cfg, nullable_dates, max_workers,
            )

            nn_cols = _not_null_cols(conn, cfg.database)
            logger.info("5/8 NULLs in NOT NULL cols (%d cols)", len(nn_cols))
            report.nulls_in_not_null = _scan_nulls_in_not_null(
                cfg, nn_cols, max_workers,
            )

            foreign_keys = _foreign_key_defs(conn, cfg.database, logger)
            logger.info(
                "6/8 orphaned foreign_key refs (%d single-col foreign_keys)", len(foreign_keys),
            )
            report.orphaned_foreign_keys = _scan_orphan_foreign_keys(
                cfg, foreign_keys, max_workers,
            )

            varlen = _varlen_cols(conn, cfg.database)
            logger.info(
                "7/8 row-length outliers (%d TEXT/BLOB cols)", len(varlen),
            )
            report.row_length_outliers = _scan_row_length_outliers(
                cfg, varlen, max_workers,
            )

            logger.info("8/8 charset mismatches")
            report.charset_mismatches = _scan_charset_mismatches(
                conn, cfg.database,
            )

            total = report.total_issues()
            logger.info(
                "scan complete: %d issue(s) across 8 categories", total,
            )

            artifact = write_text(
                logs_dir / "data_quality.yaml",
                _report_to_yaml(report),
            )
            result.artifacts.append(artifact)
            result.notes.append(
                f"{total} issue(s) across 8 checks" if total
                else "0 issues across 8 checks",
            )

            _render_stderr(report)
        finally:
            conn.close()

    return result


if __name__ == "__main__":
    make_component_cli("data_quality", run)()
