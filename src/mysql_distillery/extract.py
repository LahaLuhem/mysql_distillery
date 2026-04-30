"""Orchestrator: for each requested database, run each component in parallel,
then metadata last. Output is written to ``<out_dir>/<database>/…``.

Usage:
    python src/main.py \\
        --host localhost --port 3306 --user root --password rootpass \\
        --db mydb --db mydb_audit \\
        --out ./snapshots

A single ``--db`` still works (backwards-compat). When no ``--db`` is passed,
the orchestrator falls back to ``$MYSQL_DATABASES``, which may be a single name
or a comma-separated list (e.g. ``MYSQL_DATABASES=foo,bar``).

Or (once installed) via the ``mysql_distillery`` console script.
See ``--help`` for full flags.
"""
from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable

import click
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

from mysql_distillery.data.dtos import ComponentResult
from mysql_distillery.data.models import ServerConnectionConfig
from mysql_distillery.data.utils.files import ensure_empty_dir
from mysql_distillery.data.utils.safety import warn_if_prod
from mysql_distillery.components import (
    constraints,
    data,
    data_quality,
    events,
    metadata,
    nullable_zerodates,
    routines,
    schema,
    triggers,
    views,
)

_console = Console()

# Order here is the order shown in the summary table; execution order is
# determined by the parallel pool plus "metadata always last".
#
# ``data_quality`` is registered here so ``--only`` / ``--skip`` accept it,
# but it's NOT included in the default selection — it's a diagnostic
# component gated on ``--data-quality-report`` / ``--data-quality-only``
# (see cli() below). Its scans are expensive (table-scan per date col,
# per NOT-NULL col, per TEXT/BLOB col, plus foreign_key-orphan LEFT JOINs) and
# would bloat every routine extract run.
_PARALLEL_COMPONENTS: dict[str, Callable] = {
    "schema": schema.run,
    "constraints": constraints.run,
    "data": data.run,
    "nullable_zerodates": nullable_zerodates.run,
    "data_quality": data_quality.run,
    "views": views.run,
    "routines": routines.run,
    "triggers": triggers.run,
    "events": events.run,
}
_ALWAYS_LAST = ("metadata", metadata.run)
_DEFAULT_OFF = frozenset({"data_quality"})

_ALL_NAMES = list(_PARALLEL_COMPONENTS) + [_ALWAYS_LAST[0]]


def _select_components(only: str | None, skip: str | None) -> list[str]:
    """Resolve --only / --skip into a concrete component list.

    `metadata` is always included (and always last), unless explicitly excluded
    via --skip metadata — in which case you're on your own without a manifest.

    Components in ``_DEFAULT_OFF`` (currently ``data_quality``) are excluded
    from the default list but MAY be named explicitly in ``--only``. The
    caller (cli) separately honours ``--data-quality-report`` to opt them
    back in for a default-selection run.
    """
    if only:
        requested = [c.strip() for c in only.split(",") if c.strip()]
        unknown = set(requested) - set(_ALL_NAMES)
        if unknown:
            raise SystemExit(f"Unknown components in --only: {sorted(unknown)}")
        return requested

    selected = [c for c in _ALL_NAMES if c not in _DEFAULT_OFF]
    if skip:
        excluded = {c.strip() for c in skip.split(",") if c.strip()}
        unknown = excluded - set(_ALL_NAMES)
        if unknown:
            raise SystemExit(f"Unknown components in --skip: {sorted(unknown)}")
        selected = [c for c in selected if c not in excluded]
    return selected


def _resolve_databases(cli_dbs: tuple[str, ...]) -> list[str]:
    """CLI --db wins; otherwise fall back to $MYSQL_DATABASES (comma-split)."""
    if cli_dbs:
        # A single `--db "foo,bar"` is still a valid way to pass two DBs.
        resolved: list[str] = []
        for chunk in cli_dbs:
            resolved.extend(d.strip() for d in chunk.split(",") if d.strip())
        return resolved
    return ServerConnectionConfig.databases_from_env()


def _render_summary(rows: list[tuple[str, ComponentResult]]) -> None:
    """Summary table grouped by (database, component)."""
    table = Table(title="Extraction summary")
    table.add_column("Database")
    table.add_column("Component")
    table.add_column("Status")
    table.add_column("Duration (s)", justify="right")
    table.add_column("Artifacts", justify="right")
    table.add_column("Notes / error")
    for db, r in rows:
        status_style = {"ok": "green", "error": "red", "skipped": "yellow"}[r.status]
        note = r.error or ("; ".join(r.notes[:2]) + (" …" if len(r.notes) > 2 else ""))
        table.add_row(
            db,
            r.component,
            f"[{status_style}]{r.status}[/{status_style}]",
            f"{r.duration_s:.2f}",
            str(len(r.artifacts)),
            note,
        )
    _console.print(table)


def _run_one_database(
    cfg: ServerConnectionConfig,
    db_out_dir: Path,
    selected: list[str],
    workers: int,
    data_workers: int,
) -> list[ComponentResult]:
    """Run the selected components for a single DB, write into db_out_dir."""
    parallel = [c for c in selected if c in _PARALLEL_COMPONENTS]
    run_metadata = _ALWAYS_LAST[0] in selected

    results: list[ComponentResult] = []

    # 1. Run the parallelizable components concurrently. ``data`` and
    #    ``data_quality`` are both per-table-scan-heavy — reuse the
    #    ``--data-workers`` knob for both rather than proliferating flags.
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _PARALLEL_COMPONENTS[name],
                cfg,
                db_out_dir,
                max_workers=(
                    data_workers if name in ("data", "data_quality") else 1
                ),
            ): name
            for name in parallel
        }
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                results.append(fut.result())
            except Exception as exc:
                results.append(
                    ComponentResult(
                        component=name,
                        status="error",
                        duration_s=0.0,
                        error=f"{type(exc).__name__}: {exc}",
                    )
                )

    # 2. Metadata runs last — needs other components' artifacts for checksums.
    if run_metadata:
        try:
            results.append(_ALWAYS_LAST[1](cfg, db_out_dir))
        except Exception as exc:
            results.append(
                ComponentResult(
                    component="metadata",
                    status="error",
                    duration_s=0.0,
                    error=f"{type(exc).__name__}: {exc}",
                )
            )

    results.sort(key=lambda r: _ALL_NAMES.index(r.component))
    return results


@click.command()
@click.option("--host", default=None, help="MySQL host (else $MYSQL_HOST).")
@click.option("--port", type=int, default=None, help="MySQL port (else $MYSQL_PORT).")
@click.option("--user", default=None, help="MySQL user (else $MYSQL_USER).")
@click.option("--password", default=None, help="MySQL password (else $MYSQL_PASSWORD).")
@click.option(
    "--db",
    "databases",
    multiple=True,
    help=(
        "Database to extract. Pass multiple times for multi-DB, or a single "
        "comma-separated value. Falls back to $MYSQL_DATABASES (also "
        "comma-separatable)."
    ),
)
@click.option(
    "--out",
    "out_dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help=(
        "Parent output directory. Each DB lands in <out>/<database>/. "
        "Defaults to $OUTPUT_DIR if set, else ./snapshots."
    ),
)
@click.option(
    "--only",
    default=None,
    help="Comma-separated component names to run (default: all).",
)
@click.option(
    "--skip",
    default=None,
    help="Comma-separated component names to skip.",
)
@click.option(
    "--workers",
    type=int,
    default=8,
    show_default=True,
    help="Top-level parallelism across components (per DB).",
)
@click.option(
    "--data-workers",
    type=int,
    default=4,
    show_default=True,
    help="Per-table parallelism inside the data component.",
)
@click.option(
    "--prod", is_flag=True, help="Required for non-local hosts (prompts for confirmation)."
)
@click.option(
    "--force", is_flag=True, help="Overwrite non-empty per-DB output subdirectories."
)
@click.option(
    "--data-quality-report",
    is_flag=True,
    default=False,
    help=(
        "Run the data-quality diagnostic scan (zero-dates, orphan foreign_keys, "
        "charset drift, row-length outliers, missing prim_keys) as an extra "
        "component. Off by default — scans are read-only but can add minutes."
    ),
)
@click.option(
    "--data-quality-only",
    is_flag=True,
    default=False,
    help=(
        "Skip the real extract; run ONLY the data-quality scan. Implies "
        "--data-quality-report. Useful for triage on an already-extracted DB."
    ),
)
def cli(
    host: str | None = None,
    port: int | None = None,
    user: str | None = None,
    password: str | None = None,
    databases: tuple[str, ...] = (),
    out_dir: Path | None = None,
    only: str | None = None,
    skip: str | None = None,
    workers: int = 8,
    data_workers: int = 4,
    prod: bool = False,
    force: bool = False,
    data_quality_report: bool = False,
    data_quality_only: bool = False,
) -> None:
    """Decompose one or more MySQL DBs into per-component snapshot artifacts.

    Defaults on the signature match the click defaults above — they exist solely
    so static analyzers (PyCharm, mypy) don't complain about the no-arg
    ``cli()`` call in the ``__main__`` block. At runtime click decorates this
    function and always fills every parameter itself.
    """
    # Populate os.environ from a nearby .env *before* anything reads env vars.
    # No-op if .env is absent; real env vars always win over .env values.
    load_dotenv()

    # Resolve --out: explicit flag > $OUTPUT_DIR > ./snapshots. Done after
    # load_dotenv() so a path set in .env is honored.
    if out_dir is None:
        out_dir = Path(os.environ.get("OUTPUT_DIR") or "./snapshots")

    resolved_dbs = _resolve_databases(databases)
    if not resolved_dbs:
        raise SystemExit(
            "No databases specified. Pass --db one-or-more-times or set "
            "MYSQL_DATABASES (single name or comma-separated list)."
        )

    # Warn/confirm once up front — same host for every DB.
    # Build a throwaway config just to read host (so we honor --host / env).
    first_cfg = ServerConnectionConfig.from_overrides(
        host=host, port=port, user=user, password=password, database=resolved_dbs[0],
    )
    first_cfg.validate()
    warn_if_prod(first_cfg.host, prod)

    selected = _select_components(only, skip)

    # Resolve data-quality gating:
    #   --data-quality-only  → replace selection with just [data_quality]
    #                          (also implies --data-quality-report, which is
    #                          what the logs/summary label the run as).
    #   --data-quality-report → opt the default-off component back in without
    #                           disturbing --only/--skip semantics.
    if data_quality_only:
        selected = ["data_quality"]
    elif data_quality_report and "data_quality" not in selected:
        selected.append("data_quality")

    # Pre-flight: make sure every per-DB subdir is either empty or --force'd.
    # --data-quality-only writes only a YAML log file under <db>/logs/, so we
    # skip the emptiness check in that mode — nothing in the existing snapshot
    # is being rewritten.
    out_dir.mkdir(parents=True, exist_ok=True)
    if not data_quality_only:
        for db in resolved_dbs:
            ensure_empty_dir(out_dir / db, force=force)
    else:
        for db in resolved_dbs:
            (out_dir / db / "logs").mkdir(parents=True, exist_ok=True)

    _console.print(
        f"[bold]Extracting[/bold] databases=[cyan]{resolved_dbs}[/cyan] "
        f"host=[cyan]{first_cfg.host}[/cyan] → [cyan]{out_dir}[/cyan]"
    )
    _console.print(
        f"Components: {selected}  "
        f"(parallel workers={workers}, data-workers={data_workers})"
    )

    # Accumulate (db, result) pairs for a combined summary.
    all_rows: list[tuple[str, ComponentResult]] = []

    # Sequential across DBs, parallel across components inside each DB.
    # Sequential keeps log output readable and memory predictable.
    for db in resolved_dbs:
        cfg = ServerConnectionConfig.from_overrides(
            host=host, port=port, user=user, password=password, database=db,
        )
        cfg.validate()
        db_out = out_dir / db
        _console.print(f"\n[bold magenta]→ {db}[/bold magenta]  ({db_out})")
        db_results = _run_one_database(cfg, db_out, selected, workers, data_workers)
        all_rows.extend((db, r) for r in db_results)

    _render_summary(all_rows)

    failed = [(db, r) for db, r in all_rows if r.status == "error"]
    if failed:
        bad_dirs = sorted({str((out_dir / db) / "logs") for db, _ in failed})
        _console.print(
            f"[red]{len(failed)} component run(s) failed across "
            f"{len({db for db, _ in failed})} database(s). "
            f"See logs under: {', '.join(bad_dirs)}.[/red]"
        )
        raise SystemExit(1)

    _console.print(
        f"[green]Snapshot written to {out_dir} "
        f"({len(resolved_dbs)} database(s)).[/green]"
    )


if __name__ == "__main__":
    cli()
