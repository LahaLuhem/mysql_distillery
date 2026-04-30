"""Shared click scaffolding for standalone component CLIs.

Standalone components accept the same multi-DB interface as the orchestrator —
each DB gets its own subdirectory under ``--out``, mirroring the full-pipeline
layout so partial reruns drop their files into exactly the right place.
"""
from __future__ import annotations

import os
from pathlib import Path

import click
from dotenv import load_dotenv
from rich.console import Console

from mysql_distillery.data.models.server_connection_config import ServerConnectionConfig
from mysql_distillery.data.utils.safety import warn_if_prod

_console = Console()


def _resolve_databases(cli_dbs: tuple[str, ...]) -> list[str]:
    """CLI --db wins; otherwise fall back to $MYSQL_DATABASES (comma-split)."""
    if cli_dbs:
        resolved: list[str] = []
        for chunk in cli_dbs:
            resolved.extend(d.strip() for d in chunk.split(",") if d.strip())
        return resolved
    return ServerConnectionConfig.databases_from_env()


def make_component_cli(component_name: str, run_fn):
    """Build a click command that wraps a component's `run()` for standalone use.

    Each component module calls this in its `__main__` block so they all share
    the same flags (--host, --port, --user, --password, --db, --out, --prod).
    """

    @click.command(name=component_name)
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
            "comma-separated value. Falls back to $MYSQL_DATABASES."
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
    @click.option("--prod", is_flag=True, help="Required for non-local hosts.")
    @click.option(
        "--max-workers",
        type=int,
        default=1,
        show_default=True,
        help="Per-component worker count (only used by data.py).",
    )
    def _cmd(
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        databases: tuple[str, ...] = (),
        out_dir: Path | None = None,
        prod: bool = False,
        max_workers: int = 1,
    ) -> None:
        # Defaults mirror the click defaults above — present only so static
        # analyzers don't flag the no-arg call in the ``__main__`` block. click
        # always fills every parameter at runtime.
        #
        # Standalone component invocations (``python -m mysql_distillery.components.X``)
        # are entry points too, so they also need to auto-load .env before any
        # code reads env vars. No-op if .env is absent; real env vars always win.
        load_dotenv()

        # Resolve --out: explicit flag > $OUTPUT_DIR > ./snapshots.
        if out_dir is None:
            out_dir = Path(os.environ.get("OUTPUT_DIR") or "./snapshots")

        resolved_dbs = _resolve_databases(databases)
        if not resolved_dbs:
            raise SystemExit(
                "No databases specified. Pass --db or set MYSQL_DATABASES."
            )

        # Warn/confirm once — same host for every DB.
        first_cfg = ServerConnectionConfig.from_overrides(
            host=host, port=port, user=user, password=password, database=resolved_dbs[0],
        )
        first_cfg.validate()
        warn_if_prod(first_cfg.host, prod)
        out_dir.mkdir(parents=True, exist_ok=True)

        exit_code = 0
        for db in resolved_dbs:
            cfg = ServerConnectionConfig.from_overrides(
                host=host, port=port, user=user, password=password, database=db,
            )
            cfg.validate()
            db_out = out_dir / db
            db_out.mkdir(parents=True, exist_ok=True)
            result = run_fn(cfg, db_out, max_workers=max_workers)
            if result.status == "error":
                _console.print(
                    f"[red]{component_name} failed for {db}:[/red] {result.error}"
                )
                exit_code = 1
                continue
            _console.print(
                f"[green]{component_name} ok[/green] for [cyan]{db}[/cyan] "
                f"({len(result.artifacts)} artifacts in {result.duration_s:.2f}s)"
            )

        if exit_code:
            raise SystemExit(exit_code)

    return _cmd
