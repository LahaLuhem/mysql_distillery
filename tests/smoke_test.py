"""Wheel/sdist smoke test — verify the installed package imports cleanly and
the orchestrator's CLI runs.

Invoked from .github/workflows/publish.yml as:

    uv run --isolated --no-project --with dist/*.whl   tests/smoke_test.py
    uv run --isolated --no-project --with dist/*.tar.gz tests/smoke_test.py

Plain script (no pytest) — keeps the smoke env minimal: just the installed
distribution + its declared deps + the std lib. Pytest's `python_files =
["test_*.py"]` collection rule means `smoke_test.py` is NOT picked up by
the local `uv run pytest` either.
"""
from __future__ import annotations

import subprocess
import sys


def main() -> None:
    # The package imports cleanly.
    import mysql_distillery  # noqa: F401

    # Every component module imports — catches accidental missing-file
    # regressions in the wheel layout (a forgotten file in
    # tool.hatch.build.targets.wheel.packages would surface here).
    from mysql_distillery.components import (  # noqa: F401
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

    # The orchestrator's CLI runs end-to-end. Use `python -m
    # mysql_distillery.extract` so we use the same interpreter (and same
    # installed deps) the script is running under, regardless of whether
    # the console-script shim is on PATH inside the isolated env.
    result = subprocess.run(
        [sys.executable, "-m", "mysql_distillery.extract", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit(
            f"`mysql_distillery --help` exited {result.returncode}\n"
            f"--- stdout ---\n{result.stdout}\n"
            f"--- stderr ---\n{result.stderr}"
        )
    if "Decompose one or more MySQL DBs" not in result.stdout:
        raise SystemExit(
            "--help output is missing the expected docstring; "
            f"got:\n{result.stdout}"
        )

    print("smoke test: ok")


if __name__ == "__main__":
    main()
