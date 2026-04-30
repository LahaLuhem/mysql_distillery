"""Guardrails for connecting to non-local (i.e., production) hosts."""
from __future__ import annotations

from rich.console import Console

_console = Console(stderr=True)

# Hosts considered "safe" — anything else requires --prod.
_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}


def warn_if_prod(host: str, prod_flag: bool) -> None:
    """Enforce the --prod guardrail from .ai/CLAUDE.md.

    If host is non-local and --prod wasn't passed: abort.
    If --prod was passed: print a red warning and require typed 'yes'.
    """
    is_local = host in _LOCAL_HOSTS
    if is_local and not prod_flag:
        return

    if not is_local and not prod_flag:
        raise SystemExit(
            f"Refusing to connect to non-local host {host!r} without --prod. "
            "Add --prod and confirm the prompt if this is intentional."
        )

    # prod_flag is True
    _console.print(
        f"[bold red]WARNING:[/bold red] about to connect to PRODUCTION host "
        f"[bold]{host}[/bold]. This is a read-only export but still touches prod.",
    )
    _console.print("Type [bold]yes[/bold] to continue, anything else to abort: ", end="")
    answer = input().strip()
    if answer != "yes":
        raise SystemExit("Aborted by user.")
