"""The result each component's ``run()`` returns to the orchestrator."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path

from mysql_distillery.data.enums.component_status import ComponentStatus


@dataclass
class ComponentResult:
    """Return value from each component's `run()`.

    The orchestrator collects these and renders a summary.
    """

    component: str
    status: ComponentStatus
    duration_s: float
    artifacts: list[Path] = field(default_factory=list)
    error: str | None = None
    notes: list[str] = field(default_factory=list)

    @classmethod
    def timed(cls, component: str) -> "_TimedResult":
        """Context manager that times a component run and builds the result.

        Usage:
            with ComponentResult.timed("schema") as r:
                ...  # populate r.artifacts, r.notes
                # on exit: r.status + r.duration_s are filled in
        """
        return _TimedResult(component)


class _TimedResult:
    def __init__(self, component: str):
        self.result = ComponentResult(component=component, status="ok", duration_s=0.0)
        self._start = 0.0

    def __enter__(self) -> ComponentResult:
        self._start = time.perf_counter()
        return self.result

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.result.duration_s = time.perf_counter() - self._start
        if exc is not None:
            self.result.status = "error"
            self.result.error = f"{exc_type.__name__}: {exc}"
            # Propagate: the component caller should decide whether to swallow.
            return False
        return False
