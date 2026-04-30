"""Outcome of a single component's run."""
from __future__ import annotations

from typing import Literal

ComponentStatus = Literal["ok", "error", "skipped"]
