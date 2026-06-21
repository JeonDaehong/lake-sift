"""Machine-consumable JSON output. (Module name avoids shadowing stdlib `json`.)"""

from __future__ import annotations

from lakesift.result import DiffResult


def render_json(result: DiffResult) -> str:
    return result.to_json()
