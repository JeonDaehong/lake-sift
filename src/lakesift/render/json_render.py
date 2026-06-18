"""기계 소비용 JSON 출력. (파일명은 stdlib `json` 섀도잉 회피용.)"""

from __future__ import annotations

from lakesift.result import DiffResult


def render_json(result: DiffResult) -> str:
    return result.to_json()
