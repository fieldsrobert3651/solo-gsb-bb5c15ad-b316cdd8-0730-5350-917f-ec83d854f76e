"""Per-service exclusive (self) time for one trace.

For every span, the exclusive time is the length of its own
``[start_ns, end_ns)`` interval minus the union of the intersections with
its *direct* children (spans of the same trace whose ``parent_span_id``
points at it). Self-referencing parent links are ignored, overlapping child
intervals are subtracted only once, and zero-length spans contribute 0.

The module is pure computation over span dicts; the engine feeds it rows
read from SQLite.
"""
from __future__ import annotations

from typing import Any, Iterable


def _union_length(intervals: Iterable[tuple[int, int]]) -> int:
    """Total length covered by ``[start, end)`` intervals, overlaps once."""
    total = 0
    cur_start: int | None = None
    cur_end: int | None = None
    for start, end in sorted(intervals):
        if cur_start is None:
            cur_start, cur_end = start, end
        elif start <= cur_end:
            cur_end = max(cur_end, end)
        else:
            total += cur_end - cur_start
            cur_start, cur_end = start, end
    if cur_start is not None:
        total += cur_end - cur_start
    return total


def service_time_breakdown(
    spans: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Aggregate exclusive time per service.

    ``spans`` items must provide ``span_id``, ``parent_span_id``,
    ``service``, ``start_ns`` and ``end_ns``. Returns one row per distinct
    service (``None`` for spans without one), sorted by ``exclusive_ns``
    descending, then service name ascending, with the ``None`` group last.
    """
    spans = list(spans)
    # parent span_id -> child [start, end) intervals (self-links ignored).
    children: dict[str, list[tuple[int, int]]] = {}
    for s in spans:
        pid = s["parent_span_id"]
        if pid is None or pid == s["span_id"]:
            continue
        children.setdefault(pid, []).append((s["start_ns"], s["end_ns"]))

    per_service: dict[str | None, dict[str, int]] = {}
    for s in spans:
        start, end = s["start_ns"], s["end_ns"]
        length = max(0, end - start)
        covered = 0
        if length:
            covered = _union_length(
                (max(start, c_start), min(end, c_end))
                for c_start, c_end in children.get(s["span_id"], ())
                if max(start, c_start) < min(end, c_end)
            )
        bucket = per_service.setdefault(
            s["service"], {"span_count": 0, "exclusive_ns": 0}
        )
        bucket["span_count"] += 1
        bucket["exclusive_ns"] += length - covered

    rows = [
        {
            "service": service,
            "span_count": bucket["span_count"],
            "exclusive_ns": bucket["exclusive_ns"],
        }
        for service, bucket in per_service.items()
    ]
    rows.sort(
        key=lambda r: (
            -r["exclusive_ns"],
            r["service"] is None,           # null service sorts last
            r["service"] or "",
        )
    )
    return rows
