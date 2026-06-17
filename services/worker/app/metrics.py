"""Prometheus metrics for workload worker operations."""

from __future__ import annotations

from prometheus_client import Counter

_dead_letter_total = Counter(
    "moiraweave_worker_dead_letter_total",
    "Run dispatch messages moved to the dead-letter stream.",
    labelnames=("reason",),
)

_pending_reclaim_total = Counter(
    "moiraweave_worker_pending_reclaim_messages_total",
    "Pending Redis Stream messages inspected by reclaim outcome.",
    labelnames=("outcome",),
)

_run_retry_total = Counter(
    "moiraweave_worker_run_retry_total",
    "Run execution retries scheduled after transient executor failures.",
    labelnames=("workload",),
)

_stale_run_lost_total = Counter(
    "moiraweave_worker_stale_run_lost_total",
    "Runs marked lost because their worker heartbeat became stale.",
)


def record_dead_letter(reason: str) -> None:
    _dead_letter_total.labels(reason=reason).inc()


def record_pending_reclaim(outcome: str) -> None:
    _pending_reclaim_total.labels(outcome=outcome).inc()


def record_run_retry(workload_name: str) -> None:
    _run_retry_total.labels(workload=workload_name).inc()


def record_stale_run_lost() -> None:
    _stale_run_lost_total.inc()
