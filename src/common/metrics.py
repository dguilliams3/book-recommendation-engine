from __future__ import annotations
"""Common Prometheus metrics helpers.

This module exposes reusable counters/histograms **and** degrades gracefully
when ``prometheus_client`` is not installed (e.g. during unit tests or in
lightweight environments).
"""
from types import SimpleNamespace

# ---------------------------------------------------------------------------
# Optional dependency handling
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Histogram  # type: ignore
    _PROM = True
except ImportError:  # pragma: no cover – fallback when prometheus_client absent

    _PROM = False

    class _NoOp(SimpleNamespace):
        """A stand-in object that swallows all metric operations silently."""

        def labels(self, *_, **__):  # noqa: D401 – simple pass-through
            return self

        def inc(self, *_):
            return None

        def observe(self, *_):
            return None

        # Support Histogram.time() context-manager API
        def time(self):  # noqa: D401
            class _Timer:  # pylint: disable=too-few-public-methods
                def __enter__(self, *a, **k):
                    return None

                def __exit__(self, *_):
                    return False

            return _Timer()

    # Dummy factory funcs mimic prometheus_client API
    Counter = lambda *a, **k: _NoOp()  # type: ignore
    Histogram = lambda *a, **k: _NoOp()  # type: ignore

from common.structured_logging import SERVICE_NAME  # keep at bottom to avoid cycles

# ---------------------------------------------------------------------------
# Metric definitions (add new ones here)
# ---------------------------------------------------------------------------

REQUEST_COUNTER = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["service", "method", "endpoint", "status_code"],
)

REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "Latency of HTTP requests in seconds",
    ["service", "endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)

JOB_RUNS_TOTAL = Counter(
    "batch_job_runs_total",
    "Number of batch job executions",
    ["job", "status"],
)

JOB_DURATION_SECONDS = Histogram(
    "batch_job_duration_seconds",
    "Duration of batch jobs in seconds",
    ["job", "status"],
    buckets=(1, 2, 5, 10, 30, 60, 120, 300, 600),
)

__all__ = [
    "_PROM",
    "REQUEST_COUNTER",
    "REQUEST_LATENCY",
    "JOB_RUNS_TOTAL",
    "JOB_DURATION_SECONDS",
] 