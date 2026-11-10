"""
Tests for the DQ gate's decision logic.

`evaluate()` is deliberately pure - no AWS calls, no clock reads - so every threshold
boundary is testable without Athena. That matters because the interesting behaviour of
a quality gate is exactly what it does at the edges, and edges are miserable to
reproduce against live data: you cannot ask the market to produce 5.01% empty bars on
demand.
"""

import os
import sys
import types
from datetime import datetime, timedelta, timezone

import pytest

# The module reads configuration at import time and constructs boto3 clients. Stub
# both so it can be imported on a laptop with no AWS credentials.
os.environ.setdefault("ATHENA_DATABASE", "db")
os.environ.setdefault("ATHENA_WORKGROUP", "wg")
os.environ.setdefault("SILVER_TABLE", "db.clean_bars")
os.environ.setdefault("DQ_RESULTS_BUCKET", "bucket")
os.environ.setdefault("SYMBOL_COUNT", "17")

sys.modules.setdefault(
    "boto3", types.SimpleNamespace(client=lambda *a, **k: types.SimpleNamespace())
)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lambdas", "dq_gate"))

import lambda_function as dq  # noqa: E402

NOW = datetime(2026, 9, 10, 12, 0, 0, tzinfo=timezone.utc)
SYMBOLS = 17
WINDOW = 24
EXPECTED_ROWS = SYMBOLS * 60 * WINDOW  # 24,480


def healthy_metrics(**overrides):
    """A run where everything is fine. Tests override one field at a time."""
    metrics = {
        "row_count": EXPECTED_ROWS,
        "symbol_count": SYMBOLS,
        "ohlc_violations": 0,
        "nonpositive_prices": 0,
        "zero_trade_bars": 100,  # 0.41% - close to the measured production rate
        # 4m23s behind, the measured structural lag (M-025)
        "max_open_time": int((NOW - timedelta(minutes=4, seconds=23)).timestamp()),
        "comparable_pairs": EXPECTED_ROWS - SYMBOLS,
        "gap_count": 0,
        "non_monotonic": 0,
    }
    metrics.update(overrides)
    return metrics


def by_name(results):
    return {r["check_name"]: r for r in results}


def blocking_failures(results):
    return [r["check_name"] for r in results if not r["passed"] and r["severity"] == dq.FAIL]


# -----------------------------------------------------------------------------
# The healthy baseline
# -----------------------------------------------------------------------------


def test_a_healthy_run_passes_every_check(spark_free=None):
    results = dq.evaluate(healthy_metrics(), NOW, WINDOW, SYMBOLS)

    assert all(r["passed"] for r in results), by_name(results)
    assert blocking_failures(results) == []
    assert len(results) == 8


def test_the_measured_structural_lag_does_not_trip_freshness(spark_free=None):
    """
    README §11 specifies "latest bar within one interval of now" - one minute at
    1-minute bars. The measured structural lag is 4m23s (M-025), so that rule as
    written would fail on EVERY run. This asserts our threshold accommodates reality.
    """
    results = by_name(dq.evaluate(healthy_metrics(), NOW, WINDOW, SYMBOLS))

    assert results["freshness"]["passed"]
    assert 4.0 < results["freshness"]["observed"] < 5.0
    assert results["freshness"]["observed"] > 1.0, (
        "the lag genuinely exceeds one interval - README §11's rule is unusable"
    )


def test_the_measured_empty_bar_rate_does_not_trip_the_warning(spark_free=None):
    """
    SHIBUSDT measured 1.89% untraded minutes (M-039). The tolerance must sit above
    that or the gate fires every run and gets ignored.
    """
    metrics = healthy_metrics(zero_trade_bars=int(EXPECTED_ROWS * 0.0189))
    results = by_name(dq.evaluate(metrics, NOW, WINDOW, SYMBOLS))

    assert results["empty_bar_ratio"]["passed"]
    assert 1.8 < results["empty_bar_ratio"]["observed"] < 2.0


# -----------------------------------------------------------------------------
# Blocking checks
# -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "override,expected_check",
    [
        ({"ohlc_violations": 1}, "ohlc_integrity"),
        ({"nonpositive_prices": 1}, "positive_prices"),
        ({"non_monotonic": 1}, "monotonic_time"),
        ({"symbol_count": SYMBOLS - 1}, "symbols_present"),
    ],
)
def test_a_single_corrupt_row_blocks(override, expected_check):
    """
    These are zero-tolerance on purpose. One OHLC violation means either upstream
    corruption or a positional-mapping bug in our own cast - there is no benign
    explanation, and analytics built on it would be wrong.
    """
    results = dq.evaluate(healthy_metrics(**override), NOW, WINDOW, SYMBOLS)

    assert expected_check in blocking_failures(results)
    assert not by_name(results)[expected_check]["passed"]


def test_a_stalled_feed_blocks(spark_free=None):
    metrics = healthy_metrics(
        max_open_time=int((NOW - timedelta(minutes=45)).timestamp())
    )
    results = dq.evaluate(metrics, NOW, WINDOW, SYMBOLS)

    assert "freshness" in blocking_failures(results)


def test_an_empty_silver_layer_blocks_rather_than_dividing_by_zero(spark_free=None):
    """
    The degenerate case. A completely empty window must produce a clean block, not a
    ZeroDivisionError - a gate that crashes fails open in the state machine, which is
    the opposite of what a circuit breaker should do.
    """
    metrics = healthy_metrics(
        row_count=0, symbol_count=0, comparable_pairs=0, max_open_time=0,
        zero_trade_bars=0,
    )
    results = dq.evaluate(metrics, NOW, WINDOW, SYMBOLS)

    failures = blocking_failures(results)
    assert "row_count" in failures
    assert "freshness" in failures
    assert by_name(results)["freshness"]["observed"] == float("inf")


def test_row_count_threshold_boundary(spark_free=None):
    """Just under blocks, just over passes. Off-by-one in a gate is expensive."""
    just_under = dq.evaluate(
        healthy_metrics(row_count=int(EXPECTED_ROWS * 0.69)), NOW, WINDOW, SYMBOLS
    )
    just_over = dq.evaluate(
        healthy_metrics(row_count=int(EXPECTED_ROWS * 0.71)), NOW, WINDOW, SYMBOLS
    )

    assert "row_count" in blocking_failures(just_under)
    assert "row_count" not in blocking_failures(just_over)


# -----------------------------------------------------------------------------
# The WARN tier - the design decision that keeps the gate credible
# -----------------------------------------------------------------------------


def test_an_elevated_empty_bar_ratio_warns_but_does_not_block(spark_free=None):
    """
    THE CENTRAL DESIGN DECISION.

    An illiquid pair on a quiet Sunday can push the empty-bar ratio above tolerance
    without anything being wrong. Blocking the whole pipeline for that would mean the
    gate fires routinely - and a gate that fires routinely is one operators learn to
    ignore, which is strictly worse than having no gate.
    """
    metrics = healthy_metrics(zero_trade_bars=int(EXPECTED_ROWS * 0.09))  # 9%
    results = dq.evaluate(metrics, NOW, WINDOW, SYMBOLS)

    assert not by_name(results)["empty_bar_ratio"]["passed"]
    assert "empty_bar_ratio" not in blocking_failures(results), (
        "an elevated empty-bar ratio must warn, never block"
    )


def test_excess_sequence_gaps_warn_but_do_not_block(spark_free=None):
    metrics = healthy_metrics(gap_count=int(EXPECTED_ROWS * 0.02))
    results = dq.evaluate(metrics, NOW, WINDOW, SYMBOLS)

    assert not by_name(results)["sequence_gaps"]["passed"]
    assert "sequence_gaps" not in blocking_failures(results)


def test_warnings_and_blocks_are_reported_separately(spark_free=None):
    """Both firing at once must still block, and must still report the warning."""
    metrics = healthy_metrics(
        ohlc_violations=3,
        zero_trade_bars=int(EXPECTED_ROWS * 0.09),
    )
    results = dq.evaluate(metrics, NOW, WINDOW, SYMBOLS)

    failed = [r["check_name"] for r in results if not r["passed"]]
    assert "ohlc_integrity" in blocking_failures(results)
    assert "empty_bar_ratio" in failed
    assert "empty_bar_ratio" not in blocking_failures(results)


# -----------------------------------------------------------------------------
# Result shape
# -----------------------------------------------------------------------------


def test_every_result_records_observed_and_threshold(spark_free=None):
    """
    A check that reports only pass/fail is nearly useless in an incident. "empty bars
    at 9.1% against a 5% tolerance" tells you what to do; "empty_bar_ratio: false"
    does not.
    """
    for result in dq.evaluate(healthy_metrics(), NOW, WINDOW, SYMBOLS):
        assert set(result) == {
            "check_name", "severity", "passed", "observed", "threshold", "detail"
        }
        assert result["severity"] in (dq.FAIL, dq.WARN)
        assert result["observed"] is not None
        assert result["detail"]


def test_check_names_are_unique(spark_free=None):
    """They become a GROUP BY key in the persisted DQ history table."""
    names = [r["check_name"] for r in dq.evaluate(healthy_metrics(), NOW, WINDOW, SYMBOLS)]
    assert len(names) == len(set(names))


# -----------------------------------------------------------------------------
# THE HANDLER (added 2026-11-09 after the first production invocation crashed)
#
# Every test above calls evaluate(). None called lambda_handler(), so when bug #12
# renamed the result key `check` -> `check_name`, the two lines in the handler that
# build `blocking_failures` and `warnings` kept reading r["check"]. The first real run
# raised KeyError: 'check' - AFTER persisting results, so the history row exists but
# the caller (Step Functions, later) gets an error instead of quality_passed.
# These tests run the real handler with only the AWS-touching functions stubbed.
# -----------------------------------------------------------------------------


def run_handler(monkeypatch, **metric_overrides):
    real_now = datetime.now(timezone.utc)
    metrics = healthy_metrics(**metric_overrides)
    # healthy_metrics is pinned to NOW; re-anchor freshness to the handler's real clock
    # unless the test overrides it.
    if "max_open_time" not in metric_overrides:
        metrics["max_open_time"] = int((real_now - timedelta(minutes=4)).timestamp())
    monkeypatch.setattr(dq, "collect_metrics", lambda *a, **k: metrics)
    monkeypatch.setattr(dq, "persist", lambda *a, **k: "s3://bucket/dq_results/x.json")
    ctx = types.SimpleNamespace(aws_request_id="test-run")
    return dq.lambda_handler({}, ctx)


def test_handler_reports_a_healthy_run(monkeypatch):
    resp = run_handler(monkeypatch)
    assert resp["quality_passed"] is True
    assert resp["blocking_failures"] == []
    assert resp["warnings"] == []
    assert resp["checks_run"] == 8


def test_handler_names_blocking_failures_and_warnings(monkeypatch):
    stale = int((datetime.now(timezone.utc) - timedelta(hours=9)).timestamp())
    resp = run_handler(
        monkeypatch,
        max_open_time=stale,
        row_count=int(EXPECTED_ROWS * 0.6),
        zero_trade_bars=int(EXPECTED_ROWS * 0.6 * 0.10),  # 10% untraded -> WARN
    )
    assert resp["quality_passed"] is False
    assert sorted(resp["blocking_failures"]) == ["freshness", "row_count"]
    assert resp["warnings"] == ["empty_bar_ratio"]
