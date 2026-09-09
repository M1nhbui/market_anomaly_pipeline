"""
Tests for the z-score anomaly detector.

The failure modes here are all quiet. A wrong window boundary, a missing degenerate-
variance guard, or a self-referential baseline all produce a table full of
plausible-looking events. Nothing errors, nothing looks obviously wrong, and the
output is simply not measuring what it claims to.
"""

import math

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import silver_to_gold as s2g

SILVER_SCHEMA = StructType(
    [
        StructField("symbol", StringType()),
        StructField("open_time", TimestampType()),
        StructField("close", DoubleType()),
        StructField("volume", DoubleType()),
        StructField("num_trades", IntegerType()),
        StructField("log_return", DoubleType()),
        StructField("date", StringType()),
    ]
)

BASE = 1787443200  # 2026-08-23T00:00:00Z, epoch seconds


def silver_df(spark, returns, symbol="BTCUSDT", volumes=None, start=BASE):
    """
    Build a minimal Silver-shaped frame from a list of log returns.

    `returns[i] is None` represents a bar with no computable return, exactly as the
    real Silver layer produces for a symbol's first bar.
    """
    volumes = volumes if volumes is not None else [1.0] * len(returns)
    rows = []
    for i, (r, v) in enumerate(zip(returns, volumes)):
        ts = start + i * 60
        rows.append(
            (
                symbol,
                __import__("datetime").datetime.utcfromtimestamp(ts),
                100.0,
                float(v),
                10,
                None if r is None else float(r),
                "2026-08-23",
            )
        )
    return spark.createDataFrame(rows, SILVER_SCHEMA)


def events_for(spark, returns, window=5, k=3.0, volumes=None):
    df = silver_df(spark, returns, volumes=volumes)
    return (
        s2g.build_anomaly_events(df, window_bars=window, k=k)
        .filter(F.col("rule") == "return_zscore")
        .orderBy("event_time")
        .collect()
    )


# -----------------------------------------------------------------------------
# Window semantics
# -----------------------------------------------------------------------------


def test_huge_move_during_warmup_does_not_fire(spark):
    """
    The warm-up exclusion, tested with a move that WOULD have fired if scored.

    Bar 0 is an enormous +50% return, but it has zero prior observations, so it must
    be silently unscored rather than scored against a partial baseline. Bar 6 is a
    much smaller +5% move against a full, quiet window and must fire.

    A test that only used small early values could not distinguish "correctly
    excluded during warm-up" from "too small to fire anyway".
    """
    returns = [0.5] + [0.001, -0.0012, 0.0009, -0.0008, 0.0011] + [0.05]
    rows = events_for(spark, returns, window=5, k=3.0)

    assert len(rows) == 1, "only the bar with a full prior window may fire"
    assert math.isclose(rows[0].observed_value, 0.05, rel_tol=1e-9), (
        "the +50% bar at index 0 must not fire - it had no baseline"
    )


def test_current_bar_is_excluded_from_its_own_baseline(spark):
    """
    THE SUBTLE ONE. If the current observation contributed to its own mean and
    standard deviation, an extreme value would inflate the baseline it is measured
    against and shrink its own z-score - weakest exactly where detection matters most.

    Asserted directly on the recorded baseline: rolling_mean must equal the mean of
    the five PRIOR returns (0.0002), not the mean of all six including the outlier
    (0.00835). The two differ by more than 40x, so this cannot pass by coincidence.
    """
    import statistics

    prior = [0.001, -0.0012, 0.0009, -0.0008, 0.0011]
    observed = 0.05
    rows = events_for(spark, prior + [observed], window=5, k=3.0)

    assert len(rows) == 1
    mean_excluding = statistics.fmean(prior)
    mean_including = statistics.fmean(prior + [observed])

    assert math.isclose(rows[0].rolling_mean, mean_excluding, rel_tol=1e-9)
    assert not math.isclose(rows[0].rolling_mean, mean_including, rel_tol=1e-3), (
        "baseline includes the current bar - an outlier is inflating its own baseline"
    )
    assert math.isclose(rows[0].rolling_std, statistics.stdev(prior), rel_tol=1e-9)


def test_degenerate_variance_is_suppressed_not_infinite(spark):
    """
    Untraded minutes leave the close unchanged, so log_return is exactly 0. A run of
    them drives the trailing std to 0 and (x-mean)/std to infinity.

    SHIBUSDT has ~1.9% zero-trade bars in production (M-039), so this is a real
    scenario, not a contrived one. Without the guard, the least liquid symbol would
    dominate the anomaly table purely through its own illiquidity.
    """
    returns = [0.0] * 6 + [0.01]  # flat, then a move
    rows = events_for(spark, returns, window=5, k=3.0)

    assert rows == [], "a zero-variance baseline must yield no event, not an infinite z"


def test_tiny_nonzero_variance_is_also_suppressed(spark):
    """
    The test the previous one only appeared to cover.

    `returns = [0.0]*6` gives a standard deviation of EXACTLY zero, which even a
    naive `std > 0` check would filter. The guard exists for the harder case: a
    baseline that is technically nonzero but numerically meaningless.

    Here the prior returns are ~1e-9 (one ten-millionth of a basis point). Their
    standard deviation is ~1.1e-9, far below MIN_ROLLING_STD = 1e-6. A 1% move
    against that baseline scores a z of roughly nine million - a finite number, so
    nothing errors, and a table full of them would look like a detector working
    extremely well rather than one dividing by noise.

    Found by mutation testing: replacing MIN_ROLLING_STD with 0.0 left the rest of
    this suite green.
    """
    returns = [1e-9, -1e-9, 1e-9, -1e-9, 1e-9, 0.01]
    rows = events_for(spark, returns, window=5, k=3.0)

    assert rows == [], (
        "a baseline standard deviation below MIN_ROLLING_STD is numerically "
        "meaningless; the z-score must be NULL, not merely finite"
    )


def test_a_real_outlier_against_a_varied_baseline_does_fire(spark):
    """
    The complement of the previous test: the guard must suppress DEGENERATE
    baselines without suppressing genuine detections. A test suite that only proves
    things are filtered out would pass on a detector that emits nothing at all.
    """
    returns = [0.001, -0.0012, 0.0009, -0.0008, 0.0011, 0.05]
    rows = events_for(spark, returns, window=5, k=3.0)

    assert len(rows) == 1
    assert rows[0].zscore > 3.0
    assert rows[0].rule == "return_zscore"
    assert rows[0].observed_transform == "none"


def test_zscore_matches_hand_computation(spark):
    """Verify the arithmetic against an independent calculation."""
    import statistics

    prior = [0.001, -0.0012, 0.0009, -0.0008, 0.0011]
    observed = 0.05
    returns = prior + [observed]

    rows = events_for(spark, returns, window=5, k=3.0)
    expected_z = (observed - statistics.fmean(prior)) / statistics.stdev(prior)

    assert math.isclose(rows[0].zscore, expected_z, rel_tol=1e-9)
    assert math.isclose(rows[0].rolling_mean, statistics.fmean(prior), rel_tol=1e-9)
    assert math.isclose(rows[0].rolling_std, statistics.stdev(prior), rel_tol=1e-9)


def test_negative_moves_fire_too(spark):
    """abs(zscore) > k, not zscore > k. A crash is as anomalous as a spike."""
    returns = [0.001, -0.0012, 0.0009, -0.0008, 0.0011, -0.05]
    rows = events_for(spark, returns, window=5, k=3.0)

    assert len(rows) == 1
    assert rows[0].zscore < -3.0


def test_null_returns_are_not_scored(spark):
    """A symbol's first bar has no return. It cannot be an anomaly."""
    returns = [None] + [0.001, -0.0012, 0.0009, -0.0008, 0.0011] + [0.05]
    rows = events_for(spark, returns, window=5, k=3.0)

    assert all(r.observed_value is not None for r in rows)


# -----------------------------------------------------------------------------
# Per-symbol isolation
# -----------------------------------------------------------------------------


def test_symbols_are_scored_independently(spark):
    """
    Each asset is judged against its own history. A quiet symbol must not inherit a
    volatile symbol's baseline - that is what "self-relative" means, and without
    partitionBy it would silently be false.
    """
    calm = silver_df(spark, [0.001, -0.0012, 0.0009, -0.0008, 0.0011, 0.05], "CALMUSDT")
    wild = silver_df(spark, [0.10, -0.12, 0.09, -0.08, 0.11, 0.05], "WILDUSDT")

    events = s2g.build_anomaly_events(calm.unionByName(wild), window_bars=5, k=3.0)
    fired = {r.symbol for r in events.filter(F.col("rule") == "return_zscore").collect()}

    assert "CALMUSDT" in fired, "0.05 is enormous relative to a calm baseline"
    assert "WILDUSDT" not in fired, "0.05 is ordinary relative to a volatile baseline"


# -----------------------------------------------------------------------------
# Volume rule
# -----------------------------------------------------------------------------


def test_volume_rule_uses_log1p_and_handles_zero_volume(spark):
    """
    log1p is defined at zero; plain log is not. Untraded minutes have volume exactly
    0, so using log() would produce -infinity and poison the rolling mean for the
    next 60 bars.
    """
    volumes = [10.0, 11.0, 9.0, 0.0, 10.5, 5000.0]
    returns = [0.001] * 6
    df = silver_df(spark, returns, volumes=volumes)

    events = s2g.build_anomaly_events(df, window_bars=5, k=3.0)
    rows = events.filter(F.col("rule") == "volume_zscore").collect()

    for r in rows:
        assert r.observed_transform == "log1p"
        assert not math.isinf(r.observed_value)
        assert not math.isinf(r.rolling_mean)
        # observed_value is the TRANSFORMED value that was scored...
        assert math.isclose(r.observed_value, math.log1p(5000.0), rel_tol=1e-9)
        # ...while `volume` carries the raw figure for context.
        assert math.isclose(r.volume, 5000.0)


def test_volume_rule_ignores_the_lower_tail(spark):
    """
    REGRESSION TEST for the artifact found in production (METRICS.md M-043).

    An untraded minute has volume 0, so log1p(0) = 0 against a positive baseline -
    a large NEGATIVE z. Under a two-sided test that fired, and three SHIBUSDT
    untraded minutes landed in the top fifteen events by magnitude.

    An absence of trading is not an event. The volume rule is upper-tail only; the
    lower tail belongs to the DQ gate's empty-bar ratio check.
    """
    volumes = [10.0, 11.0, 9.0, 10.5, 10.2, 0.0]  # collapse to zero on the last bar
    returns = [0.001, -0.0012, 0.0009, -0.0008, 0.0011, 0.0002]
    df = silver_df(spark, returns, volumes=volumes)

    events = s2g.build_anomaly_events(df, window_bars=5, k=3.0)
    volume_rows = events.filter(F.col("rule") == "volume_zscore").collect()

    assert volume_rows == [], (
        "a volume COLLAPSE must not fire - that is illiquidity, not an anomaly"
    )


def test_volume_rule_still_fires_on_spikes(spark):
    """The complement: making the rule one-sided must not disable it entirely."""
    volumes = [10.0, 11.0, 9.0, 10.5, 10.2, 5000.0]
    returns = [0.001, -0.0012, 0.0009, -0.0008, 0.0011, 0.0002]
    df = silver_df(spark, returns, volumes=volumes)

    rows = (
        s2g.build_anomaly_events(df, window_bars=5, k=3.0)
        .filter(F.col("rule") == "volume_zscore")
        .collect()
    )

    assert len(rows) == 1
    assert rows[0].zscore > 3.0
    assert rows[0].rule_direction == "upper"


def test_return_rule_stays_two_sided(spark):
    """Returns keep both tails. A crash is as worth surfacing as a spike."""
    returns = [0.001, -0.0012, 0.0009, -0.0008, 0.0011, -0.05]
    rows = events_for(spark, returns, window=5, k=3.0)

    assert len(rows) == 1
    assert rows[0].zscore < -3.0
    assert rows[0].rule_direction == "both"


def test_both_rules_share_one_schema(spark):
    """unionByName requires identical columns; a mismatch would fail at write time."""
    df = silver_df(spark, [0.001, -0.0012, 0.0009, -0.0008, 0.0011, 0.05])
    events = s2g.build_anomaly_events(df, window_bars=5, k=3.0)

    assert set(events.columns) >= set(s2g.GOLD_COLUMNS)
    rules = {r.rule for r in events.collect()}
    assert rules <= {"return_zscore", "volume_zscore"}


# -----------------------------------------------------------------------------
# Parameters are recorded on every row
# -----------------------------------------------------------------------------


@pytest.mark.parametrize("k", [2.0, 3.0, 4.0])
def test_threshold_is_recorded_and_respected(spark, k):
    """
    Every row carries the window and threshold that produced it, so events from
    different parameter choices remain distinguishable in the same table without
    consulting a changelog. README §10 asks for the knobs to be explicit; this makes
    them explicit in the data itself.
    """
    returns = [0.001, -0.0012, 0.0009, -0.0008, 0.0011, 0.05]
    rows = events_for(spark, returns, window=5, k=k)

    for r in rows:
        assert r.zscore_threshold == k
        assert r.zscore_window_bars == 5
        assert abs(r.zscore) > k


def test_lower_threshold_fires_at_least_as_often(spark):
    """Monotonicity: relaxing k can only add events, never remove them."""
    returns = [0.001, -0.0012, 0.0009, -0.0008, 0.0011, 0.01, 0.05, 0.002]

    counts = {k: len(events_for(spark, returns, window=5, k=k)) for k in (2.0, 3.0, 5.0)}
    assert counts[2.0] >= counts[3.0] >= counts[5.0]
