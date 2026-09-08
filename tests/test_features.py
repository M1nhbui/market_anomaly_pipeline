"""
Tests for the derived time-series features.

These matter more than the parsing tests. A parsing bug produces obviously wrong
data; a window-function bug produces plausible-looking numbers that are quietly
incorrect, and it survives all the way to the anomaly detector where it becomes
false positives nobody can explain.
"""

import math
import statistics

from conftest import make_bar, write_bronze_doc
from pyspark.sql import Window
from pyspark.sql import functions as F

import bronze_to_silver as b2s

T0 = 1787443200000  # 2026-08-23T00:00:00Z
MINUTE = 60_000


def build_series(spark, tmp_path, closes, start_ms=T0, skip=()):
    """
    Build a Silver DataFrame from a price series.

    `skip` is a set of bar indices to omit, creating deliberate gaps in the
    minute sequence. Ingested far enough in the future that no bar counts as
    unfinished.
    """
    bars = [
        make_bar(start_ms + i * MINUTE, close=c)
        for i, c in enumerate(closes)
        if i not in skip
    ]
    write_bronze_doc(
        tmp_path, "series.json", "BTCUSDT", "2026-08-24T00:00:00.000000Z", bars
    )
    return b2s.transform(b2s.read_bronze(spark, str(tmp_path)))


def collect_ordered(df, *columns):
    rows = df.orderBy("open_time").select(*columns).collect()
    return [tuple(r) for r in rows]


# -----------------------------------------------------------------------------
# log_return
# -----------------------------------------------------------------------------


def test_log_return_matches_hand_computed_values(spark, tmp_path):
    closes = [100.0, 110.0, 99.0]
    df = build_series(spark, tmp_path, closes)

    values = [r[0] for r in collect_ordered(df, "log_return")]

    assert values[0] is None
    assert values[1] == float(f"{math.log(110.0 / 100.0):.12f}") or math.isclose(
        values[1], math.log(1.10), rel_tol=1e-12
    )
    assert math.isclose(values[2], math.log(99.0 / 110.0), rel_tol=1e-12)


def test_first_bar_log_return_is_null_not_zero(spark, tmp_path):
    """
    The distinction that matters. NULL means "unknown - no prior close exists".
    0.0 means "the price did not move", which is a factual claim we cannot make and
    which would be scored as a perfectly normal observation by the z-score in Gold.
    """
    df = build_series(spark, tmp_path, [100.0, 101.0])

    first = df.orderBy("open_time").first()
    assert first.log_return is None
    assert first.log_return != 0.0


def test_log_return_is_computed_after_dedup_not_before(spark, tmp_path):
    """
    If features were derived before deduplication, lag() would compare a bar to a
    duplicate copy of itself and return log_return = 0 - a fabricated "no movement"
    reading. This asserts the stage ordering in transform() holds.
    """
    # Same two bars delivered twice, in two documents.
    bars = [make_bar(T0, close=100.0), make_bar(T0 + MINUTE, close=110.0)]
    write_bronze_doc(tmp_path, "r1.json", "BTCUSDT", "2026-08-24T00:00:00.000000Z", bars)
    write_bronze_doc(tmp_path, "r2.json", "BTCUSDT", "2026-08-24T00:05:00.000000Z", bars)

    df = b2s.transform(b2s.read_bronze(spark, str(tmp_path)))
    values = [r[0] for r in collect_ordered(df, "log_return")]

    assert df.count() == 2, "dedup must run first"
    assert values[0] is None
    assert math.isclose(values[1], math.log(1.10), rel_tol=1e-12), (
        "a log_return of 0.0 here would mean lag() saw a duplicate row"
    )


# -----------------------------------------------------------------------------
# rolling_vol
# -----------------------------------------------------------------------------


def test_rolling_vol_is_null_during_warmup_and_defined_after(spark, tmp_path):
    """
    20 returns require 21 closes. Bars 1..20 must be NULL; bar 21 is the first with
    a full window.
    """
    closes = [100.0 + i for i in range(25)]
    df = build_series(spark, tmp_path, closes)

    vols = [r[0] for r in collect_ordered(df, "rolling_vol")]

    assert all(v is None for v in vols[:20]), "partial windows must be NULL"
    assert vols[20] is not None, "the 21st bar has 20 returns - the first full window"
    assert all(v is not None for v in vols[20:])


def test_rolling_vol_matches_python_sample_stddev(spark, tmp_path):
    """
    Verify against an independent implementation. Uses statistics.stdev, which is the
    SAMPLE standard deviation (n-1) - the same estimator as stddev_samp. If this
    passes with stdev but fails with pstdev, the estimator choice is confirmed.
    """
    closes = [100.0 * (1.0 + 0.01 * math.sin(i)) for i in range(25)]
    df = build_series(spark, tmp_path, closes)

    rows = collect_ordered(df, "log_return", "rolling_vol")
    returns = [r[0] for r in rows]

    # Bar index 20 - its window spans returns from index 1 through 20 inclusive.
    expected = statistics.stdev(returns[1:21])
    assert math.isclose(rows[20][1], expected, rel_tol=1e-9)

    # And it should NOT match the population estimator.
    assert not math.isclose(rows[20][1], statistics.pstdev(returns[1:21]), rel_tol=1e-9)


def test_rolling_vol_is_null_when_a_gap_breaks_the_time_window(spark, tmp_path):
    """
    THE TEST THAT JUSTIFIES rangeBetween OVER rowsBetween.

    With a missing minute, a time-based window honestly contains 19 observations
    instead of 20, so the result is NULL. A ROW-based window would have silently
    reached back one extra bar, produced a number, and labelled a 21-minute
    volatility as a 20-minute one.

    Below we compute the row-based version alongside and assert they disagree -
    documenting the divergence rather than asserting it never happens.
    """
    closes = [100.0 + i for i in range(30)]
    df = build_series(spark, tmp_path, closes, skip={5})  # minute 5 missing

    row_window = (
        Window.partitionBy("symbol")
        .orderBy(F.col("open_time").cast("long"))
        .rowsBetween(-(b2s.VOL_WINDOW_BARS - 1), 0)
    )
    compared = df.withColumn(
        "rolling_vol_rowbased",
        F.when(
            F.count(F.col("log_return")).over(row_window) >= b2s.VOL_WINDOW_BARS,
            F.stddev_samp(F.col("log_return")).over(row_window),
        ),
    )

    rows = collect_ordered(compared, "rolling_vol", "rolling_vol_rowbased")

    disagreements = [
        (i, t, r) for i, (t, r) in enumerate(rows) if (t is None) != (r is None)
    ]
    assert disagreements, (
        "with a gap present, time-based and row-based windows must differ - "
        "if they agree, rangeBetween is not doing what we think"
    )
    # Every disagreement is time-based being NULL where row-based invented a value.
    for _, time_based, row_based in disagreements:
        assert time_based is None and row_based is not None


def test_time_and_row_windows_agree_when_there_are_no_gaps(spark, tmp_path):
    """
    The other half of the argument: on gap-free data the two are identical, so
    choosing rangeBetween costs nothing. This is the case our production data is
    currently in (METRICS.md M-022, zero gaps).
    """
    closes = [100.0 * (1.0 + 0.01 * math.sin(i)) for i in range(30)]
    df = build_series(spark, tmp_path, closes)

    row_window = (
        Window.partitionBy("symbol")
        .orderBy(F.col("open_time").cast("long"))
        .rowsBetween(-(b2s.VOL_WINDOW_BARS - 1), 0)
    )
    compared = df.withColumn(
        "rolling_vol_rowbased",
        F.when(
            F.count(F.col("log_return")).over(row_window) >= b2s.VOL_WINDOW_BARS,
            F.stddev_samp(F.col("log_return")).over(row_window),
        ),
    )

    for time_based, row_based in collect_ordered(
        compared, "rolling_vol", "rolling_vol_rowbased"
    ):
        if time_based is None:
            assert row_based is None
        else:
            assert math.isclose(time_based, row_based, rel_tol=1e-12)


# -----------------------------------------------------------------------------
# dollar_volume and range_pct
# -----------------------------------------------------------------------------


def test_dollar_volume_and_range_pct(spark, tmp_path):
    write_bronze_doc(
        tmp_path, "a.json", "BTCUSDT", "2026-08-24T00:00:00.000000Z",
        [make_bar(T0, close=100.0, volume=3.0)],
    )
    row = b2s.transform(b2s.read_bronze(spark, str(tmp_path))).first()

    assert math.isclose(row.dollar_volume, 100.0 * 3.0)
    # make_bar builds high = close + 1, low = close - 1, open = close
    assert math.isclose(row.range_pct, (101.0 - 99.0) / 100.0)


def test_features_are_isolated_per_symbol(spark, tmp_path):
    """
    partitionBy("symbol") must prevent one symbol's prices leaking into another's
    return. Without it the first bar of the second symbol would compute a return
    against the last bar of the first - a spurious, often enormous move.
    """
    for sym, price in (("BTCUSDT", 100.0), ("ETHUSDT", 5000.0)):
        write_bronze_doc(
            tmp_path, f"{sym}.json", sym, "2026-08-24T00:00:00.000000Z",
            [make_bar(T0, close=price), make_bar(T0 + MINUTE, close=price * 1.01)],
        )

    df = b2s.transform(b2s.read_bronze(spark, str(tmp_path)))

    for sym in ("BTCUSDT", "ETHUSDT"):
        vals = [
            r[0]
            for r in collect_ordered(df.filter(F.col("symbol") == sym), "log_return")
        ]
        assert vals[0] is None, f"{sym}'s first bar must have no prior close"
        assert math.isclose(vals[1], math.log(1.01), rel_tol=1e-9)
