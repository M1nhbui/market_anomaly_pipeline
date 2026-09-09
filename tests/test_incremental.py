"""
Tests for incremental Bronze -> Silver processing.

The central test is an EQUALITY test: full reprocess is the reference
implementation - simple enough to be obviously correct - and incremental must
reproduce it byte for byte on the dates it claims to rewrite.

This is only possible because we built the slow-and-correct version first. Adding
the lookback and the window features in one change would have left no trustworthy
baseline to compare against.
"""

from datetime import datetime, timezone

import pytest
from conftest import simulate_ingestion
from pyspark.sql import functions as F

import bronze_to_silver as b2s


def utc(iso):
    return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)


def ms(iso):
    return int(utc(iso).timestamp() * 1000)


# A continuous minute series spanning two midnight boundaries:
# 2026-09-01 23:00 -> 2026-09-03 01:00 (26 hours, 1560 bars, dates 09-01/02/03).
SERIES_START = ms("2026-09-01T23:00:00")
SERIES_BARS = 26 * 60


@pytest.fixture(scope="module")
def bronze_root(tmp_path_factory):
    root = tmp_path_factory.mktemp("bronze")
    simulate_ingestion(root, "BTCUSDT", SERIES_START, SERIES_BARS)
    # Older history, deliberately outside any lookback window we test. Without it,
    # every date in the fixture falls inside the read window and the pruning test
    # cannot distinguish "incremental works" from "incremental read everything
    # anyway" - which is exactly what the first version of this fixture did.
    simulate_ingestion(root, "BTCUSDT", ms("2026-08-25T12:00:00"), 120)
    return root


# -----------------------------------------------------------------------------
# Window arithmetic - no Spark needed
# -----------------------------------------------------------------------------


def test_lookback_within_one_day_targets_one_date():
    targets, reads = b2s.compute_incremental_windows(utc("2026-09-03T14:00:00"), 3)
    assert targets == ["2026-09-03"]
    assert reads == ["2026-09-02", "2026-09-03"]


def test_lookback_spanning_midnight_targets_both_dates():
    """
    A 3-hour lookback at 01:00 reaches back to 22:00 the previous day, so BOTH days
    have bars in the window and both must be rewritten. Rewriting only the current
    date would leave the previous day's final hours computed from a run that did not
    yet have them.
    """
    targets, reads = b2s.compute_incremental_windows(utc("2026-09-03T01:00:00"), 3)
    assert targets == ["2026-09-02", "2026-09-03"]
    assert reads == ["2026-09-01", "2026-09-02", "2026-09-03"]


def test_read_window_always_includes_one_warmup_day():
    targets, reads = b2s.compute_incremental_windows(utc("2026-09-03T14:00:00"), 1)
    assert reads[0] == "2026-09-02"
    assert reads[0] not in targets, "the warm-up day must not be a rewrite target"


# -----------------------------------------------------------------------------
# Path resolution
# -----------------------------------------------------------------------------


def test_missing_dates_are_skipped_not_fatal(spark, bronze_root):
    """
    Spark raises on a nonexistent input path. The warm-up date legitimately has no
    data on a pipeline's first day, so the resolver must filter rather than fail.
    """
    patterns = b2s.bronze_globs_for_dates(
        str(bronze_root), ["2025-01-01", "2026-09-02"]
    )
    found = b2s.existing_paths(spark, patterns)

    assert len(found) == 1
    assert "2026-09-02" in found[0]


def test_run_returns_empty_stats_when_nothing_matches(spark, bronze_root, tmp_path):
    stats = b2s.run(
        spark, str(bronze_root), str(tmp_path / "out"),
        lookback_hours=3, as_of=utc("2020-01-01T00:00:00"),
    )
    assert stats["unique_bars_written"] == 0
    assert stats["mode"] == "incremental"


# -----------------------------------------------------------------------------
# THE EQUALITY TEST
# -----------------------------------------------------------------------------


def test_incremental_output_matches_full_reprocess_exactly(spark, bronze_root, tmp_path):
    """
    Run both, compare the rewritten dates row for row.

    rolling_vol is the column that matters here. It depends on 20 minutes of prior
    history, so the first bars of each target date can only be correct if the
    warm-up day was read. If the lookback were even slightly too short, those rows
    would come back NULL in the incremental run and non-NULL in the full run, and
    this test would fail loudly rather than the difference hiding in Athena.
    """
    full_out = tmp_path / "full"
    inc_out = tmp_path / "incremental"

    b2s.run(spark, str(bronze_root), str(full_out), lookback_hours=0)

    # 3-hour lookback at 01:00 on the 3rd => targets 09-02 and 09-03.
    inc_stats = b2s.run(
        spark, str(bronze_root), str(inc_out),
        lookback_hours=3, as_of=utc("2026-09-03T01:00:00"),
    )
    assert inc_stats["target_dates"] == ["2026-09-02", "2026-09-03"]

    full = spark.read.parquet(str(full_out)).filter(
        F.col("date").isin(["2026-09-02", "2026-09-03"])
    )
    incremental = spark.read.parquet(str(inc_out))

    assert incremental.count() == full.count()

    # Symmetric difference across every column. Zero rows in both directions is a
    # stronger statement than comparing aggregates.
    assert full.exceptAll(incremental).count() == 0
    assert incremental.exceptAll(full).count() == 0


def test_warmup_day_is_read_but_never_written(spark, bronze_root, tmp_path):
    """
    The warm-up day must inform the features and then be discarded. Writing it would
    make dynamic partition overwrite replace that day's partition with only the tail
    that happened to be read - incremental data loss disguised as a successful run.
    """
    out = tmp_path / "inc"
    b2s.run(
        spark, str(bronze_root), str(out),
        lookback_hours=3, as_of=utc("2026-09-03T01:00:00"),
    )

    dates = {r.date for r in spark.read.parquet(str(out)).select("date").distinct().collect()}
    assert dates == {"2026-09-02", "2026-09-03"}
    assert "2026-09-01" not in dates


def test_first_bars_of_a_target_date_have_warmed_up_volatility(
    spark, bronze_root, tmp_path
):
    """
    The positive form of the warm-up assertion.

    Midnight on 09-02 is the very first bar of a rewritten date. Its rolling_vol can
    only be non-NULL if the previous day's final 20 minutes were read. This is the
    single most likely thing to break in an incremental redesign, and it fails
    silently - NULL volatility means the Gold layer simply skips those bars, so the
    anomaly table quietly loses its first 20 minutes of every day.
    """
    out = tmp_path / "inc"
    b2s.run(
        spark, str(bronze_root), str(out),
        lookback_hours=3, as_of=utc("2026-09-03T01:00:00"),
    )

    first_bars = (
        spark.read.parquet(str(out))
        .filter(F.col("date") == "2026-09-02")
        .orderBy("open_time")
        .limit(5)
        .collect()
    )

    assert len(first_bars) == 5
    for row in first_bars:
        assert row.log_return is not None, "needs the previous day's closing bar"
        assert row.rolling_vol is not None, "needs 20 minutes of previous-day history"


def test_incremental_reads_less_than_full_reprocess(spark, bronze_root, tmp_path):
    """
    The whole economic point. Incremental must actually read fewer records - if it
    reads the same amount, the lookback is not pruning anything and we have paid for
    complexity with no return.
    """
    full = b2s.run(spark, str(bronze_root), str(tmp_path / "f"), lookback_hours=0)
    inc = b2s.run(
        spark, str(bronze_root), str(tmp_path / "i"),
        lookback_hours=3, as_of=utc("2026-09-03T01:00:00"),
    )

    assert inc["raw_bar_records"] < full["raw_bar_records"]
    assert inc["mode"] == "incremental"
    assert full["mode"] == "full"
