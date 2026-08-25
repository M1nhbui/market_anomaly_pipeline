"""
Unit tests for the Bronze -> Silver transform.

These run on local Spark in a few seconds and cost nothing. Every one of them
represents a bug that would otherwise be discovered by staring at Athena output and
wondering why a number looks slightly off.
"""

from conftest import make_bar, write_bronze_doc
from pyspark.sql import functions as F

import bronze_to_silver as b2s


def fmt(df, column, pattern="yyyy-MM-dd HH:mm:ss"):
    """
    Render a timestamp column to a string INSIDE Spark, then collect.

    Do not assert on a collected Python datetime. collect() hands timestamps back as
    naive datetime objects converted to the DRIVER's local timezone, so
    `row.open_time.strftime(...)` returns a different string depending on where the
    developer is sitting - the test would pass in London and fail in Madison while
    the data was identical and correct both times.

    date_format() evaluates under spark.sql.session.timeZone, which conftest pins to
    UTC. That is the value the pipeline actually stores and that Athena will read.
    """
    return [r[0] for r in df.select(F.date_format(column, pattern)).collect()]

# Fixed epochs so tests are deterministic. 1787503260000 = 2026-08-23T16:41:00Z,
# taken from real measured data (METRICS.md M-010).
T_16_41 = 1787503260000
T_16_42 = T_16_41 + 60_000
T_16_43 = T_16_41 + 120_000


def test_positional_array_maps_to_named_typed_columns(spark, tmp_path):
    """Prices arrive as strings and must come out as doubles in the right columns."""
    write_bronze_doc(
        tmp_path,
        "a.json",
        "BTCUSDT",
        "2026-08-23T17:00:00.123456Z",
        [make_bar(T_16_41, close=77385.0, volume=3.8289, num_trades=2390)],
    )

    df = b2s.cast_bars(b2s.explode_bars(b2s.read_bronze(spark, str(tmp_path))))
    row = df.collect()[0]

    assert row.symbol == "BTCUSDT"
    assert row.close == 77385.0
    assert isinstance(row.close, float), "prices must be double, not string"
    assert row.volume == 3.8289
    assert row.num_trades == 2390
    assert isinstance(row.num_trades, int)
    # ms epoch -> timestamp, and it must be the RIGHT minute, in UTC
    assert fmt(df, "open_time") == ["2026-08-23 16:41:00"]


def test_ingested_at_parses_without_microseconds(spark, tmp_path):
    """
    datetime.isoformat() omits the fractional part when microsecond == 0. A format
    string demanding six digits would return NULL here. Rare, and miserable to debug.
    """
    write_bronze_doc(
        tmp_path, "a.json", "BTCUSDT", "2026-08-23T17:00:00Z", [make_bar(T_16_41)]
    )

    df = b2s.cast_bars(b2s.explode_bars(b2s.read_bronze(spark, str(tmp_path))))
    assert df.collect()[0].ingested_at is not None


def test_unfinished_bar_is_dropped(spark, tmp_path):
    """
    The last bar of a klines response is still forming. Its close_time is in the
    future relative to the moment we fetched it, and its volume is partial.
    """
    write_bronze_doc(
        tmp_path,
        "a.json",
        "BTCUSDT",
        # Fetched at 16:42:30 - so the 16:42 bar (closing 16:42:59.999) is unfinished
        "2026-08-23T16:42:30.000000Z",
        [make_bar(T_16_41), make_bar(T_16_42, volume=0.1, num_trades=79)],
    )

    df = b2s.transform(b2s.read_bronze(spark, str(tmp_path)))

    assert df.count() == 1, "the in-progress bar should not survive"
    assert fmt(df, "open_time", "HH:mm") == ["16:41"]


def test_duplicate_bar_keeps_most_recently_ingested_version(spark, tmp_path):
    """
    THE test. The same bar arrives twice with different values: first partial, then
    complete. Keeping an arbitrary copy - what dropDuplicates() does - would leave
    roughly one bar in fifteen permanently and non-deterministically wrong.
    """
    # Run 1 at 16:42:30 saw the 16:42 bar mid-formation: volume 0.1, 79 trades.
    write_bronze_doc(
        tmp_path,
        "run1.json",
        "BTCUSDT",
        "2026-08-23T16:42:30.000000Z",
        [make_bar(T_16_42, close=100.0, volume=0.1, num_trades=79)],
    )
    # Run 2 at 16:47:30 saw the same bar completed: volume 3.5, 421 trades.
    write_bronze_doc(
        tmp_path,
        "run2.json",
        "BTCUSDT",
        "2026-08-23T16:47:30.000000Z",
        [make_bar(T_16_42, close=100.0, volume=3.5, num_trades=421)],
    )

    df = b2s.transform(b2s.read_bronze(spark, str(tmp_path)))
    rows = df.collect()

    assert len(rows) == 1, "one bar, one row"
    assert rows[0].volume == 3.5, "must keep the COMPLETED version, not the partial"
    assert rows[0].num_trades == 421


def test_dedup_is_per_symbol(spark, tmp_path):
    """
    Two symbols sharing a timestamp are different bars. If the dedup key were
    open_time alone, one of them would vanish.
    """
    write_bronze_doc(
        tmp_path, "btc.json", "BTCUSDT", "2026-08-23T17:00:00.000000Z",
        [make_bar(T_16_41, close=77385.0)],
    )
    write_bronze_doc(
        tmp_path, "eth.json", "ETHUSDT", "2026-08-23T17:00:00.000000Z",
        [make_bar(T_16_41, close=2400.0)],
    )

    df = b2s.transform(b2s.read_bronze(spark, str(tmp_path)))
    symbols = {r.symbol for r in df.collect()}

    assert symbols == {"BTCUSDT", "ETHUSDT"}
    assert df.count() == 2


def test_partition_date_comes_from_bar_time_not_ingest_time(spark, tmp_path):
    """
    A bar from 23:59 fetched at 00:01 the next day belongs to the EARLIER date.
    Partitioning on ingestion time would file it under the wrong day and make every
    date-filtered query subtly wrong at midnight.
    """
    t_2359 = 1787529540000  # 2026-08-23T23:59:00Z
    write_bronze_doc(
        tmp_path, "a.json", "BTCUSDT",
        "2026-08-24T00:01:00.000000Z",  # ingested the NEXT day
        [make_bar(t_2359)],
    )

    df = b2s.transform(b2s.read_bronze(spark, str(tmp_path)))
    assert df.collect()[0].date == "2026-08-23"


def test_run_reports_duplicate_statistics(spark, tmp_path):
    """The counts that feed METRICS.md must actually be correct."""
    out = tmp_path / "silver"
    src = tmp_path / "bronze"
    src.mkdir()

    # Three runs, 5 minutes apart, each fetching 3 bars with heavy overlap.
    write_bronze_doc(src, "r1.json", "BTCUSDT", "2026-08-23T16:44:30.000000Z",
                     [make_bar(T_16_41), make_bar(T_16_42), make_bar(T_16_43)])
    write_bronze_doc(src, "r2.json", "BTCUSDT", "2026-08-23T16:49:30.000000Z",
                     [make_bar(T_16_41), make_bar(T_16_42), make_bar(T_16_43)])
    write_bronze_doc(src, "r3.json", "BTCUSDT", "2026-08-23T16:54:30.000000Z",
                     [make_bar(T_16_41), make_bar(T_16_42), make_bar(T_16_43)])

    stats = b2s.run(spark, str(src), str(out))

    assert stats["raw_bar_records"] == 9
    assert stats["unfinished_dropped"] == 0  # all bars long closed by 16:44:30
    assert stats["unique_bars_written"] == 3
    assert stats["duplicates_removed"] == 6
    assert stats["duplicate_rate_pct"] == 66.67


def test_written_parquet_is_readable_and_partitioned(spark, tmp_path):
    """End to end: the job's output must actually be loadable."""
    src = tmp_path / "bronze"
    src.mkdir()
    out = tmp_path / "silver"

    write_bronze_doc(src, "a.json", "BTCUSDT", "2026-08-23T17:00:00.000000Z",
                     [make_bar(T_16_41), make_bar(T_16_42)])

    b2s.run(spark, str(src), str(out))

    reread = spark.read.parquet(str(out))
    assert reread.count() == 2
    # symbol and date are partition columns, recovered from the directory layout
    assert "symbol" in reread.columns
    assert "date" in reread.columns
    assert reread.select("symbol").distinct().collect()[0].symbol == "BTCUSDT"


def test_exactly_one_parquet_file_per_partition_directory(spark, tmp_path):
    """
    Regression test for the small-file problem.

    write.partitionBy() emits one file per (spark partition x output directory), so
    without repartition("symbol","date") the file count multiplies. Measured at
    17 symbols x 7 days: 476 files instead of 119, ~9.5% wasted on repeated Parquet
    footers, and 476 S3 GETs per Athena scan instead of 119.

    Asserting on the LAYOUT rather than just the row count is the point - the data
    was never wrong in that scenario, only expensively arranged, which is exactly
    the kind of thing that survives review and shows up later as a query bill.
    """
    src = tmp_path / "bronze"
    src.mkdir()
    out = tmp_path / "silver"

    # 3 symbols x 2 dates = 6 output directories.
    t_2359 = 1787529540000  # 2026-08-23T23:59:00Z
    t_0000 = t_2359 + 60_000  # rolls into 2026-08-24
    for sym in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        write_bronze_doc(
            src, f"{sym}.json", sym, "2026-08-24T00:30:00.000000Z",
            [make_bar(t_2359), make_bar(t_0000)],
        )

    b2s.run(spark, str(src), str(out))

    parquet_files = list(out.rglob("*.parquet"))
    partition_dirs = {f.parent for f in parquet_files}

    assert len(partition_dirs) == 6, "expected 3 symbols x 2 dates of directories"
    assert len(parquet_files) == 6, (
        f"expected exactly one file per partition directory, got "
        f"{len(parquet_files)} files across {len(partition_dirs)} directories"
    )
