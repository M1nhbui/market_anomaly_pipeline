"""Shared pytest fixtures for local Spark tests."""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "glue_jobs"))


@pytest.fixture(scope="session")
def spark():
    """
    One local SparkSession for the whole test session.

    Session scope matters: creating a SparkSession takes several seconds, so a
    function-scoped fixture would make the suite painfully slow.

    Two settings worth knowing:
      master("local[2]")            - run Spark in this process with 2 worker
                                      threads. Two rather than one so that shuffle
                                      and partitioning behaviour is at least
                                      slightly exercised rather than trivially
                                      single-threaded.
      shuffle.partitions = 2        - Spark's default is 200 shuffle partitions,
                                      which on tiny test data creates 198 empty
                                      tasks and makes every test slow for no reason.
    """
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("bronze_to_silver_tests")
        .config("spark.sql.shuffle.partitions", "2")
        # Must match the production job exactly. Without this the tests would run in
        # the developer's local timezone and pass or fail depending on where the
        # developer happens to be sitting - which is how the timezone bug this line
        # guards against was found in the first place.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def make_bar(open_time_ms, close=100.0, volume=1.0, num_trades=10):
    """
    Build one Binance-shaped positional bar.

    Deliberately mirrors the real payload exactly: every price and volume is a
    STRING, timestamps and trade counts are numbers, and there are 12 elements with
    the last one being the unused 'ignore' field. Tests that use a tidied-up fixture
    do not actually test the parsing you have in production.
    """
    return [
        open_time_ms,
        f"{close:.8f}",
        f"{close + 1:.8f}",
        f"{close - 1:.8f}",
        f"{close:.8f}",
        f"{volume:.8f}",
        open_time_ms + 59999,  # close_time: 1 ms before the next minute
        f"{close * volume:.8f}",
        num_trades,
        f"{volume / 2:.8f}",
        f"{close * volume / 2:.8f}",
        "0",
    ]


def write_bronze_doc(tmp_path, filename, symbol, ingested_at, bars):
    """Write one Bronze document in exactly the shape the ingestion Lambda produces."""
    payload = {
        "symbol": symbol,
        "interval": "1m",
        "source_host": "https://data-api.binance.vision",
        "ingested_at": ingested_at,
        "request_id": "test-request-id",
        "bar_count": len(bars),
        "bars": bars,
    }
    path = tmp_path / filename
    # One line, no indentation - matching json.dumps() in the Lambda. Multi-line JSON
    # would need spark.read.option("multiLine", "true") and is a real source of
    # "why does Spark see zero rows" confusion.
    path.write_text(json.dumps(payload))
    return path
