"""
Bronze -> Silver: parse, cast, drop unfinished bars, deduplicate.

SCOPE: this job does NOT compute derived features (log_return, rolling_vol, etc).
Those arrive in slice 4. Splitting them apart is deliberate - it means a failure here
is unambiguously a parsing/dedup problem, not a window-function problem.

WHY PLAIN PYSPARK AND NOT GLUE'S DynamicFrame API
-------------------------------------------------
AWS Glue offers its own DataFrame-like abstraction (GlueContext, DynamicFrame) with
features like job bookmarks and schema-flexible "choice" types. We use plain Spark
DataFrames instead:

  - Portability. This file runs unchanged on local Spark, EMR, Databricks, or any
    open-source Spark. DynamicFrame code runs only on Glue.
  - Testability. Because there is no `awsglue` import, every function below can be
    imported and unit-tested on a laptop. That is what makes the local dev loop in
    tests/ possible, which is worth far more than bookmarks at this scale.
  - Transferable skill. `Window.partitionBy(...).orderBy(...)` is the Spark API you
    would use anywhere. DynamicFrame is Glue trivia.

What we give up: Glue job bookmarks (automatic "only process new files" tracking).
We handle incrementality explicitly with a lookback window instead, which is more
code but has behaviour we can actually reason about and test.
"""

import argparse
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# -----------------------------------------------------------------------------
# Schema
# -----------------------------------------------------------------------------

# An EXPLICIT schema, never inferred.
#
# spark.read.json() will happily infer a schema by scanning the data, and that is a
# trap in a pipeline: inference costs an extra full pass over the input, and worse,
# the inferred types depend on the data it happens to see. A column that is all
# integers today infers as long; tomorrow one string value arrives and it infers as
# string, and a downstream cast silently starts producing nulls. Declaring the schema
# makes the job deterministic and makes schema drift a loud failure instead of a
# quiet one.
#
# Note `bars` is array<array<string>>: we read EVERY positional field as a string,
# including the epoch integers, and cast explicitly below. Binance sends prices as
# strings and timestamps as numbers; normalising everything to string on read means
# there is exactly one place where types are decided, and it is our code.
BRONZE_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("interval", StringType(), nullable=True),
        StructField("source_host", StringType(), nullable=True),
        StructField("ingested_at", StringType(), nullable=False),
        StructField("request_id", StringType(), nullable=True),
        StructField("bar_count", IntegerType(), nullable=True),
        StructField("bars", ArrayType(ArrayType(StringType())), nullable=True),
    ]
)

# Binance klines are positional arrays with no field names. This mapping IS the
# Bronze->Silver contract; if Binance ever reorders these, everything downstream is
# silently wrong, which is why the DQ gate's OHLC sanity checks matter.
BAR_OPEN_TIME = 0
BAR_OPEN = 1
BAR_HIGH = 2
BAR_LOW = 3
BAR_CLOSE = 4
BAR_VOLUME = 5
BAR_CLOSE_TIME = 6
BAR_QUOTE_VOLUME = 7
BAR_NUM_TRADES = 8
BAR_TAKER_BUY_BASE = 9
BAR_TAKER_BUY_QUOTE = 10
# index 11 is Binance's unused "ignore" field - deliberately not carried forward


# -----------------------------------------------------------------------------
# Transform stages
#
# Each stage is a pure DataFrame -> DataFrame function so it can be tested in
# isolation. transform() composes them.
# -----------------------------------------------------------------------------


def read_bronze(spark, bronze_path):
    """
    Read Bronze JSON.

    recursiveFileLookup=true is important and non-obvious. Bronze paths are
    Hive-style (symbol=BTCUSDT/date=.../hour=...), and Spark would normally treat
    those as partition columns and append them to the DataFrame. That would collide
    with the `symbol` field that already exists INSIDE each JSON document.

    Disabling partition discovery means the only `symbol` we get is the one the
    Lambda wrote into the payload, which is the authoritative one. It also
    sidesteps the Bronze-partitioned-by-ingest-time vs Silver-partitioned-by-bar-time
    mismatch entirely: we never read the path, only the contents.
    """
    return (
        spark.read.option("recursiveFileLookup", "true")
        .schema(BRONZE_SCHEMA)
        .json(bronze_path)
    )


def explode_bars(df):
    """
    One Bronze document holds many bars. explode() turns each element of the `bars`
    array into its own row, carrying the document-level metadata along.

    15 bars per document means 15 output rows per input row. This is the step that
    turns "22 files" into "330 bar records".
    """
    return df.select(
        F.col("symbol"),
        F.col("source_host"),
        F.col("ingested_at").alias("ingested_at_raw"),
        F.explode(F.col("bars")).alias("bar"),
    )


def cast_bars(df):
    """
    Map positional array -> named, typed columns.

    On ingested_at: we truncate to whole seconds before parsing, rather than parsing
    the full ISO-8601 string with microseconds.

    Why: Python's datetime.isoformat() omits the fractional part entirely when
    microsecond happens to be exactly 0. A format string expecting six fractional
    digits would return NULL for those rows - a once-in-a-million bug that would be
    miserable to find. Truncating to the first 19 characters ("2026-08-23T16:55:51")
    is deterministic regardless. Sub-second precision buys us nothing here:
    ingested_at is used to order duplicate versions of a bar (minutes apart) and to
    compare against close_time (where one second of granularity is ample).
    """
    bar = F.col("bar")

    return df.select(
        F.col("symbol"),
        F.col("source_host"),
        F.to_timestamp(
            F.substring(F.col("ingested_at_raw"), 1, 19), "yyyy-MM-dd'T'HH:mm:ss"
        ).alias("ingested_at"),
        F.timestamp_seconds(bar[BAR_OPEN_TIME].cast("long") / 1000.0).alias("open_time"),
        F.timestamp_seconds(bar[BAR_CLOSE_TIME].cast("long") / 1000.0).alias(
            "close_time"
        ),
        bar[BAR_OPEN].cast("double").alias("open"),
        bar[BAR_HIGH].cast("double").alias("high"),
        bar[BAR_LOW].cast("double").alias("low"),
        bar[BAR_CLOSE].cast("double").alias("close"),
        bar[BAR_VOLUME].cast("double").alias("volume"),
        bar[BAR_QUOTE_VOLUME].cast("double").alias("quote_volume"),
        bar[BAR_NUM_TRADES].cast("int").alias("num_trades"),
        bar[BAR_TAKER_BUY_BASE].cast("double").alias("taker_buy_base"),
    )


def drop_unfinished_bars(df):
    """
    Binance always returns the currently-forming candle as the last element of a
    klines response. Its close, volume, and num_trades are partial and WILL be
    revised. Measured in our own data (METRICS.md M-010): a bar whose minute still
    had 8.5 seconds to run, carrying volume 1.94609 and 525 trades - values that are
    simply wrong as a record of that minute.

    Storing those is not a cosmetic problem. An unfinished bar looks exactly like a
    low-volume bar, which is precisely what the Gold layer's volume z-score is built
    to flag. Ingesting them naively means manufacturing your own false anomalies.

    THE COMPARISON DIRECTION MATTERS. We keep a bar only when its close_time is
    strictly before the moment we fetched it. Because ingested_at was truncated DOWN
    to the second, this errs toward dropping borderline bars. That is the safe
    direction: a bar dropped here is re-fetched by the next run anyway (the 15-bar
    overlap guarantees it) and admitted once it is genuinely complete. A partial bar
    wrongly kept is corrupt data that survives into analytics.

    CONSEQUENCE WORTH KNOWING: the newest bar is intentionally absent from Silver
    until the following ingestion cycle. Silver's max(open_time) therefore lags
    wall-clock by one to two minutes BY DESIGN. The DQ gate's freshness check must
    be set with this in mind, or it will fail every single run.
    """
    return df.filter(F.col("close_time") < F.col("ingested_at"))


def deduplicate(df):
    """
    Collapse to one row per (symbol, open_time), keeping the most recently ingested
    version.

    WINDOW FUNCTIONS, THE MENTAL MODEL
    ----------------------------------
    A window function computes a value for each row by looking at a GROUP of related
    rows - without collapsing them the way GROUP BY does. Three parts:

      partitionBy  - split rows into independent groups. Nothing in one group can
                     ever affect another. Here: each (symbol, open_time) pair is its
                     own little group containing every version of that one bar.
      orderBy      - sort within each group. Here: newest ingestion first.
      the function - what to compute. Here: row_number(), which labels rows 1, 2,
                     3... in that sort order.

    So: group all copies of "BTCUSDT at 16:41:00", sort them newest-first, number
    them, keep number 1. That is "keep the freshest version of each bar".

    WHY ORDER BY ingested_at DESC AND NOT ANYTHING ELSE
    ----------------------------------------------------
    This is the load-bearing detail. The SAME bar can legitimately arrive with
    DIFFERENT values: once as an unfinished candle, later as a completed one. Keeping
    an arbitrary copy - which is what dropDuplicates() would do - means roughly one
    bar in fifteen is permanently wrong, non-deterministically.

    (drop_unfinished_bars runs before this, so most partial copies are already gone.
    Ordering by ingested_at desc is the belt-and-braces guarantee: even if the
    unfinished filter missed an edge case, the freshest observation still wins.)

    ORDERING THE STAGES
    -------------------
    Filtering before deduplicating is cheaper - fewer rows enter the shuffle that
    partitionBy requires - and clearer in intent. Both orders happen to produce the
    same result here, which is a property worth knowing rather than relying on.

    COST NOTE: partitionBy forces a shuffle, moving every row across the cluster so
    that all rows sharing a key land on the same executor. Shuffles are the expensive
    operation in Spark. At our volume it is irrelevant; at a billion rows the choice
    of partition key is the single biggest performance decision in the job.
    """
    version_window = Window.partitionBy("symbol", "open_time").orderBy(
        F.col("ingested_at").desc()
    )

    return (
        df.withColumn("_version_rank", F.row_number().over(version_window))
        .filter(F.col("_version_rank") == 1)
        .drop("_version_rank")
    )


def add_partition_column(df):
    """
    Silver partitions by (symbol, date) where date comes from the BAR's open_time -
    not from ingestion time, and not from the Bronze path.

    Why bar time: queries ask "what happened on 2026-08-23", not "what did we happen
    to download on 2026-08-23". Partitioning on the column people filter by is what
    lets Athena skip files instead of scanning them.

    Why no `hour` partition: 1440 bars/symbol/day split 24 ways would be ~60 rows per
    Parquet file. Many tiny files cost more in per-file open overhead than the
    partition pruning saves. Day-level granularity keeps files usefully sized.
    """
    return df.withColumn("date", F.date_format(F.col("open_time"), "yyyy-MM-dd"))


def transform(df_bronze):
    """The whole Bronze -> Silver pipeline, composed. Pure: DataFrame in, out."""
    return add_partition_column(
        deduplicate(drop_unfinished_bars(cast_bars(explode_bars(df_bronze))))
    )


# -----------------------------------------------------------------------------
# Job entry point
# -----------------------------------------------------------------------------

SILVER_COLUMNS = [
    "symbol",
    "open_time",
    "close_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "num_trades",
    "taker_buy_base",
    "ingested_at",
    "source_host",
    "date",
]


def run(spark, bronze_path, silver_path):
    """
    Execute the job and return the row counts collected along the way.

    The counts exist for METRICS.md. Each count() is a separate Spark ACTION,
    meaning a full pass over the data - so this is three extra passes we would not
    otherwise do. That is a deliberate trade: at 330 rows it is free, and it is the
    only way to ever state a measured duplicate rate. On a large job you would cache
    the intermediate DataFrame first, or sample, or emit the counts as Spark
    accumulators during the single pass you were doing anyway.
    """
    bronze = read_bronze(spark, bronze_path)

    exploded = cast_bars(explode_bars(bronze))
    exploded.cache()  # counted twice below; cache avoids re-reading S3
    raw_bar_records = exploded.count()

    finished = drop_unfinished_bars(exploded)
    finished.cache()
    finished_records = finished.count()

    silver = add_partition_column(deduplicate(finished)).select(*SILVER_COLUMNS)
    silver.cache()
    unique_bars = silver.count()

    stats = {
        "raw_bar_records": raw_bar_records,
        "unfinished_dropped": raw_bar_records - finished_records,
        "finished_records": finished_records,
        "unique_bars_written": unique_bars,
        "duplicates_removed": finished_records - unique_bars,
        "duplicate_rate_pct": (
            round(100.0 * (finished_records - unique_bars) / finished_records, 2)
            if finished_records
            else 0.0
        ),
    }

    # Dynamic partition overwrite: replace only the (symbol, date) partitions this
    # run actually produced, leaving every other partition untouched.
    #
    # The default, "static", would delete EVERYTHING under silver_path and write only
    # what this run computed - so a job reading one hour of Bronze would silently
    # destroy all history. That is a genuinely dangerous default and the reason this
    # line exists.
    #
    # This is also the honest limitation that Apache Iceberg would fix properly
    # (README section 19): partition overwrite is not atomic. A job that dies
    # mid-write leaves a partition partially replaced. Iceberg's MERGE gives real
    # transactional semantics. Deferred on purpose - v1 is plain Parquet.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # THE SMALL-FILE PROBLEM, and why repartition() rather than coalesce().
    #
    # write.partitionBy() makes each Spark partition emit its own file in EVERY
    # output directory it happens to contain rows for. So the file count is
    # (spark partitions) x (partition directories touched), not one per directory.
    #
    # Measured at 17 symbols x 7 days = 119 output directories:
    #
    #   as written, AQE on     476 files    4,674 KiB    avg  9.8 KiB
    #   repartition/coalesce   119 files    4,267 KiB    avg 35.9 KiB
    #   as written, AQE off  23,776 files   33,056 KiB   avg  1.4 KiB
    #
    # Two things in that table. First, ~9.5% of Silver's bytes at 476 files were
    # pure per-file Parquet overhead - every file repeats the schema, footer, and
    # dictionaries. The AQE-off row shows where this ends up if nothing restrains
    # it: 7.8x storage bloat and 23,776 objects for 4 MB of actual data. Athena
    # pays that cost again on every query as per-file open requests.
    #
    # Second, Adaptive Query Execution is already doing heavy lifting (476 rather
    # than 23,776) by collapsing the 200 default shuffle partitions down to 4. It is
    # on by default in Spark 3.2+. Do not disable it.
    #
    # WHY NOT coalesce(1), which measured identically here:
    #   coalesce(1) funnels every row through a SINGLE task. At this volume that is
    #   free, but it does not scale - one executor would eventually write the entire
    #   dataset alone, serialising the write and risking OOM as history accumulates.
    #   It also cannot be undone by adding workers.
    #
    #   repartition("symbol", "date") hash-partitions on exactly the columns we
    #   write by, so every (symbol, date) lands in exactly one Spark partition and
    #   therefore produces exactly one file - while different directories are still
    #   written in parallel across all executors. Same result today, correct
    #   behaviour at 17 symbols x 365 days.
    #
    # The cost is one extra shuffle. Negligible here, and worth it for an output
    # layout that stays sane as the dataset grows.
    (
        silver.repartition("symbol", "date")
        .write.mode("overwrite")
        .partitionBy("symbol", "date")
        .parquet(silver_path)
    )

    return stats


def parse_args(argv):
    """
    Plain argparse, not awsglue.utils.getResolvedOptions.

    Glue passes job arguments as `--KEY value` on sys.argv, which argparse handles
    natively. Avoiding the awsglue import is what keeps this file importable - and
    therefore testable - on a laptop with nothing but pyspark installed.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--silver-path", required=True)
    # Glue injects --JOB_NAME and friends; ignore anything we did not declare.
    known, _unknown = parser.parse_known_args(argv)
    return known


def main(argv=None):
    args = parse_args(argv if argv is not None else sys.argv[1:])

    spark = (
        SparkSession.builder.appName("bronze_to_silver")
        # ---------------------------------------------------------------------
        # THE MOST IMPORTANT LINE IN THIS FILE.
        #
        # Spark's session timezone defaults to the HOST machine's timezone. Two
        # things in cast_bars() then disagree:
        #
        #   to_timestamp("2026-08-23T16:42:30")  -> interpreted in session tz
        #   timestamp_seconds(1787503379999/1000) -> an absolute instant
        #
        # On a laptop in America/Chicago those differ by five hours, so
        # `close_time < ingested_at` compares two different clocks and the
        # unfinished-bar filter silently stops working. Caught by the local test
        # suite; it would NOT have been caught on Glue, whose containers run UTC,
        # which is the worst kind of bug - correct in one environment, wrong in
        # another, with no error either way.
        #
        # Binance epochs are UTC, the Lambda writes UTC, the partitions are UTC.
        # Pinning the session to UTC makes that true end to end instead of
        # accidentally true.
        .config("spark.sql.session.timeZone", "UTC")
        # ---------------------------------------------------------------------
        # Parquet timestamps: Spark's default is INT96 for legacy Hive/Impala
        # compatibility, which Athena reads but which is a deprecated format.
        # TIMESTAMP_MICROS is the modern, portable choice.
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .getOrCreate()
    )

    stats = run(spark, args.bronze_path, args.silver_path)

    # Printed as one line of JSON so it is greppable in CloudWatch logs.
    import json

    print("BRONZE_TO_SILVER_STATS " + json.dumps(stats))

    spark.stop()
    return stats


if __name__ == "__main__":
    main()
