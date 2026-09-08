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


# Volatility window length, in bars. 20 one-minute bars = 20 minutes of history.
#
# Stated as an explicit constant because it is a MODELLING CHOICE, not a fact.
# Shorter reacts faster and is noisier; longer is smoother and slower to respond.
# README §10 calls these out as "tunable knobs that should be stated explicitly
# rather than hidden," and this is where that promise gets kept.
VOL_WINDOW_BARS = 20
BAR_INTERVAL_SECONDS = 60


def add_derived_features(
    df, vol_window=VOL_WINDOW_BARS, interval_seconds=BAR_INTERVAL_SECONDS
):
    """
    Derive log_return, rolling_vol, dollar_volume, range_pct.

    MUST RUN AFTER DEDUPLICATION. lag() reaches to the physically previous row in
    the window, so if duplicates were still present it would compare a bar against
    another copy of itself and produce a log_return of 0. Every one of these features
    is only meaningful on a deduplicated series.

    TIME-BASED WINDOWS, NOT ROW-BASED
    ---------------------------------
    The obvious implementation uses rowsBetween(-19, 0): "the previous 20 rows."
    That is what most tutorials do and it is subtly wrong.

    A row-based window counts ROWS, not TIME. If a minute is missing, the "trailing
    20 bars" silently span 21 minutes of wall-clock instead of 20 — and nothing tells
    you. The volatility of a 21-minute span gets recorded as a 20-minute figure, and
    downstream comparisons between symbols quietly stop being apples-to-apples.

    rangeBetween operates on the ORDERING COLUMN'S VALUE. Ordering by open_time cast
    to epoch seconds and bounding by -(N-1)*60 gives a genuine 20-MINUTE window. If
    a bar is missing, the window honestly contains 19 observations and we null the
    result rather than pretending.

    On our current data these are identical, because ingestion has zero gaps
    (METRICS.md M-022). That is exactly why it is worth doing now: we can prove the
    two agree today, and the time-based one stays correct when that changes.
    README §19 lists this as a future improvement; it is cheap enough to do in v1.

    THE WARM-UP PERIOD
    ------------------
    Spark will happily compute stddev over 3 observations and return a number. That
    number is not comparable to one computed over 20, and feeding it into a z-score
    produces confident-looking nonsense for the first minutes of every symbol.

    So we count actual observations in the window and return NULL unless the window
    is full. NULL, not 0 — README §10 is explicit that undefined z-scores "must be
    excluded, not treated as zero," and 0 would read as "no volatility," which is the
    single most misleading value available.

    Note the arithmetic: rolling_vol first becomes non-null at the 21st bar, not the
    20th. The first bar has no log_return (no previous close), so 21 closes are
    required to produce 20 returns.
    """
    # lag() ignores the frame, so a plain ordered window is all it needs.
    time_ordered = Window.partitionBy("symbol").orderBy(F.col("open_time").cast("long"))

    previous_close = F.lag(F.col("close")).over(time_ordered)

    # ln(close / previous_close).
    #
    # Log returns rather than simple percentage returns because they are additive
    # over time (the sum of log returns is the log return of the whole period) and
    # roughly symmetric — a +1% and a −1% move have equal and opposite log returns,
    # whereas simple returns do not. Both properties matter for the z-score in Gold.
    #
    # The guards are not paranoia: a zero or negative price would make the logarithm
    # undefined or produce -Infinity, which propagates silently through every
    # downstream aggregate. NULL is loud; -Infinity is not.
    df = df.withColumn(
        "log_return",
        F.when(
            previous_close.isNotNull()
            & (previous_close > 0)
            & (F.col("close") > 0),
            F.log(F.col("close") / previous_close),
        ),
    )

    trailing_window = (
        Window.partitionBy("symbol")
        .orderBy(F.col("open_time").cast("long"))
        .rangeBetween(-(vol_window - 1) * interval_seconds, 0)
    )

    # count() ignores NULLs, so this is the number of REAL return observations in
    # the trailing time span - which is what "is the window full?" actually means.
    observations_in_window = F.count(F.col("log_return")).over(trailing_window)

    # stddev_samp (n-1 denominator), stated explicitly rather than relying on
    # F.stddev's default. With n=20 the sample and population estimators differ by
    # sqrt(20/19) ≈ 2.6% - small, but it would be silently baked into every z-score
    # downstream. Sample is the convention for financial volatility because the
    # window is a sample of the return process, not the entire population.
    df = df.withColumn(
        "rolling_vol",
        F.when(
            observations_in_window >= vol_window,
            F.stddev_samp(F.col("log_return")).over(trailing_window),
        ),
    )

    # close × volume, as specified in README §9.
    #
    # HONEST CAVEAT: this is an APPROXIMATION and Binance already gives us the exact
    # figure. `volume` is base-asset quantity (BTC), so close × volume values every
    # trade in the minute at the CLOSING price. `quote_volume` is Binance's actual
    # sum of price × quantity across the real trades - the true USDT turnover.
    #
    # We compute it anyway for README fidelity and because it is the conventional
    # field name, but quote_volume is strictly better and downstream consumers should
    # prefer it. The size of the discrepancy is measurable in Athena and worth
    # recording rather than assuming.
    df = df.withColumn("dollar_volume", F.col("close") * F.col("volume"))

    # (high − low) / open. Intrabar range as a fraction of the opening price.
    # Guarded because a zero open would divide by zero; NULL is the honest answer.
    df = df.withColumn(
        "range_pct",
        F.when(
            F.col("open") > 0,
            (F.col("high") - F.col("low")) / F.col("open"),
        ),
    )

    return df


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
    """
    The whole Bronze -> Silver pipeline, composed. Pure: DataFrame in, out.

    STAGE ORDER IS LOAD-BEARING, specifically that features come after dedup:

        explode -> cast -> drop unfinished -> DEDUPLICATE -> features -> partition

    Deriving features before deduplication would let lag() compare a bar against a
    duplicate copy of itself, yielding log_return = 0 and a collapsed rolling_vol.
    The job would succeed and the numbers would be wrong.
    """
    return finalize(drop_unfinished_bars(cast_bars(explode_bars(df_bronze))))


def finalize(df_finished):
    """
    The post-filter stages, in one place.

    This exists because run() needs to interleave row counts between the early
    stages, so it cannot simply call transform(). Previously both functions composed
    the stage list independently - and when add_derived_features was introduced, only
    one of them was updated. The job then asked for columns that were never created.

    Any stage ordering that both callers must agree on belongs here, so there is
    exactly one definition of it to keep correct.
    """
    return add_partition_column(add_derived_features(deduplicate(df_finished)))


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
    # Derived (slice 4). Column ORDER here must stay in sync with the column list in
    # terraform/catalog.tf, or rather - it need not, because the table is configured
    # with parquet.column.index.access = false, so Athena matches by name. Adding a
    # column here still requires adding it there, or it simply will not be queryable.
    "log_return",
    "rolling_vol",
    "dollar_volume",
    "range_pct",
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

    silver = finalize(finished).select(*SILVER_COLUMNS)
    silver.cache()
    unique_bars = silver.count()

    # Feature warm-up counts, in one pass rather than three separate count() actions.
    #
    # These are not diagnostics for their own sake. A NULL rolling_vol means "we did
    # not have 20 observations of return history here", and every such row is a row
    # the Gold anomaly detector must skip rather than score. Knowing how many there
    # are is how we confirm the warm-up behaves as designed instead of silently
    # nulling far more than expected.
    null_counts = silver.select(
        F.sum(F.when(F.col("log_return").isNull(), 1).otherwise(0)).alias("lr"),
        F.sum(F.when(F.col("rolling_vol").isNull(), 1).otherwise(0)).alias("rv"),
    ).collect()[0]

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
        "null_log_return": int(null_counts["lr"] or 0),
        "null_rolling_vol": int(null_counts["rv"] or 0),
        "vol_window_bars": VOL_WINDOW_BARS,
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
