"""
Silver -> Gold: anomaly_events.

Produces a per-symbol, self-relative statistical filter: for each bar, how unusual is
this observation compared to that symbol's OWN recent behaviour?

WHAT THIS IS AND IS NOT
-----------------------
This is a FILTER, not a trading signal. A z-score spike catches news events, flash
moves, and data glitches indiscriminately. No claim is made that these events are
profitable or predictive - that is a separate research question this project does not
attempt to answer. The defensible claim is narrower: the infrastructure and a
first-pass statistical filter are built and measured.

Two limitations are inherent to the method and are NOT bugs to be fixed later:

  1. The mean/standard-deviation estimator is itself sensitive to the very outliers
     it is trying to detect. One large move inflates the baseline, making the NEXT
     large move look less unusual. A median/MAD estimator would be more robust
     (README §19).
  2. A global threshold conflates genuine market events with data-quality artifacts
     and scheduled volatility (funding-rate resets, session opens).

Both are stated rather than hidden, because a filter whose failure modes are known is
more useful than one whose aren't.

WHY THIS JOB READS ALL OF SILVER RATHER THAN A LOOKBACK
-------------------------------------------------------
Bronze->Silver needed incremental processing because Bronze is JSON, one small object
every 5 minutes per symbol - 4,688 objects and growing by ~4,900/day at 17 symbols.

Silver is Parquet: ~35,000 rows in a few dozen files, roughly 2 MB. Reading all of it
is cheap, and it means the z-score window always has complete history with no
boundary handling at all. Complexity should follow cost, and here there is none to
justify it. The crossover is around a year of accumulation (17 symbols x 365 days
= ~6,200 files); that is when this decision needs revisiting, and it is written down
here so the revisit is deliberate rather than a surprise.
"""

import argparse
import json
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# -----------------------------------------------------------------------------
# Tunable knobs.
#
# README §10 says these "should be stated explicitly rather than hidden". They are
# constants here, overridable as job arguments, AND written onto every emitted row -
# so an event produced under k=3 is distinguishable from one produced under k=4
# without consulting a changelog.
# -----------------------------------------------------------------------------

ZSCORE_WINDOW_BARS = 60
ZSCORE_THRESHOLD_K = 3.0
BAR_INTERVAL_SECONDS = 60

# Minimum trailing standard deviation for a z-score to be considered defined.
#
# THIS GUARD IS NOT THEORETICAL. Untraded minutes leave the close price unchanged,
# so log_return is exactly 0. SHIBUSDT has ~1.9% such bars (METRICS.md M-039). A run
# of them drives the trailing standard deviation toward zero, and then
# (x - mean) / std explodes - a symbol that barely trades would dominate the anomaly
# table with enormous z-scores generated purely by its own illiquidity.
#
# 1e-6 in log-return space is 0.0001% - a window over which the asset effectively did
# not move at all. Below that there is no meaningful "normal" to measure against, so
# the honest answer is NULL rather than a large number. This is exactly the
# "conflates genuine events with data-quality glitches" failure README §4 names.
MIN_ROLLING_STD = 1e-6


def zscore_frame(window_bars, interval_seconds):
    """
    A trailing window that EXCLUDES the current bar.

    rangeBetween(-window*60, -60) covers the `window` bars immediately before this
    one. The current observation is deliberately left out of its own baseline.

    Why: the question is "is this bar unusual relative to what came before it?" If
    the current value contributed to its own mean and standard deviation, an extreme
    observation would inflate the very baseline it is being compared against, pulling
    its own z-score down. The effect is largest for exactly the events we most want
    to catch. Excluding it is both more interpretable and more sensitive.

    Time-based (rangeBetween) rather than row-based (rowsBetween) for the same reason
    as Silver's rolling_vol: with a gap in the series, a row-based window silently
    spans more wall-clock time than it claims.
    """
    return (
        Window.partitionBy("symbol")
        .orderBy(F.col("open_time").cast("long"))
        .rangeBetween(-window_bars * interval_seconds, -interval_seconds)
    )


def score(
    df,
    value_column,
    rule_name,
    transform_name,
    window_bars,
    k,
    interval_seconds,
    direction="both",
):
    """
    Compute trailing z-scores for one value column and return the rows that breach k.

    `direction` selects which tail counts as an event:
      "both"  - abs(z) > k. Used for returns: a crash is as anomalous as a spike.
      "upper" -     z > k. Used for volume.

    WHY VOLUME IS ONE-SIDED (changed after measuring the output - METRICS.md M-043)
    ------------------------------------------------------------------------------
    The first version tested abs(z) > k for volume too. Three of the top fifteen
    events by magnitude came back as SHIBUSDT bars with num_trades = 0 and z around
    -9: untraded minutes flagged as "anomalously LOW volume".

    The arithmetic was right and the result was useless. An absence of trading is not
    an event a human should investigate, and the least liquid symbol was crowding the
    top of the table with its own illiquidity.

    README §10's wording is "to catch volume spikes" - the upper tail is what was
    intended. The lower tail is already covered more appropriately by the DQ gate's
    empty-bar check, which treats untraded minutes as a data-quality ratio rather
    than a market event.

    What this gives up, stated plainly: a genuine volume COLLAPSE - a trading halt or
    an exchange issue - will no longer be flagged by this rule.

    Returns the full anomaly_events schema so the two rules can be unioned.
    """
    frame = zscore_frame(window_bars, interval_seconds)

    observations = F.count(F.col(value_column)).over(frame)
    rolling_mean = F.avg(F.col(value_column)).over(frame)
    rolling_std = F.stddev_samp(F.col(value_column)).over(frame)

    scored = (
        df.withColumn("_observations", observations)
        .withColumn("_mean", rolling_mean)
        .withColumn("_std", rolling_std)
        .withColumn(
            "_zscore",
            F.when(
                # All three conditions must hold for a z-score to be meaningful:
                #   - the window is full (a partial window is not comparable)
                #   - the baseline is not degenerate (see MIN_ROLLING_STD)
                #   - this bar actually has a value to score
                (F.col("_observations") >= window_bars)
                & (F.col("_std") > MIN_ROLLING_STD)
                & F.col(value_column).isNotNull(),
                (F.col(value_column) - F.col("_mean")) / F.col("_std"),
            ),
        )
    )

    breach = (
        F.abs(F.col("_zscore")) > k if direction == "both" else F.col("_zscore") > k
    )

    return (
        scored.filter(breach)
        .select(
            F.col("symbol"),
            F.col("open_time").alias("event_time"),
            F.lit(rule_name).alias("rule"),
            F.lit(direction).alias("rule_direction"),
            F.col(value_column).alias("observed_value"),
            F.lit(transform_name).alias("observed_transform"),
            F.col("_mean").alias("rolling_mean"),
            F.col("_std").alias("rolling_std"),
            F.col("_zscore").alias("zscore"),
            F.col("close").alias("close_price"),
            F.col("volume").alias("volume"),
            F.col("num_trades").alias("num_trades"),
            F.lit(window_bars).cast("int").alias("zscore_window_bars"),
            F.lit(float(k)).alias("zscore_threshold"),
            F.col("date"),
        )
    )


def prepare(df):
    """
    Add the transformed volume column the volume rule scores against.

    WHY log1p(volume) AND NOT RAW VOLUME
    ------------------------------------
    A z-score assumes something roughly symmetric around a mean. Volume is neither:
    it is bounded below at zero and heavily right-skewed, so mean - 3*sigma is
    routinely NEGATIVE - a lower bound no observation can ever reach. Half the test
    is meaningless, and the upper tail is mis-scaled.

    log1p (that is, ln(1+x)) compresses the right tail into something far closer to
    symmetric, and unlike plain log it is defined at zero - which matters, because
    untraded minutes have volume exactly 0 and ln(0) is -infinity.

    README §10 says to run "the same procedure on volume". This is a deliberate
    deviation: the same procedure on untransformed volume would be statistically
    unsound, and saying so is more defensible than reproducing it.
    """
    return df.withColumn("log_volume", F.log1p(F.col("volume")))


def build_anomaly_events(
    df_silver,
    window_bars=ZSCORE_WINDOW_BARS,
    k=ZSCORE_THRESHOLD_K,
    interval_seconds=BAR_INTERVAL_SECONDS,
):
    """Both rules, unioned. Pure: DataFrame in, DataFrame out."""
    prepared = prepare(df_silver)

    returns = score(
        prepared, "log_return", "return_zscore", "none",
        window_bars, k, interval_seconds, direction="both",
    )
    volumes = score(
        prepared, "log_volume", "volume_zscore", "log1p",
        window_bars, k, interval_seconds, direction="upper",
    )

    return returns.unionByName(volumes)


GOLD_COLUMNS = [
    "symbol",
    "event_time",
    "rule",
    "rule_direction",
    "observed_value",
    "observed_transform",
    "rolling_mean",
    "rolling_std",
    "zscore",
    "close_price",
    "volume",
    "num_trades",
    "zscore_window_bars",
    "zscore_threshold",
    "date",
]


def run(spark, silver_path, gold_path, window_bars=ZSCORE_WINDOW_BARS,
        k=ZSCORE_THRESHOLD_K):
    silver = spark.read.parquet(silver_path)
    silver.cache()

    total_bars = silver.count()
    scorable_bars = silver.filter(F.col("log_return").isNotNull()).count()

    events = build_anomaly_events(silver, window_bars, k).select(*GOLD_COLUMNS)
    events.cache()

    total_events = events.count()
    per_rule = {
        row["rule"]: row["n"]
        for row in events.groupBy("rule").agg(F.count("*").alias("n")).collect()
    }

    stats = {
        "total_bars": total_bars,
        "scorable_bars": scorable_bars,
        "total_events": total_events,
        "events_by_rule": per_rule,
        # The headline number. README §2 describes a "short, ranked list"; M-030
        # predicts this will be far above the ~0.27% that normal theory implies,
        # because minute-level crypto returns are heavily fat-tailed. Measuring it
        # rather than assuming it is the whole point.
        "firing_rate_pct": (
            round(100.0 * total_events / (2 * scorable_bars), 4)
            if scorable_bars
            else 0.0
        ),
        "zscore_window_bars": window_bars,
        "zscore_threshold": k,
        "min_rolling_std": MIN_ROLLING_STD,
    }

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # Partitioned by date only, not (symbol, date).
    #
    # anomaly_events is SPARSE - a few hundred rows a day across all symbols, versus
    # Silver's ~24,000. Adding symbol to the partition key would create 17 files a
    # day of a few KB each, and the per-file overhead would exceed anything partition
    # pruning could save. Partition granularity should follow row counts, not habit.
    (
        events.repartition("date")
        .write.mode("overwrite")
        .partitionBy("date")
        .parquet(gold_path)
    )

    return stats


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--gold-path", required=True)
    parser.add_argument("--zscore-window-bars", type=int, default=ZSCORE_WINDOW_BARS)
    parser.add_argument("--zscore-threshold", type=float, default=ZSCORE_THRESHOLD_K)
    known, _unknown = parser.parse_known_args(argv)
    return known


def main(argv=None):
    args = parse_args(argv if argv is not None else sys.argv[1:])

    spark = (
        SparkSession.builder.appName("silver_to_gold")
        # Identical to bronze_to_silver. Both settings exist for reasons documented
        # there; the important thing is that the two jobs agree, since one reads what
        # the other wrote.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .getOrCreate()
    )

    stats = run(
        spark,
        args.silver_path,
        args.gold_path,
        window_bars=args.zscore_window_bars,
        k=args.zscore_threshold,
    )
    stats["generated_at"] = datetime.now(timezone.utc).isoformat()

    print("SILVER_TO_GOLD_STATS " + json.dumps(stats))
    spark.stop()
    return stats


if __name__ == "__main__":
    main()
