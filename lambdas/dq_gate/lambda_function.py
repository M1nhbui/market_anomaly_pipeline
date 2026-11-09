"""
Data-quality gate: query Silver via Athena, evaluate checks, return quality_passed.

This is the circuit breaker. A Step Functions Choice state reads `quality_passed` and
refuses to run Silver->Gold when it is false, so bad data never reaches the analytics
layer or the anomaly detector.

THREE DESIGN DECISIONS THAT DIFFER FROM README §11
--------------------------------------------------

1. CHECKS ARE RATIOS WITH TOLERANCES, NOT BOOLEANS.
   README §11 says to "flag num_trades = 0". Measured reality (METRICS.md M-039):
   SHIBUSDT has 1.89% untraded minutes, ATOMUSDT 0.63%, ADAUSDT 0.16%. A hard fail on
   any empty bar would trip on roughly one bar in fifty for SHIB - every single run,
   forever. A gate that always fires is a gate everyone learns to ignore, which is
   strictly worse than no gate at all.

2. TWO SEVERITIES, NOT ONE.
   FAIL means the data is corrupt or the feed has stalled - do not build analytics on
   it. WARN means the data is degraded but usable - worth surfacing, not worth
   blocking an hourly pipeline over. Only FAIL affects quality_passed.

   The split follows from what each check actually detects. An OHLC violation means
   either upstream corruption or a positional-mapping bug in our own cast: analytics
   built on it would be wrong. An elevated empty-bar ratio on an illiquid pair on a
   quiet Sunday means exactly that, and nothing downstream is harmed by it.

3. EVERY RUN'S FULL RESULT IS PERSISTED TO S3, one JSON line per check.
   If this Lambda only returned a boolean, "how many runs passed last week" and "did
   the empty-bar check ever actually fire" would be unanswerable a day later. The
   results are written as JSON Lines under a Hive-style date prefix so they are
   queryable in Athena as a table - DQ history becomes SQL rather than log archaeology.

WHY ATHENA AND NOT SPARK
------------------------
These are aggregate queries over a few tens of thousands of Parquet rows. Glue would
cost ~90 seconds of cluster startup to answer questions Athena answers in two. The
heavier hammer is not always the right one, and README §6 makes the same argument.
"""

import json
import math
import os
import time
from datetime import datetime, timedelta, timezone

import boto3

athena = boto3.client("athena")
s3 = boto3.client("s3")

# --- configuration -----------------------------------------------------------

ATHENA_DATABASE = os.environ["ATHENA_DATABASE"]
ATHENA_WORKGROUP = os.environ["ATHENA_WORKGROUP"]
SILVER_TABLE = os.environ["SILVER_TABLE"]

RESULTS_BUCKET = os.environ["DQ_RESULTS_BUCKET"]
RESULTS_PREFIX = os.environ.get("DQ_RESULTS_PREFIX", "dq_results")

# How much recent Silver to evaluate.
#
# 24 hours rather than the 3-hour transform lookback. Ratio-based checks need a
# sample large enough to be stable: over 3 hours SHIBUSDT sees ~180 bars, so a single
# quiet stretch swings its empty-bar ratio by several percentage points and the gate
# becomes a coin flip. Over 24 hours (~1,440 bars/symbol) the ratio is meaningful.
#
# Freshness is unaffected by window size - it reads MAX(open_time) - so scoping wide
# costs nothing there.
WINDOW_HOURS = int(os.environ.get("DQ_WINDOW_HOURS", "24"))

# Number of symbols expected, fed from config/symbols.json by Terraform. Used to
# compute the expected row count rather than hardcoding one that rots when the basket
# changes.
SYMBOL_COUNT = int(os.environ["SYMBOL_COUNT"])

# --- thresholds, every one traceable to a measurement ------------------------

# Fraction of theoretically-possible bars that must be present.
# 17 symbols x 60 bars/hour x 24 hours = 24,480 expected. 0.70 tolerates a symbol
# being briefly unavailable or a couple of missed ingestion cycles without blocking.
MIN_ROW_RATIO = float(os.environ.get("DQ_MIN_ROW_RATIO", "0.70"))

# Empty-bar tolerance. Measured worst case is SHIBUSDT at 1.89% (M-039) over 10.6
# hours, which did NOT include a full overnight cycle when liquidity is thinnest.
# 5% leaves headroom above the measured rate while still catching a feed that has
# started returning empty bars wholesale.
MAX_EMPTY_BAR_PCT = float(os.environ.get("DQ_MAX_EMPTY_BAR_PCT", "5.0"))

# Sequence-gap tolerance. Measured: ZERO gaps across 23,327 bars over 16 days
# (M-022, M-028). The tolerance is not calibrated to observed noise - there is none -
# but to avoid blocking on a single missed ingestion cycle, which the 15-bar overlap
# would normally backfill anyway.
MAX_GAP_PCT = float(os.environ.get("DQ_MAX_GAP_PCT", "0.5"))

# Freshness. Measured structural lag is 4 min 23 s (M-025): the 5-minute ingestion
# interval plus the deliberate exclusion of the in-progress candle.
#
# README §11 specifies "latest bar within one interval of now", which at 1-minute
# bars means 1 minute - that would fail on EVERY run. 15 minutes covers the
# structural lag plus two missed ingestion cycles.
MAX_FRESHNESS_MINUTES = float(os.environ.get("DQ_MAX_FRESHNESS_MINUTES", "15"))

FAIL = "fail"
WARN = "warn"

QUERY_TIMEOUT_SECONDS = 60
POLL_INTERVAL_SECONDS = 1.0


class AthenaQueryError(Exception):
    pass


def run_query(sql):
    """
    Submit, poll, fetch. Athena has no synchronous 'run and return rows' call.

    Returns the first result row as a dict of column name -> string value. Every check
    query here is a single-row aggregate, so that is sufficient and keeps the parsing
    trivial.
    """
    execution_id = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=ATHENA_WORKGROUP,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
    )["QueryExecutionId"]

    deadline = time.time() + QUERY_TIMEOUT_SECONDS
    while True:
        execution = athena.get_query_execution(QueryExecutionId=execution_id)
        status = execution["QueryExecution"]["Status"]
        state = status["State"]

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.time() > deadline:
            raise AthenaQueryError(f"query {execution_id} exceeded {QUERY_TIMEOUT_SECONDS}s")
        time.sleep(POLL_INTERVAL_SECONDS)

    if state != "SUCCEEDED":
        reason = status.get("StateChangeReason", "no reason given")
        raise AthenaQueryError(f"query {execution_id} {state}: {reason}")

    result = athena.get_query_results(QueryExecutionId=execution_id)
    rows = result["ResultSet"]["Rows"]
    header = [c["VarCharValue"] for c in rows[0]["Data"]]
    values = [c.get("VarCharValue") for c in rows[1]["Data"]] if len(rows) > 1 else []

    return dict(zip(header, values))


def collect_metrics(window_start, cutoff_date):
    """
    Two queries, not seven.

    Athena bills a 10 MB minimum PER QUERY, so seven single-purpose queries would cost
    seven times as much as two combined ones while scanning the same data. Grouping by
    scan shape rather than by check keeps both cost and latency down.

    Query 1 covers everything expressible as a simple aggregate. Query 2 covers the
    two checks that need a window function over consecutive bars.

    Both carry a `date` partition predicate as well as an open_time predicate. The
    open_time filter alone would be correct but would scan every partition - Athena
    prunes on partition columns, not on arbitrary expressions.
    """
    epoch = int(window_start.timestamp())

    aggregates = run_query(f"""
        SELECT
          COUNT(*)                            AS row_count,
          COUNT(DISTINCT symbol)              AS symbol_count,
          SUM(CASE WHEN high < low
                     OR high < open OR high < close
                     OR low  > open OR low  > close
                   THEN 1 ELSE 0 END)         AS ohlc_violations,
          SUM(CASE WHEN open <= 0 OR high <= 0
                     OR low  <= 0 OR close <= 0
                   THEN 1 ELSE 0 END)         AS nonpositive_prices,
          SUM(CASE WHEN num_trades = 0 THEN 1 ELSE 0 END) AS zero_trade_bars,
          CAST(to_unixtime(MAX(open_time)) AS BIGINT)     AS max_open_time
        FROM {SILVER_TABLE}
        WHERE "date" >= '{cutoff_date}'
          AND open_time >= from_unixtime({epoch})
    """)

    sequence = run_query(f"""
        WITH ordered AS (
          SELECT
            symbol,
            open_time,
            LAG(open_time) OVER (PARTITION BY symbol ORDER BY open_time) AS prev_time
          FROM {SILVER_TABLE}
          WHERE "date" >= '{cutoff_date}'
            AND open_time >= from_unixtime({epoch})
        )
        SELECT
          COUNT(prev_time)                                                  AS comparable_pairs,
          SUM(CASE WHEN date_diff('second', prev_time, open_time) > 60
                   THEN 1 ELSE 0 END)                                       AS gap_count,
          SUM(CASE WHEN date_diff('second', prev_time, open_time) <= 0
                   THEN 1 ELSE 0 END)                                       AS non_monotonic
        FROM ordered
    """)

    def as_int(mapping, key):
        value = mapping.get(key)
        return int(value) if value not in (None, "") else 0

    return {
        "row_count": as_int(aggregates, "row_count"),
        "symbol_count": as_int(aggregates, "symbol_count"),
        "ohlc_violations": as_int(aggregates, "ohlc_violations"),
        "nonpositive_prices": as_int(aggregates, "nonpositive_prices"),
        "zero_trade_bars": as_int(aggregates, "zero_trade_bars"),
        "max_open_time": as_int(aggregates, "max_open_time"),
        "comparable_pairs": as_int(sequence, "comparable_pairs"),
        "gap_count": as_int(sequence, "gap_count"),
        "non_monotonic": as_int(sequence, "non_monotonic"),
    }


def check(name, severity, passed, observed, threshold, detail):
    return {
        # `check_name`, not `check`: CHECK is a reserved word in Trino/Athena, and
        # this column becomes a GROUP BY key in the persisted DQ history table.
        "check_name": name,
        "severity": severity,
        "passed": bool(passed),
        "observed": observed,
        "threshold": threshold,
        "detail": detail,
    }


def evaluate(metrics, now, window_hours=WINDOW_HOURS, symbol_count=SYMBOL_COUNT):
    """
    Turn measured values into pass/fail decisions.

    PURE ON PURPOSE - no AWS calls, no clock reads. Every threshold boundary is
    unit-testable without Athena, which matters because the interesting behaviour of
    a quality gate is precisely what it does at the edges, and those are miserable to
    reproduce against live data.
    """
    expected_rows = symbol_count * 60 * window_hours
    row_ratio = metrics["row_count"] / expected_rows if expected_rows else 0.0

    empty_pct = (
        100.0 * metrics["zero_trade_bars"] / metrics["row_count"]
        if metrics["row_count"]
        else 0.0
    )
    gap_pct = (
        100.0 * metrics["gap_count"] / metrics["comparable_pairs"]
        if metrics["comparable_pairs"]
        else 0.0
    )
    freshness_minutes = (
        (now.timestamp() - metrics["max_open_time"]) / 60.0
        if metrics["max_open_time"]
        else float("inf")
    )

    return [
        check(
            "row_count", FAIL,
            row_ratio >= MIN_ROW_RATIO,
            round(row_ratio, 4), MIN_ROW_RATIO,
            f"{metrics['row_count']} rows of {expected_rows} possible "
            f"({symbol_count} symbols x {window_hours}h)",
        ),
        check(
            "symbols_present", FAIL,
            metrics["symbol_count"] >= symbol_count,
            metrics["symbol_count"], symbol_count,
            "a symbol missing entirely means ingestion is failing for it silently",
        ),
        check(
            "ohlc_integrity", FAIL,
            metrics["ohlc_violations"] == 0,
            metrics["ohlc_violations"], 0,
            "high must be the max and low the min of the bar; a violation means "
            "upstream corruption or a positional-mapping bug in cast_bars()",
        ),
        check(
            "positive_prices", FAIL,
            metrics["nonpositive_prices"] == 0,
            metrics["nonpositive_prices"], 0,
            "a zero or negative price makes log_return undefined and poisons every "
            "downstream aggregate",
        ),
        check(
            "freshness", FAIL,
            freshness_minutes <= MAX_FRESHNESS_MINUTES,
            round(freshness_minutes, 2), MAX_FRESHNESS_MINUTES,
            "newest bar age in minutes; ~4.4 min of this is structural (M-025)",
        ),
        check(
            "monotonic_time", FAIL,
            metrics["non_monotonic"] == 0,
            metrics["non_monotonic"], 0,
            "consecutive bars must strictly advance; a violation means the dedup key "
            "or the ordering is broken",
        ),
        check(
            "empty_bar_ratio", WARN,
            empty_pct <= MAX_EMPTY_BAR_PCT,
            round(empty_pct, 4), MAX_EMPTY_BAR_PCT,
            "untraded minutes are normal for illiquid pairs (SHIBUSDT measured at "
            "1.89%); a large jump suggests a degraded feed",
        ),
        check(
            "sequence_gaps", WARN,
            gap_pct <= MAX_GAP_PCT,
            round(gap_pct, 4), MAX_GAP_PCT,
            "missing minutes break the trailing-window assumption; the 15-bar "
            "ingestion overlap should normally backfill any single missed run",
        ),
    ]


def persist(results, run_id, run_at):
    """
    One JSON line per check, under a Hive-style date prefix.

    Flat rather than nested so it can be read as an Athena table without UNNEST -
    "what is the pass rate per check over the last N runs" should be one GROUP BY, not
    a scripting exercise.
    """
    lines = []
    for result in results:
        record = {
            "run_id": run_id,
            "run_at": run_at.isoformat().replace("+00:00", "Z"),
            **result,
        }

        # json.dumps() emits bare `Infinity` and `NaN` for non-finite floats. Those
        # are valid Python but INVALID JSON, and Athena's SerDe rejects the record -
        # so the one run most worth having a record of (an empty Silver layer, where
        # freshness is infinite) would be the one that fails to persist.
        #
        # Serialised as null instead: "not measurable" is what infinity means here,
        # and null is how SQL says that.
        observed = record.get("observed")
        if isinstance(observed, float) and not math.isfinite(observed):
            record["observed"] = None

        lines.append(json.dumps(record))

    key = (
        f"{RESULTS_PREFIX}/date={run_at:%Y-%m-%d}"
        f"/dq_{run_at:%H%M%S}_{run_id[:8]}.json"
    )
    s3.put_object(
        Bucket=RESULTS_BUCKET,
        Key=key,
        Body=("\n".join(lines) + "\n").encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    return f"s3://{RESULTS_BUCKET}/{key}"


def lambda_handler(event, context):
    run_at = datetime.now(timezone.utc)
    run_id = getattr(context, "aws_request_id", "local")

    window_start = run_at - timedelta(hours=WINDOW_HOURS)
    # One extra day on the partition predicate: a 24-hour window starting at 02:00
    # spans two date partitions, and pruning to only today would silently drop half
    # the window.
    cutoff_date = (window_start - timedelta(days=1)).strftime("%Y-%m-%d")

    metrics = collect_metrics(window_start, cutoff_date)
    results = evaluate(metrics, run_at)

    failed = [r for r in results if not r["passed"]]
    blocking = [r for r in failed if r["severity"] == FAIL]

    location = persist(results, run_id, run_at)

    response = {
        # The field Step Functions' Choice state reads. WARN failures deliberately do
        # not affect it.
        "quality_passed": len(blocking) == 0,
        "run_id": run_id,
        "run_at": run_at.isoformat().replace("+00:00", "Z"),
        "window_hours": WINDOW_HOURS,
        "checks_run": len(results),
        "checks_failed": len(failed),
        "blocking_failures": [r["check"] for r in blocking],
        "warnings": [r["check"] for r in failed if r["severity"] == WARN],
        "metrics": metrics,
        "results_location": location,
    }

    print(json.dumps(response))
    return response
