-- =============================================================================
-- Silver layer checks, run in Athena.
--
-- Table: crypto_anomaly_silver.clean_bars
--
-- IMPORTANT SYNTAX NOTE: `date` is a RESERVED WORD in Athena (which is Trino under
-- the hood). Our partition column is named `date`, so every reference to it must be
-- double-quoted: "date". An unquoted `WHERE date = ...` fails with a parse error
-- that points at the wrong place.
--
-- (README section 16's example queries use unquoted `date` and will not run as
-- written. Noted for correction.)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. Does the table work at all?
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS total_bars
FROM crypto_anomaly_silver.clean_bars;


-- -----------------------------------------------------------------------------
-- 2. Partition pruning, demonstrated.
--
-- Run both and compare the "Data scanned" figure. The second reads one partition
-- instead of all of them. This is the entire economic argument for partitioning:
-- Athena bills per byte scanned, so a WHERE clause on a partition column is
-- literally a discount.
--
-- Note these SELECT a real column rather than COUNT(*) - Parquet can answer
-- COUNT(*) from file metadata alone, scanning almost nothing, which would make the
-- comparison meaningless.
-- -----------------------------------------------------------------------------
SELECT AVG(close) AS avg_close
FROM crypto_anomaly_silver.clean_bars;

SELECT AVG(close) AS avg_close
FROM crypto_anomaly_silver.clean_bars
WHERE "date" = '2026-08-24';


-- -----------------------------------------------------------------------------
-- 3. THE GAP QUERY - explains the missing bars (METRICS.md M-018).
--
-- LAG() is a window function: for each row it fetches a value from the PREVIOUS
-- row, within the same symbol, in time order. Comparing each bar's open_time to
-- the one before it turns "is anything missing?" into arithmetic.
--
-- Any gap_minutes > 1 is a minute with no bar. Causes, in order of likelihood:
--   - a missed ingestion run (our fault - a pipeline reliability problem)
--   - a genuinely untraded minute (the market's doing - a data property, not a bug)
--   - a Binance outage or data hole (upstream)
--
-- These have completely different implications, which is why M-018 stays
-- provisional until this query distinguishes them. This is also the "sequence gaps"
-- check from README section 11, arriving early.
-- -----------------------------------------------------------------------------
WITH ordered AS (
  SELECT
    symbol,
    open_time,
    LAG(open_time) OVER (PARTITION BY symbol ORDER BY open_time) AS prev_open_time
  FROM crypto_anomaly_silver.clean_bars
)
SELECT
  symbol,
  prev_open_time,
  open_time,
  date_diff('minute', prev_open_time, open_time) AS gap_minutes
FROM ordered
WHERE prev_open_time IS NOT NULL
  AND date_diff('minute', prev_open_time, open_time) > 1
ORDER BY open_time;


-- -----------------------------------------------------------------------------
-- 4. Completeness summary - one row per symbol.
-- -----------------------------------------------------------------------------
SELECT
  symbol,
  COUNT(*)                                                   AS bars_present,
  MIN(open_time)                                             AS first_bar,
  MAX(open_time)                                             AS last_bar,
  date_diff('minute', MIN(open_time), MAX(open_time)) + 1    AS bars_expected,
  date_diff('minute', MIN(open_time), MAX(open_time)) + 1
    - COUNT(*)                                               AS bars_missing,
  ROUND(
    100.0 * COUNT(*)
    / (date_diff('minute', MIN(open_time), MAX(open_time)) + 1),
    3
  )                                                          AS completeness_pct
FROM crypto_anomaly_silver.clean_bars
GROUP BY symbol;


-- -----------------------------------------------------------------------------
-- 5. Dedup verification, against the STORED data.
--
-- The Spark job reported a 64.09% duplicate rate, but that is the job describing
-- its own work. This asks the output directly: does any (symbol, open_time) appear
-- more than once?
--
-- MUST RETURN ZERO ROWS. That is independent confirmation the dedup key holds -
-- checking the artifact rather than trusting the process that made it.
-- -----------------------------------------------------------------------------
SELECT symbol, open_time, COUNT(*) AS versions
FROM crypto_anomaly_silver.clean_bars
GROUP BY symbol, open_time
HAVING COUNT(*) > 1;


-- -----------------------------------------------------------------------------
-- 6. OHLC sanity - a preview of the DQ gate (README section 11).
--
-- By definition high must be the maximum and low the minimum of the bar. A
-- violation means either corrupt upstream data or a positional-mapping bug in our
-- Bronze->Silver cast - the exact failure mode of reading a positional array by
-- index. MUST RETURN 0.
-- -----------------------------------------------------------------------------
SELECT
  SUM(CASE WHEN high < low                      THEN 1 ELSE 0 END) AS high_below_low,
  SUM(CASE WHEN high < open OR high < close     THEN 1 ELSE 0 END) AS high_not_max,
  SUM(CASE WHEN low  > open OR low  > close     THEN 1 ELSE 0 END) AS low_not_min,
  SUM(CASE WHEN open <= 0 OR high <= 0
             OR low  <= 0 OR close <= 0         THEN 1 ELSE 0 END) AS nonpositive_price,
  COUNT(*)                                                          AS total_bars
FROM crypto_anomaly_silver.clean_bars;


-- -----------------------------------------------------------------------------
-- 7. Empty bars - the quirk found during validation (README section 8).
--
-- num_trades = 0 with volume = 0 is a real, illiquid, untraded minute. NOT
-- corruption. Expect a small nonzero count even for BTCUSDT, and a much larger one
-- for illiquid pairs once the basket expands.
--
-- This is why the DQ gate must treat empty bars as a RATIO with a tolerance rather
-- than a binary fail - a hard "fail if any bar is empty" would trip constantly and
-- teach you to ignore the alert.
-- -----------------------------------------------------------------------------
SELECT
  symbol,
  COUNT(*)                                                  AS total_bars,
  SUM(CASE WHEN num_trades = 0 THEN 1 ELSE 0 END)           AS zero_trade_bars,
  SUM(CASE WHEN volume = 0     THEN 1 ELSE 0 END)           AS zero_volume_bars,
  ROUND(100.0 * SUM(CASE WHEN num_trades = 0 THEN 1 ELSE 0 END) / COUNT(*), 4)
                                                            AS zero_trade_pct
FROM crypto_anomaly_silver.clean_bars
GROUP BY symbol;
