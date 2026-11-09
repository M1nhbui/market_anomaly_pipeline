-- =============================================================================
-- Reimplement the volume z-score in pure SQL, to answer two questions at once:
--
--   1. RECOVERY: how many volume events were LOWER-tail before the rule was made
--      one-sided? The Gold table has already been overwritten, but Gold is derived -
--      Silver still holds everything needed to recompute it.
--
--   2. CROSS-VALIDATION: does an independent implementation, in a different engine,
--      reproduce the Spark job's numbers? If the `upper_tail` count below equals the
--      `volume_zscore` count from SILVER_TO_GOLD_STATS, the z-score logic is
--      confirmed by two implementations that share no code.
--
-- The window must match glue_jobs/silver_to_gold.py exactly:
--   PARTITION BY symbol
--   ORDER BY open_time
--   RANGE 3600s PRECEDING .. 60s PRECEDING   (60 one-minute bars, EXCLUDING current)
--   require 60 observations, and stddev > 1e-6
--
-- to_unixtime() is used for the ORDER BY so the RANGE bounds are plain numbers -
-- seconds - rather than intervals, which is the most portable form.
-- =============================================================================

WITH scored AS (
  SELECT
    symbol,
    open_time,
    num_trades,
    volume,
    ln(1 + volume) AS log_volume,
    AVG(ln(1 + volume)) OVER (
      PARTITION BY symbol ORDER BY to_unixtime(open_time)
      RANGE BETWEEN 3600 PRECEDING AND 60 PRECEDING
    ) AS rolling_mean,
    STDDEV_SAMP(ln(1 + volume)) OVER (
      PARTITION BY symbol ORDER BY to_unixtime(open_time)
      RANGE BETWEEN 3600 PRECEDING AND 60 PRECEDING
    ) AS rolling_std,
    COUNT(ln(1 + volume)) OVER (
      PARTITION BY symbol ORDER BY to_unixtime(open_time)
      RANGE BETWEEN 3600 PRECEDING AND 60 PRECEDING
    ) AS observations
  FROM crypto_anomaly_silver.clean_bars
),

zscored AS (
  SELECT
    symbol,
    num_trades,
    (log_volume - rolling_mean) / rolling_std AS zscore
  FROM scored
  WHERE observations >= 60
    AND rolling_std > 1e-6
)

SELECT
  SUM(CASE WHEN zscore >  3 THEN 1 ELSE 0 END)                        AS upper_tail,
  SUM(CASE WHEN zscore < -3 THEN 1 ELSE 0 END)                        AS lower_tail,
  SUM(CASE WHEN zscore < -3 AND num_trades = 0 THEN 1 ELSE 0 END)     AS lower_untraded,
  SUM(CASE WHEN ABS(zscore) > 3 THEN 1 ELSE 0 END)                    AS two_sided_total
FROM zscored;
