# METRICS

Every number in this file traces to a command you can re-run or a console view you
can re-open. Nothing here is a guess dressed up as a measurement.

**Only entries labelled `MEASURED` or `DERIVED` are eligible for a resume bullet.**

---

## Labels

| Label | Meaning | Resume-safe? |
|---|---|---|
| `MEASURED` | Read directly from a command output, Athena result, or AWS console. Reproducible. | Yes |
| `DERIVED` | Arithmetic on `MEASURED` inputs. The inputs and the formula are both recorded. | Yes, if the formula is stated |
| `EXTRAPOLATED` | A measured short-window rate projected to a longer period. | Only with the window named ("~X/day measured over N days") |
| `ESTIMATED` | No measurement. A design-time expectation. | **No** |

Rules:

- Never round a measured number up. Round down or quote it exactly.
- Record the measurement date. Rates change; a number without a date is a claim.
- Record parameters alongside any metric that depends on them (e.g. anomaly counts
  are meaningless without `k` and the window length).
- If a measurement contradicts an earlier estimate, keep both. The delta is the
  interesting part.

---

## Scope note

These are **system and engineering metrics**: throughput, cost, latency, data
quality, and signal volume. This project makes no business-impact, profitability,
or predictive-accuracy claims, and no metric in this file should be phrased as if
it does. See README §4.

---

## Entry template

```
### <metric name>
- **Value:** <exact number + unit>
- **Label:** MEASURED | DERIVED | EXTRAPOLATED | ESTIMATED
- **Measured on:** YYYY-MM-DD (window: <what period the data covers>)
- **Parameters:** <anything the number depends on>
- **How:** <exact command or console path>
- **Raw output:**
  ```
  <paste>
  ```
- **Notes:** <caveats, what would change this number>
```

---

## Measurement schedule

What becomes measurable at each slice. Nothing is filled in until it is actually run.

| Slice | Newly measurable | Status |
|---|---|---|
| 0 | — (enable Cost Explorer + budget alarm so cost data starts accruing) | pending |
| 1 | Bars per ingestion run; Bronze object count and size; Lambda duration | pending |
| 2 | Bronze row count vs Silver row count; **duplicate rate removed by dedup**; Glue job duration (cold) | pending |
| 3 | Athena `count(*)` on Silver; bytes scanned per query | pending |
| 4 | Null rate at window edges; feature computation overhead (job duration delta) | pending |
| 5 | Bars/day across full basket; ingestion success rate; per-symbol failure count | pending |
| 6 | Anomaly events/day at \|z\|>3; firing rate as % of bars; **end-to-end latency** | pending |
| 7 | DQ check count; pass/fail counts per run; zero-volume detection rate | pending |
| 8 | **Two-cadence cost experiment**; state machine success rate | pending |
| 9 | Month-to-date AWS cost from Cost Explorer | pending |

---

## Entries

### M-001 — Symbol basket reachability
- **Value:** 18 / 18 candidate symbols returned HTTP 200 from the klines endpoint; 0 errors
- **Label:** MEASURED
- **Measured on:** 2026-07-27
- **Parameters:** host `data-api.binance.vision`, `interval=1m`, `limit=1`
- **How:** `scripts/validate_binance.py` probe variant, run locally by the operator
- **Raw output:**
  ```
  OK  18 ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'ADAUSDT',
          'DOGEUSDT', 'AVAXUSDT', 'LINKUSDT', 'DOTUSDT', 'MATICUSDT', 'POLUSDT',
          'LTCUSDT', 'ATOMUSDT', 'UNIUSDT', 'NEARUSDT', 'PEPEUSDT', 'SHIBUSDT']
  BAD []
  ```
- **Notes:** **This metric is weaker than it looks and should not be quoted as
  "18 symbols validated."** HTTP 200 proves the symbol is known to the exchange, not
  that it is actively trading — a retired pair can still serve its final historical
  bars. MATICUSDT and POLUSDT both returning 200 is the tell that this check is
  insufficient. Superseded by M-002 (pending) which adds a bar-freshness assertion.
  Prediction that MATICUSDT would return HTTP 400 was **wrong**.

### M-003 — Binance returns an in-progress (unfinished) bar
- **Value:** the final element of every klines response is an open candle; observed
  partial bar had `volume` 0.10631 and 79 trades against 1.13365 / 0.84351 volume and
  321 / 198 trades for the two preceding closed bars
- **Label:** MEASURED (n=1)
- **Measured on:** 2026-07-27
- **Parameters:** `BTCUSDT`, `interval=1m`, `limit=3`
- **How:** probe compared each bar's `close_time` against wall-clock `now`
- **Raw output:**
  ```
  open=1785088320000 close_time=1785088379999 closed=True  vol=1.13365000 trades=321
  open=1785088380000 close_time=1785088439999 closed=True  vol=0.84351000 trades=198
  open=1785088440000 close_time=1785088499999 closed=False vol=0.10631000 trades=79
  ```
- **Notes:** Confirms the correctness hazard that drives the Silver dedup design. Do
  **not** quote the volume ratio as a statistic — the partial bar's size depends
  entirely on how far into the minute the sample was taken, so a single observation
  says nothing about the average. The defensible claim is qualitative and binary:
  *the API returns an unfinished bar, and ingesting it without handling produces
  incorrect stored data.* The quantitative version of this becomes available at
  slice 2 as a measured duplicate/correction rate.

### M-006 — Ingestion Lambda duration
- **Value:** mean **841.7 ms**, min 800.9 ms, max 868.6 ms (n=6 warm invocations)
- **Label:** MEASURED
- **Measured on:** 2026-08-23 (six consecutive scheduled runs, 18:23–18:48 UTC)
- **Parameters:** 1 symbol (BTCUSDT), `limit=15`, 256 MB allocated, python3.12, us-east-1
- **How:** `aws logs tail /aws/lambda/crypto-anomaly-ingestion --since 30m | grep REPORT`
- **Raw output:**
  ```
  Duration: 868.56 ms  Billed Duration: 869 ms  Memory Size: 256 MB  Max Memory Used: 99 MB
  Duration: 836.90 ms  Billed Duration: 837 ms  Memory Size: 256 MB  Max Memory Used: 99 MB
  Duration: 800.91 ms  Billed Duration: 801 ms  Memory Size: 256 MB  Max Memory Used: 99 MB
  Duration: 838.87 ms  Billed Duration: 839 ms  Memory Size: 256 MB  Max Memory Used: 99 MB
  Duration: 862.59 ms  Billed Duration: 863 ms  Memory Size: 256 MB  Max Memory Used: 99 MB
  Duration: 842.59 ms  Billed Duration: 843 ms  Memory Size: 256 MB  Max Memory Used: 99 MB
  ```
- **Notes:** All six are warm invocations; no cold start is represented in this sample,
  so this is **not** a p99. Variance is tight (±4%), consistent with a network-bound
  workload. Per-symbol cost is what matters for the slice-5 projection.

### M-007 — Ingestion Lambda memory utilisation
- **Value:** **99 MB used of 256 MB allocated** (38.7%), identical across all 6 runs
- **Label:** MEASURED
- **Measured on:** 2026-08-23
- **How:** `Max Memory Used` field of the CloudWatch REPORT line
- **Notes:** Suggests over-allocation, **but tuning is deliberately deferred to slice 5.**
  This was measured at 1 symbol; the production config is 17. Re-sizing on a
  single-symbol measurement would be exactly the kind of estimate-dressed-as-measurement
  this file exists to prevent. Also note the headroom math: 128 MB would leave only
  29 MB of margin over the observed 99 MB, so the safe step is 192 MB, not 128 MB.

### M-008 — Bronze object size
- **Value:** **2.86 KiB per object** (63.0 KiB across 22 objects)
- **Label:** MEASURED
- **Measured on:** 2026-08-23
- **Parameters:** 1 symbol, 15 bars per object, raw JSON, uncompressed
- **How:** `aws s3 ls s3://crypto-anomaly-bronze-.../ --recursive --human-readable --summarize`
- **Notes:** Basis for the Bronze storage projection. Do not extrapolate to Silver —
  Silver is Parquet (columnar, compressed) and will be dramatically smaller per bar,
  which is itself a measurable comparison at slice 3.

### M-009 — Scheduled ingestion cadence accuracy
- **Value:** 6 consecutive runs at **exactly 300 s spacing** (18:23:24, 18:28:24,
  18:33:24, 18:38:24, 18:43:24, 18:48:24 UTC); 0 missed runs
- **Label:** MEASURED
- **Measured on:** 2026-08-23 (30-minute observation window)
- **Parameters:** EventBridge Scheduler, `rate(5 minutes)`, `flexible_time_window = OFF`
- **Notes:** Confirms `flexible_time_window = OFF` produces exact-time firing. A
  30-minute window is far too short to claim a reliability figure — revisit after the
  multi-day metrics window at slice 8.

### M-010 — Unfinished bar present in stored Bronze data
- **Value:** last bar of a 15-bar response had `close_time` **8.5 s after** `ingested_at`
- **Label:** MEASURED
- **Measured on:** 2026-08-23
- **How:** `close_time` 1787504159999 (16:55:59.999Z) vs `ingested_at` 16:55:51.535Z
- **Raw output:**
  ```
  last bar: [1787504100000, '77406.01', '77412.00', '77406.00', '77412.00',
             '1.94609000', 1787504159999, '150641.73465930', 525, ...]
  ingested_at: 2026-08-23T16:55:51.535338Z
  ```
- **Notes:** Confirms M-003 in our own pipeline rather than in a probe script. That
  bar's volume (1.94609) and trade count (525) are partial and Binance will revise
  them. This is the concrete justification for the Silver rule
  `drop where close_time >= ingested_at`. Also confirms `limit=15` is behaving: the
  first and last `open_time` span exactly 14 minutes across 15 bars.

### M-011 — Lambda free-tier headroom (1-symbol basis)
- **Value:** **~1,818 GB-seconds/month**, or **0.45%** of the 400,000 GB-s always-free
  allowance; 8,640 invocations/month against 1,000,000 free requests (0.86%)
- **Label:** DERIVED
- **Measured on:** 2026-08-23
- **How:** `8,640 invocations/mo × 0.8417 s (M-006) × 0.25 GB = 1,818 GB-s`
- **Notes:** Inputs are measured; the projection assumes the observed duration holds.
  At 17 symbols this rises roughly 17x to ~30,900 GB-s — still under 8% of the free
  allowance. **Free-tier eligibility itself is an assumption** until confirmed in
  Cost Explorer, since the account's legacy 12-month tier has expired and only the
  "always free" component should remain.

### M-012 — Bronze→Silver duplicate rate (the headline dedup number)
- **Value:** **63.95%** of finished bar records were duplicates
  (5,110 finished records → 1,842 unique bars; 3,268 removed)
- **Label:** MEASURED
- **Measured on:** 2026-08-23
- **Parameters:** 1 symbol (BTCUSDT), ingestion every 5 min at `limit=15`,
  dedup key `(symbol, open_time)` ordered by `ingested_at DESC`
- **How:** local Spark run of `glue_jobs/bronze_to_silver.py` against real Bronze
  synced from S3; `BRONZE_TO_SILVER_STATS` log line
- **Raw output:**
  ```
  BRONZE_TO_SILVER_STATS {"raw_bar_records": 5475, "unfinished_dropped": 365,
    "finished_records": 5110, "unique_bars_written": 1842,
    "duplicates_removed": 3268, "duplicate_rate_pct": 63.95}
  ```
- **Notes:** Predicted 60–65% before running, from the overlap arithmetic (fetch 15
  bars every 5 min ⇒ ~5 new + ~10 repeats per run ⇒ 2/3 duplicates). Measured 63.95%
  lands inside the predicted band, which is evidence the dedup key is right rather
  than merely producing a plausible-looking number. **This rate is a property of the
  chosen `limit=15` overlap, not of the data** — quoting it without that parameter
  would be misleading.

### M-013 — Unfinished bars removed
- **Value:** **365 of 5,475** raw bar records (6.67%) were in-progress candles
- **Label:** MEASURED
- **Measured on:** 2026-08-23
- **How:** same run as M-012, `unfinished_dropped` field
- **Notes:** 365 is exactly the number of ingestion documents in the sample — one
  unfinished bar per API response, precisely as expected since Binance always
  returns the currently-forming candle last. The arithmetic agreeing this cleanly is
  a strong signal the filter is doing what it claims. Confirms M-003 and M-010
  quantitatively. Had these not been dropped, 6.67% of Silver would carry partial
  volume and trade counts — directly feeding false positives into the volume
  z-score at slice 6.

### M-014 — Small-file overhead in the Silver write layout
- **Value:** at 17 symbols × 7 days (119 partition directories):
  **476 files → 119 files**, total size **4,674 KiB → 4,267 KiB** (**8.7% saved**),
  average file size 9.8 KiB → 35.9 KiB
- **Label:** MEASURED (synthetic data at production scale)
- **Measured on:** 2026-08-23
- **Parameters:** Spark 3.5.3 local, AQE enabled, `spark.sql.shuffle.partitions=200`,
  comparing default write vs `repartition("symbol","date")` before write
- **How:** benchmark writing the same DataFrame three ways and counting
  `*.parquet` files plus total bytes on disk
- **Raw output:**
  ```
  as-written (AQE on)             files=476    total=  4673.9 KiB  avg=  9.82 KiB
  coalesce(1)                     files=119    total=  4267.1 KiB  avg= 35.86 KiB
  repartition('symbol','date')    files=119    total=  4267.2 KiB  avg= 35.86 KiB
  as-written (AQE OFF)            files=23776  total= 33055.9 KiB  avg=  1.39 KiB
  ```
- **Notes:** Data volume is **synthetic**, generated to match the production shape
  (17 symbols × 7 days × 1440 bars). The file counts and the ratio are real
  measurements of Spark's write behaviour; the byte totals are representative, not
  from live market data. The AQE-off row is the counterfactual showing what the
  default would cost without Adaptive Query Execution: 7.8x storage bloat.
  `coalesce(1)` and `repartition` measured identically at this scale;
  `repartition` was chosen because `coalesce(1)` serialises the write through one
  task and will not scale. Change originated as a review suggestion from the
  operator, not from the original design.

### M-015 — Glue job execution time (first production run)
- **Value:** **90 seconds**, 2 workers, state SUCCEEDED
- **Label:** MEASURED
- **Measured on:** 2026-08-25
- **Parameters:** Glue 5.0, 2 × G.1X (2 DPU), input 607 Bronze documents /
  9,105 bar records, output 3,052 rows
- **How:** `aws glue get-job-run --job-name crypto-anomaly-bronze-to-silver --run-id <id>`
- **Raw output:**
  ```
  {"State": "SUCCEEDED", "ExecutionSeconds": 90, "Workers": 2, "Error": null}
  ```
- **Notes:** n=1. A single run is not a distribution — cold-start variance on Glue is
  real. Needs 3–5 runs before quoting as a typical figure.

### M-016 — Measured Glue cost per run, and the cadence comparison
- **Value:** **$0.022 per job run**. Hourly cadence: **$31.68/month** for both jobs.
  5-minute cadence: **$380.16/month**. Ratio **12.0x**.
- **Label:** DERIVED
- **Measured on:** 2026-08-25
- **How:** `90 s ÷ 3600 × 2 DPU × $0.44/DPU-hr = $0.022/run`
  Hourly: `$0.022 × 720 runs × 2 jobs = $31.68/mo`
  5-minute: `$0.022 × 8,640 runs × 2 jobs = $380.16/mo`
- **Notes:** **Supersedes M-004**, which estimated $42/mo hourly. The estimate was
  **33% too high** because it assumed 2-minute runs; actual is 90 s.
  **One assumption remains unmeasured and it is load-bearing:** this treats per-run
  cost as constant across cadences. That holds only if execution time is dominated
  by startup rather than data volume — plausible at 3,052 rows, but *assumed*, not
  shown. Until the 5-minute-window run is measured (slice 8, or the cheap version
  below), the 12.0x figure is arithmetic on one measured data point, not an
  experimental result. Do not put "12x" on a resume until both durations are measured.

### M-019 — Glue execution time is startup-dominated (the two-cadence experiment)
- **Value:** **90 s** on 607 Bronze documents (9,105 bar records) vs **94 s** on
  ~12 documents (~180 bar records). A **50× input reduction changed runtime by
  −4.4%** — and in the slower direction.
- **Label:** MEASURED (n=1 per arm)
- **Measured on:** 2026-08-25
- **Parameters:** identical job, Glue 5.0, 2 × G.1X, only `--bronze-path` differed;
  small-arm output redirected to scratch to avoid partition-overwriting real Silver
- **How:** two `aws glue start-job-run` invocations, `ExecutionTime` from
  `aws glue get-job-run`
- **Raw output:**
  ```
  full dataset : {"State": "SUCCEEDED", "ExecutionSeconds": 90}
  one hour     : {"State": "SUCCEEDED", "ExecutionSeconds": 94}
  ```
- **Notes:** This is the load-bearing measurement behind the two-cadence design.
  Runtime is dominated by cluster startup, Spark session init, and S3 listing, not by
  computation — so per-run cost is effectively constant and **run frequency is the
  only cost lever.** n=1 per arm; the 4 s gap is within plausible run-to-run
  variance and should not be read as the small job being genuinely slower. Three
  runs per arm would make this a distribution rather than two points.

### M-020 — Two-cadence cost saving (MEASURED both sides)
- **Value:** hourly **$33.09/month** vs 5-minute **$397.10/month** ⇒ **12.0×**
- **Label:** DERIVED (from measured durations on both arms)
- **Measured on:** 2026-08-25
- **How:** `94 s ÷ 3600 × 2 DPU × $0.44 = $0.02298/run`
  hourly `× 720 × 2 jobs = $33.09`; 5-min `× 8,640 × 2 jobs = $397.10`
- **Notes:** Supersedes M-016's provisional 12.0× and removes its unmeasured
  assumption — M-019 demonstrates rather than assumes that per-run cost holds across
  cadences. The multiple equals the run-count ratio exactly *because* runtime is
  volume-independent; that causal link is the interesting part, not the number.
  README §12 estimated "~10x" with no measurement behind it. **Resume-eligible**,
  provided the method is stated: two Glue runs differing only in input size.

### M-021 — Bronze → Silver storage reduction, decomposed
- **Value:** **8.95× total** (1,748 KiB Bronze JSON → 195.3 KiB Silver Parquet),
  decomposing into **2.98× from record elimination** (dedup + unfinished-bar removal)
  and **3.00× from format change** (JSON → Parquet/Snappy).
  Per unit: **196.6 bytes per Bronze bar record → 65.5 bytes per Silver row.**
- **Label:** DERIVED (from measured byte counts and record counts)
- **Measured on:** 2026-08-25
- **How:**
  ```
  Bronze total : 2,067,180 bytes / 701 objects = 2,948.9 bytes per object
  At job time  : 607 objects × 2,948.9        = 1,789,982 bytes  (1,748 KiB)
  Silver       : 30.3 + 90.9 + 74.1 KiB       =   199,987 bytes  (195.3 KiB)
  Total        : 1,789,982 / 199,987          = 8.95×
  Records      : 9,105 / 3,052                = 2.98×
  Format       : 8.95 / 2.98                  = 3.00×
  ```
- **Notes:** Bronze had grown to 701 objects by the time it was measured, so the
  607-object figure is **reconstructed from the measured per-object average**, not
  read directly. That reconstruction is the weakest link here; a clean version would
  snapshot both totals in the same minute.
  The 3.00× "format" factor is **not pure compression.** It bundles Snappy-compressed
  columnar encoding, dropping Binance's unused `ignore` and `taker_buy_quote` fields,
  and eliminating the per-document JSON envelope — partly offset by Silver *adding*
  `close_time`, `ingested_at`, and `source_host` to every row. Calling it "3× Parquet
  compression" would overstate what the format alone contributes.
  Full-day partition check: 90.9 KiB ÷ 1,440 bars = **64.6 bytes/bar**, consistent
  with the 65.5 bytes/row overall — encoding efficiency is stable across partitions.

### M-017 — Duplicate rate reproduced on Glue
- **Value:** **64.09%** (8,498 finished records → 3,052 unique; 5,446 removed)
- **Label:** MEASURED
- **Measured on:** 2026-08-25
- **How:** `BRONZE_TO_SILVER_STATS` in CloudWatch `/aws-glue/jobs/output`
- **Raw output:**
  ```
  {"raw_bar_records": 9105, "unfinished_dropped": 607, "finished_records": 8498,
   "unique_bars_written": 3052, "duplicates_removed": 5446, "duplicate_rate_pct": 64.09}
  ```
- **Notes:** Local run on a smaller sample measured 63.95% (M-012); Glue on 1.7x the
  data measured 64.09%. The 0.14 pp difference across a different runtime, different
  Spark deployment, and a larger sample is strong evidence the dedup logic is
  deterministic rather than incidentally correct. `unfinished_dropped` = 607 = exactly
  the number of Bronze documents, again one unfinished candle per API response.

### M-018 — Ingestion completeness (SUPERSEDED — the shortfall was my arithmetic error)
- **Value:** ~~3 missing bars, ≥99.9% completeness~~ — **retracted**
- **Label:** DERIVED — **withdrawn 2026-08-25**
- **Notes:** The "3 missing bars" were an error in the *expected* figure, not a real
  shortfall. I computed elapsed minutes using the Glue job's start time (19:36:23) as
  the endpoint, but Silver's newest bar necessarily lags that: the last ingestion ran
  ~19:33, and its final bar was in-progress and therefore dropped by design. Correct
  endpoint is 19:32. Superseded by M-022, which measures 100.000%.
  Kept rather than deleted because "the metric was wrong before the data was" is
  worth remembering — the error was in the baseline, not the pipeline.

### M-028 — Feature warm-up NULLs are exactly as designed (and detect gaps)
- **Value:** of 23,327 bars: **1 NULL `log_return`**, **20 NULL `rolling_vol`**
- **Label:** MEASURED
- **Measured on:** 2026-09-08
- **Parameters:** BTCUSDT, `VOL_WINDOW_BARS = 20`, time-based `rangeBetween` window
- **How:** `SELECT COUNT(*), COUNT(log_return), COUNT(rolling_vol) FROM ... clean_bars`
- **Raw output:**
  ```
  bars   with_return  with_vol
  23327  23326        23307
  ```
- **Notes:** Predicted 1 and 20 before running; both exact. One NULL return = one
  symbol with no prior close. Twenty NULL volatilities = the warm-up and nothing else.
  **Second-order finding:** because the window is TIME-based, any gap in the minute
  sequence would under-fill the window for ~20 subsequent bars and add NULLs. There
  are exactly 20. So this doubles as a gap check and extends M-022's zero-gap result
  from 50 hours to **16.2 days / 23,327 bars**.

### M-029 — Glue runtime scales with Bronze volume above ~600 documents
- **Value:** **205 s** at 4,688 Bronze documents, vs 90 s at ~607 and 94 s at ~12.
  Implied: **~90 s startup floor + ~0.028 s per document** beyond it.
- **Label:** MEASURED (three points); the linear fit is ESTIMATED
- **Measured on:** 2026-09-08
- **Parameters:** full reprocess, Glue 5.0, 2 × G.1X, 4,688 docs / 13.75 MB Bronze
- **How:** `ExecutionTime` from `aws glue get-job-run`; Bronze count from
  `aws s3 ls --recursive --summarize`
- **Notes:** **PARTIALLY CORRECTS M-019.** "Execution time is volume-independent" was
  true only within the narrow range measured (12–607 docs, all below the startup
  floor's shadow). With a third point 8× further out, volume clearly matters.
  A caution about the earlier conclusion rather than a refutation: M-020's 12.0×
  cadence multiple still holds, because *both* cadences read a bounded lookback in
  the final design — but the multiple was derived under an assumption that only
  survives if incremental processing exists.
  **Projection (ESTIMATED, linear fit on 3 points, and S3 listing is often
  superlinear so this may be optimistic):**
  ```
  1 symbol,  day 30   ~8,600 docs   ~5 min
  1 symbol,  day 90  ~26,000 docs  ~13 min
  17 symbols, today  ~79,700 docs  ~39 min   <-- exceeds the 15-minute job timeout
  ```
  **Consequence: slice 4.5 (incremental) is blocking for slice 5 (17 symbols),** not
  merely well-sequenced. Full reprocess would fail outright on the full basket.

### M-030 — Return distribution is heavily fat-tailed (sets slice 6 expectations)
- **Value:** mean |log_return| **3.354 bps**, mean `rolling_vol` **4.396 bps**,
  max |log_return| **142.84 bps** = **32.5× typical volatility**
- **Label:** MEASURED
- **Measured on:** 2026-09-08
- **Parameters:** BTCUSDT, 23,326 one-minute log returns, 16.2 days
- **Notes:** Under a normal distribution the largest of 23,327 draws sits near 4σ.
  Observing 32.5σ is overwhelming evidence of fat tails, not a near-miss.
  Internal consistency check: for a normal distribution E|X| = σ√(2/π) ≈ 0.798σ, so
  mean |return| of 3.354 bps implies σ ≈ 4.20 bps against a measured mean
  `rolling_vol` of 4.396 bps — close, with the excess in the direction fat tails
  predict.
  **Direct consequence for slice 6:** a |z| > 3 threshold will fire far more often
  than the ~0.27% normal theory implies. Not a bug — a property of the data. But it
  means `anomaly_events` will not be the "short, ranked list" README §2 describes,
  and the **measured** firing rate must replace the theoretical one in the README.

### M-031 — Bronze growth rate
- **Value:** **4,688 objects / 13,748,973 bytes** after 16.2 days at 1 symbol
  ⇒ **2,932 bytes/object**, **~289 objects and ~847 KB per day per symbol**
- **Label:** MEASURED
- **Measured on:** 2026-09-08
- **How:** `aws s3 ls s3://crypto-anomaly-bronze-.../ --recursive --summarize`
- **Notes:** Per-object size is stable across the whole history (2,932 bytes now vs
  2,949 measured at 701 objects vs 2,860 at 22). Extrapolating to 17 symbols:
  ~4,900 objects and ~14 MB per day, ~430 MB/month of Bronze. Storage cost is
  negligible (~1¢/month); the **object count** is what drives the runtime problem
  in M-029.

### M-027 — Athena scan reduction, decomposed (columnar vs partition pruning)
- **Value:** **26.6× fewer bytes scanned**, decomposing into **12.6× from columnar
  storage** and **2.10× from partition pruning**
- **Label:** MEASURED (all three arms)
- **Measured on:** 2026-08-25
- **Parameters:** BTCUSDT, 3,052 rows, 3 date partitions, 195.3 KiB Parquet/Snappy
- **How:** `DataScannedInBytes` from `aws athena get-query-execution` for three queries
- **Raw output:**
  ```
  all 12 columns, all partitions  (12 × MAX(...))            190,301 bytes
  one column,     all partitions  (AVG(close))                15,050 bytes
  one column,     one partition   (AVG(close) WHERE "date")    7,153 bytes
  ```
  ```
  columnar  : 190,301 / 15,050 = 12.64×
  pruning   :  15,050 /  7,153 =  2.10×
  combined  : 190,301 /  7,153 = 26.60×      (12.64 × 2.10 = 26.6 ✓)
  ```
- **Notes:** The 2.10× pruning figure has independent corroboration: the
  `date=2026-08-24` partition is 90.9 KiB of 195.3 KiB on disk = **46.5%**, and the
  scan ratio was 7,153/15,050 = **47.5%**. Two unrelated measurements agreeing.
  **CRITICAL CAVEAT FOR ANY COST CLAIM:** Athena bills a **10 MB minimum per query**.
  At 190 KB, all three of these queries cost *exactly the same*. The ratio is real and
  is what matters at scale — at 17 symbols over a year it is the difference between a
  ~$2 and a ~$56 query — but **it is saving zero dollars today.** Quoting a scan
  ratio on a 200 KB dataset as realised cost savings would be overclaiming.
  The two effects are independent and multiply: columnar comes from Parquet,
  pruning from the `symbol`/`date` partition layout.

### M-022 — Sequence completeness: zero gaps
- **Value:** **0 missing minutes** across 3,052 consecutive bars.
  `bars_present` = `bars_expected` = **3,052** ⇒ **100.000%**
- **Label:** MEASURED
- **Measured on:** 2026-08-25
- **Parameters:** BTCUSDT, 2026-08-23 16:41:00 → 2026-08-25 19:32:00 UTC (~50.9 h)
- **How:** Athena `LAG(open_time) OVER (PARTITION BY symbol ORDER BY open_time)`,
  filtering `date_diff('minute', prev, cur) > 1`
- **Raw output:**
  ```
  symbol  prev_open_time  open_time  gap_minutes
  (0 rows)

  symbol   bars  first_bar                last_bar                 expected
  BTCUSDT  3052  2026-08-23 16:41:00.000  2026-08-25 19:32:00.000  3052
  ```
- **Notes:** Two independent queries agree — no gap between any consecutive pair,
  and the count matches the expected span exactly. **Scope: 50.9 hours, one symbol.**
  This is not a long-run reliability claim and must not be quoted as uptime. Whether
  this reflects zero ingestion failures or failures healed by the 15-bar overlap is a
  separate question, answered by the Lambda `Errors` metric (pending).

### M-023 — Dedup verified against stored data
- **Value:** **0 rows** where `(symbol, open_time)` appears more than once
- **Label:** MEASURED
- **Measured on:** 2026-08-25
- **How:** `GROUP BY symbol, open_time HAVING COUNT(*) > 1` in Athena
- **Notes:** Distinct from M-017. That was the Spark job reporting on its own work;
  this interrogates the written artifact through a different engine entirely. The
  dedup key holds in the data, not just in the log line.

### M-024 — OHLC integrity
- **Value:** **0 violations** across 3,052 bars — no `high < low`, no `high` below
  open/close, no `low` above open/close
- **Label:** MEASURED
- **Measured on:** 2026-08-25
- **How:** aggregate CASE query in Athena (query 6 of `queries/silver_checks.sql`)
- **Notes:** Confirms the positional-index mapping in `cast_bars()` is correct. A
  mis-indexed array — reading `high` from position 3 instead of 2, say — would
  produce OHLC violations at scale immediately. Zero across 3,052 bars is meaningful
  evidence the Bronze→Silver contract is right.

### M-025 — Silver freshness lag (sets the DQ gate threshold)
- **Value:** newest Silver bar `open_time` was **4 min 23 s** behind the Glue job's
  start time (bar 19:32:00 vs job at 19:36:23); ~3 min 23 s behind that bar's close
- **Label:** MEASURED (n=1)
- **Measured on:** 2026-08-25
- **Notes:** This lag is **structural, not a defect.** It is the sum of the 5-minute
  ingestion interval and the deliberate exclusion of the in-progress candle.
  **Directly determines the DQ gate's freshness threshold at slice 7:** README §11
  specifies "latest bar within one interval of now," which at 1-minute bars would
  fail on every single run. A ~15-minute threshold leaves room for this 4.4-minute
  structural lag plus one missed ingestion cycle. n=1; worth re-measuring across
  several runs before fixing the constant.

### M-026 — Zero-trade bars: none yet, and why that matters
- **Value:** **0 bars** with `num_trades = 0` out of 3,052 (BTCUSDT)
- **Label:** MEASURED
- **Measured on:** 2026-08-25
- **Notes:** Expected — BTCUSDT trades every minute. The consequence is planning, not
  data: **the DQ gate's empty-bar check cannot be tested until the basket expands.**
  PEPEUSDT and SHIBUSDT are in the basket partly to produce these. Until slice 5 that
  check would be dead code validated only by unit tests, which is worth knowing
  before writing it.

### M-004 — Glue cost per cadence (SUPERSEDED by M-015 / M-016)
- **Value:** hourly cadence ~$42/month; 5-minute cadence ~$507/month; ratio ~12x
- **Label:** ESTIMATED — **not resume-eligible; superseded 2026-08-25**
- **Estimated on:** 2026-08-07
- **Parameters:** 2 Glue jobs per cycle, 2 DPU per job (2× G.1X, the ETL minimum),
  assumed 2 min execution per run, $0.44/DPU-hour, 1-minute minimum billing
- **How:** arithmetic on published AWS pricing, **not measurement**.
  `2 DPU × (2/60) hr × $0.44 = $0.0293/job/run` → `× 2 jobs × 720 cycles = $42.19/mo`
- **Notes:** The 2-minute execution time is a **guess** and is the entire load-bearing
  assumption. If real runs finish in under 1 minute they hit the billing floor and
  hourly drops to ~$21/mo; if they take 5 minutes it rises to ~$105/mo. Replaced at
  slice 8 by measured `ExecutionTime` from `aws glue get-job-run`.
  Superseded-by: M-0xx (slice 8).
- **Consequence recorded:** this estimate is why the Glue jobs are run **manually**
  during slices 1–7 rather than on a schedule, and why the hourly schedule is only
  enabled for a bounded ~3-day metrics window at slice 8.

### M-005 — Budget alarm correctness
- **Value:** $10/month alarm was set **before** the Glue cost was computed; the
  design as specified in the README would exceed it by ~4x
- **Label:** MEASURED (config error, self-reported)
- **Measured on:** 2026-08-07
- **Notes:** Kept at $10 deliberately after the discovery, re-purposed as a detector
  for "the hourly schedule was left running." Recorded because the sequence — set a
  budget, then discover the design exceeds it — is a real finding about the design,
  not just a mistake to erase.

---

## Open measurement-design questions

Resolved as we reach each stage. Recorded here so the reasoning is visible later.

1. **Two-cadence saving.** Cannot be measured by running both cadences for a month.
   Planned method: measure `ExecutionTime` for the same Glue job over a 5-minute
   input window and over a 60-minute input window, then compute hourly cost under
   each cadence, honoring Glue's per-run minimum billing duration (confirm the exact
   minimum against AWS docs at slice 8 — do not assume). Result is `DERIVED`, with
   both measured durations recorded.

2. **Monthly volume.** Measure bars/day over ≥3 complete days, report the measured
   daily figure, and label any monthly number `EXTRAPOLATED`. Note that the
   theoretical ceiling (17 × 1440 × 30 = 734,400) is an upper bound the real number
   will fall *below*, because of gaps and untraded minutes. The README's "~730K/month"
   is currently `ESTIMATED`.

3. **Latency is a distribution, not a number.** End-to-end `open_time` → queryable in
   Gold is dominated by the hourly batch window, so it ranges from roughly the Glue
   runtime to that plus ~60 minutes. Report p50 and max over ≥20 bars, never the
   minimum alone.

4. **Anomaly firing rate.** Must be recorded with `k`, the window length, and the
   symbol set. A rate measured on a basket containing PEPE/SHIB is not the same
   number as one measured on majors only.
