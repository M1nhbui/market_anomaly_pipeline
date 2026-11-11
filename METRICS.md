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

## Metrics taxonomy — the full collection plan

Capture broadly now, select for the resume at the end. A metric that turns out to be
uninteresting costs one command; a metric you wish you had recorded three weeks ago
costs three weeks.

Organising principle: at this data scale, **volume metrics are the weakest thing to
lead with** ("processed 730K rows" invites "that's a spreadsheet"). What persuades is
evidence the data is *correct*, and evidence a tradeoff was *measured* rather than
assumed. Secondary filter: **prefer deterministic metrics.** Glue runtime varies
enough at n=1 to be noise; bytes read does not vary at all.

### A. Correctness — "how do you know the data is right?"

| Metric | Status | How to capture |
|---|---|---|
| Duplicate rate removed by dedup | ✅ M-012, M-017 | job stats line |
| Duplicate keys in stored output | ✅ M-023 | Athena `GROUP BY ... HAVING COUNT(*) > 1` |
| Sequence gaps | ✅ M-022, M-028 | Athena `LAG(open_time)` |
| OHLC integrity violations | ⚠️ M-024 at 3,052 rows — **re-run at current scale** | Athena CASE aggregate |
| Unfinished bars removed | ✅ M-013 | job stats line |
| NULL rates match designed warm-up | ✅ M-028 | `COUNT(col)` vs `COUNT(*)` |
| Cross-engine agreement (Spark vs Athena) | ✅ M-017 vs M-023 | compare job log to SQL |
| **Idempotency: re-run → identical output** | ❌ **high value, cheap** | run job twice unchanged, diff per-date counts + checksums |
| Full-reprocess vs incremental equality in prod | ⚠️ unit-tested only | reprocess to scratch, `EXCEPT` both ways |
| Monotonic timestamps per symbol | ❌ | Athena `LAG` with `>=` check |
| Price continuity (no impossible jumps) | ❌ | Athena, flag `|log_return|` above a threshold |

### B. Cost — "what does it cost and what drives it?"

| Metric | Status | How to capture |
|---|---|---|
| Cost per Glue run | ✅ M-016 | `ExecutionTime` × DPU × rate |
| Two-cadence saving | ✅ M-020 | run-count ratio × per-run cost |
| Dominant cost driver identified | ✅ | reasoning + Cost Explorer |
| Actual month-to-date spend | ❌ **pending** | Cost Explorer, filter tag `Project` |
| Cost per million rows processed | ❌ | normalises across scale; comparable figure |
| Cost per GB scanned in Athena | ❌ | `DataScannedInBytes` × $5/TB |
| Failed-run cost | ⚠️ noted informally | failures bill too — 103 s failure = $0.025 |
| Storage cost per month | ❌ | S3 bytes × $0.023/GB |

### C. Latency / freshness — "how fresh is the data?"

| Metric | Status | How to capture |
|---|---|---|
| Structural freshness lag | ⚠️ M-025, n=1 | job run time − `MAX(open_time)` |
| **End-to-end p50 / p95 / max** | ❌ slice 8 | bar `open_time` → queryable in Gold, ≥20 samples |
| Lag by design vs lag by failure | ❌ | distinguishing these is the point |
| Ingestion-to-Bronze latency | ❌ | `ingested_at` − bar `close_time` |

### D. Reliability — "does it stay up?"

| Metric | Status | How to capture |
|---|---|---|
| Lambda error count | ⚠️ 2 errors, **unattributed** | CloudWatch `Errors`, hourly buckets |
| Run success rate (needs denominator) | ❌ | `Errors` ÷ `Invocations` |
| Self-healing evidence from overlap | ❌ | errors > 0 AND gaps = 0 |
| Data completeness over time | ✅ M-022 | gap query |
| Glue job success rate | ❌ | `get-job-runs` state histogram |
| DQ gate pass/fail history | ❌ slice 7 | per-run JSON written to S3 |

### E. Efficiency / scaling — "does it scale?"

| Metric | Status | How to capture |
|---|---|---|
| Storage reduction Bronze→Silver | ✅ M-021 | byte counts, decomposed |
| Athena scan reduction | ✅ M-027 | `DataScannedInBytes`, three arms |
| Files per partition | ✅ M-014 | object count ÷ partition count |
| **Read amplification (bytes read per new bar)** | ❌ **high value, deterministic** | bytes read ÷ new rows produced, full vs incremental |
| Rows/sec/DPU throughput | ❌ | rows ÷ (ExecutionTime × DPU) |
| Runtime vs input size curve | ⚠️ M-029/M-034 — **model not trustworthy** | needs ≥3 runs per point |
| Bronze growth rate | ✅ M-031 | object count and bytes over time |

### F. Operability — "could someone else run this?"

| Metric | Status | How to capture |
|---|---|---|
| Test count | ✅ 27 | pytest |
| Tests verified by mutation testing | ✅ | deliberately break code, confirm failures |
| Manual steps to deploy | ✅ zero beyond `terraform apply` | — |
| Recovery path exercised | ❌ | actually rebuild Silver from Bronze, time it |
| Time from clone to running | ❌ | wall-clock a fresh `apply` |
| Infrastructure resource count | ❌ | `terraform state list \| wc -l` |

### G. Signal quality — specific to the anomaly layer (slice 6)

| Metric | Status | How to capture |
|---|---|---|
| Anomaly firing rate at \|z\| > 3 | ❌ | count ÷ total bars, per day |
| Firing rate vs normal-theory expectation | ❌ | measured vs 0.27%; M-030 predicts a large gap |
| **Composition of firings** | ❌ **the honest-framing metric** | split genuine moves / DQ artifacts / scheduled volatility |
| Rate stability across days | ❌ | daily counts, coefficient of variation |
| Sensitivity to `k` and window length | ❌ | firing rate at k = 2, 3, 4, 5 |
| Precision / recall | 🚫 **NOT AVAILABLE** | no labels. Claiming it would breach the honesty boundary. |

### Claims to avoid

- **Raw volume as an achievement.** Context, never headline.
- **Unrealised cost savings.** The 26.6× Athena scan reduction is real but saves $0
  today because of the 10 MB per-query minimum. Design property, not a P&L line.
- **Uptime or SLA figures** from a few weeks of data.
- **Any implication the anomaly signal has predictive value.**
- **Percentage improvements against a baseline nobody would have chosen.**

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

### M-041 — Anomaly firing rate (the headline signal-volume number)
- **Value:** **1.3273%** of scored observations breach |z| > 3 — **916 events** from
  34,507 scorable bars × 2 rules. Split: **524 return_zscore, 392 volume_zscore.**
- **Label:** MEASURED
- **Measured on:** 2026-09-09
- **Parameters:** k = 3.0, 60-bar trailing window excluding the current bar,
  `MIN_ROLLING_STD` = 1e-6, volume scored as log1p, 17 symbols
- **How:** `SILVER_TO_GOLD_STATS` log line, cross-checked in Athena
- **Raw output:**
  ```
  {"total_bars": 34524, "scorable_bars": 34507, "total_events": 916,
   "events_by_rule": {"return_zscore": 524, "volume_zscore": 392},
   "firing_rate_pct": 1.3273, "zscore_window_bars": 60, "zscore_threshold": 3.0}
  ```
- **Notes:** Predicted 0.5–3% before running; landed inside. **4.9× the ~0.27% a
  normal distribution implies**, which is the fat-tail effect M-030 anticipated,
  now measured on the actual detector rather than inferred from a max-move statistic.
  **README §2 describes a "short, ranked list." 916 events in 17 days is not short**
  — that phrasing needs correcting (slice 9) to reflect the measured rate.
  **Do not quote the aggregate rate without the composition caveat below (M-043).**

### M-042 — Firing rate is not uniform across symbols
- **Value:** **BTCUSDT 1.468%** vs **1.099%** across the other 16 symbols combined.
  BTCUSDT produced **77.9% of all events** while holding **70.6% of all bars.**
- **Label:** MEASURED
- **Measured on:** 2026-09-09
- **Notes:** The aggregate 1.33% is dominated by BTCUSDT purely because it has 17
  days of history and the other 16 have ~1.5 days. **Per-symbol rates are the honest
  view; the aggregate is a weighted average of very unequal samples.** Re-measure once
  all symbols have comparable history before drawing any conclusion about which
  assets are more anomaly-prone — right now the difference is more plausibly a
  different observation window than a different market character.
  Tail comparison: `return_zscore` mean|z| 4.07 / max 33.4; `volume_zscore` mean|z|
  3.84 / max 9.3. The much shorter volume tail is direct evidence the log1p transform
  is doing its job — on raw volume that tail would be far heavier and one-sided.

### M-043 — Signal composition: a measured data-quality artifact in the output
- **Value:** 3 of the top 15 events by |z| are **SHIBUSDT `volume_zscore` with
  `num_trades = 0`** and NEGATIVE z-scores (−9.34, −8.46, −7.89)
- **Label:** MEASURED
- **Measured on:** 2026-09-09
- **Notes:** These are **untraded minutes being flagged as "anomalously low volume."**
  Volume was 0, so log1p(0) = 0 against a positive baseline mean, producing a large
  negative z. Nothing is broken — the arithmetic is correct — but the *event* is an
  absence of activity, not a market event worth a human's attention.
  This is README §4's "conflates genuine market events with data-quality glitches"
  **measured in our own output rather than disclaimed in prose.** It is the single
  best piece of evidence that the honesty boundary is a finding, not a hedge.
  Open design question: whether the volume rule should be **one-sided** (spikes only,
  `z > k`), which README §10's wording — "to catch volume spikes" — arguably implies,
  and which would remove all three artifacts.

### M-046 — Cross-engine validation of the z-score implementation
- **Value:** an independent SQL reimplementation in Athena/Trino reproduced the Spark
  job's volume-rule count **exactly: 392 = 392**
- **Label:** MEASURED
- **Measured on:** 2026-09-09
- **How:** `queries/volume_tail_composition.sql` recomputes log1p volume z-scores from
  Silver using Trino window functions (`RANGE BETWEEN 3600 PRECEDING AND 60
  PRECEDING`, ≥60 observations, stddev > 1e-6) — matching the PySpark window exactly
  but sharing no code
- **Raw output:**
  ```
  upper_tail  lower_tail  lower_untraded  two_sided_total
  334         58          13              392
  ```
- **Notes:** Two implementations, two engines, no shared code, identical answer. This
  is a stronger correctness claim than the Spark job reporting its own counts — the
  same reason M-023 was more convincing than M-017.
  **Triple accounting check:** original run 524 + 392 = 916; SQL says 334 + 58 = 392;
  new one-sided run 524 + 334 = 858; and the hour-of-day histogram totals **858**
  independently. Four numbers, three derivations, all consistent.

### M-047 — What the one-sided volume change actually removed
- **Value:** removed **58 events (14.8% of 392 volume events)**, of which only
  **13 were untraded minutes**. The other **45 had real trading, just unusually
  little.**
- **Label:** MEASURED
- **Measured on:** 2026-09-09
- **Notes:** **This corrects my own framing.** I justified the one-sided change
  primarily on the SHIBUSDT untraded-minute artifact, which implied the lower tail was
  mostly junk. It wasn't: the pure artifact is **1.42% of all 916 events**, and 45 of
  the 58 removed events were legitimate low-volume observations.
  What justifies the change is not volume of junk but **where** the junk sat: the
  untraded-minute events were **20% of the top 15 by |z|** — concentrated at the
  extreme tail, which is precisely the part of a ranked list anyone actually reads.
  A 1.4% contaminant that occupies a fifth of the top of the list matters far more
  than its share suggests.
  The honest statement of the tradeoff: the rule now misses genuine low-volume
  events, including a real trading halt, in exchange for a clean upper tail.

### M-048 — Hour-of-day structure in anomaly events
- **Value:** significant non-uniformity. Against 35.8 events/hour expected
  (Poisson sd 5.98): **hour 07 UTC = 70 (z=+5.73)**, **hour 13 UTC = 66 (z=+5.06)**,
  **hour 09 = 54 (z=+3.05)**, **hour 17 = 12 (z=−3.97)**.
- **Label:** MEASURED
- **Measured on:** 2026-09-09 (858 events, 17 days, one-sided volume rule)
- **How:** `GROUP BY hour(event_time)` in Athena; Poisson z against a uniform null
- **Notes:** All four survive a Bonferroni correction for 48 tests (24 hours × 2
  statistics, threshold |z| > 2.87). Candidate explanation for hour 13: the US equity
  open at 13:30 UTC falls inside it. Hour 07 has no explanation I am confident in and
  **should not be given one** without evidence.
  **Major caveat: 78% of these events are BTCUSDT** (M-042), and the other 16 symbols
  have only ~1.5 days of history. This is close to a BTC hour-of-day profile, not a
  market-wide one. Re-measure once all symbols have comparable coverage.

### M-044 — Candidate scheduled-volatility clustering (HYPOTHESIS NOT SUPPORTED)
- **Value:** two events at **exactly 12:30:00 UTC** on different days, including the
  largest event in the dataset (2026-09-04, z = −33.39) and 2026-08-26 (z = −8.39)
- **Label:** **HYPOTHESIS TESTED AND NOT SUPPORTED** — retained as a record of a
  rejected idea
- **Tested on:** 2026-09-09 via the hour-of-day histogram (M-048)
- **Result:** Hour 12 is **not** a count outlier — 44 events against 35.8 expected
  (z = +1.38). It *is* a magnitude outlier at first glance: mean|z| 4.91 against 3.94
  across hours (z = +3.17).
  **But that finding is driven almost entirely by one event.** The single
  2026-09-04 12:30 event (|z| = 33.39) contributes **15.5% of hour 12's total |z|**.
  Removing it drops hour 12's mean|z| to 4.25, or **z = +1.00 — no longer
  significant, and comfortably below the Bonferroni threshold of 2.87.**
- **Verdict:** the original observation was **pattern-matching on n=2**. There is no
  evidence of elevated anomaly frequency at 12:30 UTC in this dataset; a single very
  large event happened to land there, and I built a story around it.
  Recorded rather than deleted because "I formed a hypothesis, tested it properly,
  and it failed" is worth as much as a confirmed one — and because a one-event-driven
  mean is a mistake that is easy to make and easy to miss.
  The genuine hour-of-day structure that *does* survive testing is in M-048, and it
  is at hours 07, 09, 13 and 17 — not 12.

### M-045 — Silver→Gold job runtime
- **Value:** **98 s** for 34,524 input rows → 916 output rows
- **Label:** MEASURED (n=1)
- **Measured on:** 2026-09-09
- **Notes:** Near the ~90 s startup floor, consistent with reading ~2 MB of Parquet
  being essentially free. Supports the decision to read all of Silver rather than
  build incremental logic for this job. n=1 and Glue timing is nondeterministic.

### M-037 — Warm-up invariant holds identically across all 17 symbols
- **Value:** **every one of 17 symbols has exactly 20 NULL `rolling_vol`** — the 16
  newly added symbols and BTCUSDT alike, despite BTCUSDT having 24,377 bars and the
  others 634
- **Label:** MEASURED
- **Measured on:** 2026-09-09
- **How:** Athena `SELECT symbol, COUNT(*), COUNT(rolling_vol) ... GROUP BY symbol`
- **Raw output:**
  ```
  BTCUSDT   24377  24357   -> 20 NULL
  ADAUSDT     634    614   -> 20 NULL
  ... all 17 symbols: bars - with_vol = 20
  ```
- **Notes:** Predicted "+20 NULL per new symbol" before running; confirmed for all 16.
  This is **17 independent confirmations of the same invariant** — a symbol beginning
  its history today warms up exactly as BTCUSDT did seventeen days ago, and adding
  symbols did not disturb the existing one (BTCUSDT still exactly 20).
  Also confirms `partitionBy("symbol")` isolates the series correctly: without it,
  the first bar of each new symbol would have computed a return against a different
  asset's price, and the NULL counts would not be uniform.

### M-038 — Ingestion Lambda at 17 symbols
- **Value:** **10,746 ms** mean (vs 738 ms at 1 symbol) ⇒ **113 ms fixed overhead +
  625 ms marginal per symbol**. Memory **99 MB, unchanged from 1 symbol.**
- **Label:** MEASURED (n=2 at 17 symbols, n=3 at 1 symbol)
- **Measured on:** 2026-09-09
- **How:** CloudWatch REPORT lines before and after the basket expansion
- **Raw output:**
  ```
  1 symbol :  763.57 / 714.11 / 736.43 ms      Max Memory Used: 99 MB
  17 symbols: 10804.72 (cold, init 470ms) / 10687.27 ms   Max Memory Used: 99 / 98 MB
  ```
- **Notes:** Predicted ~14,300 ms by scaling the 1-symbol figure linearly; actual is
  **25% faster**, because the 738 ms single-symbol time included ~113 ms of fixed
  per-invocation overhead that does not repeat. Linear extrapolation from a
  measurement containing fixed costs overstates — a small lesson worth keeping.
  **Memory closes out M-007:** 99 MB at both 1 and 17 symbols confirms memory does
  not scale with basket size, since symbols are processed one at a time rather than
  accumulated. **Recommendation: leave `memory_size` at 256 MB.** Reducing it would
  save nothing — 23,211 GB-s/month is 5.8% of the always-free 400,000 allowance — and
  headroom costs nothing. Tuning here would be optimisation theatre.
- **RISK IDENTIFIED (not yet mitigated):** timeout utilisation is a comfortable
  10.7 s of 60 s, **but the retry path is not bounded.** With `MAX_ATTEMPTS = 3` and a
  20-second cap on honouring `Retry-After`, a broad HTTP 429 event could cost
  `17 symbols × 2 sleeps × 20 s = 680 s` against a 60-second timeout. The function
  would be killed mid-batch with no return value and no structured failure report.
  Fix: a run-level deadline that stops retrying as the timeout approaches.

### M-039 — Empty-bar rates over a real sample (sets the DQ threshold)
- **Value:** over 634 bars per symbol — **SHIBUSDT 1.89%** (12), **ATOMUSDT 0.63%**
  (4), **ADAUSDT 0.16%** (1), all other 14 symbols **0%**
- **Label:** MEASURED
- **Measured on:** 2026-09-09 (~10.6 hours of coverage per new symbol)
- **How:** Athena `SUM(CASE WHEN num_trades = 0 THEN 1 ELSE 0 END) GROUP BY symbol`
- **Notes:** Supersedes M-036's 30-bar snapshot, which suggested ~3.4% for three
  symbols. Over a 21× larger sample the real rate is lower and concentrated in
  SHIBUSDT. **PEPEUSDT shows 0%** — contrary to the expectation that both meme pairs
  would be the illiquid ones; PEPE is evidently the more actively traded of the two.
  **Slice 7 design input:** README §11 specifies flagging `num_trades = 0`. A hard
  fail on any empty bar would trip on SHIBUSDT constantly. The check must be a
  per-symbol **ratio with a tolerance**, and on this evidence the tolerance belongs
  around 5% — comfortably above the observed 1.89% but low enough to catch a genuine
  feed failure.
  **Caveat: 10.6 hours does not include a full overnight cycle**, when liquidity is
  thinnest and rates will be worse. Re-measure across a full 24 hours before fixing
  the constant.

### M-040 — Glue runtime at 17 symbols (partial history)
- **Value:** **178 s** (2m58s) reading ~2,900 Bronze documents across 17 symbols
- **Label:** MEASURED (n=1)
- **Measured on:** 2026-09-09
- **Parameters:** incremental, 3-hour lookback; BTCUSDT has full history, the other
  16 symbols only ~10.6 hours, so the 3-day lookback is not yet full for them
- **Notes:** Against 150 s at ~600 documents (1 symbol). My earlier projection for
  17 symbols was ~487 s; the trajectory now suggests ~320 s at steady state — but
  **both figures come from a runtime model that has been wrong twice**, and the
  operator has correctly observed Glue timing is nondeterministic at n=1. Treat as
  directional only.
  **Testable prediction: runtime will RISE over the next ~3 days** as the new symbols
  accumulate enough history to fill the 3-day lookback window, then plateau. That
  plateau is the number worth measuring — everything before it understates.
  Comfortably inside the 25-minute timeout either way.

### M-035 — Delisted symbol caught by bar-freshness validation
- **Value:** **1 of 18 candidates rejected.** MATICUSDT returned HTTP 200 with a
  newest closed bar **1,049,854 minutes (729 days) old**; 17 symbols admitted.
- **Label:** MEASURED
- **Measured on:** 2026-09-09
- **Parameters:** newest closed bar under 15 min old, plus nonzero trading activity
  across the last 30 bars
- **How:** `python3 scripts/validate_basket.py`
- **Raw output:**
  ```
  MATICUSDT  NO   1049854.0  100.0  524  STALE - newest closed bar is 1049854 min old
  POLUSDT    YES        0.0   96.6 1482  ok
  ADMITTED (17)   REJECTED (1): ['MATICUSDT']
  ```
- **Notes:** Corresponds to Polygon's MATIC→POL migration (~Sept 2024). **The earlier
  status-only probe reported "18/18 symbols OK" and would have admitted it** — HTTP
  200 proves a symbol is *known* to the exchange, not that it is *trading*.
  Concrete consequence had it been admitted: ingestion would refetch the same 15
  bars from 2024 every 5 minutes forever; Silver would file them under
  `date=2024-09-11`, outside the partition-projection range and therefore **invisible
  to Athena**; and the DQ freshness check would fail permanently on that symbol. A
  silent failure with no error surface anywhere.
  README §18 still lists MATICUSDT and needs correcting (slice 9).

### M-036 — Empty-bar rate for less liquid symbols (unblocks the DQ gate design)
- **Value:** **96.6% of the last 30 bars traded** for ATOMUSDT, POLUSDT and SHIBUSDT
  — i.e. **~3.4% untraded minutes.** BTCUSDT and the majors: 100%.
- **Label:** MEASURED (n=30 bars per symbol, single point in time)
- **Measured on:** 2026-09-09
- **How:** `scripts/validate_basket.py`, `traded%` column
- **Notes:** Supersedes M-026's "cannot test until the basket expands" — the case is
  visible already. **Directly sets a design constraint for slice 7:** README §11
  specifies flagging `num_trades = 0`, but a hard fail on any empty bar would trip on
  roughly 1 bar in 30 for three of the seventeen symbols, firing constantly and
  training the operator to ignore the alert. The check must be a **ratio with a
  tolerance**, and the tolerance must sit above ~3.4%.
  Caveat: 30 bars at one moment is a small sample and liquidity varies by hour —
  overnight rates will be worse. Re-measure across a full day before fixing the
  threshold.

### M-032 — Incremental processing verified non-destructive on production data
- **Value:** **16 of 17 date partitions byte-identical** before and after an
  incremental run. Only `2026-09-08` changed (1,288 → 1,440 bars, completing the
  day) and `2026-09-09` was created (113 bars).
- **Label:** MEASURED
- **Measured on:** 2026-09-09
- **Parameters:** `--lookback-hours 3`, targets `[2026-09-08, 2026-09-09]`,
  warm-up read of `2026-09-07`; 23,327 → 23,592 rows (+265 bars = 4.42 h elapsed)
- **How:** identical `GROUP BY "date"` Athena query run immediately before and after
- **Raw output:**
  ```
  before: 08-23:439/419  08-24..09-07: 1440/1440 each  09-08: 1288/1288
  after : 08-23:439/419  08-24..09-07: 1440/1440 each  09-08: 1440/1440  09-09: 113/113
  ```
- **Notes:** This is the property the design exists to guarantee: dynamic partition
  overwrite replaces WHOLE `(symbol, date)` partitions, so a lookback that wrote a
  partial day would have destroyed the rest of it. Fifteen untouched dates with
  unchanged counts is that risk measured away on real data rather than argued about.

### M-033 — Warm-up read works across a midnight boundary (real data)
- **Value:** `2026-09-09` has **113 bars and 113 non-NULL `rolling_vol`** — zero NULLs
  at the day boundary. Total NULL `rolling_vol` across all 23,592 rows: **exactly 20.**
- **Label:** MEASURED
- **Measured on:** 2026-09-09
- **Notes:** The sharpest single confirmation in the project so far. Without the
  warm-up day, the first ~20 bars of every newly-written date would have NULL
  volatility, and 09-09 would read `113 / 93`. Instead the only NULLs in the entire
  dataset are the original 20 at the very start of history (08-23: 439/419), which is
  the one place they are genuinely correct.
  This is a **silent** failure mode: NULL volatility means the Gold layer skips those
  bars, so a broken warm-up would quietly delete the first 20 minutes of every day
  from `anomaly_events` with no error anywhere.

### M-034 — Incremental vs full reprocess runtime
- **Value:** **150 s incremental** vs **205 s full reprocess** on the same dataset —
  **27% faster today**, and bounded rather than growing
- **Label:** MEASURED (n=1 each)
- **Measured on:** 2026-09-09
- **Parameters:** incremental read ≈ 3 days (~600 Bronze docs); full read = 4,688 docs
- **Notes:** **My prediction was wrong.** I predicted incremental would drop "toward
  the ~90 s floor" since it reads a comparable number of documents to the 607-doc run
  that took 90 s. It took 150 s. Same input size, 67% slower — so the M-029 model
  (`90 s + 0.028 s/doc`) does not capture everything. Unexplained candidates, none
  measured: dynamic partition overwrite must enumerate and delete existing files, and
  Silver now has 17 partitions instead of 3; plus ordinary Glue cold-start variance
  on n=1. **Do not quote a runtime model until this is re-measured** with several
  runs of each mode on the same day.
  The durable claim is not the 27%: it is that incremental runtime is **bounded by
  the lookback** while full reprocess grows with accumulated history without limit.
- **FORWARD RISK for slice 5:** at 17 symbols the incremental read becomes ~14,688
  docs (3 days × 17 symbols), projecting to **~8 minutes against a 15-minute job
  timeout.** Uncomfortably tight. Mitigations, in preference order: replace the
  whole-day warm-up with a precise hour-level one (~33% less read); raise the
  timeout; add workers. Note that **adding workers would now actually help** — at
  slice 2 it would not have, because the job was startup-bound. The right answer
  changed because the workload did.

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

### M-049 — Unattended ingestion over a two-month gap
- **Value:** **303,678 Bronze objects / 831,439,491 bytes** on 2026-11-09, after ~61
  days running unattended at 17 symbols. 298,990 objects added since M-031.
- **Label:** MEASURED
- **Measured on:** 2026-11-09
- **How:** `aws s3 ls s3://crypto-anomaly-bronze-.../ --recursive --summarize`
- **Notes:** Expected from schedule arithmetic (17,583 five-minute runs × 17 symbols,
  plus the BTC-only tail and one manual invoke) ≈ 299,000 — the actual count lands
  within roughly one run's worth of that. **Consistent with essentially every
  scheduled run writing all 17 symbols for two months, but my baseline is only
  accurate to about ±1 run, so this is not yet a reliability figure.** The CloudWatch
  `Errors` and `Invocations` metrics over the same period are the precise version.
  Per-object size in the 17-symbol era: **2,735 bytes** (vs 2,932 for BTC alone —
  smaller price strings for the other pairs).

### M-050 — Ingestion Lambda after two months
- **Value:** **10,960 ms** mean (n=6), **95 MB** max memory, 2026-11-09 05:23–05:48 UTC
- **Label:** MEASURED
- **Notes:** Within 2% of the 10,746 ms measured at expansion (M-038), memory flat.
  No drift over two months of continuous operation.

### M-051 — Projected ingestion cost: S3 PUT requests, not storage
- **Value:** ~146,880 PUTs/month ⇒ **~$0.73/month**; storage of 831 MB ⇒ **~$0.02/month**
- **Label:** ESTIMATED — to be checked against Cost Explorer
- **How:** `17 symbols × 288 runs/day × 30 days` PUTs × $0.005 per 1,000 (S3 Standard
  list price, assumed); bytes × $0.023/GB-month
- **Notes:** The prediction is that S3's line item is dominated by **request count**,
  not bytes — tiny objects every 5 minutes are cheap to store and comparatively
  expensive to write. Over the 61-day gap that is ~$1.49 of PUTs. If Cost Explorer
  shows S3 near $1.50 for Sep–Nov, this is confirmed; if it shows cents, the pricing
  assumption is wrong. Lambda should show ~$0 (≈23,700 GB-s/month, inside the
  always-free allowance).

### M-052 — Actual cost to date; M-051 prediction CONFIRMED
- **Value:** **$1.96 total** project spend, 2026-08-01 → 2026-11-09. By month: Aug
  $0.099, Sep $0.872, Oct $0.787, Nov (8 days) $0.201. October (first full month of
  unattended 17-symbol ingestion): **S3 $0.783, of which Tier-1 requests $0.759
  (151,815 requests) and storage $0.011**; Lambda $0.0039; Glue $0 (not scheduled).
- **Label:** MEASURED
- **Measured on:** 2026-11-09
- **How:** `aws ce get-cost-and-usage --time-period Start=2026-08-01,End=2026-11-09
  --granularity MONTHLY --metrics UnblendedCost --group-by Type=DIMENSION,Key=SERVICE`,
  then the same filtered to S3 and grouped by `USAGE_TYPE`.
- **Notes:** M-051 predicted 146,880 PUTs for a 30-day month; October has 31 days, so
  the prediction becomes 17 × 288 × 31 = **151,776 — measured 151,815 (+0.03%)**, the
  residual being Glue/Athena writes. Requests are **69× storage** cost, as predicted.
  Ingestion's steady-state cost is therefore **~$0.79/month**, essentially all S3 PUTs.
  Lifetime Glue spend is $0.345 — every manual run of both jobs since slice 2.
  **Caveat — these are account totals, not project totals.** ~$0.012/month of
  `APS1-TimedStorage` is ap-southeast-1, where three unrelated buckets live
  (`my-soccer-bigdata-*`, `news-sentiment-*`, `aws-logs-*`). The project is ~$0.012/month
  less than the S3 line shows; immaterial, but a tag-based cost query (`Project` default
  tag, slice 0) is the exact version.

### M-053 — Backfill calibration step: correctness predicted exactly, 323 s
- **Value:** one step (`as_of=2026-09-10T00:30Z`, lookback 1h) **SUCCEEDED in 323 s**.
  Silver `2026-09-09`: **20,259 bars / 19,939 with `rolling_vol`** — predicted
  20,259 / 19,939 before the run. Every symbol's last bar 23:59.
- **Label:** MEASURED
- **Measured on:** 2026-11-09
- **How:** `./scripts/backfill_silver.sh 2026-09-09 2026-09-09` (run id in
  `backfill_log.csv`); before/after Athena `GROUP BY ROLLUP(symbol)` on `date='2026-09-09'`.
- **Notes:**
  - **Before:** 11,045 bars (frozen at 14:57 when Silver was last built). **After:**
    20,259. Prediction derived from each symbol's first bar: BTC 1,440 (full day);
    BNB/ETH/SOL first bar 04:23 ⇒ 1,177; the other 13 first bar 04:24 ⇒ 1,176. NULL
    `rolling_vol` = 16 new symbols × 20 warm-up minutes = 320, unchanged.
  - **An earlier prediction of mine was wrong first:** I had told the operator to
    expect 24,480 (17 × 1,440). The 16 new symbols only began ingesting at ~04:23 on
    09-09. Corrected before the run, from the data.
  - `2026-09-08` untouched (1,440, BTC only) — the warm-up day was read and not written.
  - `2026-09-10` written **24,446 of 24,480 — short by exactly 34 = 17 × 2 minutes**
    (23:58, 23:59, ingested on 09-11). I had said the tail day would hold "only its
    first few minutes"; wrong — the job reads whole Bronze *ingest-days*, and `as_of`
    selects dates, it does not truncate them. The next step rewrites it either way.
  - Input ≈ 9,250 Bronze docs (09-08 BTC only, 09-09 partly 17 symbols, 09-10 full).
    323 s fits the slice-4 model (~90 s + ~0.025 s/doc) better than anything since —
    n=1, not yet quotable. Cost ≈ 2 DPU × 323 s × $0.44/DPU-h ≈ **$0.08**.

### M-054 — Backfill runtime distribution (n=20, constant input size)
- **Value:** 20 × 3-day steps, all SUCCEEDED. **p50 574 s, mean 575.5 s, min 517,
  max 627, stdev 25.3 s (CV 4.4%).** Total 11,510 s of Glue ⇒ 2 DPU × 3.20 h × $0.44 ≈
  **$2.81** (DERIVED from list price; confirm in Cost Explorer after the ~24 h lag).
- **Label:** MEASURED (timings); DERIVED (cost)
- **Measured on:** 2026-11-09, 07:06–10:25 UTC
- **How:** `./scripts/backfill_silver.sh 2026-09-10 2026-11-08 3` (resumed from
  2026-09-13 after bug #16); timings from `backfill_log.csv`.
- **Notes:**
  - **Input size is constant for 18 of the 20.** Each reads 5 Bronze ingest-days at 17
    symbols (≈ 24,480 documents). Two are smaller: the first (warm-up day 09-09, only
    partly 17-symbol; 576 s) and the last (read 11-05 → 11-09, where 11-09 held ~10 h;
    517 s — the minimum). **Over the 18 full-size steps: p50 575 s, mean 578.7 s, min
    548, max 627, stdev 22.4 s (CV 3.9%).**
  - At fixed input, run-to-run spread is ±3.9%. **September's 90 s vs 150 s on similar
    input remains unexplained** — those runs also used different code (features
    arrived in slice 4, the incremental path in 4.5), so this sample does not explain
    them.
  - Predicted ~700 s per step from the slice-4 model (~90 s + 0.025 s/doc); measured
    ~575 s — **over-predicts by ~18%** at this size.
  - 18 of 18 back-to-back starts needed exactly one 30 s retry for the concurrency
    slot (bug #16), plus the original failure; the first step after resuming did
    not. Wall-clock per step ≈ 10.5 min.

### M-055 — Zero gaps across 79 days and 17 symbols (Silver after backfill)
- **Value:** **1,522,959 bars; 0 missing minutes for any symbol over its entire
  history** (BTCUSDT from 2026-08-23 16:41, the other 16 from 2026-09-09 04:23/04:24,
  through 2026-11-09 10:12). **Every day 2026-09-10 → 2026-11-08 has exactly 24,480
  bars (17 × 1,440) with 24,480 non-NULL `rolling_vol`.**
- **Label:** MEASURED
- **Measured on:** 2026-11-09 ~10:30 UTC
- **How:** Athena — (1) per-day `COUNT(*)` / `COUNT(rolling_vol)` with
  `HAVING COUNT(*) <> 24480 OR COUNT(rolling_vol) <> COUNT(*)` → only 09-09 (partial
  first day, 20,259 as M-053) and 11-09 (today, 10,421 = 17 × 613 min to 10:12);
  (2) `LAG(open_time)` gap query → **0 rows**; (3) totals: NULL `rolling_vol` = **340
  = 17 symbols × 20 warm-up minutes**, NULL `log_return` = **17 = 1 per symbol** —
  both exact, and both would grow with any gap since the windows are time-based.
- **Supporting:** CloudWatch `AWS/Lambda Errors` for `crypto-anomaly-ingestion`,
  2026-09-09 → 2026-11-09: **0**. `Invocations`: **288 on every one of 60 days**
  (17,280 total) — not one missed scheduled run.
- **Notes:** Extends the zero-gap result from 16 days at 1 symbol (M-022/M-026) to
  **79 days, and 61 days at 17 symbols, unattended.** Three independent methods agree
  (row counts, gap query, warm-up NULL arithmetic) plus the ingestion side's own
  metrics. Quotable: "ingested 1.52M one-minute bars across 17 pairs with zero missing
  minutes over two months of unattended operation."

### M-056 — First production DQ gate run: predicted failures, exactly
- **Value:** 2026-11-09 19:35 UTC against Silver ending 10:12 UTC. **Blocking: `freshness`
  (563.0 min vs 15) and `row_count` (60.97% vs 70%)** — the two predicted before the
  run. The other 6 checks passed: 17/17 symbols, 0 OHLC violations, 0 non-positive
  prices, 0 non-monotonic, empty bars 1.21% (WARN limit 5%), gaps 0.0% (WARN limit 0.5%).
  Duration 3,773 ms, 94 MB, cold start 446 ms.
- **Label:** MEASURED
- **Measured on:** 2026-11-09
- **How:** `aws lambda invoke --function-name crypto-anomaly-dq-gate dq_response.json`;
  results read from `s3://crypto-anomaly-gold-*/dq_results/date=2026-11-09/dq_193500_77018376.json`.
- **Notes:** The prediction was derived from the window arithmetic: 10:12 → 19:35 is 563
  min, so (1440 − 563) / 1440 = 60.9% of the window present — under 70% once Silver is
  more than ~7.2 h stale. **The invocation itself crashed** (`KeyError: 'check'`, bug
  #17) *after* persisting, so the history row exists but no `quality_passed` was
  returned. Empty-bar ratio 1.21% across 17 symbols sits well inside the 5% WARN limit
  set from SHIB's 1.89% (M-039).

### M-057 — DQ gate demonstration: FAIL on stale Silver, PASS after refresh
- **Value:** 19:38 UTC `quality_passed: false` (blocking: row_count 60.97%, freshness
  563 min) → Silver incremental run (308 s) → 19:44 UTC **`quality_passed: true`**,
  8/8 checks: row_count **99.51%**, freshness **7.75 min**, 17/17 symbols, 0 OHLC / price /
  ordering violations, empty bars 0.98%, gaps 0.0%. No warnings.
- **Label:** MEASURED
- **Measured on:** 2026-11-09
- **How:** `aws lambda invoke --function-name crypto-anomaly-dq-gate`, before and after
  `aws glue start-job-run --job-name crypto-anomaly-bronze-to-silver` (default 3 h
  lookback); per-check rows in `gold/dq_results/date=2026-11-09/`.
- **Notes:**
  - Predicted before the refresh: newest bar ~19:37, freshness 8–12 min, pass.
    Measured: newest bar **19:37:00**, freshness 7.75 min. Silver run 308 s against a
    4.5–5.5 min prediction (similar input to the 323 s calibration step).
  - Nov 9 rewritten as 20,026 bars = 17 × 1,178 minutes (00:00–19:37): complete.
  - **The job's `duplicate_rate_pct` read 83.94% on this run — not a duplication
    measure in incremental mode.** Raw records include the warm-up day (Nov 8), which
    is read and then filtered out rather than written, and the stat counts those as
    "duplicates removed." Only full-reprocess runs (M-012, M-017) measure the true
    duplicate rate. Do not quote incremental-run values.
  - First demonstration of the gate doing its job on real data. With slice 8, the
    FAIL path routes to an SNS alert instead of running silver_to_gold.

### M-058 — Gold rebuilt over 79 days: firing rate 0.98%, three predictions missed
- **Value:** silver_to_gold **SUCCEEDED in 133 s** over **1,532,564** Silver bars
  (1,532,547 scorable). **30,156 events = 0.9839% firing rate**: 21,029 return_zscore
  (1.372% of bars), 9,127 volume_zscore (0.596%). Return events 49.7% negative (10,450),
  i.e. symmetric. **0 events on untraded minutes** (the one-sided volume rule holds).
- **Label:** MEASURED
- **Measured on:** 2026-11-09 20:26 UTC
- **How:** `aws glue start-job-run --job-name crypto-anomaly-silver-to-gold`;
  `SILVER_TO_GOLD_STATS` log line; per-symbol rates by Athena join of Gold on Silver.
- **Predictions (logged before the run) vs measured:**
  - Runtime 3–6 min → **133 s. Missed.** 44× the input of the 98 s September run cost
    only 1.36× the time. silver_to_gold is far more startup-dominated than
    bronze_to_silver: it reads ~1.5M rows of compact Parquet, not ~25k small JSON files.
  - Events 33k–43k / firing 1.1–1.4% → **30,156 / 0.98%. Missed low.** The prediction
    extrapolated September's rate, which was ~95% BTC data. Per symbol: BTC fires 3.04%
    of bars (1.74% return + 1.30% volume); ATOM's volume rule 0.055%, SHIB 0.063%, DOT
    0.094%. **The volume rule fires 24× less on thin pairs than on BTC.** Hypothesis
    (not tested): a thin pair's minute volume is so erratic that its trailing σ of
    log1p(volume) is wide, so a spike rarely clears 3σ. SHIB and PEPE also fire the
    *return* rule least (0.60%, 0.91%) — possibly price-tick discreteness; untested.
  - 13 UTC remains a peak → **held** (1,921 events, the top hour).
  - 07 UTC peak (M-048) → **gone** (1,243 events, Poisson z −0.38). It was a feature of
    the BTC-only sample.
- **Notes:** The **return rule alone fires at 1.37%**, close to September's 1.52%, so the
  headline drop is almost entirely the volume rule meeting illiquid pairs. Supersedes
  M-041 as the firing-rate figure; M-041 stays as the 17-day, BTC-dominated value.

### M-059 — Return anomalies cluster on specific US-market minutes
- **Value:** return_zscore events by minute of day (UTC); mean per minute slot 14.6.
  **12:30 → 100 events on 19 distinct days, all 17 pairs, mean |z| 9.43** (the highest
  of any minute). **13:30–13:37 → 74–105 per minute** on 14–20 days. **22:00–22:02 →
  93–132** on 24–26 days. **20:01 → 68** on 14 days.
- **Label:** MEASURED (counts); the explanations are **ALIGNMENT, not tested cause**
- **Measured on:** 2026-11-09
- **How:** `SELECT date_format(event_time,'%H:%i'), COUNT(*), COUNT(DISTINCT date(event_time)),
  COUNT(DISTINCT symbol), AVG(abs(zscore)) FROM gold.anomaly_events WHERE rule='return_zscore'
  GROUP BY 1 ORDER BY 2 DESC`
- **Notes:**
  - Times in US Eastern daylight time (in force until 2026-11-01): 12:30 UTC = 08:30 ET,
    the slot for US macro releases; 13:30 = 09:30 ET, NYSE open; 20:00 = 16:00 ET, NYSE
    close; 22:00 = 18:00 ET, when CME futures reopen after the daily break. None of these
    has been matched against an actual release calendar.
  - **Revisits M-044.** The 12:30 hypothesis was marked NOT SUPPORTED in September on two
    events, one carrying the whole effect. That verdict was right for that evidence.
    With 79 days it appears on 19 separate days across every pair. Recorded as a
    revision driven by more data, not a reversal of method.
  - Events within one minute are **not independent**: a market-wide shock flags most
    of the 17 pairs at once. "Distinct days" is the honest count, which is why it is
    shown. Hour-level Poisson z-scores (13 UTC z = 18.8, 12 UTC z = 15.4) overstate
    significance for the same reason.
  - From 2026-11-01 (EST), the same ET events land one hour later in UTC; 13:30 UTC
    then carries both the 08:30 releases and nothing of the open. The minute profile
    will blur unless analysed in ET.

### M-060 — Per-run cost at 17 pairs: M-020's dollar figures superseded
- **Value:** one run (Silver → gate → Gold) ≈ **$0.108**. Hourly ≈ **$77.6/month**;
  daily ≈ **$3.23/month**.
- **Label:** DERIVED (from two MEASURED durations, each n=1)
- **Measured on:** 2026-11-10 (derivation; durations from 2026-11-09)
- **How:** Silver 308 s (M-057, 3 h lookback) + Gold 133 s (M-058, full rebuild) =
  441 s × 2 DPU ÷ 3600 × $0.44/DPU-h = $0.1078. Gate Lambda, Athena and Step Functions
  transitions add < $0.001. × 720 runs = $77.6; × 30 = $3.23.
- **Notes:**
  - **Correction, 2026-11-10:** M-020's "$33.09/month hourly" was derived from
    94 s runs at 1 pair (August) and no longer holds at 17 pairs. Its **12.0×**
    hourly vs 5-minute multiple is unaffected, because it is the run-count ratio.
    The dollar amounts in README, tour page and any resume line need updating
    (slice 9). M-020 itself is left as written.
  - This is why ADR-0002 bounds the hourly design to one 24-run measurement window
    and runs daily afterwards: hourly-forever would exceed the $10 budget ~8×.
  - Both durations are single runs; slice 8 adds a 26 h lookback, untested so far.
    The window's 24 runs replace this derivation with a measured distribution.

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
