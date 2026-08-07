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
| 6 | Anomaly events/day at |z|>3; firing rate as % of bars; **end-to-end latency** | pending |
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

### M-004 — Glue cost per cadence (placeholder, to be replaced)
- **Value:** hourly cadence ~$42/month; 5-minute cadence ~$507/month; ratio ~12x
- **Label:** ESTIMATED — **not resume-eligible**
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
