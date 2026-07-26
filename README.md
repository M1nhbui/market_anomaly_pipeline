# Crypto Market Anomaly Pipeline

A cloud-native, medallion-architecture data pipeline that ingests cryptocurrency
market data on a schedule, cleans and enriches it with Apache Spark, gates it
behind automated data-quality checks, and produces analytics-ready tables plus a
statistical **anomaly signal** that surfaces unusual price and volume moves.

Built on AWS serverless (S3, Lambda, Glue, Athena, Step Functions, SNS),
provisioned with Terraform, and querying live data from the Binance public
market-data mirror.

---

## Table of Contents

1. [Background](#1-background)
2. [What This Project Solves](#2-what-this-project-solves)
3. [Who Uses It and How](#3-who-uses-it-and-how)
4. [Scope and Honesty Boundaries](#4-scope-and-honesty-boundaries)
5. [Architecture Overview](#5-architecture-overview)
6. [Technology Stack: What Each Piece Does and Why](#6-technology-stack-what-each-piece-does-and-why)
7. [The Data Source](#7-the-data-source)
8. [What the Data Looks Like](#8-what-the-data-looks-like)
9. [The Medallion Layers and Every Schema](#9-the-medallion-layers-and-every-schema)
10. [The Anomaly Detection Logic](#10-the-anomaly-detection-logic)
11. [Data Quality Gate](#11-data-quality-gate)
12. [Orchestration and the Two-Cadence Design](#12-orchestration-and-the-two-cadence-design)
13. [IAM and Security](#13-iam-and-security)
14. [Repository Structure](#14-repository-structure)
15. [Setup and Deployment](#15-setup-and-deployment)
16. [How to Run and Query](#16-how-to-run-and-query)
17. [Cost and Scale](#17-cost-and-scale)
18. [Symbol Basket](#18-symbol-basket)
19. [Future Improvements](#19-future-improvements)

---

## 1. Background

Market data is one of the highest-volume, highest-velocity data sources in the
world. Prices update continuously, arrive with imperfections (gaps, duplicate
bars, illiquid empty minutes, values encoded as strings), and accumulate faster
than any human can inspect by eye. Turning that raw firehose into something
clean, trustworthy, and *useful* is a canonical data-engineering problem.

This project is modeled structurally on a well-known YouTube trending-data
pipeline (S3 / Lambda / Glue / Athena / Step Functions in a Bronze → Silver →
Gold medallion pattern), but it deliberately swaps both the **data source** and
the **analytical question**. Instead of aggregating trending videos, it ingests
crypto OHLCV candlesticks and produces a genuine analytical output the original
lacks: a statistical filter that flags when an asset moves or trades far outside
its own recent normal behaviour.

The domain swap is intentional. Rebuilding a tutorial line-for-line demonstrates
plumbing but signals little independent judgment. Choosing a new domain forces
original decisions about schema, data quality, feature engineering, and modeling,
which are exactly the decisions that separate a portfolio project from a copied
one.

---

## 2. What This Project Solves

At its core, the pipeline does one thing:

> **It turns a messy, continuous firehose of market data into two things: a
> clean, trustworthy history anyone can query, and a short, ranked list of
> moments that were statistically unusual enough for a human to look at.**

That decomposes into three stacked jobs:

1. **Reliable ingestion and cleaning.** Raw market feeds have gaps, duplicate
   bars from overlapping fetches, and occasional bad or empty prints. The
   Bronze → Silver layers guarantee downstream consumers get deduplicated,
   type-correct, sanity-checked bars.

2. **Derived features computed once, correctly.** Log returns, rolling
   volatility, dollar volume, and range — the building blocks every downstream
   consumer would otherwise re-derive inconsistently. Computing them once in the
   Silver layer creates a single source of truth.

3. **Anomaly surfacing.** Rather than "here is all the data, go look," the Gold
   layer answers a specific question: *which symbol, at which minute, moved or
   traded so far outside its recent normal range that it warrants
   investigation?* This is a produced **signal**, not just an aggregation.

---

## 3. Who Uses It and How

In a realistic setting the consumers are **downstream systems and analysts, not
an end user clicking around the pipeline**. The pipeline feeds things:

- A **quant researcher** queries the Gold analytics tables in Athena as a clean
  feature store, e.g. to backtest whether anomaly days predict next-day
  volatility. They never touch the Lambda or Glue internals.
- A **monitoring / alerting system** reads the `anomaly_events` table and posts
  to a channel when a fresh high-z-score event lands. The "user" is whoever
  receives that alert.
- A **data analyst** builds a dashboard (QuickSight or a plotting script) on top
  of the Gold tables for a desk to glance at.

The accurate framing: this is **infrastructure that produces a curated dataset
and a signal**. The consumers are analytics, backtests, and alerting.

---

## 4. Scope and Honesty Boundaries

These boundaries are deliberate and should be kept attached to any description of
the project. They are what make its claims defensible.

- **It produces a filter, not alpha.** A z-score spike catches news events, flash
  moves, and data glitches indiscriminately. It is *not* a trading signal, and no
  claim is made that the anomalies are profitable. The defensible claim is
  narrower and stronger: *the infrastructure and a first-pass statistical filter
  are built; whether the filter has predictive value is a separate research
  question this project does not attempt to answer.*
- **The consumers are systems and analysts, not an end user.**
- **The symbol basket is a demo sample, not a considered trading universe.** A
  real universe would weight by liquidity, handle delistings, and adjust for
  correlation.
- **Known limitation, named up front:** a global rolling z-score conflates
  genuine market events with data-quality glitches and scheduled volatility
  (e.g. funding-rate resets), and the mean/standard-deviation estimator is itself
  sensitive to the very outliers it is trying to detect. A more robust version is
  discussed in [Future Improvements](#19-future-improvements).

---

## 5. Architecture Overview

```
EventBridge (every 5 min)                 EventBridge (hourly)
        │                                          │
        ▼                                          ▼
┌───────────────────┐              ┌──────────────────────────────────────┐
│ Ingestion Lambda  │              │        Step Functions state machine   │
│ • loop symbols    │              │                                       │
│ • GET klines      │              │  [1] Glue: Bronze → Silver (PySpark)  │
│ • wrap + metadata │              │        • parse, cast, dedup           │
│ • SNS on failure  │              │        • derive features              │
└─────────┬─────────┘              │            │                          │
          │ writes JSON            │            ▼                          │
          ▼                        │  [2] DQ Gate Lambda (Athena queries)  │
   ┌──────────────┐                │            │                          │
   │  S3 BRONZE   │───────────────▶│            ▼                          │
   │ (raw JSON)   │                │  [3] Choice: quality_passed?          │
   └──────────────┘                │        ├── false ─▶ SNS "DQ failed"   │
                                   │        └── true ─▶ continue           │
   ┌──────────────┐                │            ▼                          │
   │  S3 SILVER   │◀───────────────│  [4] Glue: Silver → Gold (PySpark)    │
   │  (Parquet)   │───────────────▶│        • 3 analytics tables           │
   └──────────────┘                │            │                          │
                                   │            ▼                          │
   ┌──────────────┐                │  [5] SNS "pipeline success"           │
   │   S3 GOLD    │◀───────────────│                                       │
   │  (Parquet)   │                └──────────────────────────────────────┘
   └──────┬───────┘
          │
          ▼
   Glue Data Catalog ──▶ Amazon Athena ──▶ QuickSight / analysts / alerting
```

The design uses **two cadences** (explained in
[Section 12](#12-orchestration-and-the-two-cadence-design)): a fast, cheap
ingestion loop and a slower, batched transformation pipeline.

---

## 6. Technology Stack: What Each Piece Does and Why

| Layer | Technology | What it does here | Why this choice |
|---|---|---|---|
| Scheduling | **Amazon EventBridge Scheduler** | Triggers ingestion every 5 min and the transform state machine hourly | Serverless cron, native to AWS, two independent schedules |
| Ingestion | **AWS Lambda (Python 3.12)** | Calls the Binance klines endpoint per symbol, wraps bars with metadata, writes raw JSON to Bronze | Ingestion is short, periodic, stateless — Lambda's sweet spot; near-zero cost |
| Raw storage | **Amazon S3 (Bronze)** | Immutable landing zone for raw JSON, partitioned by symbol/date/hour | Cheap, durable, "never lose the source" |
| Transformation | **AWS Glue (PySpark)** | Two jobs: Bronze→Silver (clean + enrich) and Silver→Gold (aggregate + anomalies) | Serverless Spark: real distributed window functions without managing a cluster |
| Processed storage | **Amazon S3 (Silver, Gold) as Parquet** | Columnar cleaned bars and aggregated analytics tables | Columnar format Athena scans cheaply; industry standard |
| Data quality | **AWS Lambda (Python)** | Queries Silver via Athena, runs checks, returns `quality_passed` | Checks are simple aggregate queries; no need for the heavier Spark hammer |
| Catalog | **AWS Glue Data Catalog** | Schema registry that makes Parquet queryable as SQL tables | The metadata layer Athena reads from |
| Query | **Amazon Athena** | Serverless SQL over S3 for consumers and the DQ gate | Pay-per-scan; no warehouse to run |
| Orchestration | **AWS Step Functions** | State machine wiring transforms, DQ gate, and notifications with retries | Serverless, native to these services, a visual Choice-state gate |
| Alerting | **Amazon SNS** | Failure and DQ-rejection notifications | Simple pub/sub to email or Slack |
| Infra-as-code | **Terraform** | Defines every bucket, role, Lambda, Glue job, and the state machine | Reproducibility — the key upgrade over console-clicked pipelines |
| CI (optional) | **GitHub Actions** | Lints Python and validates Terraform on push | Cheap credibility signal |
| Visualization | **QuickSight or matplotlib/Plotly** | Price series with anomaly points marked; anomaly counts per symbol | AWS-native, or in-repo and free |

### A note on each layer's *purpose* in the flow

- **EventBridge** exists so nothing runs manually; the pipeline is autonomous.
- **Ingestion Lambda** is the only component that talks to the outside world. It
  is intentionally thin: fetch, wrap, store. No transformation happens here so
  that raw data is preserved exactly as received.
- **Bronze S3** is the immutability guarantee. If a transform has a bug, the raw
  data is still there to reprocess.
- **Glue Bronze→Silver** is where correctness lives: typing, dedup, and the
  time-series feature derivations that need Spark window functions.
- **DQ Gate Lambda** is the circuit breaker. It is the reason bad data never
  reaches the analytics layer.
- **Glue Silver→Gold** is where the analytical value is produced, especially the
  anomaly table.
- **Athena + Catalog** is the serving interface — how anyone actually consumes
  the output.
- **Step Functions** is the conductor that guarantees ordering, retries, and the
  quality gate.
- **Terraform** is what makes the whole thing reproducible by someone else (or
  future-you) from scratch.

---

## 7. The Data Source

**Endpoint:** `GET /api/v3/klines` (candlestick / OHLCV bars)
**Host:** `https://data-api.binance.vision` — the Binance public market-data
mirror. It requires **no API key**, is **not geo-restricted**, and returns data
identical in shape to the main API.

**Why this host specifically:** the global host `api.binance.com` returns HTTP
451 (geo-blocked) from many regions, including where this project was validated,
even over a VPN. The `.vision` mirror returned HTTP 200 with live data from the
same location. Always build against `data-api.binance.vision`; never depend on
`api.binance.com`.

**Key request parameters:**

| Parameter | Example | Meaning |
|---|---|---|
| `symbol` | `BTCUSDT` | Trading pair, quote asset USDT |
| `interval` | `1m` | Bar size (1m, 5m, 1h, ...) |
| `limit` | `1000` | Bars per call (max 1000) |
| `startTime` / `endTime` | ms epoch | Optional range; omit for most recent |

**Rate limits:** the endpoint costs 2 request-weight for up to 500 candles (5 for
up to 1000) against a 6000-weight-per-minute budget — the pipeline uses a
negligible fraction. Exceeding limits returns HTTP 429 with a `Retry-After`
header; repeatedly ignoring it escalates to an automated IP ban (HTTP 418). The
ingestion Lambda honours `Retry-After` and backs off.

---

## 8. What the Data Looks Like

A call to
`data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=1`
returns a JSON **array of arrays**. Each inner array is one bar, and the fields
are **positional** (no names — you map by index):

```json
[
  [
    1783325940000,      // [0]  open_time     (ms epoch)
    "63037.52000000",   // [1]  open          (STRING)
    "63037.52000000",   // [2]  high          (STRING)
    "63037.51000000",   // [3]  low           (STRING)
    "63037.52000000",   // [4]  close         (STRING)
    "0.00774000",       // [5]  volume         (STRING, base asset = BTC)
    1783325999999,      // [6]  close_time    (ms epoch)
    "487.91035840",     // [7]  quote_volume   (STRING, quote asset = USDT)
    5,                  // [8]  num_trades    (int)
    "0.00310000",       // [9]  taker_buy_base_volume
    "195.41631200",     // [10] taker_buy_quote_volume
    "0"                 // [11] ignore (unused)
  ]
]
```

Two properties drive the Silver logic:

1. **Every price and volume is a string** and must be cast to `double`.
2. **The bar is a bare positional array with no field names** — mapping
   index → name is the first Bronze → Silver transformation.

### A real data-quality quirk (discovered during validation)

For the *same* minute, two Binance hosts returned genuinely different bars:

| field | `.vision` (global market) | `.us` (US market) |
|---|---|---|
| open_time | 1783325940000 | 1783325940000 |
| close | 63037.52 | 63043.46 |
| volume | 0.00774 | **0.00000** |
| num_trades | 5 | **0** |
| quote_volume | 487.91 | **0.00** |

Same timestamp, same rough price, but the US bar has zero volume and zero
trades — a real, illiquid, empty minute. This is exactly the kind of stale/empty
bar the DQ gate flags (`num_trades = 0`) before it reaches analytics, and it is
why the pipeline builds against `.vision`.

---

## 9. The Medallion Layers and Every Schema

### Bronze — raw JSON

One object per ingestion call per symbol, stored untouched.

```json
{
  "symbol": "BTCUSDT",
  "interval": "1m",
  "source_host": "data-api.binance.vision",
  "ingested_at": "2026-07-06T08:19:05.123Z",
  "bars": [[1783325940000, "63037.52000000", "...", "0"]]
}
```

**Path:** `s3://<prefix>-bronze/symbol=BTCUSDT/date=2026-07-06/hour=08/ingest_081905.json`

### Silver — `clean_bars` (one typed, deduplicated row per bar)

| column | type | source |
|---|---|---|
| symbol | string | source |
| open_time | timestamp | parsed from ms epoch |
| open | double | cast from string |
| high | double | cast from string |
| low | double | cast from string |
| close | double | cast from string |
| volume | double | cast from string |
| quote_volume | double | cast from string |
| num_trades | int | source |
| taker_buy_base | double | cast from string |
| log_return | double | derived: ln(close / lag(close)) over time-ordered window |
| rolling_vol | double | derived: stddev(log_return) over trailing 20 bars |
| dollar_volume | double | derived: close × volume |
| range_pct | double | derived: (high − low) / open |
| date | string (partition) | derived from open_time |

- **Primary key / dedup key:** `(symbol, open_time)`.
- **Partitioning:** `symbol`, `date`.
- **Dedup rationale:** bars genuinely re-arrive across overlapping ingestion
  windows, so deduplication here is load-bearing, not cosmetic.

### Gold — three analytics tables

**`symbol_daily_analytics`** (one row per symbol per day)

| column | type | meaning |
|---|---|---|
| symbol | string | |
| date | date | |
| daily_return | double | close-to-close return for the day |
| realized_vol | double | sqrt of sum of squared intraday returns |
| total_dollar_volume | double | sum of dollar_volume |
| max_drawdown | double | largest peak-to-trough drop within the day |
| anomaly_count | int | number of anomaly events that day |

**`cross_sectional_ranking`** (one row per symbol per time bucket)

| column | type | meaning |
|---|---|---|
| window_time | timestamp | the bucket being ranked |
| symbol | string | |
| period_return | double | return over the bucket |
| return_rank | int | rank vs. other symbols at window_time (window function) |
| volume_rank | int | rank by dollar volume at window_time |

**`anomaly_events`** — the centerpiece (one row per flagged bar)

| column | type | meaning |
|---|---|---|
| symbol | string | |
| event_time | timestamp | when the anomaly occurred |
| rule | string | `return_zscore` or `volume_zscore` |
| observed_value | double | the return or volume that fired the rule |
| rolling_mean | double | baseline mean over the trailing window |
| rolling_std | double | baseline standard deviation |
| zscore | double | (observed − mean) / std |
| close_price | double | price context |
| volume | double | volume context |
| date | string (partition) | |

---

## 10. The Anomaly Detection Logic

The heart of the project, expressed in Spark terms:

1. Partition the Silver data **by symbol** and order **by time**.
2. Define a trailing window (e.g. 60 bars).
3. Over that window compute the **rolling mean** and **rolling standard
   deviation** of `log_return`.
4. For each bar, compute `zscore = (log_return − rolling_mean) / rolling_std`.
5. If `abs(zscore) > k` (e.g. `k = 3`), emit a row into `anomaly_events` with
   `rule = 'return_zscore'`.
6. Repeat the same procedure on `volume` to catch volume spikes
   (`rule = 'volume_zscore'`).

This is a **per-symbol, self-relative statistical filter**: each asset is judged
against its own recent behaviour, not against other assets. The window length and
threshold `k` are tunable knobs that should be stated explicitly rather than
hidden.

**Edge cases to handle (these are where "it runs" and "it is correct" diverge):**

- The first `N` bars of any symbol have no full trailing window — their z-scores
  are undefined and must be excluded, not treated as zero.
- Gaps in the bar sequence (a missing minute) break the "trailing N bars"
  assumption; the window is over *rows*, so a gap silently changes the real time
  span.
- A near-zero `rolling_std` can produce an exploding z-score; guard against
  division by a tiny denominator.

---

## 11. Data Quality Gate

A lightweight Lambda sits between Silver and Gold. It queries Silver via Athena
and runs market-specific checks, returning `quality_passed: true | false`. A
Step Functions Choice state blocks the Gold stage on failure and publishes an
SNS alert.

**Checks:**

| Check | Rule | Why it matters |
|---|---|---|
| Row count | at least N rows present | catches empty/failed ingestion |
| OHLC sanity | `high >= low`, `high >= close >= low`, `high >= open >= low` | catches corrupt bars |
| Non-negative prices | no zero/negative `open/high/low/close` | catches bad prints |
| Stale/empty bars | flag `num_trades = 0` and `volume = 0` | the real quirk found in validation |
| Sequence gaps | consecutive `open_time`s spaced by the interval | catches missing bars that break windows |
| Freshness | latest bar within one interval of now | catches a stalled feed |
| Monotonic time | timestamps strictly increasing per symbol | catches ordering corruption |

These checks are domain-real, which reads far better than generic
null-percentage checks and directly protects the anomaly logic downstream.

---

## 12. Orchestration and the Two-Cadence Design

The pipeline deliberately separates **ingestion cadence** from **processing
cadence**, which is the single biggest cost-and-correctness optimization in the
design.

**Why:** Spark (Glue) has a fixed start-up cost on every run — spinning up
executors, initializing the session, reading the catalog — that is roughly the
same whether it processes 17 bars or 17,000. Running a Glue job every 5 minutes
to crunch a handful of new bars pays that start-up tax constantly for almost no
work. Glue is the pipeline's dominant cost, so this matters.

**The design:**

- **Fast ingestion loop** — EventBridge schedule A triggers the ingestion Lambda
  **every 5 minutes**. Lambda has no start-up tax; data lands promptly and
  nothing is lost.
- **Batched transform loop** — EventBridge schedule B triggers the Step Functions
  state machine **hourly**, processing all Bronze accumulated since the last run
  in one batch. One start-up tax per hour instead of twelve.

**Trade-off:** Gold tables are up to an hour stale. For an anomaly *demonstration*
this is fine. If fresher output were needed, the transform cadence would shorten
(e.g. every 15 minutes) at higher Glue cost — an explicit, defensible knob.

**Step Functions flow (transform loop):**

```
Bronze→Silver Glue  →  Wait  →  DQ Gate Lambda  →  Choice(quality_passed)
   ├─ false → SNS "DQ failed" → STOP
   └─ true  → Silver→Gold Glue → SNS "success"
```

Every Glue and Lambda step has retry-with-exponential-backoff and a dedicated SNS
failure state.

### Partition projection (Athena)

Athena is configured with **partition projection** so new `date`/`hour`/`symbol`
partitions are queryable the instant they are written, with no `MSCK REPAIR` or
partition-registration step. Instead of reading a stored partition list from the
catalog, Athena computes S3 paths from a declared pattern:

```
projection.enabled = true
projection.date.type = date
projection.date.range = 2026-01-01,NOW
projection.date.format = yyyy-MM-dd
projection.hour.type = integer
projection.hour.range = 0,23
projection.symbol.type = enum
projection.symbol.values = BTCUSDT,ETHUSDT,SOLUSDT,...
storage.location.template = s3://<prefix>-silver/symbol=${symbol}/date=${date}/hour=${hour}/
```

The only maintenance is updating the `symbol` enum when the basket changes, which
lives in version-controlled Terraform.

---

## 13. IAM and Security

Every compute component runs as its own role under **least privilege** — each
role can touch only the specific resources it needs, never `s3:*` on `*`. There
are five roles.

| Role | Key permissions | Scoped to |
|---|---|---|
| **Ingestion Lambda** | `s3:PutObject`; `sns:Publish`; CloudWatch logs | Bronze bucket only; alert topic |
| **Glue job** | `s3:Get/Put/ListBucket`; `glue:Get/Create/UpdateTable`, partitions; logs | Bronze+Silver read, Silver+Gold write |
| **DQ Gate Lambda** | `athena:StartQueryExecution/GetQueryExecution/GetQueryResults`; `glue:GetTable/GetPartitions`; `s3` on Silver + Athena results bucket; `sns:Publish` | Silver read; results bucket write |
| **Step Functions** | `lambda:InvokeFunction`; `glue:StartJobRun/GetJobRun`; `sns:Publish` | The two Lambdas and two Glue jobs |
| **EventBridge Scheduler** | `states:StartExecution` | The state machine ARN |

**Common pitfalls (budget debugging time here):**

- The DQ Gate role is most often under-scoped: Athena needs **catalog read** and
  **results-bucket write**, not just "read Silver."
- Each role's **trust policy** must name the service allowed to assume it
  (`lambda.amazonaws.com`, `glue.amazonaws.com`, `states.amazonaws.com`,
  `scheduler.amazonaws.com`). Trust misconfiguration fails at runtime with an
  opaque `AccessDenied`.

Policies are authored as JSON documents under `iam_policies/` and wired into
roles by Terraform via `templatefile()`, giving both readable standalone policies
and infra-as-code management.

---

## 14. Repository Structure

```
crypto-anomaly-pipeline/
├── README.md                        # this document
├── terraform/                       # all infrastructure as code
│   ├── main.tf                      # providers, backend
│   ├── s3.tf                        # bronze / silver / gold / athena-results buckets
│   ├── glue.tf                      # two Glue jobs + catalog databases/tables
│   ├── lambda.tf                    # ingestion + dq-gate functions
│   ├── stepfunctions.tf             # the transform state machine
│   ├── eventbridge.tf               # the two schedules
│   ├── sns.tf                       # alert topic + subscriptions
│   ├── iam.tf                       # five roles, wired to policy documents
│   └── variables.tf
├── iam_policies/                    # readable JSON policy documents
│   ├── ingestion_lambda.json
│   ├── glue_job.json
│   ├── dq_gate_lambda.json
│   ├── step_functions.json
│   └── eventbridge_scheduler.json
├── lambdas/
│   ├── ingestion/
│   │   └── lambda_function.py
│   └── dq_gate/
│       └── lambda_function.py
├── glue_jobs/
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
├── step_functions/
│   └── pipeline.json                # state machine definition
├── config/
│   └── symbols.json                 # the basket, in ONE place
├── scripts/
│   └── validate_binance.py          # connectivity + schema probe
└── .github/
    └── workflows/
        └── ci.yml                   # lint Python, validate terraform
```

Keeping the symbol basket in a single `config/symbols.json` avoids the drift bug
seen in the original project, where hardcoded lists in multiple files caused only
a subset of intended data to flow through.

---

## 15. Setup and Deployment

**Prerequisites:** an AWS account, AWS CLI configured, Terraform installed,
Python 3.12, and network access to `data-api.binance.vision`.

1. **Validate connectivity and schema** (do this first):
   ```bash
   pip3 install --upgrade certifi
   python3 scripts/validate_binance.py
   ```
   Confirm it prints a live bar and reports `data-api.binance.vision` reachable.

2. **Configure variables** in `terraform/variables.tf` (bucket prefix, region,
   alert email, symbol enum).

3. **Provision infrastructure:**
   ```bash
   cd terraform
   terraform init
   terraform plan
   terraform apply
   ```

4. **Confirm the SNS subscription** email that arrives after apply.

5. The EventBridge schedules begin driving the pipeline automatically. Bronze
   fills within 5 minutes; the first Gold tables appear after the first hourly
   transform run.

---

## 16. How to Run and Query

Once the pipeline has run, query the Gold tables in Athena. Examples:

**Most recent anomalies:**
```sql
SELECT symbol, event_time, rule, zscore, close_price, volume
FROM gold.anomaly_events
WHERE date = current_date
ORDER BY abs(zscore) DESC
LIMIT 20;
```

**Top movers over the last day:**
```sql
SELECT symbol, daily_return, realized_vol, total_dollar_volume, anomaly_count
FROM gold.symbol_daily_analytics
WHERE date = current_date - interval '1' day
ORDER BY abs(daily_return) DESC;
```

**Cross-sectional leaders at a point in time:**
```sql
SELECT window_time, symbol, period_return, return_rank
FROM gold.cross_sectional_ranking
WHERE return_rank <= 5
ORDER BY window_time DESC, return_rank;
```

For the visualization, point QuickSight at the Gold tables or run the plotting
script to render a price series with anomaly points marked — the recommended
screenshot for the project's documentation.

---

## 17. Cost and Scale

At ~17 symbols × 1440 one-minute bars/day ≈ **24,000 rows/day**, or
**~730,000 rows/month** — small enough to be cheap, large enough that dedup,
partitioning, and windowed Spark genuinely earn their place.

| Service | Cost driver | Approximate cost at this scale |
|---|---|---|
| Lambda | invocations | negligible (free tier covers it) |
| Glue | DPU-hours | the dominant cost; controlled by hourly (not 5-min) transforms |
| Athena | bytes scanned | pennies, kept low by partitioning + Parquet |
| S3 | storage | negligible |
| SNS | messages | negligible |

**Main cost lever:** Glue job frequency. The two-cadence design exists largely to
keep this low. Optionally, Glue **Flex** execution reduces cost further for the
non-urgent batch transforms.

---

## 18. Symbol Basket

The basket (~17 symbols) is a deliberate sample, not a default grab. All pairs
are **USDT-quoted** so returns are directly comparable across the basket.

- **Large-cap majors (anchors / baseline market):** BTCUSDT, ETHUSDT, BNBUSDT,
  SOLUSDT, XRPUSDT, ADAUSDT, DOGEUSDT, AVAXUSDT.
- **Mid-caps (where anomalies show up more):** LINKUSDT, DOTUSDT, MATICUSDT,
  LTCUSDT, ATOMUSDT, UNIUSDT, NEARUSDT.
- **Deliberately volatile (stress-tests the DQ gate and produces real
  anomalies):** PEPEUSDT, SHIBUSDT.

**Design principle:** liquid symbols so a statistical baseline is well-defined;
spanning market-cap tiers so the cross-section has structure; a single quote
asset so returns are comparable; a few volatile names on purpose so the anomaly
layer has something to catch.

---

## 19. Future Improvements

Considered enhancements after the core pipeline is complete. Several are
deliberately deferred to keep v1 appropriately scoped.

**Correctness and modeling**

- **Robust anomaly estimator.** Replace mean/standard-deviation z-scores with a
  median/MAD (median absolute deviation) approach, which is far less sensitive to
  the outliers it is trying to detect. Optionally separate genuine events from
  known scheduled volatility.
- **Cross-sectional anomalies.** Add a second anomaly notion: unusual *relative
  to the market right now*, not only relative to a symbol's own history.
- **Handle window edges and gaps explicitly.** Time-based windows rather than
  row-based, so a missing minute does not silently distort the trailing span.

**Data platform**

- **Apache Iceberg on Silver/Gold.** Adopt Iceberg table format for atomic
  `MERGE`/upsert semantics (a correct fix for idempotency versus overwrite),
  time-travel for debugging, and painless schema evolution. Recommended as the
  headline "level-up" after v1 works on plain Parquet.
- **Dead-letter queue** on the ingestion Lambda so a fully-failed batch is
  captured rather than silently lost.
- **Backfill mode.** A one-off job using the 1000-bar `limit` and
  `startTime`/`endTime` to seed historical data for richer analytics.

**Operations**

- **Glue Flex execution** for cheaper non-urgent batch transforms.
- **CI/CD hardening.** Expand GitHub Actions to run a `terraform plan` on PRs and
  basic unit tests on the Lambda handlers.
- **Alerting refinement.** Route `anomaly_events` above a severity threshold to a
  Slack channel rather than only surfacing them in the table.

**Scope reminder:** several tempting additions — streaming (Kinesis/Kafka), a
data warehouse (Redshift/Snowflake), dbt, Airflow — are intentionally **not**
adopted. At this volume and cadence they add operational complexity without
justified benefit, and choosing them would signal weaker judgment, not stronger.
The restraint is deliberate.

---

*Built as a portfolio demonstration of medallion-architecture data engineering on
AWS serverless. The pipeline produces a curated dataset and a first-pass
statistical signal; it is not a trading system.*
