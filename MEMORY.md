# MEMORY — the full project narrative

A chronological account of how this pipeline was built: every decision and the
alternative rejected, every bug and who caught it, every prediction and whether it
held. Written to be read start to finish.

**Companion files:** `METRICS.md` holds the numbers with their capture commands.
This file holds the story of how they came to be measured.

**Convention going forward:** append to the end. Never rewrite history — if something
turns out to be wrong, add a correction with the date rather than editing the original
claim. Several entries below are wrong on purpose for exactly that reason.

---

## Phase 0 — The starting point (2026-07-26)

The repository contained two things: a 32 KB README that was a complete design spec,
and `scripts/validate_binance.py`, a connectivity probe that had already confirmed the
data source worked.

Four constraints were settled before any code and were never reopened:

- Data source: Binance klines via `data-api.binance.vision` (keyless, not geo-blocked;
  the main `api.binance.com` returns HTTP 451 from many regions)
- Stack: AWS serverless + Terraform
- Two cadences: ingestion every 5 minutes, transforms hourly
- Athena with partition projection; ~17 USDT pairs

Two scope rules: **v1 on plain Parquet** (Iceberg deferred), and **the honesty
boundary holds** — this produces a filter, not a trading signal.

The operator's stated goal was not a working pipeline but understanding: fluency
enough to debug and modify it independently, and to defend every design choice in an
interview.

---

## Phase 1 — Planning, and thirteen things the README left open (2026-07-26)

Before writing code, the README was reviewed for gaps. Six were judged blocking.

**1. Binance returns an unfinished bar.** The last element of every klines response is
the *currently forming* candle. Its close, volume and trade count are partial and will
be revised. The README never mentioned this. Left unhandled it poisons the data
silently — and worse, an unfinished bar *looks like a low-volume bar*, which is
precisely what a volume anomaly detector flags. Fix adopted: drop bars where
`close_time >= ingested_at`, **and** dedup by keeping the most recent ingestion.

**2. Dedup needs a tie-break column the schema didn't have.** Added `ingested_at` and
`source_host` to Silver.

**3. Incremental strategy was undefined**, and it interacts badly with window
functions. Two options were put to the operator:
- Full reprocess every run — trivially correct, grows without bound
- **Chosen:** lookback window + dynamic partition overwrite

**4. The partition scheme contradicted itself.** README §9 said Silver partitions by
`symbol, date`; §12's projection template included `hour`. Resolved: **Bronze gets
`symbol/date/hour`** (write-side), **Silver gets `symbol/date`** (read-side, because
hourly partitions on 1440 rows/day would make 24 tiny files per symbol per day).

**5. Ownership of the table definition.** Chosen: **Terraform owns all Glue Catalog
tables; Glue jobs write files only.** The alternative — `enableUpdateCatalog` on the
job — would fight Terraform over the projection properties and produce silent drift.

**6. The symbol basket needed validating.** Prediction made: MATICUSDT would return
HTTP 400 as delisted. **This prediction was wrong** — see Phase 3.

Seven more were flagged as non-blocking: DQ checks needed thresholds not booleans;
the freshness check as specified would fail on every run; a z-score on raw volume is
statistically unsound; k=3 would fire far more than normal theory implies;
`cross_sectional_ranking` had no defined bucket size; bucket names are globally
unique; no budget alarm existed in the design.

**Decisions taken by the operator:** us-east-1; lookback + dynamic overwrite; deep
explanation on Spark windows, z-scores, the DQ gate and IAM, terse on plumbing.

### The probe, and the first prediction failure

The basket probe returned **18/18 symbols HTTP 200, including MATICUSDT.** The
prediction of a 400 was wrong.

But the more useful finding was that **the probe itself was too weak to answer the
question.** It checked only HTTP status. A retired pair can serve its final historical
bars indefinitely, so 200 proves a symbol is *known*, not *trading*. The fact that both
MATICUSDT and POLUSDT returned 200 was the tell.

The probe also confirmed the unfinished-bar hazard directly:

```
open=1787503260000 close_time=...379999 closed=True  vol=1.13365 trades=321
open=1787503320000 close_time=...439999 closed=True  vol=0.84351 trades=198
open=1787503380000 close_time=...499999 closed=False vol=0.10631 trades=79   ← partial
```

Logged as M-001 and M-003, with M-001 explicitly annotated *not* to be quoted as
"18 symbols validated."

---

## Phase 2 — The metrics protocol (2026-08-07)

The operator asked for every resume number to trace to a measurement, with
MEASURED / DERIVED / EXTRAPOLATED / ESTIMATED labels and a standing rule never to
round a measured number up.

`METRICS.md` was created with that convention and a per-slice capture schedule. Four
measurement-design problems were raised immediately:

- **The two-cadence saving cannot be A/B tested for a month.** Planned method: measure
  the same Glue job over two input window sizes and compute.
- **"~730K bars/month" will measure *lower*, not higher.** It is a ceiling.
- **Latency is a distribution**, dominated by the batch window. Report p50 and max.
- **Cost Explorer has a ~24h lag and is not retroactive** — enable it at slice 0.

Two design changes followed directly from this protocol: the Glue job would log input
and output row counts around the dedup step (otherwise the duplicate rate is
unrecoverable), and the DQ gate would persist per-run results to S3 (otherwise
pass/fail history is unrecoverable).

### The first correction, before any code ran

A `$10` budget alarm had been written into slice 0 without doing the arithmetic. Doing
it properly: Glue bills $0.44/DPU-hour with a 1-minute minimum, two jobs per cycle,
720 cycles/month ⇒ **~$42/month, not pennies.** The design as specified would blow the
alarm in the first week.

The alarm was **kept at $10 deliberately** and re-purposed as a detector for "the
hourly schedule was left running." The build plan changed instead: **Glue jobs run
manually during development**; the hourly schedule is enabled only for a bounded
metrics window at slice 8.

Recorded as M-005 — "set a budget, then discovered the design exceeded it" — because
the sequence is a finding about the design, not a mistake to erase.

---

## Slice 0 — Terraform scaffold and cost guardrails (2026-08-23)

**Built:** provider config, 4 S3 buckets (bronze/silver/gold/athena-results), public
access blocks, SSE-S3 encryption, lifecycle rules, versioning on Bronze only, and an
AWS Budgets alarm. 18 resources.

**Decisions and their alternatives:**

| Decision | Alternative rejected | Why |
|---|---|---|
| Four buckets | One bucket with prefixes | IAM legibility — a bucket ARN in a policy versus a prefix condition that is easy to get subtly wrong |
| Local Terraform state | S3 backend | Bootstrap problem: the state bucket must exist before Terraform can use it |
| Versioning on Bronze only | Version everything | Silver/Gold are derived; versioning them accumulates every superseded Parquet file forever |
| Account-ID bucket suffix | `random_id` resource | Deterministic — reruns produce the same name, no extra state |
| Provider `default_tags` | Tag per resource | Cost Explorer can only slice by tag if the tag existed when the cost was incurred |

**Also learned:** the account was created ~April 2025, before the July 2025 free-tier
change, so the legacy 12-month tier had expired — plain pay-as-you-go, with the
permanent "always free" allowances still active.

---

## Slice 1 — Ingestion Lambda (2026-08-23)

**Built:** Python 3.12 Lambda using only the standard library plus boto3, an IAM role
scoped to `s3:PutObject` on Bronze and its own log group, and a CloudWatch log group
with 14-day retention.

**Decisions:**

- **stdlib `urllib`, not `requests`** — no Lambda layer, no build step, nothing to go
  stale. The deployment package is a few KB of plain source.
- **Fetch 15 bars every 5 minutes, not 5.** Deliberate overlap: a missed run is
  backfilled by the next, and it makes dedup load-bearing rather than decorative.
  Two thirds of every response is data already held.
- **Bronze partitioned by *ingestion* time; Silver by *bar* time.** Splitting one
  response across bar-time partitions would require parsing timestamps in the raw
  layer — transformation in Bronze, which breaks its guarantee.
- **HTTP 418 aborts the run; 429 backs off.** Retrying into an IP ban extends it.
- **Per-symbol try/except.** One delisted symbol must not lose every other symbol's
  data for that window.

### Bug #1 — a corrupted file, and an error-handling lesson

First invocation failed:

```
"All 1 symbols failed: [{'symbol': 'BTCUSDT',
  'error': "NameError: name 'BAR_LIMIT' is not defined"}]"
```

Nine lines had been lost mid-write, splicing the tail of an assignment onto a comment
line so the whole statement became a comment.

The instructive part was the **stack trace, which pointed at line 201 — where the
failure was *reported*, not where it happened.** The per-symbol `except` had caught the
real error and flattened it to a string. Fix: log `traceback.format_exc()` before
discarding.

It also exposed a design tradeoff, documented in the code rather than silently changed:
`except Exception` swallows *programming* errors identically to *data* errors. With one
symbol it surfaced instantly; with seventeen, a bug in a rare code path would look like
a flaky API.

**Measured:** 842 ms mean duration, 99 MB of 256 MB memory, 2.86 KiB per Bronze object,
6/6 scheduled runs at exactly 300 s spacing.

### A re-ordering decision

The 5-minute EventBridge schedule was pulled forward from slice 5, so that real
overlapping data would accumulate while later slices were built. Ingestion is cheap
(~0.45% of the always-free Lambda allowance); Glue is what costs money.

EventBridge **Scheduler** was chosen over EventBridge **Rules** — the current service,
with a per-target retry policy — and its trust policy carries an `aws:SourceAccount`
condition against the confused-deputy problem.

---

## Slice 2a — Bronze→Silver, written and tested locally (2026-08-25)

Deliberately split into two steps: write and test locally first, then deploy. Iterating
on Glue costs 2–3 minutes and a few cents per typo.

**Decisions:**

- **Plain PySpark, not Glue's DynamicFrame API.** Portability and testability — plain
  DataFrames run on a laptop, which is what makes the local suite possible.
  DynamicFrame is Glue-only trivia. Cost: no job bookmarks.
- **Explicit schema, never inferred.** JSON inference costs an extra pass and its
  result depends on the data it happens to see.
- **`recursiveFileLookup=true`** to disable partition discovery, which would otherwise
  collide with the `symbol` field inside each document.
- **`row_number()` over a window ordered by `ingested_at DESC`, not
  `dropDuplicates()`.** The same bar legitimately arrives with *different values*
  (partial, then complete). `dropDuplicates` keeps an arbitrary copy, so ~1 bar in 15
  would be permanently and non-deterministically wrong.
- **Filter unfinished bars before dedup** — cheaper and clearer, though both orders
  happen to agree.

### Bug #2 — the timezone defect, caught locally, invisible on Glue

Two of nine tests failed. The cause was in the code, not the tests:

```
to_timestamp("2026-08-23T16:42:30")     → 2026-08-23 16:42:30   (session timezone)
timestamp_seconds(1787503379999/1000)   → 2026-08-23 11:42:59   (absolute instant)
```

Spark's session timezone defaults to the **host machine's**. The sandbox was
`America/Chicago`. So `close_time < ingested_at` — the check that drops unfinished
bars — was comparing two different clocks five hours apart, and **the filter silently
did nothing.**

The critical detail: **Glue containers run UTC, so this would have "worked" in
production.** It would have failed only on a developer's laptop. A bug that is correct
in one environment and wrong in another, with no error in either.

Fix: `spark.sql.session.timeZone = UTC`, pinned identically in the job and the test
fixture. The suite now passes under `TZ=Asia/Tokyo` as well, which is how one knows it
is fixed rather than accidentally correct.

Two follow-on test fixes: assertions were rewritten to render timestamps *inside*
Spark with `date_format` rather than on collected Python datetimes, which are converted
using the driver's local timezone.

### The operator's first design contribution

The operator asked whether `.coalesce()` before writing would avoid producing 200 small
files. Measured at production scale (17 symbols × 7 days = 119 partition directories):

```
as written, AQE on      476 files    4,674 KiB   avg  9.8 KiB
coalesce(1)             119 files    4,267 KiB   avg 35.9 KiB
repartition             119 files    4,267 KiB   avg 35.9 KiB
as written, AQE OFF   23,776 files  33,056 KiB   avg  1.4 KiB
```

The concern was right, though the 200-file fear didn't materialise — Adaptive Query
Execution already collapses shuffle partitions. But 476 files instead of 119 wastes
~8.7% of Silver's bytes on repeated Parquet footers.

**`repartition("symbol","date")` was adopted rather than `coalesce(1)`**, which
measured identically: `coalesce(1)` funnels every row through a single task and will
not scale. Recorded in METRICS as originating from operator review.

**Predicted** the duplicate rate at 60–65% from overlap arithmetic. **Measured
63.95%** locally on real Bronze data.

---

## Slice 2b — Deploying to Glue (2026-08-25)

**Built:** a fifth S3 bucket for artifacts, the Glue IAM role, the script upload, and
the job definition.

**Decisions:**

- **The IAM asymmetry is the medallion architecture in policy form.** Bronze:
  read-only. Silver: read/write/**delete** — delete is required because an S3
  "overwrite" is delete-then-write. The absence of delete on Bronze is what makes
  immutability *enforced* rather than promised.
- **Timeout 15 minutes, not Glue's 2880-minute default** — 48 hours at 2 DPU is ~$42
  for one hung job.
- **`max_retries = 0`** — every failure so far had been deterministic; retrying doubles
  the bill.
- **`max_concurrent_runs = 1`** — dynamic partition overwrite is not atomic on plain
  Parquet, so a race can leave a partition half-replaced with no error.
- **2 workers, and more would not help** — at ~1,800 rows the job is startup-bound.

### Bug #3 — `glue-temp_$folder$`, catchable only on real AWS

The cadence experiment failed with:

```
not authorized to perform: s3:PutObject on resource:
"arn:aws:s3:::crypto-anomaly-artifacts-.../glue-temp_$folder$"
```

**S3 has no directories.** Hadoop and Spark fake them by writing zero-byte marker
objects named `<prefix>_$folder$`. The policy granted `glue-temp/*` — and
`glue-temp_$folder$` is a *sibling* key, not a child. Fixed by dropping the slash:
`glue-temp*`, while keeping `scripts/` read-only so the job cannot overwrite its own
source.

This bug **could not have been caught locally** — no local Spark run touches S3
directory markers. Paired with bug #2, which could only be caught locally, it is the
argument for maintaining both loops.

Also noted: the failed run still billed $0.025. Failures are not free.

### The measurements that mattered

- Duplicate rate on Glue: **64.09%** against 63.95% locally — 0.14 pp apart across two
  runtimes and 1.7× the data
- Unfinished bars dropped: **607 from exactly 607 documents** — one per API response
- **The cadence experiment:** the same job took 90 s on 607 documents and 94 s on ~12.
  A 50× input reduction changed runtime by −4.4%. Startup-dominated, so run *count* is
  the only cost lever ⇒ **12.0×**, replacing README §12's unmeasured "~10x"
- Storage: **8.95× reduction**, decomposing into 2.98× dedup × 3.00× format

---

## Slice 3 — Glue Catalog, partition projection, Athena (2026-08-25)

**Built:** the Silver catalog database and table with partition projection, an Athena
workgroup with an enforced output location and a 1 GiB per-query scan cutoff, a query
file, and `scripts/athena.sh`.

**Decisions:**

- **Partition projection over `MSCK REPAIR` / crawlers.** Athena *computes* partition
  paths from rules instead of storing a list, so new data is queryable instantly. The
  cost is that a symbol missing from the enum is **invisible with no error** — which is
  exactly why the enum is generated from `config/symbols.json` rather than typed by
  hand.
- **`$${symbol}`** — Terraform interpolates `${...}`, so the doubled form is needed to
  emit the literal that Athena's own templating expects.
- **`date` is a reserved word in Athena.** Every reference needs quoting. **README §16's
  example queries use it unquoted and will not run** — queued for correction.
- `scripts/athena.sh` was written deliberately as start → poll → fetch, because that is
  the exact API sequence the DQ gate Lambda will need at slice 7.

### The retraction

A "3 missing bars" shortfall had been logged as provisional (M-018). The gap query
returned **zero rows**, and the completeness query showed `bars_present = bars_expected
= 3,052` exactly.

**The shortfall was an arithmetic error in the baseline, not a real gap.** Expected
minutes had been computed using the Glue job's start time (19:36:23) as the endpoint —
but Silver's newest bar necessarily lags that, because the last ingestion ran ~19:33
and its final bar was in-progress and dropped by design. Correct endpoint: 19:32.

The prediction that `last_bar` would be "around 19:32" was **exact**.

M-018 was marked withdrawn rather than deleted, because "the metric was wrong before
the data was" is worth remembering.

**Also measured:** zero duplicate keys in the stored output (independent of the job's
own report); zero OHLC violations across 3,052 bars; scan reduction of **26.6× =
12.6× columnar × 2.10× pruning**, with the pruning factor independently corroborated
by the target partition being 46.5% of bytes on disk against a 47.5% measured scan
ratio.

**Honesty note recorded:** Athena bills a 10 MB per-query minimum, so at 190 KB all
three of those queries cost the same. The ratio is a design property, not realised
savings.

---

## Slice 4 — Window features (2026-09-08)

**Built:** `log_return`, `rolling_vol`, `dollar_volume`, `range_pct`, plus nine new
tests.

**Decisions:**

- **Time-based `rangeBetween`, not row-based `rowsBetween`.** A row window counts rows,
  not time — with a missing minute, "the last 20 bars" silently spans 21 minutes.
  README §19 lists this as a *future* improvement; it costs four characters, so it was
  done in v1. Two tests pin it: one proves the approaches diverge when a gap exists,
  the other proves they are identical without one.
- **Log returns, not percentage returns** — additive over time and roughly symmetric,
  both of which the z-score depends on.
- **NULL during warm-up, never 0.** Spark will compute a standard deviation over 3
  observations and hand back a number; 0 would read as "no volatility," the single most
  misleading value available. Consequence: `rolling_vol` first appears at bar **21**,
  not 20 — 21 closes are needed for 20 returns.
- **`stddev_samp` stated explicitly**, with a test asserting it does *not* match
  `pstdev`, so the choice is pinned rather than assumed.
- **`dollar_volume` is an approximation and `quote_volume` is exact.** README §9
  specifies `close × volume`, which values every trade in the minute at the *closing*
  price. Implemented for fidelity, with the caveat recorded in code and catalog.

### Bug #4 — duplicated stage composition

Three older tests broke. `run()` and `transform()` each composed the stage list
*independently*, so adding a stage to one left the other behind and `SILVER_COLUMNS`
asked for a column never created.

Patching would have added the call in two places and left the trap set. Instead
`finalize()` was extracted — one definition of the post-filter ordering that both
callers use. The class of bug became impossible rather than fixed once.

**Measured, both predicted exactly:** 1 NULL `log_return`, 20 NULL `rolling_vol` across
23,327 bars. Because the window is time-based, any gap would add NULLs — so this
doubles as a gap check and extended the zero-gap result from 50 hours to 16 days.

**Also measured:** mean |log_return| 3.354 bps, max 142.84 bps — **32.5× typical
volatility.** Under a normal distribution the largest of 23,327 draws sits near 4σ.
This set the expectation for slice 6.

### A model that was wrong

Glue runtime came in at 205 s against 90 s earlier — **contradicting the "startup
dominated" conclusion** from the cadence experiment. That conclusion had been true only
within the narrow range measured (12–607 documents). With a third point 8× further out,
volume clearly mattered: ~90 s floor plus ~0.028 s per document.

Consequence: **slice 4.5 became blocking for slice 5**, since full reprocess at 17
symbols projected to ~39 minutes against a 15-minute timeout.

---

## Slice 4.5 — Incremental processing (2026-09-09)

Scheduled here on purpose: after window features so there is a known-correct reference
to test against, before the 17× data growth.

**The constraint that shaped everything:** dynamic partition overwrite replaces *whole*
`(symbol, date)` partitions, so writing a partial day destroys the rest of it. The rule
that follows: **if we intend to write a date, we must recompute all of it.**

```
target_dates = dates with bars in the last N hours   → rewritten in full
read_dates   = target_dates + one prior day          → warm-up, then FILTERED OUT
```

Bounded forever: at most three days of Bronze per run regardless of accumulated
history. A whole extra day of warm-up for a 20-minute window is deliberate — Bronze is
partitioned by ingest time while bars are keyed by bar time, so computing exactly which
prefixes hold "the 21 minutes before midnight" is fiddly and easy to get subtly wrong.

### Bug #5 — a test that could not detect failure

The pruning assertion failed at `1560 < 1560`. The fixture spanned exactly the three
dates the lookback reads, so incremental read everything anyway. **The test was
structurally incapable of distinguishing "pruning works" from "pruning did nothing."**
Fixed by adding older history outside any tested window.

### Bug #6 — Spark and Athena disagreed about a column type

Spark infers types from Hive-style directory names, so reading `date=2026-09-02` back
gives `DateType` — while the job *wrote* a string and the Catalog *declares* a string.
Fixed at the source with
`spark.sql.sources.partitionColumnTypeInference.enabled = false` rather than patching
the assertion, because slice 6 reads Silver with Spark and re-partitions on that column.

### Mutation testing introduced

A green suite proves nothing unless it fails on broken code. Two deliberate mutations:

| Mutation | Result |
|---|---|
| Drop the warm-up day from the read window | 5 tests fail |
| Write the warm-up day instead of filtering it | 2 tests fail |

Both mutations are silent in production — the job succeeds, the data looks plausible.

**Verified in production:** 16 of 17 date partitions byte-identical after the first
incremental run; `2026-09-09` created with 113 bars and **113 non-NULL `rolling_vol`**,
proving the warm-up read worked across a midnight boundary. Total NULL `rolling_vol`
across all 23,592 rows: exactly 20, at the very start of history.

**Prediction missed:** incremental would drop "toward the 90 s floor." It took 150 s —
same input size as an earlier 90 s run, 67% slower. Unexplained; the runtime model was
marked not-quotable.

---

## Slice 5 — Expanding to 17 symbols (2026-09-09)

**Ordering decision:** expand *before* building the anomaly detector, because **data
accumulation is the long pole** — every day at one symbol is a day that will not count
toward the final measurements. The counter-argument (easier to verify z-scores at one
symbol) dissolves: you just add `WHERE symbol = 'BTCUSDT'`.

**Insurance first:** Glue timeout raised 15 → 25 minutes. The ~8-minute projection came
from a model already contradicted twice, and optimising against an unreliable
projection is guessing. A higher ceiling costs nothing unless a job runs long.

### The delisted symbol

A proper validator was written, checking three things instead of one: endpoint
responds, newest **closed** bar under 15 minutes old, and real trading activity.

```
MATICUSDT  NO   1049854.0 min old   STALE
```

**1,049,854 minutes = 729 days.** MATICUSDT returns HTTP 200 with data from ~September
2024, when Polygon migrated MATIC→POL. The original status-only probe reported
"18/18 OK" and would have admitted it.

Concretely, admitting it would have meant: refetching the same 15 bars from 2024 every
5 minutes forever; Silver filing them under `date=2024-09-11`, outside the projection
range and therefore **invisible to Athena**; and the DQ freshness check failing
permanently. A silent failure with no error surface.

Recorded in `config/symbols.json` under `rejected` with the evidence. README §18 still
lists MATICUSDT — queued for correction.

### Bug #7 — an unbounded retry budget

Lambda scaled to 17 symbols at 10,746 ms (predicted ~14,300 ms — **25% off**, because
linear extrapolation multiplied per-invocation fixed overhead by 17). Decomposed:
113 ms fixed + 625 ms marginal per symbol.

Comfortable at 10.7 s of a 60 s timeout — **but the retry path was unbounded**:
`17 symbols × 2 sleeps × 20 s Retry-After cap = 680 s`. And Lambda does not fail
gracefully on timeout; it is *killed*, so the handler never returns and every symbol
already collected in that run is lost.

Fixed with a deadline guard using `context.get_remaining_time_in_millis()` — Lambda's
own countdown. Remaining symbols are recorded as `deferred` and the handler returns
cleanly. Nothing is lost: the 15-bar overlap backfills. **That overlap has now paid for
itself twice** — first as gap insurance, now as what makes early abandonment safe.

### Memory: the optimisation not made

99 MB at both 1 and 17 symbols, closing out a question deferred since slice 1.
**Recommendation: leave it at 256 MB.** 23,211 GB-s/month is 5.8% of the always-free
allowance, so reducing it saves exactly $0 while removing headroom. "I measured it and
the optimisation wasn't worth making" is a better answer than a change made because a
number looked tunable.

**Predicted exactly:** every one of 17 symbols showed exactly 20 NULL `rolling_vol` —
17 independent confirmations that a symbol starting fresh warms up identically to
BTCUSDT seventeen days earlier, and that `partitionBy("symbol")` isolates the series.

**Empty bars appeared:** SHIBUSDT 1.89%, ATOMUSDT 0.63%, ADAUSDT 0.16%, everything else
0%. PEPEUSDT at 0% was a surprise — the assumption that both meme pairs would be
illiquid was wrong.

---

## Slice 6 — The anomaly detector (2026-09-09)

**Built:** `silver_to_gold.py`, the Gold catalog database and `anomaly_events` table,
a second Glue job, and 15 new tests.

**Decisions:**

- **The current bar is excluded from its own baseline** —
  `rangeBetween(-60min, -1min)`. If the current observation contributed to its own mean
  and standard deviation, an extreme value would inflate the very baseline it is
  measured against, weakest exactly where detection matters most. A test asserts
  `rolling_mean` equals the mean of prior bars (0.0002) and not the mean including the
  outlier (0.00835) — a 40× difference, so it cannot pass by coincidence.
- **Volume is scored as `log1p(volume)`.** A z-score assumes rough symmetry; volume is
  bounded at zero and heavily right-skewed, so `mean − 3σ` is routinely negative — a
  bound no observation can reach. `log1p` rather than `log` because untraded minutes
  have volume exactly 0. A deliberate deviation from README §10's "same procedure."
- **`MIN_ROLLING_STD = 1e-6`.** Not theoretical: untraded minutes give `log_return`
  exactly 0, and SHIBUSDT has ~1.9% of them. A run of them drives the trailing standard
  deviation toward zero and the z-score explodes — the least liquid symbol would
  dominate the table through its own illiquidity.
- **Read all of Silver, no lookback.** Silver is Parquet, ~2 MB in a few dozen files.
  Complexity should follow cost, and here there is none. Crossover written into the
  docstring: roughly a year of accumulation.
- **Parameters written onto every row** — an event from k=3 stays distinguishable from
  one from k=4 without a changelog.

### Bug #8 — a test passing for the wrong reason

Mutation testing caught it. Replacing `MIN_ROLLING_STD` with `0.0` **failed nothing**,
because the guard test used returns of exactly `0.0` — which even a naive `std > 0`
check filters.

The guard exists for *tiny-but-nonzero* variance. A test was added with prior returns
around 1e-9: their standard deviation is ~1.1e-9, and a 1% move against that scores
**z ≈ 9,000,000** — finite, so nothing errors, and a table full of them would look like
a detector working extremely well rather than one dividing by noise.

### The headline result

```
firing rate: 1.3273%   (predicted 0.5–3%)
916 events / 34,507 scorable bars × 2 rules
524 return_zscore  |  392 volume_zscore
```

**4.9× the ~0.27% normal theory implies** — the fat-tail effect measured on the actual
detector. It also means **README §2's "short, ranked list" is wrong**: 916 events in 17
days is not short.

### Bug #9 — the detector flagging its own data-quality artifacts

Three of the top fifteen events by |z|:

```
SHIBUSDT  volume_zscore  z = -9.34  num_trades = 0
SHIBUSDT  volume_zscore  z = -8.46  num_trades = 0
SHIBUSDT  volume_zscore  z = -7.89  num_trades = 0
```

Untraded minutes flagged as "anomalously low volume." The arithmetic was correct; the
events were useless. This is README §4's *"conflates genuine market events with
data-quality glitches"* **measured in the output rather than disclaimed in prose.**

**Operator decision:** make the volume rule one-sided (`z > k`, spikes only), matching
README §10's "to catch volume spikes," and keep `anomaly_events` a flat statistical
filter rather than adding heuristic categories.

### An operator mistake that cost nothing

The Gold table was overwritten before the "before" state was captured. Nothing was
lost — Gold is derived, Silver is intact. The number was recovered by **reimplementing
the z-score in pure SQL**, which turned out better than the original plan: it also
cross-validated the Spark implementation.

**This is the concrete answer to "why three layers?"** Not tidiness — any derived layer
can be thrown away and reconstructed, so a mistake there is cheap.

### Cross-validation

```
SQL (Trino):   two_sided_total = 392
Spark:         volume_zscore   = 392
```

Two implementations, two engines, no shared code, identical answer. Accounting closes
four ways: 524+392 = 916; 334+58 = 392; 524+334 = 858; hour histogram totals 858
independently.

### Correction to my own framing

The one-sided change removed 58 events, of which **only 13 were untraded minutes** —
the other 45 had real trading, just unusually little. The pure artifact is **1.42% of
all events**, not the dominant share implied by the top-15 view.

What justifies the change is not the volume of junk but **where it sat**: 20% of the
top 15 by |z|. A 1.4% contaminant occupying a fifth of the top of a ranked list matters
far more than its share suggests. Tradeoff stated plainly: genuine volume collapses —
including a real trading halt — are no longer flagged.

### Bug #10 — a hypothesis killed by its own significance check

Two events at exactly 12:30:00 UTC, including the largest in the dataset. 12:30 UTC is
08:30 New York — the standard US macro data release slot. A plausible story.

The hour-of-day histogram appeared to support it: hour 12 had the highest mean |z| of
any hour, 4.91 against 3.94, **z = +3.17**, surviving a Bonferroni correction for 48
tests.

Then a sensitivity check:

```
hour 12: 44 events, total |z| = 216.0
the single 2026-09-04 12:30 event contributes |z| = 33.39 = 15.5% of that total
removing it → mean|z| 4.25 → z = +1.00 → not significant
```

**One event was carrying the entire finding.** Hour 12 is not a count outlier either
(44 vs 35.8 expected, z = +1.38). The original observation was pattern-matching on n=2.

Recorded as **HYPOTHESIS TESTED AND NOT SUPPORTED** rather than deleted.

### The structure that is real

```
hour 07 UTC : 70 events  z = +5.73
hour 13 UTC : 66 events  z = +5.06
hour 09 UTC : 54 events  z = +3.05
hour 17 UTC : 12 events  z = −3.97
```

All survive Bonferroni. Hour 13 contains the US equity open at 13:30 UTC — a plausible
explanation. **Hour 07 has no explanation I am confident in, and none was invented.**

Caveat recorded: 78% of these events are BTCUSDT, so this is close to a BTC hour-of-day
profile, not a market-wide one.

---

## Running tallies

### Bugs, and who caught them

| # | Bug | Caught by |
|---|---|---|
| 1 | `BAR_LIMIT` NameError from a corrupted file | first deploy |
| 2 | Session-timezone defect disabling the unfinished-bar filter | **local tests only** — would have passed on Glue |
| 3 | `glue-temp_$folder$` IAM denial | **real AWS only** — no local run touches S3 markers |
| 4 | Duplicated stage composition in `run()`/`transform()` | local tests |
| 5 | A fixture structurally unable to detect failure | reading the assertion output |
| 6 | Spark/Athena partition column type drift | local tests |
| 7 | Unbounded Lambda retry budget | arithmetic on a measurement |
| 8 | Guard test passing for the wrong reason | **mutation testing** |
| 9 | Detector flagging untraded minutes as anomalies | **looking at real output** |
| 10 | 12:30 UTC hypothesis built on one event | **checking my own claim** |
| 11 | MATICUSDT delisted; status-only probe insufficient | **writing a better probe** |

No single method found more than two. That is the argument for having all of them.

### Predictions

**Held:** duplicate rate 60–65% → 63.95%; `last_bar` ≈ 19:32 → exact; +20 NULL
`rolling_vol` per symbol → exact ×17; firing rate 0.5–3% → 1.33%; memory unchanged at
17 symbols → exact.

**Missed:** MATICUSDT would return HTTP 400 → returned 200 with 729-day-old data;
Lambda at 17 symbols ~14.3 s → 10.7 s (fixed overhead over-counted); incremental Glue
toward 90 s → 150 s (still unexplained); "execution time is volume-independent" → true
only within the measured range.

### Corrections made to my own claims

- $10 budget alarm set before computing that the design costs ~$42/month
- "Execution time is volume-independent" — true only in the range measured
- M-018's three missing bars — an error in the baseline, not the data
- The one-sided volume change justified as removing mostly artifacts — it was 13 of 58
- The 12:30 UTC clustering — pattern-matching on n=2

---

## State as of 2026-09-09

**Running:** ingestion every 5 minutes across 17 symbols; both Glue jobs deployed and
run manually; Silver 34,524 rows; Gold 858 anomaly events; Athena queryable.

**Not yet built:** DQ gate (slice 7), Step Functions orchestration and the hourly
schedule (slice 8), the remaining two Gold tables, visualisation, CI, README
corrections (slice 9).

**Total AWS spend:** under $0.50.

**Tests:** 45 local Spark tests, several verified by mutation testing.

**README corrections queued:** §18 lists MATICUSDT; §16's queries use unquoted `date`;
§12's "~10x" should be the measured 12.0×; §2's "short, ranked list" contradicts a
1.33% firing rate; §9's `dollar_volume` needs the `quote_volume` note.

---

## Slice 7 — DQ gate, written but not deployed (2026-09-10)

**Built (code only):** `lambdas/dq_gate/lambda_function.py`, its IAM policy and role,
the Lambda definition, five threshold variables, and a `dq_results` history table in
the Gold catalog. 15 new tests; full suite 60 passing.

**Decisions:**

- **Ratios with tolerances, not booleans.** README §11's "flag `num_trades = 0`" would
  trip on SHIBUSDT (1.89% untraded) every run. A gate that always fires gets ignored.
- **Two severities.** FAIL blocks the pipeline (row count, symbols present, OHLC,
  positive prices, freshness, monotonic time). WARN records and alerts but does not
  block (empty-bar ratio, sequence gaps). Corruption blocks; degradation warns.
- **Every threshold traced to a measurement:** freshness 15 min (structural lag
  4m23s, M-025 — README's "one interval" would fail every run); empty bars 5%
  (SHIB 1.89%, M-039); gaps 0.5% (zero measured, M-022); row count 70%.
- **Athena, not Spark** — aggregate queries over tens of thousands of rows; Glue
  would spend 90 s starting up to answer what Athena answers in two.
- **Two queries, not seven** — Athena bills a 10 MB minimum per query.
- **`evaluate()` is pure** — no AWS calls, no clock reads — so every threshold edge
  is unit-testable in 0.04 s without Athena.
- **Results persisted as JSON Lines, one line per check**, registered as
  `crypto_anomaly_gold.dq_results`, so DQ history is a `GROUP BY`, not log scraping.
- **IAM:** Athena runs queries *as the caller*, so the role needs catalog read, Silver
  read, results-bucket write, and `athena:GetWorkGroup` (required by the enforced
  workgroup) — the under-scoping README §13 warns about.

**Bugs found while writing (bugs #12 and #13):**

- **`check` is a reserved word in Athena** — renamed to `check_name` before it could
  break every query against the history table. Same trap as `date` in slice 3.
- **`json.dumps(float('inf'))` emits bare `Infinity`, which is invalid JSON.** The run
  most worth recording — an empty Silver, where freshness is infinite — would have
  silently failed to persist. Now written as `null`. Verified.

**Mutation testing:** making the empty-bar check blocking, reverting freshness to
README's 1 minute, lowering empty-bar tolerance below SHIB's measured rate, and
removing the divide-by-zero guard each failed at least one test. A stale
`__pycache__` briefly made a restored file look still-mutated — worth knowing.

**Not done:** `terraform apply` was never run, nothing was committed, and the planned
"watch it fail on stale Silver, then pass" demonstration never happened. Terraform
state confirms no DQ resources exist in AWS.

---

## The gap (2026-09-10 → 2026-11-08)

The project paused for about two months with slice 7 uncommitted and undeployed.

The 5-minute ingestion schedule was never disabled, so it has presumably been writing
Bronze the whole time — roughly 17 symbols × 288 runs/day × 60 days ≈ 290,000 new
objects, **unverified until checked**. Neither Glue job is scheduled, so Silver and
Gold have been frozen at their 2026-09-09 state.

**This creates a problem the incremental design did not anticipate.** A 3-hour
lookback only rewrites the last one or two dates. Run as-is, it would populate today
and leave a two-month hole in Silver between 2026-09-09 and now. Full reprocess would
cover it but reads every Bronze object ever written — by the (already unreliable)
runtime model, far beyond the 25-minute timeout. Silver needs a deliberate backfill
before anything downstream is trustworthy.

---

## Resumption (2026-11-08 / 09)

### Bug #14 — the AWS CLI would not run at all

```
zsh: bad CPU type in executable: aws
```

Not an AWS problem: macOS refusing to execute a binary built for another processor.
`uname -m` was `arm64` (Apple Silicon), and `aws` lived at `/usr/local/bin` — the
Intel-era install location — while Terraform was already native `darwin_arm64`. The
Intel `aws` had presumably been running under translation, which was no longer
available after an OS upgrade during the gap. Fixed by AWS's official installer
(`aws-cli/2.39.6 ... exe/arm64`); credentials in `~/.aws/` survived untouched.

Decision: **do not upgrade Terraform** (1.15.8 → 1.17.1 was offered). It satisfies
`>= 1.6`, and changing it right before touching state again adds risk for no gain.

### Health check

- **Tests:** 60 passed — the local Spark toolchain survived the gap.
- **Ingestion:** still running every 5 minutes, ~10.96 s, 95 MB (M-050) — no drift.
- **Bronze:** **303,678 objects, 831 MB** (M-049), within about one run's worth of
  what the schedule arithmetic predicts for two months of 17-symbol ingestion.
- **Cost Explorer:** output not yet received.

A cost prediction was logged before seeing the bill (M-051): S3 should be dominated
by **PUT requests (~$0.73/month)**, not storage (~$0.02/month).

### Bug #15 — a backfill hazard the incremental design never had to face

Planning the backfill exposed an interaction between two earlier decisions: Bronze is
partitioned by **ingest** date (slice 1), and incremental runs rewrite whole **bar**
dates (slice 4.5). Day D's last minutes are fetched after midnight, so they sit under
Bronze `date=D+1`.

In production `as_of` is "now", so the newest target date has no D+1 yet and nothing
is lost. But a backfill sets `as_of` in the past. Targeting only D would read
`[D-1, D]`, miss D's tail, and dynamic overwrite would write a permanently short day
— silently.

Proven with a test before relying on it:
`test_backfill_targeting_only_day_d_loses_its_final_minutes` confirms the loss, and
`test_backfill_with_as_of_just_after_midnight_writes_day_d_completely` confirms the
fix: `as_of = (D + step) 00:30`, so the reads always include the day after. Each step
writes its last day short and the next step rewrites it, so steps must run in order.
62 tests passing.

**Built:** `scripts/backfill_silver.sh`, which runs those steps sequentially, stops on
the first failure (a skipped step would leave a hole), and logs every run's
`ExecutionTime` to `backfill_log.csv` — sixty-odd runs over a constant input size, the
runtime distribution the n=1 measurements never provided.

Step size is a parameter because it is a real trade-off: one day per run reads 3 days
to finish 1 (3× read amplification); three days per run reads 5 to finish 3 (1.67×).
Bigger steps amortise startup and warm-up — the two-cadence lesson again — but each
run gets closer to the timeout. Plan: calibrate with a single one-day step first.

### The bill, finally (M-052)

Cost Explorer: **$1.96 total since August.** The prediction logged *before* looking
(M-051) held: October's S3 write requests were **151,815 against 151,776 predicted**
for a 31-day month (+0.03%). Requests cost 69× what storage did. Ingestion's real
running cost is ~$0.79/month, essentially all S3 PUTs — the price of one small file
per symbol every five minutes.

A small `APS1` storage charge briefly looked like a stray resource. It is
ap-southeast-1, where three unrelated buckets in the same account live. Lesson:
Cost Explorer reports the *account*, not the project — the `Project` default tag from
slice 0 is the precise filter.

### The calibration step (M-053)

**A prediction corrected before the run.** I had told the operator to expect 24,480
bars on 09-09 (17 × 1,440). Checking each symbol's first bar showed 16 symbols only
began at 04:23–04:24 that day. Re-derived from the data: **20,259 bars, 19,939 with
`rolling_vol`.** Measured after the run: **20,259 and 19,939, exact.** Every symbol
ends at 23:59, so the late-ingested tail problem (bug #15) is fixed on real data, not
only in tests. 323 s, ~$0.08.

**An explanation corrected after the run.** I had said the tail day (09-10) would be
written with "only its first few minutes." It held 24,446 of 24,480 — short by exactly
17 × 2 minutes. The job reads whole Bronze *ingest-days*; `as_of` chooses which dates
to rewrite, it does not truncate the data. The plan was unaffected (the next step
rewrites that day), but the stated mechanism was wrong.

### Bug #16 — Glue's concurrency slot outlives the run

The full backfill (`2026-09-10 → 2026-11-08`, 3 days per step) stopped after its first
step. Step 1 SUCCEEDED in 576 s (predicted ~700 s); step 2 was rejected immediately:

```
ConcurrentRunsExceededException: Concurrent runs exceeded.
```

Nothing else was running. `get-job-run` reports SUCCEEDED a few seconds before Glue
releases the job's single slot (`max_concurrent_runs = 1`, chosen in slice 2b), and the
script started the next run inside that window. The calibration run never hit it
because it was a single step.

The stop-on-first-failure rule did its job: step 2 never started, so nothing was
half-written. Fix: retry `start-job-run` on that one error (30 s, up to 6 times);
any other error still aborts. Resumed from `2026-09-13` — step 1 had written Sep 10–12
completely and left Sep 13 short, which is exactly where the next step begins.

### The backfill, completed (M-054, M-055)

With the retry in place, the remaining 19 steps ran unattended, 07:06–10:25 UTC. All 20
three-day steps SUCCEEDED. Every back-to-back start (18 of 18) needed exactly one 30 s
wait for the slot — bug #16 is systematic, not occasional.

**The runtime question got a partial answer.** Eighteen runs over the same input size
(the first and last steps read less): p50 575 s, stdev 22 s (3.9%). So at a fixed input,
run-to-run spread is small. That does **not** explain September's 90 s vs 150 s on
similar input — those runs also ran different code (slice 4 features, slice 4.5
incremental path), and that gap remains unexplained. The slice-4 model over-predicted
by ~18% (~700 s vs 575 s). Cost ≈ $2.81, derived from list price.

**Asked, before checking: "can I assume no gaps now?" Answered: no — verify.** The
backfill guarantees that what is in Bronze reaches Silver; it cannot create bars that
were never fetched. Three or more consecutive missed runs, deadline-deferred symbols,
Binance-side maintenance, or crashed invocations could all leave real holes.

Checked four ways, all clean:

- every day Sep 10 → Nov 8 at exactly **24,480 bars**, all with `rolling_vol`
- the `LAG(open_time)` gap query over all **1,522,959** bars: **0 rows**
- NULL `rolling_vol` = **340 = 17 × 20**, NULL `log_return` = **17** — exact, and the
  time-based windows mean any gap would have added more
- Lambda: **0 errors, 288 invocations on every one of 60 days**

Zero missing minutes over 79 days, 61 of them at 17 symbols, with nobody watching.
That is the strongest reliability number the project has.

Silver now ends at 2026-11-09 10:12 (the last step read Nov 5–9 and rewrote Nov 6–9,
including today's Bronze; I had said Nov 6–9 / Nov 7–9 — off by a day), so it is
already stale by the gate's 15-minute rule — the fail-then-pass demonstration is set up.

### The pipeline tour page (2026-11-09)

The operator asked for one place to see every layer and the services between them.
Options weighed: a snapshot page (chosen), a local Streamlit explorer (later, for
debugging), and QuickSight/Managed Grafana (rejected — $9–24/user/month, more than the
whole pipeline costs to run, and click-built config is weaker evidence than code).

**Decision: snapshot, rebuilt by a script, not a live page.** A live page needs a public
API in front of Athena — new infrastructure and a way for anyone with the link to run
queries on the account. `scripts/build_tour.py` (stdlib + `aws` CLI) writes
`tour/data.json`; `tour/index.html` renders it; republishing keeps the same link. To be
rebuilt at milestones: Gold rebuild, slice 8, end of slice 9.

The page follows the largest |z| event in Gold (BTCUSDT 2026-09-04 12:30, z = −33.4)
through all three layers: the 3 Bronze files that carried it (positions #12, #7, #2 of
15), the single Silver row, and the z-score worked out. **Its baseline band is
recomputed in Athena SQL for the page, and matches the `rolling_mean`/`rolling_std`
Spark stored in Gold exactly** — a second cross-engine check, after M-046, this time
on the return rule.

## Slice 7 deployed — the DQ gate in production (2026-11-09)

`terraform plan` after two months and a reinstalled CLI showed only additions — no
drift. Applied; Lambda active, `dq_results` registered.

**Prediction before the first invocation:** Silver ended at 10:12 UTC, so not only
freshness but row count should fail — a 24-hour window missing ~9 hours holds ~60% of
the expected bars, under the 70% floor. Measured (M-056): freshness 563 min, row_count
60.97%, the other six passing. Exact.

### Bug #17 — the half-finished rename from bug #12

The invocation itself crashed:

```
KeyError: 'check'   at "blocking_failures": [r["check"] for r in blocking]
```

In September, `check` was renamed to `check_name` because CHECK is reserved in Athena
(bug #12). The rename reached `evaluate()` but missed two lines in `lambda_handler()`.
All 15 gate tests called `evaluate()` directly — none called the handler — so nothing
read those lines. The crash came *after* `persist()`, so the history row was written
and the prediction could still be checked from S3; but the caller got an error instead
of `quality_passed`, which in slice 8 would have failed the Step Functions run on every
bad-data day rather than routing it to the alert branch.

A detail that explains why September's testing could not have caught it even by luck:
**on a healthy run the broken line never executes** — the list comprehensions iterate
over empty lists. The bug only fires when a check fails, i.e. exactly when the gate is
doing its job.

Fix: two-line correction plus two handler tests (AWS calls stubbed). The failure-path
test was run against the unfixed code first and reproduced the production `KeyError`;
the healthy-path test passed on the broken code — which is the bug's whole story in one
result. 64 tests passing.

Lesson recorded: "evaluate() is pure, so test evaluate()" was a good design and an
incomplete test plan. The thin glue around the pure core still needs one test.

### Fail, then pass (M-057)

With the fix deployed, the gate returned a clean `quality_passed: false` naming
`row_count` and `freshness`. One incremental Silver run (308 s, newest bar 19:37:00 —
the predicted minute exactly) and the gate passed all 8 checks at 19:44: freshness
7.75 min, 99.5% of the window present. The demonstration planned in September finally
happened — two months late, and better for it, because the "fail" half came from a
real two-month staleness rather than a staged one.

A measurement trap found on the way: the incremental run reported an 83.94% "duplicate
rate." It is not one. In incremental mode the warm-up day is read and then filtered
out, and the stat counts those rows as duplicates removed. The definition only holds
on full reprocess (M-012/M-017). Logged so the number never reaches a resume.

## Gold rebuilt over 79 days (2026-11-09)

Predictions were logged first: 3–6 min, 33k–43k events, firing 1.1–1.4%, 13 UTC holds,
07 UTC uncertain. Measured (M-058): **133 s, 30,156 events, 0.98%.** Two of the three
numeric predictions missed.

**Why the firing rate missed.** September's 1.24% came from a dataset that was ~95%
BTC. Across 17 pairs the return rule still fires at 1.37%, near September's 1.52%. The
drop is the volume rule: 1.30% of BTC's bars but 0.055% of ATOM's — 24× less on thin
pairs. The prediction carried one pair's behaviour to seventeen. The same mistake as
the Lambda extrapolation in slice 5, in a different place.

**Why runtime missed.** 44× the input, 1.36× the time. This job reads compact Parquet,
not tens of thousands of small JSON files. "Glue is startup-dominated" turns out to be
true or false per job, depending on what it reads.

### The 12:30 hypothesis, revisited (M-059)

In September, two events at 12:30 UTC suggested US data releases; a sensitivity check
showed one event carried the effect, and it was recorded as NOT SUPPORTED. That was the
right call on that evidence.

With 79 days, the minute-of-day profile of return events is unmistakable. The average
minute has 14.6 events; **12:30 has 100, on 19 separate days, across all 17 pairs, with
the highest mean |z| of any minute (9.43).** The minutes after 13:30 (the NYSE open),
22:00–22:02 (CME futures reopen) and 20:01 (the NYSE close) stand out too.

Stated carefully: these are alignments with known US-market times, not tested causes —
no economic calendar has been joined in. Events within a minute are correlated across
pairs, so "distinct days" is the honest count. And September's 07 UTC peak, which never
had an explanation, has disappeared: it belonged to the BTC-only sample.

The sequence — hypothesis, rejected on thin evidence, revisited with more data — is
the point. The September rejection was correct, and so is the revision.

---

## 2026-11-10 — Slice 8 designed before building

The design for slice 8 was settled before writing any code. It went through a structured interview of 12 questions over three rounds, with each decision written down as it was made:

- `GLOSSARY.md`, the project vocabulary
- two ADRs in `docs/adr/`
- a spec, GitHub issue #1, split into six linked tickets (#2–#7) with native "blocked by" links

**The interview found a stale number before it could do damage.** The plan was to schedule the chain hourly at "$33/month" (M-020). Checking that against current measurements showed it came from August's 1-pair, 94-second runs. At 17 pairs, one run is Silver 308 s + Gold 133 s ≈ $0.108, so hourly forever would cost ≈ $78/month against a $10 budget (M-060).

The design changed as a result (ADR-0002): one bounded window of 24 hourly runs to *measure* the hourly design, then a daily run permanently. The window switches itself off through a schedule end date, so the safety doesn't depend on anyone remembering. M-020's 12.0× multiple still stands, because it is a ratio of run counts. Only its dollar amounts were superseded.

**Decisions and their reasons:**

- **Step Functions** (ADR-0001), because only an orchestrator that branches on the gate Lambda's answer can actually stop Gold. Glue Workflows can't.
- **Retry only transient errors.** Bug #16's concurrent-runs error is retried. A crashed Glue job is never retried, because that pays for the same crash again.
- **Email only on problems.**
- **A 26-hour lookback on every run**, because a daily run with the old 3-hour lookback would silently skip 21 hours.
- **Gold stays a full rebuild** until measurements say otherwise (ticket #7).

**Data delay was redefined to match an existing METRICS rule** (open question 3). It is the time from a bar's close until it is queryable in Gold, reported as a distribution. The first draft, "age of the newest bar", measured only the best case.

---

## 2026-11-11 — Ticket #2: the transform chain ran itself for the first time

**The state machine is deployed.** `crypto-anomaly-transform-chain` runs Silver, then the gate, then Gold. The only way into Gold is the gate's Choice, on an explicit pass. The definition sits in its own file, and 9 offline tests walk its paths. To check that the tests catch mistakes, I broke the definition four ways: let a blocked gate reach Gold, dropped the wait-for-job, skipped the gate, started at the gate. Every break failed 1–3 tests.

**Before the first run, the old lookback would have hidden a gap.** Silver was 31 hours stale, and the chain still passes Silver's old 3-hour default (the 26-hour lookback is ticket #4). A run would have rewritten only Nov 11 and left Nov 9's evening and all of Nov 10 missing. The gate would then have blocked it on row_count. This is ADR-0002's argument, demonstrated rather than predicted. A direct catch-up run with a 32-hour lookback fixed Silver first (453 s, Nov 9 and 10 complete at 24,480).

**Bug #16 bit again in production.** The first execution started 18 s after the catch-up run reported SUCCEEDED, and Glue refused it with "concurrent runs exceeded". It failed in under a second at no cost. The retry worked. That error is exactly what ticket #3 retries, so the ticket now rests on a real failure instead of a hypothetical one.

**The second attempt succeeded in 7.0 minutes** (M-061): Silver 178 s, gate 3.7 s (passed 8/8), Gold 157 s. The surprise is that **67 s of the 421 s is Step Functions noticing a finished Glue job late**: 47 s after Silver ended, 20 s after Gold ended. The run-and-wait integration checks the job on an interval, so part of every run is waiting to find out it is done. That matters for data delay (ticket #6) and should be in the predictions for the window.

---

## 2026-11-11 — Ticket #3: failures now email, and bug #16 is retried

**What a run does when it goes wrong.** If the gate blocks a run, one email lists the blocking checks and gives a ready-to-paste Athena query for their observed values. If Silver, the gate or Gold crashes or times out, one email names the step and its error. Successes and warnings send nothing. The only retries are for transient AWS errors: Glue's "concurrent runs exceeded" (bug #16, which hit production during ticket #2) and Lambda service hiccups. They retry up to 3 times with growing waits. A real crash is never retried.

**How it was proven.** 15 offline tests cover the rules. Five deliberate sabotages each broke at least one test:
- removed the alert
- let a Gold crash loop back into Gold
- retried real failures
- set 5 attempts
- skipped the alert on a crash

AWS's own definition validator accepted the result. Then TestState ran the deployed Choice and both alert states with made-up inputs, and **both emails arrived** (M-062). Nothing touched Silver or Gold.

**A small departure, written down.** The ticket asked for observed values in the gate email, but the gate's response lists only check names, and this slice doesn't change the gate. The email carries the query that retrieves the values instead.

---

## 2026-11-11 — Ticket #4: every run looks back 26 hours

**What changed.** The state machine now handles its own input before doing any work. It sets a default of 26 hours, merges in the run's input (a manual run can ask for `{"lookback_hours": N}`), and admits only a number from 1 to 72. Anything else fails at once, with no Glue run, no gate and no email. The limit exists mainly for 0, which the Silver job treats as "reprocess all history" and which would blow the 25-minute timeout at today's size.

**How it was proven.**
- 4 new offline tests, each confirmed by a deliberate sabotage.
- TestState runs of the merge and the guard against AWS, *before* deploying.
- Three real executions (M-063):
  - a run with 0 failed in 0.05 s
  - the default run passed 26 to Glue and succeeded in 11.05 min
  - the override run passed 40 and succeeded in 12.9 min

**What it costs, measured.** Silver with 26 hours took 354 s, against 178 s with 3 hours, about 2×, because each run rewrites two days instead of one. A run is now ≈ $0.131, not the $0.108 assumed in ADR-0002. The window becomes ≈ $3.14 and daily running ≈ $3.93/month, still inside the budget. This is the price of the property ADR-0002 bought: a run never silently skips data, and any run catches up after a failed predecessor.

**Two of the predictions missed narrowly**: Gold by 1 s and the whole run by 3 s. They are recorded as misses rather than rounded away.

---

## 2026-11-11 — Ticket #5: the pipeline started running itself

**What changed.** Two EventBridge Scheduler schedules now start the transform chain, both in UTC and both through a role that can do one thing: start this state machine.
- **Hourly, bounded:** fires at :00 from 2026-11-11 06:00 UTC to 2026-11-12 05:00 UTC, exactly 24 times, then stops by its own end date. Forgetting about it cannot overrun the budget.
- **Daily, permanent:** 00:30 UTC from 2026-11-13. The start/end dates come from one Terraform variable, `measurement_window_start`, so the window was placed deliberately.

**How it was proven.** Predictions for the whole window were logged first (M-064). Then the first scheduled run fired at 06:00:18 UTC, nobody touching anything, and succeeded in 11.35 min with the gate passing (M-065). Two first-run misses are written down instead of waved off: Gold took 218 s against a predicted 175–200 s, and the run cost $0.149 against $0.12–0.14. Whether those hold across 24 runs is ticket #6's question.

---

## 2026-11-12 — Ticket #6: the measurement window, measured

**What happened.** For 24 hours the pipeline ran itself every hour with nobody touching it. **24 of 24 runs succeeded**, the gate passed all 24 (192 of 192 checks, no warnings), nothing was retried, and no alert email was sent. The hourly schedule then stopped on its own end date, as designed.

**The numbers (M-064)**, built by a new script, `scripts/measure_window.py`, whose data-delay logic was written test-first:
- **Data delay: p50 42.5 min, max 74.8 min.** Both inside the predictions. This is the honest answer to "how long until a minute of market data shows up in Gold, on an hourly cadence?"
- **Run duration: p50 11.69 min**, range 9.2–13.8 min.
- **Cost: p50 $0.149 per run**, $3.51 of Glue for the window.

**The misses, kept.** Most of the *range* predictions were too narrow: duration, Silver, Gold, cost, freshness and polling lag each had runs outside them. The pattern behind it: Glue's run-to-run spread (Gold anywhere from 117 to 279 s) is bigger than I assumed from 3 hand-started runs. One predicted trend held: Silver did get slower later in the day (median 387.5 s in the morning UTC vs 436.5 s in the evening). The other didn't show: Gold didn't creep up (173 s vs 176 s by half). S3 request charges were left out of the cost model entirely, and Cost Explorer shows them.

**Budget.** The window and the backfill set off the *forecast* budget alert (AWS forecast $45.93, extrapolated from the two expensive days; M-066). The realistic November total is ≈ $12–13 (ESTIMATED) against the $10 budget. The owner chose to accept the overrun and keep daily runs from Nov 13.

**Still open on #6:** confirm the first daily run (2026-11-13 00:30 UTC) and record Cost Explorer's figure for the window days.

---

## 2026-11-11 — How we work changed: skills first, then a review that found real gaps

**The process.** The owner noticed that tickets #2–#5 had been specced, ticketed and built by hand, without Matt Pocock's skills, and that none had a code review. Two rules went into CLAUDE.md:
- the owner commits and pushes
- Claude names the skill to type at every stage

The full map lives in `docs/agents/skill-flow.md`: `/grill-with-docs` → `/to-spec` → `/to-tickets` → `/implement` → `/code-review` → commit → `/retro`, plus trigger moments such as `/diagnosing-bugs` on an alert. Several of these skills can only be started by the owner, which is why they had gone unused.

**The catch-up review.** `/code-review since 1d5c726` reviewed #2–#5 together against spec #1 and raised 10 findings. Each was checked against the code. `/triage` then filed six tickets (#8–#13) and closed three findings as accepted, with reasons in a comment on #1. The most serious finding: the permanent daily schedule took its start date from the one-off window variable, so the next `terraform apply` would either prompt, restart a paid window, or send a past date (#8). The most surprising: "every failure emails" was false, because `States.ALL` doesn't catch `States.Runtime` (#9).

---

## 2026-11-12 — Ticket #8: the window is gone, the daily schedule stands on its own

The hourly window schedule, its two variables and its output were deleted once the 24 runs were measured. The daily 00:30 UTC schedule lost its `start_date`, so a cron schedule simply fires at the next 00:30. Plan showed exactly one destroy and one in-place update, and nothing was replaced. AWS now lists only the ingestion and daily schedules.

**Two review catches:**
- A daily-cost figure was rounded up ($4.48 → $4.47).
- Cost numbers were copied into both the ADR and the Terraform header. The header now points to ADR-0002 instead, so the numbers live in one place.
