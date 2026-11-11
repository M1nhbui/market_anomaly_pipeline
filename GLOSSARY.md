# Crypto Market Anomaly Pipeline

Collects one-minute market data for a fixed set of crypto pairs and flags minutes that sit far outside a pair's own recent behaviour, for a person to review. Its output is a review filter, not a trading signal.

## Market data

**Pair**:
A traded instrument quoted in USDT, such as BTCUSDT. The configured set is the *active pairs*; a pair that was considered and dropped is *rejected*.
_Avoid_: symbol (except as a column name), coin, ticker

**Bar**:
One minute of trading for one pair: open, high, low and close prices, plus volume and trade count. It is identified by its pair and its open time.
_Avoid_: candle, kline, row, tick

**Untraded bar**:
A bar with zero trades. It is normal for thinly traded pairs and is not missing data.
_Avoid_: empty row, missing bar

**Gap**:
A minute with no bar at all for a pair. A gap is missing data; an untraded bar is not.

## Layers

**Bronze**:
The raw landing zone. Every fetched response is kept exactly as received and is never modified.

**Silver**:
One cleaned, typed and deduplicated bar per pair per minute, with derived features added.

**Gold**:
The analytical outputs built from Silver: events and the gate's quality history.

## Detection

**Baseline**:
The mean and standard deviation of a pair's previous 60 bars, excluding the bar being scored.
_Avoid_: rolling window (ambiguous about whether the current bar is included)

**Rule**:
One measured quantity scored against its baseline. There are two: the *return rule*, which is two-sided, and the *volume rule*, which counts upward spikes only.

**Event**:
A bar whose z-score under one rule exceeds the threshold. A bar can produce up to two events, one per rule.
_Avoid_: anomaly (as a noun for a stored row), alert, signal

**Firing rate**:
Events divided by the number of bar-rule pairs that could have been scored.

## Orchestration

**Run**:
One execution of the transform chain: Silver, then the gate, then Gold only if the gate passes.
_Avoid_: job (a Glue job is one step of a run), pipeline run, execution (except for the Step Functions object)

**Lookback**:
How far back in time a run looks for new Bronze data. Every Silver day it touches is rewritten in full.

**Measurement window**:
The one bounded period of hourly runs that exists to measure the hourly design. Outside it, runs are daily.

**Data delay**:
The time from a bar's close until that bar is queryable in Gold. It is a distribution over bars, not one number per run.
_Avoid_: freshness (which is the gate's check on Silver, measured before Gold runs), latency

## Quality

**Gate**:
The quality check that runs between Silver and Gold and decides whether Gold may be built from the current Silver.
_Avoid_: DQ check (for the whole), validator

**Check**:
One named test inside the gate. Its severity is either *fail* or *warn*.

**Freshness**:
The age of the newest bar in Silver at the moment the gate checks it.
_Avoid_: data delay (that is the age of the output after Gold)

**Blocking failure**:
A failed check with severity *fail*. Any blocking failure means the gate does not pass.

**Warning**:
A failed check with severity *warn*. It is recorded but does not stop the gate from passing.
