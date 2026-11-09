#!/usr/bin/env bash
#
# Backfill Silver across a date range, one Glue run per step, sequentially.
#
#   ./scripts/backfill_silver.sh 2026-09-09 2026-09-09          # calibrate: one day
#   ./scripts/backfill_silver.sh 2026-09-10 2026-11-08 3        # 3 days per run
#
# WHY NOT JUST RUN A FULL REPROCESS
# ---------------------------------
# Full reprocess reads every Bronze object ever written (303,678 as of 2026-11-09).
# Runtime grows with object count (M-029), and at this size it would very likely blow
# through the 25-minute timeout. Each step here reads only (step_days + 2) days.
#
# WHY as_of IS "THE DAY AFTER THE STEP, AT 00:30"
# -----------------------------------------------
# Bronze is partitioned by INGEST date, so day D's last minutes live under Bronze
# date=D+1. A step that targets D must also read D+1, or it writes a short day and
# dynamic partition overwrite makes that permanent. Proven in tests/test_incremental.py
# (test_backfill_targeting_only_day_d_loses_its_final_minutes).
#
# Setting as_of = (D + step) 00:30 with lookback = (step-1)*24 + 1 hours makes the
# targets [D .. D+step] and the reads [D-1 .. D+step]. Days D .. D+step-1 are written
# completely. Day D+step is written short - and the NEXT step starts there and
# rewrites it in full. Steps must therefore run IN ORDER, which they do: the job
# allows one concurrent run, and this script waits for each to finish.
#
# WHY STEP SIZE IS A PARAMETER
# ----------------------------
# Each run pays ~90 s of startup and re-reads one warm-up day plus one tail day. One
# day per run reads 3 days to finish 1 (3x read amplification); 3 days per run reads
# 5 to finish 3 (1.67x); 7 reads 9 to finish 7 (1.29x). Bigger steps amortise the
# fixed costs - the same lesson as the two-cadence design - but each run gets longer
# and closer to the timeout. Calibrate with one day first, then choose.
#
# Every run's ExecutionTime is appended to backfill_log.csv. Sixty-odd runs over the
# same input size is the runtime DISTRIBUTION that the n=1 measurements never gave us.
set -euo pipefail

START="${1:?usage: backfill_silver.sh START_DATE END_DATE [STEP_DAYS]}"
END="${2:?usage: backfill_silver.sh START_DATE END_DATE [STEP_DAYS]}"
STEP="${3:-1}"
JOB="crypto-anomaly-bronze-to-silver"
LOG="backfill_log.csv"

[[ -f "$LOG" ]] || echo "step_start,as_of,lookback_hours,run_id,state,execution_seconds" > "$LOG"

# macOS ships BSD `date`, whose flags differ from GNU `date`. Python is portable.
STEPS=$(python3 - "$START" "$END" "$STEP" <<'EOF'
import sys
from datetime import date, timedelta
start, end, step = date.fromisoformat(sys.argv[1]), date.fromisoformat(sys.argv[2]), int(sys.argv[3])
d = start
while d <= end:
    as_of = d + timedelta(days=step)
    print(f"{d.isoformat()} {as_of.isoformat()}T00:30:00Z {(step - 1) * 24 + 1}")
    d += timedelta(days=step)
EOF
)

while read -r STEP_START AS_OF LOOKBACK; do
  echo "== step $STEP_START  (as_of=$AS_OF, lookback=${LOOKBACK}h)"

  # Glue reports the previous run SUCCEEDED a few seconds before it releases the
  # job's single concurrency slot (max_concurrent_runs = 1), so an immediate start
  # can fail with ConcurrentRunsExceededException. Observed 2026-11-09 between the
  # first and second step. Retry that one error only; anything else still aborts.
  RUN_ID=""
  for attempt in 1 2 3 4 5 6; do
    if OUT=$(aws glue start-job-run --job-name "$JOB" \
        --arguments "{\"--as-of\":\"$AS_OF\",\"--lookback-hours\":\"$LOOKBACK\"}" \
        --query JobRunId --output text 2>&1); then
      RUN_ID="$OUT"; break
    fi
    if [[ "$OUT" == *ConcurrentRunsExceeded* ]]; then
      echo "   slot not released yet (attempt $attempt), waiting 30s"; sleep 30
    else
      echo "$OUT" >&2; exit 1
    fi
  done
  [[ -n "$RUN_ID" ]] || { echo "Could not start step $STEP_START" >&2; exit 1; }

  while true; do
    read -r STATE SECS <<<"$(aws glue get-job-run --job-name "$JOB" --run-id "$RUN_ID" \
      --query 'JobRun.[JobRunState,ExecutionTime]' --output text)"
    case "$STATE" in
      SUCCEEDED|FAILED|TIMEOUT|STOPPED|ERROR) break ;;
    esac
    sleep 20
  done

  echo "$STEP_START,$AS_OF,$LOOKBACK,$RUN_ID,$STATE,$SECS" >> "$LOG"
  echo "   $STATE in ${SECS}s"

  # Stop on the first failure. Later steps depend on earlier ones having rewritten
  # their tail day, so continuing past a failure would leave a silent hole.
  if [[ "$STATE" != "SUCCEEDED" ]]; then
    echo "Stopping. Inspect: aws glue get-job-run --job-name $JOB --run-id $RUN_ID" >&2
    exit 1
  fi
done <<<"$STEPS"

echo "Done. Per-run timings in $LOG"
