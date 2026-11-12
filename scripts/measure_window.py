"""
Measure the transform-chain measurement window (ticket #6).

Data delay (GLOSSARY) is the time from a bar's close until it is queryable in Gold.
A bar becomes queryable when the Gold job of the first run whose Silver included it
finishes. So run k makes queryable every bar newer than run k-1's newest bar, up to
and including its own newest bar.

Delays are per MINUTE: every pair's bar for a minute shares one delay, because a run
reads all pairs together. The newest bar comes from the gate's freshness check, which
is the newest across pairs.

Usage: python3 scripts/measure_window.py 2026-11-11T06:00:00Z 24 > window.json
"""

import json
import math
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

BAR = timedelta(minutes=1)


@dataclass(frozen=True)
class Run:
    newest_bar_open: datetime  # open time of the newest bar in Silver at gate time
    gold_finished: datetime  # when this run's Gold job finished


def data_delays(runs):
    """One delay in seconds per minute of bars, for every run after the first.

    `covered` is the newest bar already queryable, so a bar is counted once, from the
    first run that made it queryable, even if a later run's Silver is behind.
    """
    delays = []
    covered = runs[0].newest_bar_open
    for run in runs[1:]:
        bar = covered + BAR
        while bar <= run.newest_bar_open:
            delays.append((run.gold_finished - (bar + BAR)).total_seconds())
            bar += BAR
        covered = max(covered, run.newest_bar_open)
    return delays


def summarize(delays):
    """p50 and max lead; min is kept for context, never quoted alone (METRICS OQ 3)."""
    return {
        "count": len(delays),
        "p50": statistics.median(delays),
        "max": max(delays),
        "min": min(delays),
    }


def floor_to(value, places):
    """Truncate, never round up (CLAUDE.md: measured numbers are never rounded up)."""
    f = 10 ** places
    return math.floor(value * f) / f


# ---------------------------------------------------------------------------------
# Reading AWS (not unit-tested; checked by hand against run 1, M-065).
# ---------------------------------------------------------------------------------

STATE_MACHINE = "arn:aws:states:us-east-1:290586778476:stateMachine:crypto-anomaly-transform-chain"
JOBS = {"silver": "crypto-anomaly-bronze-to-silver", "gold": "crypto-anomaly-silver-to-gold"}
DPU, USD_PER_DPU_HOUR = 2, 0.44
WORKGROUP = "crypto-anomaly"


def aws(*args):
    out = subprocess.run(["aws", *args, "--output", "json"], check=True,
                         capture_output=True, text=True).stdout
    return json.loads(out)


def utc(value):
    return (value if isinstance(value, datetime) else datetime.fromisoformat(value)).astimezone(timezone.utc)


def athena(sql):
    qid = aws("athena", "start-query-execution", "--work-group", WORKGROUP,
              "--query-string", sql)["QueryExecutionId"]
    while True:
        ex = aws("athena", "get-query-execution", "--query-execution-id", qid)["QueryExecution"]
        if ex["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if ex["Status"]["State"] != "SUCCEEDED":
        sys.exit(f"Athena failed: {ex['Status'].get('StateChangeReason')}")
    rows, token = [], None
    while True:
        page = aws("athena", "get-query-results", "--query-execution-id", qid,
                   *(["--next-token", token] if token else []))
        data = page["ResultSet"]["Rows"]
        if not rows:
            head = [c.get("VarCharValue") for c in data[0]["Data"]]
            data = data[1:]
        rows += [dict(zip(head, (c.get("VarCharValue") for c in r["Data"]))) for r in data]
        token = page.get("NextToken")
        if not token:
            return rows


def collect(window_start, runs_expected):
    start = utc(window_start)
    end = start + timedelta(hours=runs_expected)
    executions = [e for e in aws("stepfunctions", "list-executions", "--state-machine-arn",
                                 STATE_MACHINE, "--max-results", "100")["executions"]
                  if start - timedelta(minutes=1) <= utc(e["startDate"]) < end]
    rows = []
    for e in sorted(executions, key=lambda e: e["startDate"]):
        out = json.loads(aws("stepfunctions", "describe-execution", "--execution-arn",
                             e["executionArn"]).get("output") or "null") or {}
        history = aws("stepfunctions", "get-execution-history", "--execution-arn",
                      e["executionArn"], "--max-results", "1000")["events"]
        exits = {ev["stateExitedEventDetails"]["name"]: utc(ev["timestamp"])
                 for ev in history if ev["type"].endswith("StateExited")}
        row = {"name": e["name"], "status": e["status"], "start": utc(e["startDate"]),
               "stop": utc(e["stopDate"]) if e.get("stopDate") else None,
               "task_failures": sum(ev["type"] == "TaskFailed" for ev in history),
               "gate": out.get("gate")}
        for step, job in JOBS.items():
            if step in out:
                jr = aws("glue", "get-job-run", "--job-name", job, "--run-id",
                         out[step]["job_run_id"])["JobRun"]
                row[step] = {"billed_s": jr["ExecutionTime"], "completed": utc(jr["CompletedOn"]),
                             "polling_lag_s": (exits[step.capitalize()] - utc(jr["CompletedOn"])).total_seconds()}
        rows.append(row)
    ids = [r["gate"]["gate_run_id"] for r in rows if r["gate"]]
    fresh = athena("SELECT run_id, run_at, observed FROM crypto_anomaly_gold.dq_results "
                   "WHERE check_name = 'freshness' AND run_id IN (" + ",".join(f"'{i}'" for i in ids) + ")")
    by_id = {f["run_id"]: f for f in fresh}
    for r in rows:
        f = r["gate"] and by_id.get(r["gate"]["gate_run_id"])
        if f:
            # freshness = (gate time - newest bar open) / 60, stored to 2 dp: round the
            # reconstructed open time to the minute it must sit on.
            newest = utc(f["run_at"].replace("Z", "+00:00")) - timedelta(minutes=float(f["observed"]))
            r["newest_bar_open"] = newest.replace(second=0, microsecond=0) + (BAR if newest.second >= 30 else timedelta(0))
            r["freshness_min"] = float(f["observed"])
    return rows


def report(rows):
    ok = [r for r in rows if r["status"] == "SUCCEEDED"]
    dur = [(r["stop"] - r["start"]).total_seconds() for r in ok]
    cost = [(r["silver"]["billed_s"] + r["gold"]["billed_s"]) / 3600 * DPU * USD_PER_DPU_HOUR for r in ok]
    built = [Run(r["newest_bar_open"], r["gold"]["completed"]) for r in ok if "newest_bar_open" in r]
    lags = [r[s]["polling_lag_s"] for r in ok for s in JOBS if s in r]
    return {
        "runs": len(rows), "succeeded": len(ok),
        "gate_passed": sum(bool(r["gate"] and r["gate"]["quality_passed"]) for r in rows),
        "task_failures": sum(r["task_failures"] for r in rows),
        "start_offsets_s": [floor_to((r["start"] - r["start"].replace(minute=0, second=0, microsecond=0)).total_seconds(), 1) for r in rows],
        "duration_s": summarize(dur) | {"values": [floor_to(d, 1) for d in dur]},
        "silver_billed_s": [r["silver"]["billed_s"] for r in ok],
        "gold_billed_s": [r["gold"]["billed_s"] for r in ok],
        "polling_lag_s": summarize(lags),
        "freshness_min": [r.get("freshness_min") for r in rows],
        "cost_usd": summarize(cost) | {"total": sum(cost)},
        "data_delay_s": summarize(data_delays(built)),
        "rows": [{k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in r.items()
                  if k not in ("silver", "gold")} | {s: {k: (v.isoformat() if isinstance(v, datetime) else v)
                  for k, v in r[s].items()} for s in JOBS if s in r} for r in rows],
    }


if __name__ == "__main__":
    print(json.dumps(report(collect(sys.argv[1], int(sys.argv[2]))), indent=1, default=str))
