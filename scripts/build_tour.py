#!/usr/bin/env python3
"""
Build tour/data.json - the data behind the pipeline tour page (tour/index.html).

    python3 scripts/build_tour.py            # ~1-2 min, a few cents of Athena
    # then republish tour/index.html with tour/data.json

WHY A SCRIPT AND NOT A LIVE PAGE
--------------------------------
The page is a snapshot, rebuilt at milestones. A live page would need a public API in
front of Athena (anyone with the link could run queries on this account). A script in
the repo keeps the page reproducible from scratch - the same property that makes
Silver rebuildable from Bronze - and every number on it traces to a query below.

Standard library only: it shells out to the `aws` CLI, like scripts/athena.sh, so it
needs no boto3 install. Every Athena query runs in the enforced `crypto-anomaly`
workgroup (1 GiB scan cutoff), and each is billed at the 10 MB minimum - ~12 queries
cost well under a cent.

The FEATURED bar is the largest |z| event in Gold, chosen at build time, so the page
follows the data as Gold is rebuilt.
"""

import json
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "tour" / "data.json"

WORKGROUP = "crypto-anomaly"
SILVER = "crypto_anomaly_silver.clean_bars"
GOLD = "crypto_anomaly_gold.anomaly_events"
SYMBOLS = json.loads((ROOT / "config" / "symbols.json").read_text())

# Headline results that are not recomputable from a query (they came from
# experiments). Each one cites its METRICS.md entry; edit there first, then here.
MEASURED = [
    {"id": "M-055", "label": "Missing minutes", "value": "0",
     "detail": "1.52M bars, 17 pairs, 79 days, unattended"},
    {"id": "M-017", "label": "Duplicate rate removed", "value": "64.09%",
     "detail": "overlapping 15-bar fetches every 5 min, deduplicated on Glue"},
    {"id": "M-021", "label": "Storage reduction", "value": "8.95×",
     "detail": "2.98× dedup × 3.00× JSON→Parquet"},
    {"id": "M-027", "label": "Athena scan reduction", "value": "26.6×",
     "detail": "12.6× columnar × 2.10× partition pruning"},
    {"id": "M-020", "label": "Two-cadence saving", "value": "12.0×",
     "detail": "hourly transforms $33/mo vs every-5-min $397/mo"},
    {"id": "M-041", "label": "Anomaly firing rate", "value": "1.33%",
     "detail": "4.9× what a normal distribution implies at |z|>3"},
    {"id": "M-046", "label": "Cross-engine check", "value": "392 = 392",
     "detail": "Spark and an independent SQL rewrite agree exactly"},
    {"id": "M-052", "label": "Total AWS spend", "value": "$1.96",
     "detail": "Aug–Nov 9; S3 write requests predicted within 0.03%"},
    {"id": "M-054", "label": "Glue runtime spread", "value": "±3.9%",
     "detail": "p50 575 s over 18 backfill runs at fixed input"},
]

# Build status of each pipeline component, shown on the architecture diagram.
STATUS = {
    "scheduler": "deployed", "ingestion": "deployed", "bronze": "deployed",
    "bronze_to_silver": "deployed", "silver": "deployed", "dq_gate": "written",
    "silver_to_gold": "deployed", "gold": "deployed", "athena": "deployed",
    "step_functions": "planned", "sns": "planned",
}


def aws(*args, parse=True):
    out = subprocess.run(["aws", *args], check=True, capture_output=True, text=True).stdout
    return json.loads(out) if parse and out.strip() else out


def athena(sql):
    """start -> poll -> fetch, the same sequence as scripts/athena.sh."""
    qid = aws("athena", "start-query-execution", "--work-group", WORKGROUP,
              "--query-string", sql)["QueryExecutionId"]
    while True:
        ex = aws("athena", "get-query-execution", "--query-execution-id", qid)["QueryExecution"]
        state = ex["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if state != "SUCCEEDED":
        sys.exit(f"Athena {state}: {ex['Status'].get('StateChangeReason')}\n{sql}")
    rows = aws("athena", "get-query-results", "--query-execution-id", qid)["ResultSet"]["Rows"]
    head = [c.get("VarCharValue") for c in rows[0]["Data"]]
    return [dict(zip(head, (c.get("VarCharValue") for c in r["Data"]))) for r in rows[1:]]


def num(v):
    if v is None:
        return None
    try:
        f = float(v)
        return int(f) if f.is_integer() and "." not in v and "e" not in v.lower() else f
    except ValueError:
        return v


def typed(rows):
    return [{k: num(v) for k, v in r.items()} for r in rows]


def step(msg):
    print(f"  {msg}", file=sys.stderr, flush=True)


def bucket_series(bucket, metric, storage_type, days=100):
    end = datetime.now(timezone.utc)
    res = aws("cloudwatch", "get-metric-statistics", "--namespace", "AWS/S3",
              "--metric-name", metric,
              "--dimensions", f"Name=BucketName,Value={bucket}",
              f"Name=StorageType,Value={storage_type}",
              "--start-time", (end - timedelta(days=days)).isoformat(),
              "--end-time", end.isoformat(), "--period", "86400", "--statistics", "Average")
    pts = sorted(res["Datapoints"], key=lambda p: p["Timestamp"])
    return [{"date": p["Timestamp"][:10], "value": p["Average"]} for p in pts]


def main():
    account = aws("sts", "get-caller-identity")["Account"]
    buckets = {layer: f"crypto-anomaly-{layer}-{account}" for layer in ("bronze", "silver", "gold")}
    data = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "symbols": SYMBOLS["active"],
            "rejected": {k: v["reason"] for k, v in SYMBOLS.get("rejected", {}).items()}, "status": STATUS, "measured": MEASURED}

    # --- Storage per layer: CloudWatch's free daily S3 metrics, no listing needed ---
    step("S3 storage metrics")
    data["storage"] = {
        layer: {
            "objects": bucket_series(b, "NumberOfObjects", "AllStorageTypes"),
            "bytes": bucket_series(b, "BucketSizeBytes", "StandardStorage"),
        }
        for layer, b in buckets.items()
    }

    # --- Silver ---
    step("Silver daily counts")
    data["silver_daily"] = typed(athena(
        f'SELECT "date", COUNT(*) AS bars, COUNT(DISTINCT symbol) AS symbols, '
        f'COUNT(rolling_vol) AS with_vol FROM {SILVER} GROUP BY "date" ORDER BY "date"'))
    step("Silver totals")
    data["silver_totals"] = typed(athena(
        f"SELECT COUNT(*) AS bars, COUNT(DISTINCT symbol) AS symbols, "
        f"CAST(MIN(open_time) AS varchar) AS first_bar, CAST(MAX(open_time) AS varchar) AS last_bar, "
        f"COUNT(*) - COUNT(rolling_vol) AS null_vol, "
        f"SUM(CASE WHEN num_trades = 0 THEN 1 ELSE 0 END) AS untraded "
        f"FROM {SILVER}"))[0]

    # --- Gold ---
    step("Gold aggregates")
    data["gold_totals"] = typed(athena(
        f"SELECT COUNT(*) AS events, CAST(MIN(event_time) AS varchar) AS first_event, "
        f"CAST(MAX(event_time) AS varchar) AS last_event, "
        f"MAX(zscore_window_bars) AS window_bars, MAX(zscore_threshold) AS k FROM {GOLD}"))[0]
    data["gold_by_rule_day"] = typed(athena(
        f'SELECT "date", rule, COUNT(*) AS n FROM {GOLD} GROUP BY "date", rule ORDER BY "date", rule'))
    data["gold_by_hour"] = typed(athena(
        f"SELECT hour(event_time) AS hour, rule, COUNT(*) AS n FROM {GOLD} "
        f"GROUP BY hour(event_time), rule ORDER BY 1, 2"))
    data["gold_by_symbol"] = typed(athena(
        f"SELECT symbol, rule, COUNT(*) AS n FROM {GOLD} GROUP BY symbol, rule ORDER BY symbol, rule"))
    data["gold_zhist"] = typed(athena(
        f"SELECT LEAST(CAST(floor(abs(zscore)) AS integer), 12) AS bin, rule, COUNT(*) AS n "
        f"FROM {GOLD} GROUP BY 1, 2 ORDER BY 1, 2"))
    data["gold_top"] = typed(athena(
        f"SELECT symbol, CAST(event_time AS varchar) AS event_time, rule, zscore, close_price, "
        f"volume, num_trades FROM {GOLD} ORDER BY abs(zscore) DESC LIMIT 12"))

    # --- The featured bar: largest |z| event, followed through all three layers ---
    step("Featured event")
    ev = typed(athena(
        f"SELECT symbol, CAST(event_time AS varchar) AS event_time, rule, rule_direction, "
        f"observed_value, observed_transform, rolling_mean, rolling_std, zscore, close_price, "
        f"volume, num_trades, zscore_window_bars, zscore_threshold, \"date\" "
        f"FROM {GOLD} ORDER BY abs(zscore) DESC LIMIT 1"))[0]
    sym = ev["symbol"]
    t = datetime.fromisoformat(ev["event_time"][:19]).replace(tzinfo=timezone.utc)
    d0, dm1 = t.date().isoformat(), (t.date() - timedelta(days=1)).isoformat()
    t_sql = t.strftime("%Y-%m-%d %H:%M:%S")

    silver_row = typed(athena(
        f"SELECT symbol, CAST(open_time AS varchar) AS open_time, CAST(close_time AS varchar) AS close_time, "
        f"open, high, low, close, volume, quote_volume, num_trades, taker_buy_base, log_return, "
        f"rolling_vol, dollar_volume, range_pct, CAST(ingested_at AS varchar) AS ingested_at, source_host, \"date\" "
        f"FROM {SILVER} WHERE symbol = '{sym}' AND \"date\" = '{d0}' "
        f"AND open_time = TIMESTAMP '{t_sql}'"))[0]

    # The detector's view: each minute's return against the trailing 60-bar baseline,
    # excluding the bar itself (ROWS 60 PRECEDING .. 1 PRECEDING = rangeBetween(-60m,-1m)
    # on gap-free data, which M-055 establishes).
    window = typed(athena(
        f"WITH w AS (SELECT open_time, close, log_return, "
        f"avg(log_return) OVER (ORDER BY open_time ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) AS base_mean, "
        f"stddev_samp(log_return) OVER (ORDER BY open_time ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) AS base_std "
        f"FROM {SILVER} WHERE symbol = '{sym}' AND \"date\" IN ('{dm1}', '{d0}')) "
        f"SELECT CAST(open_time AS varchar) AS t, close, log_return, base_mean, base_std FROM w "
        f"WHERE open_time BETWEEN TIMESTAMP '{t_sql}' - INTERVAL '90' MINUTE "
        f"AND TIMESTAMP '{t_sql}' + INTERVAL '45' MINUTE ORDER BY open_time"))

    # Bronze: every raw file that carried this bar. Each 5-minute run fetches the last
    # 15 bars, so a bar appears in ~3 files - first unfinished, then final.
    step("Bronze files for the featured bar")
    t_ms = int(t.timestamp() * 1000)
    bronze_files = []
    for hour in sorted({t.hour, (t + timedelta(minutes=15)).hour}):
        hour_date = t.date() if hour >= t.hour else (t + timedelta(days=1)).date()
        prefix = f"symbol={sym}/date={hour_date}/hour={hour:02d}/"
        listing = aws("s3api", "list-objects-v2", "--bucket", buckets["bronze"], "--prefix", prefix)
        for obj in listing.get("Contents", []):
            stamp = obj["Key"].rsplit("/", 1)[1].split("_")[1]
            ingest = datetime.strptime(f"{hour_date} {stamp}", "%Y-%m-%d %H%M%S").replace(tzinfo=timezone.utc)
            if not (t <= ingest <= t + timedelta(minutes=16)):
                continue
            doc = json.loads(aws("s3", "cp", f"s3://{buckets['bronze']}/{obj['Key']}", "-", parse=False))
            match = [b for b in doc["bars"] if b[0] == t_ms]
            if match:
                bronze_files.append({
                    "key": obj["Key"], "size": obj["Size"], "ingested_at": doc["ingested_at"],
                    "n_bars": len(doc["bars"]), "bar": match[0],
                    "finished": match[0][6] < int(ingest.timestamp() * 1000),
                    "position": doc["bars"].index(match[0]) + 1,
                    "doc_head": {k: v for k, v in doc.items() if k != "bars"},
                    "first_bar": doc["bars"][0], "last_bar": doc["bars"][-1],
                })
    bronze_files.sort(key=lambda f: f["ingested_at"])

    data["featured"] = {"event": ev, "silver": silver_row, "window": window, "bronze": bronze_files}

    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(json.dumps(data, indent=1, default=str))
    step(f"wrote {OUT.relative_to(ROOT)} ({OUT.stat().st_size / 1024:.0f} KiB); "
         f"featured {sym} {ev['event_time']} z={ev['zscore']:.2f}, {len(bronze_files)} Bronze files")


if __name__ == "__main__":
    main()
