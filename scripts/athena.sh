#!/usr/bin/env bash
#
# Run one Athena query and print its results plus what it cost you to scan.
#
#   ./scripts/athena.sh "SELECT COUNT(*) FROM crypto_anomaly_silver.clean_bars"
#   ./scripts/athena.sh -f queries/some_query.sql
#
# This exists for two reasons. The obvious one is that it beats clicking through the
# console. The less obvious one: this start -> poll -> fetch cycle is EXACTLY the
# API sequence the DQ gate Lambda will run at slice 7. Athena has no synchronous
# "run this and give me rows" call - every client has to submit, poll for
# completion, then retrieve. Seeing it here in 30 lines of bash makes that Lambda
# unsurprising when we get there.
#
set -euo pipefail

WORKGROUP="${ATHENA_WORKGROUP:-crypto-anomaly}"
DATABASE="${ATHENA_DATABASE:-crypto_anomaly_silver}"

if [[ "${1:-}" == "-f" ]]; then
  SQL="$(cat "$2")"
else
  SQL="${1:?usage: athena.sh \"<SQL>\"  |  athena.sh -f <file.sql>}"
fi

# 1. SUBMIT. Returns immediately with an ID; the query has not run yet.
QID=$(aws athena start-query-execution \
  --query-string "$SQL" \
  --work-group "$WORKGROUP" \
  --query-execution-context "Database=$DATABASE" \
  --query 'QueryExecutionId' --output text)

echo "query id: $QID"

# 2. POLL. States are QUEUED -> RUNNING -> SUCCEEDED | FAILED | CANCELLED.
while true; do
  read -r STATE REASON <<<"$(aws athena get-query-execution \
    --query-execution-id "$QID" \
    --query 'QueryExecution.Status.[State,StateChangeReason]' \
    --output text)"
  [[ "$STATE" == "QUEUED" || "$STATE" == "RUNNING" ]] || break
  sleep 1
done

# 3. STATS. DataScannedInBytes is what you are billed on (~$5/TB, 10 MB minimum
#    per query). Watch this number change when you add a partition filter.
aws athena get-query-execution --query-execution-id "$QID" \
  --query 'QueryExecution.Statistics.{ScannedBytes:DataScannedInBytes,EngineMs:EngineExecutionTimeInMillis,QueueMs:QueryQueueTimeInMillis}' \
  --output table

if [[ "$STATE" != "SUCCEEDED" ]]; then
  echo "QUERY $STATE: $REASON" >&2
  exit 1
fi

# 4. FETCH. The first row of the result set is the column headers.
#
# Note this returns at most 1000 rows per page; a larger result set needs
# --next-token paging. Fine for checks and aggregates, which is all this is for.
aws athena get-query-results --query-execution-id "$QID" --output json \
  | python3 -c '
import json, sys

rows = json.load(sys.stdin)["ResultSet"]["Rows"]
if not rows:
    print("(no rows)")
    sys.exit()

table = [[c.get("VarCharValue", "NULL") for c in r["Data"]] for r in rows]
ncols = max(len(r) for r in table)
table = [r + [""] * (ncols - len(r)) for r in table]
width = [max(len(r[i]) for r in table) for i in range(ncols)]

for i, r in enumerate(table):
    print("  ".join(v.ljust(width[j]) for j, v in enumerate(r)))
    if i == 0:
        print("  ".join("-" * w for w in width))

print(f"\n({len(table) - 1} row(s))")
'
