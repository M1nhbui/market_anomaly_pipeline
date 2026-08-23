"""
Ingestion Lambda: Binance klines -> S3 Bronze.

Deliberately thin. It fetches, wraps with metadata, and stores. It does NOT parse,
cast, clean, or drop anything, because Bronze's job is to be an exact record of what
the API returned. If our cleaning logic turns out to be wrong, Bronze is what lets us
redo it. Any transformation here would destroy that guarantee.

No third-party dependencies on purpose: urllib is in the standard library and boto3
is preinstalled in the Lambda Python runtime. That means the deployment package is a
few KB of plain source with no build step, no Lambda layer, and nothing to go stale.
Using `requests` would buy nicer syntax and cost all of that.
"""

import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")

BRONZE_BUCKET = os.environ["BRONZE_BUCKET"]
SOURCE_HOST = os.environ.get("SOURCE_HOST", "https://data-api.binance.vision")
INTERVAL = os.environ.get("INTERVAL", "1m")
SYMBOLS = json.loads(os.environ["SYMBOLS"])

# How many bars to request per call.
#
# The schedule fires every 5 minutes and bars are 1 minute wide, so 5 bars would be
# the exact-fit answer. We ask for 15 on purpose:
#
#   - Resilience. A skipped or failed run leaves a hole that the next run backfills.
#     15 bars tolerates two AR_LIMIT", "15"))

HTTP_TIMEOUT_SEC = 10
MAX_ATTEMPTS = 3


class RateLimitBan(Exception):
    """HTTP 418: Binance has temporarily IP-banned us. Stop immediately."""


def fetch_klines(symbol):
    """
    GET one symbol's recent bars, with backoff.

    Binance signals rate limiting in two escalating ways:
      429 - slow down. Honour the Retry-After header.
      418 - you ignored 429 and are now IP-banned for a while.

    418 is treated as fatal for the whole invocation rather than retried. Retrying
    into a ban extends it, and the next scheduled run in 5 minutes is a better
    recovery strategy than hammering from inside this one.
    """
    url = (
        f"{SOURCE_HOST}/api/v3/klines"
        f"?symbol={symbol}&interval={INTERVAL}&limit={BAR_LIMIT}"
    )

    last_error = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            request = urllib.request.Request(
                url,
                headers={
                    "Accept": "application/json",
                    # Identifying the client is good manners against a free public
                    # endpoint and makes our traffic legible in their logs.
                    "User-Agent": "crypto-anomaly-pipeline/1.0",
                },
            )
            with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SEC) as response:
                return json.loads(response.read().decode())

        except urllib.error.HTTPError as err:
            if err.code == 418:
                raise RateLimitBan(
                    f"HTTP 418 IP ban fetching {symbol}. Aborting this run."
                ) from err

            if err.code == 429:
                # Retry-After is in seconds. Cap it: if Binance asks for longer than
                # our remaining Lambda time, sleeping is pointless.
                wait_sec = min(int(err.headers.get("Retry-After", 2**attempt)), 20)
                print(f"[{symbol}] HTTP 429, honouring Retry-After={wait_sec}s")
                time.sleep(wait_sec)
                last_error = err
                continue

            # 4xx/5xx that isn't rate limiting. A 400 here usually means the symbol
            # is not tradeable (delisted or typo'd) and retrying will not help.
            raise

        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as err:
            last_error = err
            if attempt < MAX_ATTEMPTS - 1:
                backoff = 2**attempt
                print(f"[{symbol}] transient error ({err}), retrying in {backoff}s")
                time.sleep(backoff)

    raise RuntimeError(f"{symbol}: exhausted {MAX_ATTEMPTS} attempts") from last_error


def build_s3_key(symbol, ingested_at, request_id):
    """
    Bronze layout: symbol=<sym>/date=<YYYY-MM-DD>/hour=<HH>/ingest_<HHMMSS>_<id>.json

    Note carefully: Bronze is partitioned by INGESTION time, not by bar time.

    A run at 14:00:30 fetches bars going back to 13:45, and all of them land under
    hour=14. That is intentional. One API response would otherwise have to be split
    across partitions, which would mean parsing bar timestamps here - transformation
    in the raw layer, exactly what Bronze must not do.

    Silver partitions by BAR time, which is what queries actually care about. The
    Bronze->Silver job reads a lookback window wide enough to absorb the difference.
    Keeping this distinction straight is the difference between a lookback that works
    and one that silently drops bars at hour boundaries.

    The request_id suffix prevents collisions: two invocations in the same second
    (a retry, or overlapping schedules) would otherwise write the same key and one
    would silently overwrite the other.
    """
    return (
        f"symbol={symbol}"
        f"/date={ingested_at:%Y-%m-%d}"
        f"/hour={ingested_at:%H}"
        f"/ingest_{ingested_at:%H%M%S}_{request_id[:8]}.json"
    )


def lambda_handler(event, context):
    ingested_at = datetime.now(timezone.utc)
    request_id = getattr(context, "aws_request_id", "local")

    succeeded, failed, total_bars = [], [], 0

    for symbol in SYMBOLS:
        try:
            bars = fetch_klines(symbol)

            # One object per symbol per run. Every field outside "bars" is metadata
            # we are adding; "bars" itself is byte-for-byte what Binance returned.
            #
            # ingested_at is the load-bearing one. Silver uses it twice: to pick the
            # newest version of a duplicated bar, and to identify bars whose
            # close_time is still in the future - the unfinished candle Binance
            # always returns as the last element.
            payload = {
                "symbol": symbol,
                "interval": INTERVAL,
                "source_host": SOURCE_HOST,
                "ingested_at": ingested_at.isoformat().replace("+00:00", "Z"),
                "request_id": request_id,
                "bar_count": len(bars),
                "bars": bars,
            }

            s3.put_object(
                Bucket=BRONZE_BUCKET,
                Key=build_s3_key(symbol, ingested_at, request_id),
                Body=json.dumps(payload).encode("utf-8"),
                ContentType="application/json",
            )

            succeeded.append(symbol)
            total_bars += len(bars)

        except RateLimitBan as err:
            # Fatal: stop the whole run rather than making the ban worse.
            print(f"FATAL: {err}")
            raise

        except Exception as err:  # noqa: BLE001 - deliberate catch-all, see below
            # One bad symbol must not take down the batch.
            #
            # Without this, a single delisted or typo'd symbol fails the invocation
            # and every OTHER symbol's data for that 5-minute window is lost too.
            # The failure is recorded and surfaced in the return value instead.
            print(f"[{symbol}] FAILED: {type(err).__name__}: {err}")
            failed.append({"symbol": symbol, "error": f"{type(err).__name__}: {err}"})

    result = {
        "ingested_at": ingested_at.isoformat().replace("+00:00", "Z"),
        "symbols_attempted": len(SYMBOLS),
        "symbols_succeeded": len(succeeded),
        "symbols_failed": len(failed),
        "bars_written": total_bars,
        "failures": failed,
    }
    print(json.dumps(result))

    # Total failure is an error; partial failure is not. Raising on partial failure
    # would mean a single flaky symbol marks the whole run failed and triggers alerts
    # for data we actually collected fine.
    if succeeded == [] and SYMBOLS:
        raise RuntimeError(f"All {len(SYMBOLS)} symbols failed: {failed}")

    return result
