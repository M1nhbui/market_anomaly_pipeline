"""
Validate every candidate symbol before admitting it to the basket.

Run:  python3 scripts/validate_basket.py

WHY THIS EXISTS
---------------
An earlier probe checked only HTTP status and reported "18/18 symbols OK". That was
insufficient and nearly admitted a dead pair: HTTP 200 proves a symbol is KNOWN to
the exchange, not that it is actively TRADING. A retired pair can still serve its
final historical bars indefinitely.

Admitting a stale symbol is not a cosmetic problem:
  - its last bar never advances, so the DQ gate's freshness check fails forever
  - its price series is flat, so rolling_vol collapses toward zero
  - a near-zero denominator makes z-scores explode, and the anomaly table fills with
    garbage from one dead symbol

So this checks three things, not one:
  1. the endpoint responds
  2. the most recent CLOSED bar is recent (the symbol is actually trading)
  3. there is real trading activity (nonzero volume and trades in recent bars)
"""

import json
import ssl
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

try:
    import certifi

    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()

HOST = "https://data-api.binance.vision"
INTERVAL = "1m"

# Bars to inspect for activity. 30 minutes is long enough that a genuinely liquid
# pair will have traded in at least some of them, without being so long that a
# briefly-halted pair looks healthy.
ACTIVITY_BARS = 30

# A symbol whose newest closed bar is older than this is not trading.
# Generous: covers our own 5-minute cadence plus slack.
MAX_BAR_AGE_MINUTES = 15

CANDIDATES = [
    # Large-cap majors
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT",
    "XRPUSDT", "ADAUSDT", "DOGEUSDT", "AVAXUSDT",
    # Mid-caps
    "LINKUSDT", "DOTUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT", "NEARUSDT",
    # The unresolved pair: MATIC migrated to POL. Both answered HTTP 200 in the
    # earlier probe, which is exactly why status alone was not good enough.
    "MATICUSDT", "POLUSDT",
    # Deliberately volatile - these are in the basket to stress the DQ gate and to
    # produce genuine untraded minutes for the empty-bar check.
    "PEPEUSDT", "SHIBUSDT",
]


def get_bars(symbol, limit):
    url = f"{HOST}/api/v3/klines?symbol={symbol}&interval={INTERVAL}&limit={limit}"
    request = urllib.request.Request(
        url, headers={"Accept": "application/json", "User-Agent": "crypto-anomaly-pipeline/1.0"}
    )
    with urllib.request.urlopen(request, timeout=15, context=SSL_CTX) as response:
        return json.loads(response.read().decode())


def check(symbol, now_ms):
    try:
        bars = get_bars(symbol, ACTIVITY_BARS)
    except urllib.error.HTTPError as err:
        return False, f"HTTP {err.code}", {}
    except Exception as err:  # noqa: BLE001
        return False, f"{type(err).__name__}: {err}", {}

    # Drop the in-progress candle - the same rule the Silver layer applies.
    closed = [b for b in bars if b[6] < now_ms]
    if not closed:
        return False, "no closed bars returned", {}

    newest = closed[-1]
    age_minutes = (now_ms - newest[6]) / 60000.0

    traded = sum(1 for b in closed if int(b[8]) > 0)
    total_trades = sum(int(b[8]) for b in closed)

    detail = {
        "age_min": round(age_minutes, 1),
        "closed_bars": len(closed),
        "bars_traded": traded,
        "traded_pct": round(100.0 * traded / len(closed), 1),
        "trades_30m": total_trades,
    }

    if age_minutes > MAX_BAR_AGE_MINUTES:
        return False, f"STALE - newest closed bar is {age_minutes:.0f} min old", detail
    if traded == 0:
        return False, "NO TRADING ACTIVITY in the last 30 bars", detail

    return True, "ok", detail


def main():
    now_ms = int(time.time() * 1000)
    print(f"Basket validation - {datetime.now(timezone.utc).isoformat()}")
    print(f"host={HOST}  interval={INTERVAL}  max_bar_age={MAX_BAR_AGE_MINUTES}min\n")
    print(f"{'symbol':<11}{'ok':<5}{'age_min':>9}{'traded%':>9}{'trades/30m':>12}  note")
    print("-" * 78)

    admitted, rejected = [], []
    for symbol in CANDIDATES:
        ok, note, detail = check(symbol, now_ms)
        (admitted if ok else rejected).append(symbol)
        print(
            f"{symbol:<11}{'YES' if ok else 'NO':<5}"
            f"{detail.get('age_min', '-'):>9}"
            f"{detail.get('traded_pct', '-'):>9}"
            f"{detail.get('trades_30m', '-'):>12}  {note}"
        )
        time.sleep(0.05)

    print("-" * 78)
    print(f"\nADMITTED ({len(admitted)}):")
    print(json.dumps(admitted, indent=2))
    if rejected:
        print(f"\nREJECTED ({len(rejected)}): {rejected}")
        print("Do NOT add these to config/symbols.json.")


if __name__ == "__main__":
    main()
