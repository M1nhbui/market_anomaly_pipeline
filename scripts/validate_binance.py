"""
Binance market-data validation probe (v2 — with SSL diagnostics).
Run locally:  python3 validate_binance.py

If you hit CERTIFICATE_VERIFY_FAILED, this version tells you WHY and
uses certifi's cert bundle so verification actually works.
No API key needed. Standard library + certifi.
"""
import json
import ssl
import socket
import urllib.request
import urllib.error
from datetime import datetime, timezone

# Prefer certifi's up-to-date CA bundle; fall back to system default.
try:
    import certifi
    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
    CERT_SRC = f"certifi ({certifi.where()})"
except ImportError:
    SSL_CTX = ssl.create_default_context()
    CERT_SRC = "system default (certifi not installed — run: pip3 install certifi)"

HOSTS = [
    "https://data-api.binance.vision",  # public market-data mirror, NOT geo-blocked — try first
    "https://api.binance.com",          # global
    "https://api.binance.us",           # US host
]
SYMBOL = "ETHUSDT"


def get(url, timeout=15):
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
        return r.status, r.read().decode()


def diagnose_tls(host):
    """Inspect the cert the server actually presents — reveals VPN/proxy interception."""
    hostname = host.replace("https://", "").split("/")[0]
    try:
        raw = ssl.create_default_context()
        raw.check_hostname = False
        raw.verify_mode = ssl.CERT_NONE
        with socket.create_connection((hostname, 443), timeout=10) as sock:
            with raw.wrap_socket(sock, server_hostname=hostname) as ssock:
                cert = ssock.getpeercert(binary_form=False)
                der = ssock.getpeercert(binary_form=True)
        # If we can read a cert with verification OFF but verification ON fails,
        # the issuer is the tell. Print issuer org.
        return f"cert presented ({len(der)} bytes). If verify still fails, a proxy/VPN/AV is likely injecting a root."
    except Exception as e:
        return f"could not even establish TLS socket: {e}  -> network/DNS issue, not certs."


def probe(host):
    print(f"\n{'='*60}\nHOST: {host}\n{'='*60}")
    try:
        status, _ = get(f"{host}/api/v3/ping")
        print(f"  ping        -> HTTP {status}  OK")
    except urllib.error.HTTPError as e:
        print(f"  ping        -> HTTP {e.code}  (if 451: geo-blocked; if 403: WAF) — SKIP")
        return False
    except ssl.SSLError as e:
        print(f"  ping        -> SSL ERROR: {e}")
        print(f"  TLS probe   -> {diagnose_tls(host)}")
        return False
    except Exception as e:
        print(f"  ping        -> FAILED: {e}")
        return False

    url = f"{host}/api/v3/klines?symbol={SYMBOL}&interval=1m&limit=1"
    status, body = get(url)
    bar = json.loads(body)[0]
    cols = ["open_time","open","high","low","close","volume","close_time",
            "quote_volume","num_trades","taker_buy_base","taker_buy_quote","ignore"]
    row = dict(zip(cols, bar))
    print(f"  klines      -> HTTP {status}")
    print(f"  RAW bar     -> {bar}")
    print(f"  open_time   -> {datetime.fromtimestamp(row['open_time']/1000, tz=timezone.utc)}")
    print(f"  close price -> {row['close']}  (STRING — cast to double in Silver)")
    print(f"  volume      -> {row['volume']}")
    print(f"  DEDUP KEY   -> (symbol='{SYMBOL}', open_time={row['open_time']})")
    return True


if __name__ == "__main__":
    print("Binance market-data probe v2 —", datetime.now(timezone.utc).isoformat())
    print("Cert source:", CERT_SRC)
    working = [h for h in HOSTS if probe(h)]
    print(f"\n{'='*60}\nRESULT: {len(working)}/{len(HOSTS)} hosts reachable.")
    if working:
        print(f"USE THIS HOST: {working[0]}")
    else:
        print("Still failing. Checklist:")
        print("  1. pip3 install --upgrade certifi   (then rerun)")
        print("  2. Run the 'Install Certificates.command' that shipped with your Python")
        print("  3. Turn the VPN OFF and retry (.vision mirror is not geo-blocked)")
        print("  4. Disable HTTPS scanning in any antivirus (Kaspersky/Avast/etc.)")