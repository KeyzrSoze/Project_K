import json
import urllib.error
import urllib.parse
import urllib.request

from config import AppConfig
from services.db import DatabaseManager
from services.kalshi_signing import build_auth_headers


def _base_host(config: AppConfig) -> str:
    return (
        "https://demo-api.kalshi.co/trade-api/v2"
        if config.KALSHI_ENV == "DEMO"
        else "https://api.elections.kalshi.com/trade-api/v2"
    )


def _build_auth_headers(config: AppConfig, method: str, path: str) -> dict:
    key_id = config.KALSHI_API_KEY_ID
    key_path = config.KALSHI_PRIVATE_KEY_PATH
    if not key_id or not key_path:
        return {}
    try:
        return build_auth_headers(key_id, key_path, method, path)
    except Exception as e:
        print(f"[Debug] Auth signing failed: {e}")
        return {}


def main():
    config = AppConfig()
    db = DatabaseManager()

    tickers = db.get_top_active_tickers(limit=1)
    if not tickers:
        print("No hot tickers found in DB.")
        return

    ticker = tickers[0]
    print(f"[Debug] Using ticker: {ticker}")

    base_host = _base_host(config)
    path = f"/markets/{ticker}/orderbook"
    url = f"{base_host}{path}?{urllib.parse.urlencode({'depth': 50})}"

    headers = {"User-Agent": "ObiDebug/1.0"}
    headers.update(_build_auth_headers(config, "GET", path))

    req = urllib.request.Request(url, headers=headers, method="GET")

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            status = getattr(resp, "status", resp.getcode())
            data = resp.read()
    except urllib.error.HTTPError as e:
        body = e.read() if hasattr(e, "read") else b""
        text = body.decode("utf-8", errors="replace") if body else ""
        print(f"[Debug] HTTP {e.code}")
        print(text[:500])
        return
    except Exception as e:
        print(f"[Debug] Request failed: {e}")
        return

    try:
        payload = json.loads(data.decode("utf-8"))
    except json.JSONDecodeError:
        print("[Debug] JSON decode failed")
        print(data.decode("utf-8", errors="replace")[:500])
        return

    print(f"[Debug] HTTP {status}")
    if isinstance(payload, dict):
        print(f"[Debug] top-level keys: {list(payload.keys())}")
    else:
        print(f"[Debug] payload type: {type(payload)}")
        print(payload)
        return

    orderbook = payload.get("orderbook")
    if isinstance(orderbook, dict):
        print(f"[Debug] orderbook keys: {list(orderbook.keys())}")
        yes = orderbook.get("yes", [])
        no = orderbook.get("no", [])
        print(f"[Debug] yes levels (first 3): {yes[:3]}")
        print(f"[Debug] no levels (first 3): {no[:3]}")
    else:
        print("[Debug] orderbook missing or not a dict")


if __name__ == "__main__":
    main()
