import asyncio
import json
import os
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from email.utils import parsedate_to_datetime
from typing import List, Tuple, Optional, Any, Dict

from kalshi_python import ApiClient, Configuration
from kalshi_python.api import markets_api

from config import AppConfig
from services.db import DatabaseManager, DB_PATH, OBI_DB_PATH
from services.kalshi_signing import build_auth_headers
from utils.logger import logger


class ObiTracker:
    """
    OBI Sniper Process (Deep View)
    - Reads hot tickers from market_metrics
    - Pulls orderbooks from Kalshi
    - Writes best bid/ask + depth counts into market_obi
    """

    def __init__(self):
        self.config = AppConfig()
        self.db_primary = DatabaseManager(db_path=DB_PATH, schema="primary")
        self.db_obi = DatabaseManager(db_path=OBI_DB_PATH, schema="obi")
        self._obi_db_split = OBI_DB_PATH != DB_PATH

        self.api_client: Optional[ApiClient] = None
        self.market_api: Optional[markets_api.MarketsApi] = None

        self.hot_tickers: List[str] = []
        self.hot_quote_map: Dict[str, Tuple[Optional[int], Optional[int]]] = {}

        self._orderbook_base_host: str = ""
        self._logged_orderbook_keys: bool = False

        # Rate limiting state
        self._min_ticker_interval: float = 1.2
        self._max_qps: float = 2.0
        self._min_qps: float = 0.5
        self._current_qps: float = 1.5
        self._burst: float = 1.0
        self._tokens: float = self._burst
        self._last_token_ts: float = time.time()
        self._global_cooldown_until: float = 0.0
        self._rr_index: int = 0
        self._global_next_request_ts: float = time.time()

        self._backoff_base: float = 2.0
        self._backoff_cap: float = 60.0
        self._jitter_max: float = 0.5
        self._ticker_next_allowed: Dict[str, float] = {}
        self._ticker_429_counts: Dict[str, int] = {}
        self._last_429_ts: float = time.time()
        self._last_qps_increase_ts: float = time.time()
        self._qps_ramp_interval: float = 60.0
        self._qps_ramp_step: float = 0.2
        self._qps_ramp_step_interval: float = 15.0

        # Log throttles
        self._last_http_error_log_ts: float = 0.0
        self._last_json_error_log_ts: float = 0.0
        self._last_auth_error_log_ts: float = 0.0
        self._last_idle_log_ts: float = 0.0
        self._last_global_cooldown_log_ts: float = 0.0
        self._last_db_error_log_ts: float = 0.0
        self._last_429_log_ts: Dict[str, float] = {}

    def _base_host(self) -> str:
        return (
            "https://demo-api.kalshi.co/trade-api/v2"
            if self.config.KALSHI_ENV == "DEMO"
            else "https://api.elections.kalshi.com/trade-api/v2"
        )

    def _connect_api(self) -> bool:
        logger.log_info(
            f"[ObiTracker] Initializing RSA Auth ({self.config.KALSHI_ENV})...")
        host = self._base_host()
        self._orderbook_base_host = host

        try:
            api_config = Configuration(host=host)
            self.api_client = ApiClient(api_config)
            self.api_client.set_kalshi_auth(
                key_id=self.config.KALSHI_API_KEY_ID,
                private_key_path=self.config.KALSHI_PRIVATE_KEY_PATH
            )
            self.market_api = markets_api.MarketsApi(self.api_client)

            # Connectivity check
            self.market_api.get_markets(limit=1, status="open")
            logger.log_info("[ObiTracker] API Connection Successful.")
            return True
        except Exception as e:
            logger.log_error(f"[ObiTracker] API Connection Failed: {e}")
            return False

    def _fetch_hot_list(self, limit: int = 20) -> Tuple[List[str], Dict[str, Tuple[Optional[int], Optional[int]]]]:
        cutoff_time = time.time() - 300
        query = """
            WITH latest AS (
                SELECT ticker, MAX(timestamp) as ts
                FROM market_metrics
                WHERE timestamp > ? AND volume > 100
                GROUP BY ticker
            )
            SELECT m.ticker, m.best_bid, m.best_ask, m.volume
            FROM market_metrics m
            JOIN latest l ON m.ticker = l.ticker AND m.timestamp = l.ts
            WHERE m.spread BETWEEN 1 AND 50
            ORDER BY m.volume DESC
            LIMIT ?
        """

        with self.db_primary.get_connection(role="read") as conn:
            rows = conn.execute(query, (cutoff_time, limit)).fetchall()

        tickers: List[str] = []
        quote_map: Dict[str, Tuple[Optional[int], Optional[int]]] = {}
        for row in rows:
            ticker = row["ticker"]
            tickers.append(ticker)
            quote_map[ticker] = (row["best_bid"], row["best_ask"])

        return tickers, quote_map

    async def _refresh_hot_list_loop(self):
        logger.log_info("[ObiTracker] Starting hot list refresh loop (30s)...")
        while True:
            try:
                tickers, quote_map = self._fetch_hot_list(limit=20)
                self.hot_tickers = tickers
                self.hot_quote_map = quote_map

                now = time.time()
                current = set(tickers)
                self._ticker_next_allowed = {
                    t: self._ticker_next_allowed.get(t, now) for t in current
                }
                self._ticker_429_counts = {
                    t: self._ticker_429_counts.get(t, 0) for t in current
                }
                if self._rr_index >= len(tickers):
                    self._rr_index = 0

                if not tickers:
                    logger.log_warn(
                        "[ObiTracker] Hot list is empty (no tickers meet the current volume/spread window).")
                else:
                    logger.log_info(
                        f"[ObiTracker] Refreshed hot list. Tracking {len(tickers)} tickers.")
            except Exception as e:
                logger.log_error(f"[ObiTracker] Hot list refresh failed: {e}")

            await asyncio.sleep(30)

    def _log_throttled(self, message: str, last_attr: str, interval: float = 30.0) -> None:
        now_ts = time.time()
        last_ts = getattr(self, last_attr, 0.0)
        if now_ts - last_ts >= interval:
            setattr(self, last_attr, now_ts)
            logger.log_warn(message)

    def _build_auth_headers(self, method: str, path: str) -> Dict[str, str]:
        key_id = self.config.KALSHI_API_KEY_ID
        key_path = self.config.KALSHI_PRIVATE_KEY_PATH
        if not key_id or not key_path:
            return {}

        try:
            headers = build_auth_headers(key_id, key_path, method, path)
            return headers or {}
        except ImportError:
            self._log_throttled(
                "[ObiTracker] Auth signing unavailable (cryptography missing). Sending unsigned request.",
                "_last_auth_error_log_ts",
            )
        except FileNotFoundError:
            self._log_throttled(
                "[ObiTracker] Auth signing failed (private key not found). Sending unsigned request.",
                "_last_auth_error_log_ts",
            )
        except Exception as e:
            self._log_throttled(
                f"[ObiTracker] Auth signing failed: {e}. Sending unsigned request.",
                "_last_auth_error_log_ts",
            )

        return {}

    def _parse_retry_after(self, headers: Any) -> Optional[float]:
        if not headers:
            return None
        value = headers.get("Retry-After") or headers.get("retry-after")
        if not value:
            return None
        try:
            return float(value)
        except ValueError:
            try:
                retry_dt = parsedate_to_datetime(value)
                if retry_dt:
                    return max(retry_dt.timestamp() - time.time(), 0.0)
            except Exception:
                return None
        return None

    def _fetch_orderbook_json(self, ticker: str, depth: int = 50) -> Tuple[Optional[dict], Optional[int], Optional[float]]:
        if not self._orderbook_base_host:
            self._orderbook_base_host = self._base_host()

        path = f"/markets/{ticker}/orderbook"
        params = urllib.parse.urlencode({"depth": depth})
        url = f"{self._orderbook_base_host}{path}?{params}"

        headers = {"User-Agent": "ObiTracker/1.0"}
        headers.update(self._build_auth_headers("GET", path))
        req = urllib.request.Request(url, headers=headers, method="GET")

        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                status = getattr(resp, "status", resp.getcode())
                data = resp.read()
        except urllib.error.HTTPError as e:
            body = e.read() if hasattr(e, "read") else b""
            text = body.decode("utf-8", errors="replace") if body else ""
            retry_after = self._parse_retry_after(getattr(e, "headers", None))
            if e.code == 429:
                now_ts = time.time()
                last_ts = self._last_429_log_ts.get(ticker, 0.0)
                if now_ts - last_ts >= 10.0:
                    self._last_429_log_ts[ticker] = now_ts
                    logger.log_warn(
                        f"[ObiTracker] Orderbook HTTP 429 for {ticker}: {text[:500]}")
            else:
                self._log_throttled(
                    f"[ObiTracker] Orderbook HTTP {e.code} for {ticker}: {text[:500]}",
                    "_last_http_error_log_ts",
                )
            return None, e.code, retry_after
        except urllib.error.URLError as e:
            self._log_throttled(
                f"[ObiTracker] Orderbook request error for {ticker}: {e}",
                "_last_http_error_log_ts",
            )
            return None, None, None
        except Exception as e:
            self._log_throttled(
                f"[ObiTracker] Orderbook request error for {ticker}: {e}",
                "_last_http_error_log_ts",
            )
            return None, None, None

        try:
            text = data.decode("utf-8")
        except Exception:
            text = data.decode("utf-8", errors="replace")

        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            self._log_throttled(
                f"[ObiTracker] Orderbook JSON decode failed for {ticker}: {text[:500]}",
                "_last_json_error_log_ts",
            )
            return None, status, None

        return payload if isinstance(payload, dict) else None, status, None

    def _parse_levels(self, raw_levels: Any) -> List[Tuple[int, int]]:
        if not isinstance(raw_levels, list):
            return []

        parsed: List[Tuple[int, int]] = []
        for level in raw_levels:
            price = None
            qty = 0

            if isinstance(level, (list, tuple)):
                price = level[0] if len(level) > 0 else None
                qty = level[1] if len(level) > 1 else 0
            elif isinstance(level, dict):
                price = level.get("price")
                qty = level.get("count", level.get("quantity", 0))
            else:
                continue

            if price is None:
                continue

            try:
                price_int = int(float(price))
            except Exception:
                continue

            try:
                qty_int = int(float(qty)) if qty is not None else 0
            except Exception:
                qty_int = 0

            parsed.append((price_int, qty_int))

        return parsed

    def _level_qty(self, level: Any) -> int:
        if isinstance(level, (list, tuple)) and len(level) > 1:
            try:
                return int(float(level[1]))
            except Exception:
                return 0
        return 0

    def _best_price(self, levels: List[Tuple[int, int]]) -> Optional[int]:
        if not levels:
            return None
        try:
            return int(levels[0][0])
        except Exception:
            return None

    def _sum_qty(self, levels: List[Tuple[int, int]]) -> int:
        return sum(self._level_qty(level) for level in levels)

    def _compute_obi(self, yes_levels: List[Tuple[int, int]], no_levels: List[Tuple[int, int]]) -> Dict[str, Any]:
        bid_count = self._sum_qty(yes_levels)
        ask_count = self._sum_qty(no_levels)

        if not yes_levels and not no_levels:
            return {
                "bid_count": 0,
                "ask_count": 0,
                "best_bid": None,
                "best_ask": None,
            }

        best_yes_bid = self._best_price(yes_levels)
        best_no_bid = self._best_price(no_levels)

        if best_no_bid is not None:
            best_yes_ask = 100 - best_no_bid
        else:
            best_yes_ask = 100 if yes_levels else None

        return {
            "bid_count": bid_count,
            "ask_count": ask_count,
            "best_bid": best_yes_bid,
            "best_ask": best_yes_ask,
        }

    def _refill_tokens(self) -> None:
        now = time.time()
        elapsed = now - self._last_token_ts
        self._last_token_ts = now
        self._tokens = min(self._burst, self._tokens + elapsed * self._current_qps)

    def _maybe_ramp_qps(self, now: float) -> None:
        if now - self._last_429_ts < self._qps_ramp_interval:
            return
        if now - self._last_qps_increase_ts < self._qps_ramp_step_interval:
            return
        if self._current_qps >= self._max_qps:
            return
        self._current_qps = min(self._max_qps, self._current_qps + self._qps_ramp_step)
        self._last_qps_increase_ts = now

    def _apply_429(self, ticker: str, retry_after: Optional[float]) -> None:
        now = time.time()
        self._last_429_ts = now
        self._last_qps_increase_ts = now
        self._current_qps = max(self._min_qps, self._current_qps * 0.85)

        count = self._ticker_429_counts.get(ticker, 0) + 1
        self._ticker_429_counts[ticker] = min(count, 6)

        backoff = self._backoff_base * (2 ** (self._ticker_429_counts[ticker] - 1))
        backoff = min(backoff, self._backoff_cap)
        cooldown = retry_after if retry_after is not None else backoff
        cooldown = min(cooldown, self._backoff_cap)
        cooldown *= random.uniform(0.8, 1.2)
        cooldown = max(self._min_ticker_interval, cooldown)

        self._ticker_next_allowed[ticker] = now + cooldown
        self._global_cooldown_until = max(self._global_cooldown_until, now + min(cooldown, 0.5))
        self._global_next_request_ts = max(self._global_next_request_ts, now + 1.0)

        global_cooldown = retry_after if retry_after is not None else min(cooldown, 30.0)
        global_cooldown = max(2.0, global_cooldown)
        global_cooldown *= random.uniform(0.8, 1.2)
        try:
            current = self.db_primary.get_rate_limit_cooldown_until()
            target = now + global_cooldown
            if target > current:
                self.db_primary.set_rate_limit_cooldown_until(target)
        except Exception as e:
            self._log_throttled(
                f"[ObiTracker] Failed to write global cooldown: {e}",
                "_last_db_error_log_ts",
            )

    def _select_due_tickers(self, max_count: int) -> List[str]:
        if not self.hot_tickers or max_count <= 0:
            return []

        now = time.time()
        selected: List[str] = []
        checked = 0
        total = len(self.hot_tickers)

        while checked < total and len(selected) < max_count:
            ticker = self.hot_tickers[self._rr_index % total]
            self._rr_index += 1
            checked += 1

            next_allowed = self._ticker_next_allowed.get(ticker, 0.0)
            if now < next_allowed:
                continue

            selected.append(ticker)

        return selected

    def _has_due_ticker(self, now: float) -> bool:
        if not self.hot_tickers:
            return False
        for ticker in self.hot_tickers:
            if now >= self._ticker_next_allowed.get(ticker, 0.0):
                return True
        return False

    def _next_due_time(self, now: float) -> float:
        if not self.hot_tickers:
            return now
        return min(self._ticker_next_allowed.get(ticker, now) for ticker in self.hot_tickers)

    async def _harvest_detailed_orderbooks(self, tickers: List[str]) -> Tuple[int, int, int, int, int]:
        """
        Returns: (rows_upserted, tickers_fetched, tickers_empty, rate_limited, error_count)
        """
        if not tickers:
            return 0, 0, 0, 0, 0

        obi_rows: List[Tuple] = []
        fetched = 0
        empty = 0
        rate_limited = 0
        errors = 0

        loop = asyncio.get_running_loop()

        for t in tickers:
            fetched += 1

            while True:
                now = time.time()
                cooldown_until = max(self._global_cooldown_until, self.db_primary.get_rate_limit_cooldown_until())
                if now < cooldown_until:
                    if now - self._last_global_cooldown_log_ts >= 10.0:
                        self._last_global_cooldown_log_ts = now
                        logger.log_warn(
                            f"[ObiTracker] Global cooldown active for {cooldown_until - now:.2f}s. Pausing requests.")
                    await asyncio.sleep(min(0.5, cooldown_until - now))
                    continue
                break

            while True:
                now = time.time()
                if now >= self._global_next_request_ts:
                    break
                await asyncio.sleep(min(0.25, self._global_next_request_ts - now))

            now = time.time()
            self._global_next_request_ts = max(self._global_next_request_ts, now) + (
                1.0 / max(self._current_qps, 0.1)
            )
            self._tokens = max(0.0, self._tokens - 1.0)

            payload, status, retry_after = await loop.run_in_executor(
                None, self._fetch_orderbook_json, t, 50)

            now = time.time()

            if status == 429:
                rate_limited += 1
                self._apply_429(t, retry_after)
                continue

            if status is not None and status >= 400:
                errors += 1
                self._ticker_next_allowed[t] = now + self._min_ticker_interval
                continue

            if payload is None:
                errors += 1
                self._ticker_next_allowed[t] = now + self._min_ticker_interval
                continue

            self._ticker_429_counts[t] = 0
            self._ticker_next_allowed[t] = now + self._min_ticker_interval

            if not self._logged_orderbook_keys and isinstance(payload, dict):
                orderbook = payload.get("orderbook")
                if not isinstance(orderbook, dict):
                    orderbook = payload if any(k in payload for k in ("yes", "no")) else None
                orderbook_keys = list(orderbook.keys()) if isinstance(orderbook, dict) else []
                logger.log_info(
                    f"[ObiTracker] Orderbook raw keys for {t}: keys={list(payload.keys())}, orderbook_keys={orderbook_keys}")
                self._logged_orderbook_keys = True

            orderbook = payload.get("orderbook") if isinstance(payload, dict) else None
            if not isinstance(orderbook, dict) and isinstance(payload, dict):
                if "yes" in payload or "no" in payload:
                    orderbook = payload

            yes_levels = self._parse_levels(orderbook.get("yes", [])) if isinstance(orderbook, dict) else []
            no_levels = self._parse_levels(orderbook.get("no", [])) if isinstance(orderbook, dict) else []

            if not yes_levels and not no_levels:
                empty += 1

            obi = self._compute_obi(yes_levels, no_levels)

            fallback_bid, fallback_ask = self.hot_quote_map.get(t, (None, None))
            if obi["best_bid"] is None and fallback_bid is not None:
                obi["best_bid"] = fallback_bid
            if obi["best_ask"] is None and fallback_ask is not None:
                obi["best_ask"] = fallback_ask

            ts = time.time()
            obi_rows.append(
                (t, ts, obi["bid_count"], obi["ask_count"], obi["best_bid"], obi["best_ask"]))

        if obi_rows:
            self.db_obi.bulk_upsert_obi(obi_rows)

        return len(obi_rows), fetched, empty, rate_limited, errors

    async def _tracking_loop(self):
        logger.log_info(
            "[ObiTracker] Starting high-frequency tracking loop (1s)...")

        while True:
            tick_start = time.time()
            fetched = 0
            upserted = 0
            empty = 0
            rate_limited = 0
            errors = 0

            try:
                if not self.hot_tickers:
                    await asyncio.sleep(1)
                    continue

                self._refill_tokens()
                self._maybe_ramp_qps(tick_start)

                if tick_start < self._global_cooldown_until:
                    cooldown_remaining = self._global_cooldown_until - tick_start
                    await asyncio.sleep(max(0.0, min(0.3, cooldown_remaining)))
                    self._refill_tokens()

                available = int(self._tokens)
                if available <= 0 and self._has_due_ticker(time.time()):
                    sleep_for = max(
                        0.02,
                        min(0.25, (1.0 - self._tokens) / max(self._current_qps, 0.1)),
                    )
                    await asyncio.sleep(sleep_for)
                    self._refill_tokens()
                    available = int(self._tokens)
                due = self._select_due_tickers(available)

                if due:
                    upserted, fetched, empty, rate_limited, errors = await self._harvest_detailed_orderbooks(due)
                else:
                    next_due = self._next_due_time(time.time())
                    sleep_for = max(0.02, min(0.25, next_due - time.time()))
                    if sleep_for > 0:
                        await asyncio.sleep(sleep_for)
                    if time.time() - self._last_idle_log_ts > 10.0:
                        self._last_idle_log_ts = time.time()
                        logger.log_info(
                            f"[ObiTracker] Tick: fetched=0, upserted=0, "
                            f"empty_or_zero_depth=0, rate_limited=0, errors=0, "
                            f"qps={self._current_qps:.2f}, tracking={len(self.hot_tickers)}"
                        )
                    continue

                if fetched > 0 or rate_limited > 0 or errors > 0:
                    logger.log_info(
                        f"[ObiTracker] Tick: fetched={fetched}, upserted={upserted}, "
                        f"empty_or_zero_depth={empty}, rate_limited={rate_limited}, errors={errors}, "
                        f"qps={self._current_qps:.2f}, tracking={len(self.hot_tickers)}"
                    )

            except Exception as e:
                logger.log_error(f"[ObiTracker] Tracking loop error: {e}")

            elapsed = time.time() - tick_start
            await asyncio.sleep(max(0.0, 1.0 - elapsed))

    async def run(self):
        logger.log_info(
            f"[ObiTracker] Startup: pid={os.getpid()}, cwd={os.getcwd()}, "
            f"db_primary={DB_PATH} db_obi={OBI_DB_PATH}"
        )
        if not self._connect_api():
            return

        try:
            await asyncio.gather(
                self._refresh_hot_list_loop(),
                self._tracking_loop()
            )
        except asyncio.CancelledError:
            logger.log_info("[ObiTracker] Shutdown requested.")
            return


if __name__ == "__main__":
    tracker = ObiTracker()
    try:
        asyncio.run(tracker.run())
    except KeyboardInterrupt:
        logger.log_info("[ObiTracker] Interrupted by user. Exiting.")
