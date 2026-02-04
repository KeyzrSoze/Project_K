import asyncio
import random
import time
from email.utils import parsedate_to_datetime
from typing import List, Any, Optional, Tuple
from kalshi_python.api import markets_api
from services.db import DatabaseManager
from utils.logger import Logger
from kalshi_python import ApiClient


class MarketHarvester:
    """
    A dedicated "Discovery Engine" that scans all markets and populates the
    'market_metrics' table with volume, open interest, and spread data.
    The high-frequency tracking has been moved to a separate service.
    """

    def __init__(self, api_client: ApiClient, logger: Logger):
        self.market_api = markets_api.MarketsApi(api_client)
        self.logger = logger
        self.db = DatabaseManager()
        self._last_global_cooldown_log_ts: float = 0.0
        self._harvest_429_count: int = 0
        self._last_harvest_429_ts: float = 0.0

    def _categorize(self, ticker: str, series_ticker: str) -> str:
        t = str(ticker or "").upper()
        s = str(series_ticker or "").upper()
        if "ESPORTS" in t or "ESPORTS" in s:
            return "ESPORTS"
        if s.startswith("KX") and any(coin in s for coin in ["BTC", "ETH", "SOL", "DOGE", "BITCOIN"]):
            return "CRYPTO"
        if any(k in s for k in ["FED", "INFL", "CPI", "GDP", "RATE", "FOMC", "UNEMPLOYMENT"]):
            return "ECON"
        return "MISC"

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

    def _extract_status_and_retry_after(self, error: Exception) -> Tuple[Optional[int], Optional[float]]:
        status = getattr(error, "status", None)
        if status is None:
            status = getattr(error, "status_code", None)
        if status is None:
            status = getattr(error, "code", None)

        headers = getattr(error, "headers", None)
        response = getattr(error, "response", None)
        if headers is None and response is not None:
            headers = getattr(response, "headers", None)

        retry_after = self._parse_retry_after(headers)
        return status, retry_after

    def _compute_429_cooldown(self, retry_after: Optional[float]) -> float:
        now = time.time()
        if now - self._last_harvest_429_ts > 60:
            self._harvest_429_count = 0
        self._harvest_429_count = min(self._harvest_429_count + 1, 5)
        self._last_harvest_429_ts = now

        backoff = 2.0 * (2 ** (self._harvest_429_count - 1))
        backoff = min(backoff, 30.0)
        if retry_after is not None:
            backoff = max(backoff, retry_after)
        backoff += random.uniform(0.0, 0.5)
        return backoff

    def _set_global_cooldown(self, seconds: float) -> None:
        now = time.time()
        target = now + max(0.0, seconds)
        current = self.db.get_rate_limit_cooldown_until()
        if target > current:
            self.db.set_rate_limit_cooldown_until(target)

    async def _maybe_wait_for_global_cooldown(self) -> bool:
        now = time.time()
        cooldown_until = self.db.get_rate_limit_cooldown_until()
        if now < cooldown_until:
            if now - self._last_global_cooldown_log_ts >= 10.0:
                self._last_global_cooldown_log_ts = now
                self.logger.log_warn(
                    f"[Harvester] Global cooldown active for {cooldown_until - now:.2f}s. Pausing discovery.")
            await asyncio.sleep(min(1.0, cooldown_until - now))
            return True
        return False

    async def _discovery_loop(self):
        """The main execution cycle for the broad market scan."""
        self.logger.log_info("[Harvester] Starting Discovery Loop (60s)...")
        while True:
            try:
                self.logger.log_info(
                    "[Harvester] Discovery: Starting full market scan.")
                await self._harvest_all_markets_paginated()
            except Exception as e:
                self.logger.log_error(f"[Harvester] Discovery Loop Error: {e}")

            await asyncio.sleep(60)

    async def _harvest_all_markets_paginated(self):
        """Paginates through all open markets, saving metrics to market_metrics."""
        cursor = None
        loop = asyncio.get_running_loop()
        page_count = 0
        total_found = 0
        seen_cursors = set()
        seen_tickers_this_cycle = set()

        while True:
            if await self._maybe_wait_for_global_cooldown():
                continue
            try:
                response = await loop.run_in_executor(
                    None, lambda: self.market_api.get_markets(
                        limit=100, status="open", cursor=cursor)
                )

                markets = getattr(response, 'markets', [])
                if not markets:
                    break

                first_ticker = getattr(markets[0], 'ticker', None)
                if first_ticker and first_ticker in seen_tickers_this_cycle:
                    self.logger.log_warn(
                        f"[Harvester] Loop detected. Cycle complete at {total_found} markets.")
                    break

                for m in markets:
                    t = getattr(m, 'ticker', None)
                    if t:
                        seen_tickers_this_cycle.add(t)

                await self._save_metrics_batch(markets)

                total_found += len(markets)
                page_count += 1

                if page_count % 20 == 0:
                    self.logger.log_info(
                        f"[Harvester] Discovery: Scanned {total_found} markets across {page_count} pages.")

                next_cursor = getattr(response, 'cursor', None)
                if not next_cursor or next_cursor in seen_cursors:
                    break
                seen_cursors.add(next_cursor)
                cursor = next_cursor

                await asyncio.sleep(random.uniform(0.05, 0.15))

            except Exception as e:
                status, retry_after = self._extract_status_and_retry_after(e)
                if status == 429:
                    cooldown = self._compute_429_cooldown(retry_after)
                    self._set_global_cooldown(cooldown)
                    self.logger.log_warn(
                        f"[Harvester] Discovery HTTP 429. Backing off for {cooldown:.2f}s.")
                    continue

                self.logger.log_error(f"[Harvester] Pagination break: {e}")
                break

    async def _save_metrics_batch(self, markets: List[Any]):
        """Parses markets and saves to market_metrics + metadata."""
        metrics_data, series_data, market_reg_data = [], [], []
        now = time.time()

        for m in markets:
            ticker = getattr(m, 'ticker', None)
            if not ticker:
                continue

            s_ticker = getattr(m, 'series_ticker',
                               None) or ticker.split('-')[0]
            cat = self._categorize(ticker, s_ticker)
            if cat == 'ESPORTS':
                continue

            series_data.append((s_ticker, s_ticker, cat, 'unknown'))
            market_reg_data.append(
                (ticker, s_ticker, getattr(m, 'expiration_time', '')))

            volume = getattr(m, 'volume', 0)
            open_interest = getattr(m, 'open_interest', 0)
            bid = getattr(m, 'yes_bid', 0)
            raw_ask = getattr(m, 'yes_ask', 100)
            ask = 100 if (raw_ask == 0 or raw_ask is None) else raw_ask
            spread = ask - bid

            metrics_data.append((ticker, now, volume, open_interest, spread, bid, ask, 'active'))

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.db.upsert_metadata, series_data, market_reg_data)
        if metrics_data:
            await loop.run_in_executor(None, self.db.bulk_upsert_metrics, metrics_data)

    async def run_harvest_loop(self):
        """
        Entry point for the harvester service.
        Runs only the Discovery Loop.
        """
        await self._discovery_loop()
