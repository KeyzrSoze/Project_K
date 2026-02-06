import asyncio
import random
import time
from collections import deque
from concurrent.futures import Executor
from email.utils import parsedate_to_datetime
from typing import List, Any, Optional, Tuple, Callable

import urllib3
from kalshi_python.api import markets_api
from kalshi_python import ApiClient

from services.db import DatabaseManager
from utils.logger import Logger


class MarketHarvester:
    """Discovery Engine: scans markets and upserts into market_metrics."""

    def __init__(
        self,
        api_client: ApiClient,
        logger: Logger,
        generation: int = 0,
        is_current: Optional[Callable[[int], bool]] = None,
        api_executor: Optional[Executor] = None,
        db_write_executor: Optional[Executor] = None,
        db_read_executor: Optional[Executor] = None,
        inflight_counters: Optional[object] = None,
    ):
        self.market_api = markets_api.MarketsApi(api_client)
        self.logger = logger
        self.db = DatabaseManager()

        # Generation guard: main.py can restart harvesters and mark older ones stale.
        self.generation = generation
        self._is_current = is_current or (lambda _gen: True)

        # Rate-limit state (shared across processes)
        self._last_global_cooldown_log_ts: float = 0.0
        self._harvest_429_count: int = 0
        self._last_harvest_429_ts: float = 0.0

        # Cycle / stall diagnostics
        self._cycle_id: int = 0
        self._last_metrics_write_ts: float = 0.0

        # Executors (isolate API calls from DB work).
        self._api_executor = api_executor
        self._db_write_executor = db_write_executor
        self._db_read_executor = db_read_executor
        self._inflight = inflight_counters

        # Timeouts + pacing
        # IMPORTANT: enforce *real* HTTP timeouts via kalshi-python's `_request_timeout`.
        self._request_timeout: Tuple[float, float] = (5.0, 20.0)  # (connect, read)
        # Secondary protection: asyncio timeout must exceed HTTP timeouts to avoid leaking stuck threads.
        self._api_wait_timeout_s: float = 35.0
        self._timeout_backoff_s: float = 2.0
        self._page_log_every: int = 50

        # Timeout tracking for circuit breaking in main.py.
        self._timeout_window_s: float = 120.0
        self._timeout_events = deque()
        self._consecutive_timeouts: int = 0

    def get_timeout_stats(self) -> Tuple[int, int]:
        """Returns: (timeouts_in_last_window, consecutive_timeouts)."""
        now = time.time()
        while self._timeout_events and now - self._timeout_events[0] > self._timeout_window_s:
            self._timeout_events.popleft()
        return len(self._timeout_events), self._consecutive_timeouts

    def _record_timeout(self) -> None:
        now = time.time()
        self._timeout_events.append(now)
        self._consecutive_timeouts += 1

        # Prevent unbounded growth if the process runs for days.
        while self._timeout_events and now - self._timeout_events[0] > (self._timeout_window_s * 2.0):
            self._timeout_events.popleft()

    def _record_success(self) -> None:
        self._consecutive_timeouts = 0

    def _still_current(self) -> bool:
        try:
            return bool(self._is_current(self.generation))
        except Exception:
            # If the guard function fails, stay alive rather than killing discovery.
            return True

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

    async def _set_global_cooldown(self, seconds: float) -> None:
        now = time.time()
        target = now + max(0.0, seconds)
        loop = asyncio.get_running_loop()

        try:
            if self._inflight is not None:
                self._inflight.inflight_reads += 1
            current = await loop.run_in_executor(
                self._db_read_executor, self.db.get_rate_limit_cooldown_until
            )
            if self._inflight is not None:
                self._inflight.inflight_reads = max(
                    0, self._inflight.inflight_reads - 1
                )
        except Exception:
            if self._inflight is not None:
                self._inflight.inflight_reads = max(
                    0, self._inflight.inflight_reads - 1
                )
            current = 0.0

        if target <= float(current or 0.0):
            return

        try:
            if self._inflight is not None:
                self._inflight.inflight_writes += 1
            await loop.run_in_executor(
                self._db_write_executor, self.db.set_rate_limit_cooldown_until, target
            )
            if self._inflight is not None:
                self._inflight.inflight_writes = max(
                    0, self._inflight.inflight_writes - 1
                )
        except Exception as e:
            if self._inflight is not None:
                self._inflight.inflight_writes = max(
                    0, self._inflight.inflight_writes - 1
                )
            if now - self._last_global_cooldown_log_ts >= 10.0:
                self._last_global_cooldown_log_ts = now
                self.logger.log_warn(
                    f"[Harvester] Failed to write global cooldown: {e}"
                )

    async def _maybe_wait_for_global_cooldown(self) -> bool:
        now = time.time()
        loop = asyncio.get_running_loop()
        try:
            if self._inflight is not None:
                self._inflight.inflight_reads += 1
            cooldown_until = await loop.run_in_executor(
                self._db_read_executor, self.db.get_rate_limit_cooldown_until
            )
            if self._inflight is not None:
                self._inflight.inflight_reads = max(
                    0, self._inflight.inflight_reads - 1
                )
        except Exception as e:
            if self._inflight is not None:
                self._inflight.inflight_reads = max(
                    0, self._inflight.inflight_reads - 1
                )
            if now - self._last_global_cooldown_log_ts >= 10.0:
                self._last_global_cooldown_log_ts = now
                self.logger.log_warn(
                    f"[Harvester] Failed to read global cooldown: {e}"
                )
            return False
        if now < cooldown_until:
            if now - self._last_global_cooldown_log_ts >= 10.0:
                self._last_global_cooldown_log_ts = now
                self.logger.log_warn(
                    f"[Harvester] Global cooldown active for {cooldown_until - now:.2f}s. Pausing discovery."
                )
            await asyncio.sleep(min(1.0, cooldown_until - now))
            return True
        return False

    async def _discovery_loop(self):
        """The main execution cycle for the broad market scan."""
        self.logger.log_info(
            f"[Harvester] Starting Discovery Loop (60s)... gen={self.generation}"
        )

        while True:
            if not self._still_current():
                self.logger.log_warn(
                    f"[Harvester] Stale generation detected (gen={self.generation}). Exiting discovery loop."
                )
                return

            self._cycle_id += 1
            cycle_id = self._cycle_id
            cycle_start = time.time()
            self._last_metrics_write_ts = 0.0

            self.logger.log_info(
                f"[Harvester] Cycle {cycle_id} start (gen={self.generation})."
            )

            pages = 0
            markets = 0
            try:
                pages, markets = await self._harvest_all_markets_paginated(
                    cycle_id=cycle_id,
                    cycle_start_ts=cycle_start,
                )
            except asyncio.CancelledError:
                self.logger.log_warn(
                    f"[Harvester] Cycle {cycle_id} cancelled (gen={self.generation})."
                )
                raise
            except Exception as e:
                self.logger.log_error(
                    f"[Harvester] Cycle {cycle_id} error (gen={self.generation}): {e}"
                )

            duration = time.time() - cycle_start
            if self._last_metrics_write_ts:
                local = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(self._last_metrics_write_ts)
                )
                last_write_str = f"{self._last_metrics_write_ts:.0f} ({local})"
            else:
                last_write_str = "n/a"

            self.logger.log_info(
                f"[Harvester] Cycle {cycle_id} complete (gen={self.generation}): "
                f"pages={pages}, markets={markets}, duration_s={duration:.1f}, last_write_ts={last_write_str}"
            )

            await asyncio.sleep(60)

    async def _harvest_all_markets_paginated(self, cycle_id: int, cycle_start_ts: float) -> Tuple[int, int]:
        """Paginates through all open markets, saving metrics to market_metrics."""
        cursor = None
        loop = asyncio.get_running_loop()
        page_count = 0
        total_found = 0
        seen_cursors = set()
        seen_tickers_this_cycle = set()

        while True:
            if not self._still_current():
                self.logger.log_warn(
                    f"[Harvester] Cycle {cycle_id} exiting (stale gen={self.generation})."
                )
                break

            if await self._maybe_wait_for_global_cooldown():
                continue

            try:
                fetch_fut = loop.run_in_executor(
                    self._api_executor,
                    lambda: self.market_api.get_markets_with_http_info(
                        limit=100,
                        status="open",
                        cursor=cursor,
                        _request_timeout=self._request_timeout,
                    ).data,
                )
                response = await asyncio.wait_for(fetch_fut, timeout=self._api_wait_timeout_s)
                self._record_success()
            except asyncio.TimeoutError:
                self._record_timeout()
                self.logger.log_warn(
                    f"[Harvester] Cycle {cycle_id} get_markets timeout after {self._api_wait_timeout_s:.0f}s "
                    f"(cursor={cursor}). Backing off."
                )
                await asyncio.sleep(self._timeout_backoff_s + random.uniform(0.0, 0.5))
                continue
            except urllib3.exceptions.TimeoutError as e:
                self._record_timeout()
                self.logger.log_warn(
                    f"[Harvester] Cycle {cycle_id} get_markets HTTP timeout (cursor={cursor}): {e}. Backing off."
                )
                await asyncio.sleep(self._timeout_backoff_s + random.uniform(0.0, 0.5))
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                status, retry_after = self._extract_status_and_retry_after(e)
                if status == 429:
                    cooldown = self._compute_429_cooldown(retry_after)
                    await self._set_global_cooldown(cooldown)
                    self.logger.log_warn(
                        f"[Harvester] Cycle {cycle_id} HTTP 429 (cursor={cursor}). Backing off for {cooldown:.2f}s."
                    )
                    continue

                self.logger.log_error(
                    f"[Harvester] Cycle {cycle_id} pagination error (cursor={cursor}): {e}"
                )
                break

            markets = getattr(response, "markets", [])
            if not markets:
                break

            first_ticker = getattr(markets[0], "ticker", None)
            if first_ticker and first_ticker in seen_tickers_this_cycle:
                self.logger.log_warn(
                    f"[Harvester] Cycle {cycle_id} loop detected. Ending scan at {total_found} markets."
                )
                break

            for m in markets:
                t = getattr(m, "ticker", None)
                if t:
                    seen_tickers_this_cycle.add(t)

            try:
                await self._save_metrics_batch(markets)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger.log_error(
                    f"[Harvester] Cycle {cycle_id} save_metrics failed (cursor={cursor}): {e}"
                )

            total_found += len(markets)
            page_count += 1

            if page_count % self._page_log_every == 0:
                elapsed = time.time() - cycle_start_ts
                self.logger.log_info(
                    f"[Harvester] Cycle {cycle_id} progress (gen={self.generation}): "
                    f"pages={page_count}, markets={total_found}, elapsed_s={elapsed:.1f}"
                )

            next_cursor = getattr(response, "cursor", None)
            if not next_cursor or next_cursor in seen_cursors:
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor

            await asyncio.sleep(random.uniform(0.05, 0.15))

        return page_count, total_found

    async def _save_metrics_batch(self, markets: List[Any]):
        """Parses markets and saves to market_metrics + metadata."""
        if not self._still_current():
            self.logger.log_warn(
                f"[Harvester] Skipping DB write (stale gen={self.generation})."
            )
            return

        metrics_data, series_data, market_reg_data = [], [], []
        now = time.time()

        for m in markets:
            ticker = getattr(m, "ticker", None)
            if not ticker:
                continue

            s_ticker = getattr(m, "series_ticker", None) or ticker.split("-")[0]
            cat = self._categorize(ticker, s_ticker)
            if cat == "ESPORTS":
                continue

            series_data.append((s_ticker, s_ticker, cat, "unknown"))
            market_reg_data.append(
                (ticker, s_ticker, getattr(m, "expiration_time", ""))
            )

            volume = getattr(m, "volume", 0)
            open_interest = getattr(m, "open_interest", 0)
            bid = getattr(m, "yes_bid", 0)
            raw_ask = getattr(m, "yes_ask", 100)
            ask = 100 if (raw_ask == 0 or raw_ask is None) else raw_ask
            spread = ask - bid

            metrics_data.append(
                (ticker, now, volume, open_interest, spread, bid, ask, "active")
            )

        loop = asyncio.get_running_loop()

        try:
            if self._inflight is not None:
                self._inflight.inflight_writes += 1
            try:
                ok_meta = await loop.run_in_executor(
                    self._db_write_executor,
                    self.db.upsert_metadata,
                    series_data,
                    market_reg_data,
                )
            finally:
                if self._inflight is not None:
                    self._inflight.inflight_writes = max(
                        0, self._inflight.inflight_writes - 1
                    )
            if (series_data or market_reg_data) and not ok_meta:
                self.logger.log_warn(
                    f"[Harvester] Metadata upsert failed (db locked) gen={self.generation}."
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger.log_error(
                f"[Harvester] Metadata upsert error (gen={self.generation}): {e}"
            )

        if metrics_data:
            try:
                if self._inflight is not None:
                    self._inflight.inflight_writes += 1
                try:
                    ok_metrics = await loop.run_in_executor(
                        self._db_write_executor,
                        self.db.bulk_upsert_metrics,
                        metrics_data,
                    )
                finally:
                    if self._inflight is not None:
                        self._inflight.inflight_writes = max(
                            0, self._inflight.inflight_writes - 1
                        )
                if ok_metrics:
                    self._last_metrics_write_ts = now
                else:
                    self.logger.log_warn(
                        f"[Harvester] Metrics upsert failed (db locked) gen={self.generation}."
                    )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger.log_error(
                    f"[Harvester] Metrics upsert error (gen={self.generation}): {e}"
                )


    async def run_harvest_loop(self):
        """Entry point for the harvester service."""
        await self._discovery_loop()
