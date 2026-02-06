import asyncio
from typing import Dict, Any, Optional

from concurrent.futures import Executor
import urllib3
from kalshi_python import ApiClient, Configuration
from kalshi_python.api import markets_api

from config import AppConfig
from utils.logger import Logger
from services.db import DatabaseManager, DB_PATH, OBI_DB_PATH


class MarketFeed:
    def __init__(
        self,
        config: AppConfig,
        logger: Logger,
        *,
        api_executor: Optional[Executor] = None,
        db_read_executor: Optional[Executor] = None,
        inflight_counters: Optional[object] = None,
    ):
        self.config = config
        self.logger = logger
        self.api_client = None
        self.market_api = None
        self.is_connected = False
        self.db_primary = DatabaseManager(db_path=DB_PATH, schema="primary")
        self.db_obi = DatabaseManager(db_path=OBI_DB_PATH, schema="obi")
        self._obi_db_split = OBI_DB_PATH != DB_PATH
        self.api_executor = api_executor
        self.db_read_executor = db_read_executor
        self._inflight = inflight_counters

        # Real network timeouts (connect, read) for kalshi-python requests.
        self._request_timeout = (5.0, 20.0)
        self._connect_wait_timeout_s = 35.0

    def set_executors(
        self,
        *,
        api_executor: Optional[Executor] = None,
        db_read_executor: Optional[Executor] = None,
    ) -> None:
        if api_executor is not None:
            self.api_executor = api_executor
        if db_read_executor is not None:
            self.db_read_executor = db_read_executor

    async def connect(self) -> bool:
        self.logger.log_info(
            f"Initializing RSA Authentication ({self.config.KALSHI_ENV})..."
        )
        host = (
            "https://demo-api.kalshi.co/trade-api/v2"
            if self.config.KALSHI_ENV == "DEMO"
            else "https://api.elections.kalshi.com/trade-api/v2"
        )
        api_config = Configuration()
        api_config.host = host

        try:
            self.api_client = ApiClient(api_config)
            self.api_client.set_kalshi_auth(
                key_id=self.config.KALSHI_API_KEY_ID,
                private_key_path=self.config.KALSHI_PRIVATE_KEY_PATH,
            )
            self.market_api = markets_api.MarketsApi(self.api_client)

            # Connectivity check with *real* HTTP timeouts and a secondary asyncio timeout.
            loop = asyncio.get_running_loop()
            fut = loop.run_in_executor(
                self.api_executor,
                lambda: self.market_api.get_markets_with_http_info(
                    limit=1,
                    status="open",
                    _request_timeout=self._request_timeout,
                ).data,
            )
            await asyncio.wait_for(fut, timeout=self._connect_wait_timeout_s)

            self.is_connected = True
            self.logger.log_info("RSA Authentication Successful.")
            return True
        except asyncio.TimeoutError:
            self.logger.log_error("RSA Authentication check timed out.")
            self.is_connected = False
            return False
        except urllib3.exceptions.TimeoutError as e:
            self.logger.log_error(f"RSA Authentication HTTP timeout: {e}")
            self.is_connected = False
            return False
        except Exception as e:
            self.logger.log_error(f"RSA Authentication Failed: {e}")
            self.is_connected = False
            return False

    async def get_active_tickers(self):
        loop = asyncio.get_running_loop()
        if self._inflight is not None:
            self._inflight.inflight_reads += 1
        try:
            return await loop.run_in_executor(
                self.db_read_executor, lambda: self.db_primary.get_top_active_tickers(limit=50)
            )
        except Exception as e:
            self.logger.log_warn(f"Ticker query failed: {e}")
            return []
        finally:
            if self._inflight is not None:
                self._inflight.inflight_reads = max(0, self._inflight.inflight_reads - 1)

    async def poll_orderbook(self, ticker: str) -> Dict[str, Any]:
        """Reads the latest combined snapshot from the DB."""
        try:
            loop = asyncio.get_running_loop()
            if not self._obi_db_split:
                if self._inflight is not None:
                    self._inflight.inflight_reads += 1
                try:
                    row = await loop.run_in_executor(
                        self.db_read_executor,
                        self.db_primary.get_combined_market_view,
                        ticker,
                    )
                finally:
                    if self._inflight is not None:
                        self._inflight.inflight_reads = max(
                            0, self._inflight.inflight_reads - 1
                        )

                if not row:
                    return {}

                bid = row.get("best_bid")
                ask = row.get("best_ask")
                spread = row.get("spread")
                if spread is None and bid is not None and ask is not None:
                    spread = ask - bid

                ts = row.get("obi_ts") or row.get("metrics_ts") or 0.0

                return {
                    "ticker": row.get("ticker", ticker),
                    "timestamp": ts,
                    "bid": bid,
                    "ask": ask,
                    "spread": spread or 0,
                    "volume": row.get("volume", 0),
                    "status": row.get("status", "active"),
                    "series_ticker": row.get("series_ticker", "unknown"),
                    "category": row.get("category", "MISC"),
                    "bid_count": row.get("bid_count", 0),
                    "ask_count": row.get("ask_count", 0),
                }

            if self._inflight is not None:
                self._inflight.inflight_reads += 1
            try:
                metrics = await loop.run_in_executor(
                    self.db_read_executor,
                    self.db_primary.get_market_snapshot,
                    ticker,
                )
            finally:
                if self._inflight is not None:
                    self._inflight.inflight_reads = max(
                        0, self._inflight.inflight_reads - 1
                    )

            if not metrics:
                return {}

            if self._inflight is not None:
                self._inflight.inflight_reads += 1
            try:
                obi = await loop.run_in_executor(
                    self.db_read_executor,
                    self.db_obi.get_market_obi,
                    ticker,
                )
            finally:
                if self._inflight is not None:
                    self._inflight.inflight_reads = max(
                        0, self._inflight.inflight_reads - 1
                    )

            bid = (obi or {}).get("best_bid")
            ask = (obi or {}).get("best_ask")
            if bid is None:
                bid = metrics.get("best_bid")
            if ask is None:
                ask = metrics.get("best_ask")

            spread = metrics.get("spread")
            if spread is None and bid is not None and ask is not None:
                spread = ask - bid

            ts = (obi or {}).get("timestamp") or metrics.get("timestamp") or 0.0

            return {
                "ticker": ticker,
                "timestamp": ts,
                "bid": bid,
                "ask": ask,
                "spread": spread or 0,
                "volume": metrics.get("volume", 0),
                "status": metrics.get("status", "active"),
                "series_ticker": "unknown",
                "category": "MISC",
                "bid_count": int((obi or {}).get("bid_count") or 0),
                "ask_count": int((obi or {}).get("ask_count") or 0),
            }
        except Exception as e:
            self.logger.log_error(f"DB poll failed for {ticker}: {e}")
            return {}
