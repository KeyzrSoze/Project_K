import asyncio
import json
from typing import List, Dict, Any, Optional
from kalshi_python import ApiClient, Configuration
from kalshi_python.api import markets_api
from config import AppConfig
from utils.logger import Logger
from services.db import DatabaseManager
import time


class MarketFeed:
    def __init__(self, config: AppConfig, logger: Logger):
        self.config = config
        self.logger = logger
        self.api_client = None
        self.market_api = None
        self.is_connected = False
        self.db = DatabaseManager()

    async def connect(self) -> bool:
        """Establishes the authenticated session using RSA Keys."""
        self.logger.log_info(
            f"Initializing RSA Authentication ({self.config.KALSHI_ENV})...")

        host = "https://demo-api.kalshi.co/trade-api/v2" if self.config.KALSHI_ENV == "DEMO" else "https://api.elections.kalshi.com/trade-api/v2"

        api_config = Configuration()
        api_config.host = host

        try:
            # 1. Initialize Client
            self.api_client = ApiClient(api_config)

            # 2. Authenticate using the FILE PATH directly
            self.api_client.set_kalshi_auth(
                key_id=self.config.KALSHI_API_KEY_ID,
                private_key_path=self.config.KALSHI_PRIVATE_KEY_PATH
            )

            # 3. Initialize Market API
            self.market_api = markets_api.MarketsApi(self.api_client)

            # 4. Verify Connection
            self.market_api.get_markets(limit=1, status="open")

            self.is_connected = True
            self.logger.log_info("RSA Authentication Successful.")
            return True

        except Exception as e:
            self.logger.log_error(f"RSA Authentication Failed: {e}")
            self.is_connected = False
            return False

    async def get_active_tickers(self) -> List[str]:
        """
        Retrieves the top 50 most active tickers from the local database,
        based on metrics defined in the DatabaseManager.
        """
        self.logger.log_info("Querying database for top active tickers...")
        try:
            # This call is now directed to the optimized, centralized query in db.py
            top_tickers = self.db.get_top_active_tickers(limit=50)

            if top_tickers:
                self.logger.log_info(
                    f"Tracking top {len(top_tickers)} tickers from DB: {', '.join(top_tickers)}")
                return top_tickers
            else:
                self.logger.log_warn(
                    "Database returned no active tickers. The harvester may need to run.")
                return []

        except Exception as e:
            self.logger.log_error(
                f"Database query for active tickers failed: {e}")
            return []

    async def poll_orderbook(self, ticker: str) -> Dict[str, Any]:
        """
        Retrieves a comprehensive, enriched market snapshot from the local DB,
        joining across tables to get metadata required for feature engineering and recording.
        """
        query = """
            SELECT
                s.ticker, s.timestamp, s.bid, s.ask, s.spread, s.volume, s.status,
                m.series_ticker,
                sr.category
            FROM market_snapshots s
            LEFT JOIN market_registry m ON s.ticker = m.ticker
            LEFT JOIN series_registry sr ON m.series_ticker = sr.series_ticker
            WHERE s.ticker = ?
            ORDER BY s.timestamp DESC
            LIMIT 1
        """
        try:
            loop = asyncio.get_running_loop()
            row = await loop.run_in_executor(
                None,
                lambda: self._fetch_snapshot_from_db(query, ticker)
            )

            if row:
                # The FeatureExtractor expects bid/ask counts. Since they are not in the
                # DB, we will pass them as 0. The OBI will be 0, but other features will work.
                return {
                    "ticker": row['ticker'],
                    "timestamp": row['timestamp'],
                    "bid": row['bid'],
                    "ask": row['ask'],
                    "spread": row['spread'],
                    "volume": row['volume'],
                    "status": row.get('status', 'unknown'),
                    "series_ticker": row.get('series_ticker', 'unknown'),
                    "category": row.get('category', 'MISC'),
                    "bid_count": 0,  # Placeholder for OBI calculation
                    "ask_count": 0  # Placeholder for OBI calculation
                }
            else:
                self.logger.log_warn(f"No snapshot found in DB for {ticker}")
                return {}

        except Exception as e:
            self.logger.log_error(f"DB poll failed for {ticker}: {e}")
            return {}

    def _fetch_snapshot_from_db(self, query: str, ticker: str) -> Optional[Dict[str, Any]]:
        """Helper to run the SQL query for a single row."""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            row = cursor.execute(query, (ticker,)).fetchone()
            return dict(row) if row else None
