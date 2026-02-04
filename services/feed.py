import asyncio
from typing import Dict, Any, Optional
from kalshi_python import ApiClient, Configuration
from kalshi_python.api import markets_api
from config import AppConfig
from utils.logger import Logger
from services.db import DatabaseManager


class MarketFeed:
    def __init__(self, config: AppConfig, logger: Logger):
        self.config = config
        self.logger = logger
        self.api_client = None
        self.market_api = None
        self.is_connected = False
        self.db = DatabaseManager()

    async def connect(self) -> bool:
        self.logger.log_info(
            f"Initializing RSA Authentication ({self.config.KALSHI_ENV})...")
        host = "https://demo-api.kalshi.co/trade-api/v2" if self.config.KALSHI_ENV == "DEMO" else "https://api.elections.kalshi.com/trade-api/v2"
        api_config = Configuration()
        api_config.host = host

        try:
            self.api_client = ApiClient(api_config)
            self.api_client.set_kalshi_auth(
                key_id=self.config.KALSHI_API_KEY_ID,
                private_key_path=self.config.KALSHI_PRIVATE_KEY_PATH
            )
            self.market_api = markets_api.MarketsApi(self.api_client)
            self.market_api.get_markets(limit=1, status="open")
            self.is_connected = True
            self.logger.log_info("RSA Authentication Successful.")
            return True
        except Exception as e:
            self.logger.log_error(f"RSA Authentication Failed: {e}")
            self.is_connected = False
            return False

    async def get_active_tickers(self):
        return self.db.get_top_active_tickers(limit=50)

    async def poll_orderbook(self, ticker: str) -> Dict[str, Any]:
        """
        Reads the latest combined snapshot from the DB using the new accessor.
        """
        try:
            loop = asyncio.get_running_loop()
            # Use the new, safe accessor method from the DatabaseManager
            row = await loop.run_in_executor(None, self.db.get_combined_market_view, ticker)

            if row:
                return {
                    "ticker": row['ticker'],
                    "timestamp": row['obi_ts'],  # Prioritize OBI timestamp
                    "bid": row['best_bid'],
                    "ask": row['best_ask'],
                    "spread": row.get('spread', row['best_ask'] - row['best_bid']),
                    "volume": row.get('volume', 0),
                    "status": row.get('status', 'active'),
                    "series_ticker": row.get('series_ticker', 'unknown'),
                    "category": row.get('category', 'MISC'),
                    "bid_count": row.get('bid_count', 0),
                    "ask_count": row.get('ask_count', 0)
                }
            return {}
        except Exception as e:
            self.logger.log_error(f"DB poll failed for {ticker}: {e}")
            return {}

