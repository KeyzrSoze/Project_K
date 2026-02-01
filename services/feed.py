import asyncio
import json
from typing import List, Dict, Any, Optional
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
        Retrieves the top 10 most active tickers from the local database,
        excluding 'ESPORTS' markets.
        """
        self.logger.log_info("Querying database for top active tickers...")
        try:
            # Query the database for the top 10 active tickers
            top_tickers = self.db.get_top_active_tickers(limit=10)

            if top_tickers:
                self.logger.log_info(
                    f"Tracking top {len(top_tickers)} tickers from DB: {', '.join(top_tickers)}")
                return top_tickers
            else:
                self.logger.log_warn(
                    "Database returned no active tickers. The harvester may need to run.")
                return []

        except Exception as e:
            self.logger.log_error(f"Database query for active tickers failed: {e}")
            return []

    async def poll_orderbook(self, ticker: str) -> Dict[str, Any]:
        """Fetch Level 2 Order Book Data."""
        if not self.is_connected:
            return {}

        try:
            # 1. Get the raw response object
            # Note: We use 'get_market_orderbook' as confirmed by your diagnostic
            response = self.market_api.get_market_orderbook(ticker, depth=5)

            # 2. Extract the inner orderbook object
            # Based on diagnostic, the attribute is lowercase 'orderbook'
            if hasattr(response, 'orderbook'):
                ob = response.orderbook
            else:
                # Fallback in case structure varies
                ob = response

            # 3. Extract Liquidity (The var_true/var_false Fix)
            # var_true = Bids for YES
            # var_false = Bids for NO (which we convert to Asks for YES)

            yes_bids = getattr(ob, 'var_true', []) or []
            no_bids = getattr(ob, 'var_false', []) or []

            # 4. Calculate Prices
            # best_yes_bid is simply the top price in var_true
            best_bid = yes_bids[0][0] if yes_bids else 0

            # best_yes_ask is (100 - best_no_bid)
            best_no_bid = no_bids[0][0] if no_bids else 0
            best_ask = 100 - best_no_bid if best_no_bid > 0 else 100

            spread = best_ask - best_bid

            # Log it
            self.logger.log_market(ticker, best_bid, best_ask, spread)

            return {
                "ticker": ticker,
                "bid": best_bid,
                "ask": best_ask,
                "spread": spread
            }

        except Exception as e:
            self.logger.log_warn(f"Poll failed for {ticker}: {e}")
            return {}
