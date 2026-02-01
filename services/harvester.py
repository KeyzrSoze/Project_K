import asyncio
import time
from typing import List, Any
from kalshi_python.api import markets_api
from services.db import DatabaseManager
from utils.logger import Logger
from kalshi_python import ApiClient


class MarketHarvester:
    """
    A background service that fetches market data from the API and writes it to
    a local SQLite database, ensuring the async event loop is not blocked.
    """

    def __init__(self, api_client: ApiClient, logger: Logger):
        """
        Initializes the harvester.
        :param api_client: An authenticated Kalshi API client.
        :param logger: An instance of the application logger.
        """
        self.market_api = markets_api.MarketsApi(api_client)
        self.logger = logger
        self.db = DatabaseManager()

    def _categorize(self, ticker: str, series_ticker: str) -> str:
        """
        Categorizes markets based on strict ticker patterns to ensure data quality.

        - ESPORTS: Filters out high-volume/low-quality gaming markets.
        - CRYPTO: Strict check. Must start with 'KX' AND contain a specific coin symbol.
        - ECON: Broad check. Captures Fed rates, inflation, GDP, and employment data.
        """
        t = ticker.upper()
        s = series_ticker.upper()

        # 1. ESPORTS (The Junk Filter)
        if "ESPORTS" in t or "ESPORTS" in s:
            return "ESPORTS"

        # 2. CRYPTO (Strict Mode)
        # Prevents "KXWOSSKATE" (Skateboarding) from being flagged as Crypto.
        # Must start with KX *AND* contain a known coin symbol.
        if s.startswith("KX") and any(coin in s for coin in ["BTC", "ETH", "SOL", "DOGE", "SHIB", "BITCOIN"]):
            return "CRYPTO"

        # 3. ECONOMICS (Broad Mode)
        # Captures major macro-economic indicators.
        if any(k in s for k in ["FED", "INFL", "CPI", "GDP", "RATE", "FOMC", "UNEMPLOYMENT", "CUT", "HIKE"]):
            return "ECON"

        return "MISC"

    async def _process_and_save_batch(self, markets: List[Any]):
        """
        Transforms a batch of market data and saves it to the database.
        """
        if not markets:
            return

        self.logger.log_info(f"Processing batch of {len(markets)} markets...")
        loop = asyncio.get_running_loop()

        # 1. Transform API objects into DB tuples
        series_data = []
        market_registry_data = []
        snapshot_data = []
        current_timestamp = time.time()

        for market in markets:
            # --- Data Sanitization Block ---
            ticker = getattr(market, 'ticker', None)
            if not ticker:
                continue  # Skip invalid markets

            # Handle series_ticker being None or missing
            raw_series = getattr(market, 'series_ticker', None)
            if raw_series:
                series_ticker = raw_series
            else:
                # Fallback: extract from ticker (e.g., "KXETH-24DEC" -> "KXETH")
                series_ticker = ticker.split('-')[0]
            # -------------------------------

            category = self._categorize(ticker, series_ticker)

            # OPTIMIZATION: Do not save ESPORTS to the DB to save space/IO
            if category == 'ESPORTS':
                continue

            # Prepare Snapshot Data (ticker, time, bid, ask, spread, vol, oi, status)
            yes_bid = getattr(market, 'yes_bid', 0)
            yes_ask = getattr(market, 'yes_ask', 0)

            # Calculate spread safely
            if yes_bid and yes_ask:
                spread = yes_ask - yes_bid
            else:
                spread = 99

            # Discovery Log - log any market with a tight spread, regardless of category
            if spread <= 5:
                self.logger.log_info(
                    f"Harvester: Found high-liquidity '{category}' market: {ticker} (Spread: {spread})")

            snapshot_data.append((
                ticker,
                current_timestamp,
                yes_bid,
                yes_ask,
                spread,
                getattr(market, 'volume', 0),
                getattr(market, 'open_interest', 0),
                'active'
            ))

        # 2. Perform bulk DB writes in a background thread executor
        # We align these calls with the DatabaseManager methods defined in services/db.py
        db_ops_start_time = time.time()

        # Write Metadata (Series + Registry)
        await loop.run_in_executor(None, self.db.upsert_metadata, series_data, market_registry_data)

        # Write Snapshots
        await loop.run_in_executor(None, self.db.bulk_upsert_markets, snapshot_data)

        db_ops_duration = time.time() - db_ops_start_time
        self.logger.log_info(
            f"Database write for batch completed in {db_ops_duration:.2f}s.")

    async def _harvest_all_markets(self):
        """
        Fetches all open markets via pagination, transforms them, and writes
        them to the database in non-blocking, bulk operations.
        """
        self.logger.log_info("Starting full market harvest cycle...")
        start_time = time.time()
        loop = asyncio.get_running_loop()

        # 1. Fetch markets page by page and process in batches
        current_batch = []
        cursor = None
        pages_fetched = 0

        while True:
            pages_fetched += 1
            self.logger.log_info(
                f"Harvester: Fetching page {pages_fetched}...")

            # Run the blocking API call in an executor
            response = await loop.run_in_executor(
                None,
                lambda: self.market_api.get_markets(
                    limit=100, status="open", cursor=cursor)
            )

            if not hasattr(response, 'markets') or not response.markets:
                self.logger.log_info("Harvester: No more markets returned.")
                break

            current_batch.extend(response.markets)

            # Batched Ingestion: Write to DB every 500 markets
            if len(current_batch) >= 500:
                await self._process_and_save_batch(current_batch)
                self.logger.log_info(
                    f"Harvester: Flushed batch of {len(current_batch)} markets to DB.")
                current_batch = []  # Clear the batch

            cursor = getattr(response, 'cursor', None)
            if cursor is None:
                self.logger.log_info("Harvester: End of market data reached.")
                break

        # Process any remaining markets in the last batch
        if current_batch:
            self.logger.log_info(
                f"Processing final batch of {len(current_batch)} markets.")
            await self._process_and_save_batch(current_batch)

        total_duration = time.time() - start_time
        self.logger.log_info(
            f"Harvest cycle finished. Total time: {total_duration:.2f}s.")

    async def run_harvest_loop(self):
        """
        Runs the main infinite loop for the harvester service, with error handling.
        """
        self.logger.log_info("MarketHarvester service starting main loop.")
        while True:
            try:
                await self._harvest_all_markets()
            except Exception as e:
                self.logger.log_error(f"Harvester crash: {e}")

            self.logger.log_info("Cycle complete. Sleeping for 60 seconds...")
            await asyncio.sleep(60)
