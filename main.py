import asyncio
import time
from typing import List, Optional

from pydantic import ValidationError

from utils.logger import logger
try:
    from config import AppConfig
    from services.feed import MarketFeed
    from services.execution import OrderExecutor
    from services.accounting import PortfolioTracker
    from strategies.momentum import MomentumStrategy
    from services.harvester import MarketHarvester
    from services.recorder import AsyncMarketRecorder
    from services.features import FeatureExtractor
except ImportError as e:
    logger.log_error(
        f"FATAL: A required project file could not be imported: {e}")
    exit(1)


async def amain():
    """The main asynchronous entry point for the application."""
    logger.log_info("--- Starting Project K-Alpha (Phase 4: AI-Data) ---")
    recorder: Optional[AsyncMarketRecorder] = None
    try:
        config = AppConfig()

        # Initialize services
        feed = MarketFeed(config, logger)
        strategy = MomentumStrategy()
        tracker = PortfolioTracker()
        recorder = AsyncMarketRecorder()  # Upgraded recorder
        feature_extractor = FeatureExtractor()  # New feature extractor
        executor: Optional[OrderExecutor] = None

        tickers: List[str] = []
        last_ticker_refresh_time: float = 0.0
        TICKER_REFRESH_INTERVAL: int = 60  # 1 minute
        ORDERBOOK_POLL_INTERVAL: int = 2      # 2 seconds

        while True:
            try:
                # 1. Connect and initialize services
                if not feed.is_connected:
                    if not await feed.connect():
                        logger.log_warn("Connection failed. Retrying in 15 seconds...")
                        await asyncio.sleep(15)
                        continue
                    else:
                        logger.log_info("Connection successful. Initializing services.")
                        executor = OrderExecutor(feed.api_client, logger, tracker)
                        harvester = MarketHarvester(feed.api_client, logger)
                        asyncio.create_task(harvester.run_harvest_loop())
                        logger.log_info("MarketHarvester started in the background.")

                # 2. Refresh tickers
                now = time.time()
                if not tickers or (now - last_ticker_refresh_time > TICKER_REFRESH_INTERVAL):
                    logger.log_info("Refreshing ticker list from database...")
                    tickers = await feed.get_active_tickers()
                    last_ticker_refresh_time = now
                    if not tickers:
                        logger.log_warn("Could not refresh tickers. Retrying in 60s.")
                        await asyncio.sleep(60)
                        continue
                    logger.log_info(f"Scanner Update: Tracking {len(tickers)} markets.")

                # 3. Poll, Enrich, Record, and Analyze
                for ticker in tickers:
                    market_data = await feed.poll_orderbook(ticker)

                    if market_data and market_data.get('bid') is not None:
                        # --- AI-DATA UPGRADE ---
                        # 3a. Enrich data with features
                        enriched_data = feature_extractor.transform(market_data)
                        
                        # 3b. Record enriched data with metadata for TFT training
                        await recorder.record(
                            enriched_data=enriched_data,
                            category=market_data['category'],
                            series_ticker=market_data['series_ticker'],
                            status=market_data['status']
                        )
                        # --- END UPGRADE ---

                        # Update portfolio valuation with the latest market price
                        tracker.update_valuation(ticker, enriched_data['bid'])

                        # Standard strategy execution
                        if executor:
                            try:
                                signal = strategy.analyze_ticker(enriched_data)
                                if signal:
                                    logger.log_info(f"STRATEGY SIGNAL for {ticker}: {signal}")
                                    await executor.place_limit_order(
                                        ticker=ticker,
                                        side=signal['side'],
                                        count=1,
                                        price=signal['price']
                                    )
                            except Exception as e:
                                logger.log_error(f"Error during strategy/execution for {ticker}: {e}")

                # 4. Log portfolio summary
                logger.log_info(tracker.get_portfolio_summary())

                # 5. Wait for the next cycle
                await asyncio.sleep(ORDERBOOK_POLL_INTERVAL)

            except Exception as e:
                logger.log_error(f"An unexpected error occurred in the main loop: {e}")
                logger.log_info("Resetting connection and restarting loop in 10 seconds...")
                if feed:
                    feed.is_connected = False
                executor = None
                await asyncio.sleep(10)
    finally:
        if recorder:
            logger.log_info("Flushing any remaining data before exit...")
            await recorder.close()


if __name__ == "__main__":
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        logger.log_info("\nShutdown signal received. Exiting gracefully.")
    except Exception as e:
        logger.log_error(f"A fatal, unhandled exception occurred: {e}")