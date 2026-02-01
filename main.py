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
except ImportError as e:
    logger.log_error(
        f"FATAL: A required project file could not be imported: {e}")
    exit(1)


async def amain():
    """The main asynchronous entry point for the application."""
    logger.log_info("--- Starting Project K-Alpha ---")
    try:
        config = AppConfig()
    except ValidationError as e:
        logger.log_error(
            f"FATAL: Configuration validation failed. Check .env file. {e}")
        return

    # Initialize services
    feed = MarketFeed(config, logger)
    strategy = MomentumStrategy()
    tracker = PortfolioTracker()  # Initialize the portfolio tracker
    executor: Optional[OrderExecutor] = None

    tickers: List[str] = []
    last_ticker_refresh_time: float = 0.0
    TICKER_REFRESH_INTERVAL: int = 60  # 1 minute
    ORDERBOOK_POLL_INTERVAL: int = 2      # 2 seconds

    while True:
        try:
            # 1. Connect and initialize executor
            if not feed.is_connected:
                if not await feed.connect():
                    logger.log_warn(
                        "Connection failed. Retrying in 15 seconds...")
                    await asyncio.sleep(15)
                    continue
                else:
                    # Once connected, create the executor with the authenticated client and tracker
                    logger.log_info(
                        "Connection successful. Initializing Order Executor.")
                    executor = OrderExecutor(feed.api_client, logger, tracker)
                    
                    # Initialize and start the MarketHarvester in the background
                    logger.log_info("Initializing Market Harvester...")
                    harvester = MarketHarvester(feed.api_client, logger)
                    asyncio.create_task(harvester.run_harvest_loop())
                    logger.log_info(
                        "MarketHarvester started in the background.")

            # 2. Refresh tickers hourly
            now = time.time()
            if not tickers or (now - last_ticker_refresh_time > TICKER_REFRESH_INTERVAL):
                logger.log_info("Refreshing ticker list...")
                tickers = await feed.get_active_tickers()
                last_ticker_refresh_time = now  # Reset timer after refresh
                if not tickers:
                    logger.log_warn(
                        "Could not refresh tickers. Retrying in 60s.")
                    await asyncio.sleep(60)
                    continue
                logger.log_info(
                    f"Scanner Update: Tracking {len(tickers)} markets.")

            # 3. Poll order book, analyze, and execute
            for ticker in tickers:
                market_data = await feed.poll_orderbook(ticker)

                if market_data and market_data.get('bid') is not None:
                    # Update portfolio valuation with the latest market price
                    tracker.update_valuation(ticker, market_data['bid'])

                    if executor:
                        try:
                            signal = strategy.analyze_ticker(market_data)
                            if signal:
                                logger.log_info(
                                    f"STRATEGY SIGNAL for {ticker}: {signal}")
                                await executor.place_limit_order(
                                    ticker=ticker,
                                    side=signal['side'],
                                    count=1,  # Hardcoded count as requested
                                    price=signal['price']
                                )
                        except Exception as e:
                            logger.log_error(
                                f"Error during strategy/execution for {ticker}: {e}")

            # 4. Log portfolio summary at the end of the cycle
            logger.log_info(tracker.get_portfolio_summary())

            # 5. Wait for the next polling cycle
            await asyncio.sleep(ORDERBOOK_POLL_INTERVAL)

        except Exception as e:
            logger.log_error(
                f"An unexpected error occurred in the main loop: {e}")
            logger.log_info(
                "Resetting connection and restarting loop in 10 seconds...")
            feed.is_connected = False
            executor = None
            await asyncio.sleep(10)

if __name__ == "__main__":
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        logger.log_info("\nShutdown signal received. Exiting gracefully.")
    except Exception as e:
        logger.log_error(f"A fatal, unhandled exception occurred: {e}")
