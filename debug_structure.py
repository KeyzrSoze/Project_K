import asyncio
import sys
from kalshi_python import ApiClient, Configuration
from kalshi_python.api import markets_api
from config import AppConfig

# Force RSA Auth Logic for the diagnostic


async def diagnose():
    print("--- DIAGNOSTIC: OrderBook Structure ---")
    try:
        config = AppConfig()

        # 1. Setup Client
        api_config = Configuration()
        api_config.host = "https://demo-api.kalshi.co/trade-api/v2"
        client = ApiClient(api_config)
        client.set_kalshi_auth(
            key_id=config.KALSHI_API_KEY_ID,
            private_key_path=config.KALSHI_PRIVATE_KEY_PATH
        )
        api = markets_api.MarketsApi(client)

        # 2. Get a valid ticker
        print("Fetching market list...")
        markets = api.get_markets(limit=1, status="open")
        ticker = markets.markets[0].ticker
        print(f"Targeting Ticker: {ticker}")

        # 3. Get the mysterious object
        print("Fetching OrderBook...")
        response = api.get_market_orderbook(ticker, depth=5)

        print(f"\n[TYPE]: {type(response)}")
        print(f"\n[ATTRIBUTES]: {dir(response)}")

        # Check for common variants
        if hasattr(response, 'order_book'):
            print("\nFound '.order_book'! Attributes inside:")
            print(dir(response.order_book))
        elif hasattr(response, 'orderbook'):
            print("\nFound '.orderbook'! Attributes inside:")
            print(dir(response.orderbook))

    except Exception as e:
        print(f"\nCRITICAL ERROR: {e}")

if __name__ == "__main__":
    asyncio.run(diagnose())
