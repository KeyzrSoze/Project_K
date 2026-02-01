from collections import deque
from typing import Dict, Any, Optional, Deque

class MomentumStrategy:
    """
    A strategy that identifies trading opportunities based on spread compression
    and price momentum.
    """
    def __init__(self, history_length: int = 10, momentum_period: int = 5):
        """
        Initializes the momentum strategy.

        Args:
            history_length: The total number of recent prices to store per ticker.
            momentum_period: The lookback period for calculating momentum.
        """
        if momentum_period >= history_length:
            raise ValueError("momentum_period must be less than history_length")
        
        self.history_length = history_length
        self.momentum_period = momentum_period
        self.price_history: Dict[str, Deque[int]] = {}

    def analyze_ticker(self, market_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Analyzes market data for a single ticker to find a trading signal
        using strict production logic.
        """
        ticker = market_data.get('ticker')
        bid = market_data.get('bid')
        ask = market_data.get('ask')
        spread = market_data.get('spread')

        if not all([isinstance(ticker, str), isinstance(bid, (int, float)), isinstance(ask, (int, float)), isinstance(spread, (int, float))]):
            return None

        bid_cents = int(bid * 100)
        ask_cents = int(ask * 100)

        if ticker not in self.price_history:
            self.price_history[ticker] = deque(maxlen=self.history_length)
        
        self.price_history[ticker].append(bid_cents)

        if len(self.price_history[ticker]) < self.history_length:
            return None

        # --- Production Signal Logic ---
        current_bid = self.price_history[ticker][-1]
        past_bid = self.price_history[ticker][-1 - self.momentum_period]
        price_change = current_bid - past_bid

        # Keep the debug log to see the "Brain" working
        print(f"Analyzing {ticker}: Spread={spread}¢, Price Move={price_change}¢")
        
        # 1. Liquidity Condition: Only trade if the spread is tight (<= 5 cents).
        if spread > 5:
            return None

        # 2. Momentum Condition: Only trade on significant price moves (>= 3 cents).
        if price_change >= 3:
            return {
                "action": "buy",
                "side": "yes",
                "price": ask_cents,
            }
        
        elif price_change <= -3:
            no_ask_price = 100 - bid_cents
            return {
                "action": "buy",
                "side": "no",
                "price": no_ask_price,
            }

        return None
