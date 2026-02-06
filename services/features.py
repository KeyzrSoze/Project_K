from collections import deque, defaultdict

class FeatureExtractor:
    """
    Calculates advanced financial features from raw market data snapshots.
    This class maintains state for time-series calculations like momentum and velocity.
    """
    def __init__(self):
        # defaultdict of deques to store recent values for each ticker
        # maxlen ensures memory efficiency for long-running processes
        self.recent_prices = defaultdict(lambda: deque(maxlen=21))  # For 20-period momentum
        self.recent_spreads = defaultdict(lambda: deque(maxlen=4))   # For 3-period spread velocity

    def transform(self, snapshot: dict) -> dict:
        """
        Enriches a raw market data snapshot with calculated features.

        Args:
            snapshot (dict): A dictionary containing raw market data for a single ticker.

        Returns:
            dict: The snapshot dictionary enriched with new feature keys.
        """
        ticker = snapshot.get('ticker')
        if not ticker:
            # Return original snapshot if ticker is not present
            return snapshot

        enriched_snapshot = snapshot.copy()

        # Feature 1: Order Book Imbalance (OBI)
        enriched_snapshot['obi'] = self._calculate_obi(snapshot)

        # For stateful features, we need price and spread
        bid = snapshot.get('bid', 0)
        ask = snapshot.get('ask', 0)

        # Feature 2: Spread Velocity
        spread = ask - bid if ask > 0 and bid > 0 else 0
        self.recent_spreads[ticker].append(spread)
        enriched_snapshot['spread_velocity'] = self._calculate_spread_velocity(ticker)

        # Feature 3: Rolling Momentum
        # Using mid-price as the basis for momentum calculation
        mid_price = (ask + bid) / 2 if ask > 0 and bid > 0 else 0
        self.recent_prices[ticker].append(mid_price)
        
        mom_5, mom_10, mom_20 = self._calculate_rolling_momentum(ticker)
        enriched_snapshot['momentum_5'] = mom_5
        enriched_snapshot['momentum_10'] = mom_10
        enriched_snapshot['momentum_20'] = mom_20
        
        return enriched_snapshot

    def _calculate_obi(self, snapshot: dict) -> float:
        """
        Calculates Order Book Imbalance (OBI).
        Formula: (bid_count - ask_count) / (bid_count + ask_count)
        """
        bid_count = snapshot.get('bid_count', 0)
        ask_count = snapshot.get('ask_count', 0)
        total_count = bid_count + ask_count
        return (bid_count - ask_count) / total_count if total_count > 0 else 0.0

    def _calculate_spread_velocity(self, ticker: str) -> float:
        """
        Calculates the change in spread over the last 3 snapshots.
        Implemented as the difference between the current spread and the spread 3 snapshots ago.
        """
        spreads = self.recent_spreads[ticker]
        # Need 4 points for a 3-period change (t, t-1, t-2, t-3)
        if len(spreads) < 4:
            return 0.0
        # spreads[0] is the oldest, spreads[-1] is the newest (current)
        return spreads[-1] - spreads[0]

    def _calculate_rolling_momentum(self, ticker: str) -> tuple[float, float, float]:
        """
        Calculates price change over 5, 10, and 20-period windows.
        Implemented as the difference between the current price and the price N periods ago.
        """
        prices = self.recent_prices[ticker]
        current_price = prices[-1]

        # Helper to safely get a historical price from the deque
        def get_past_price(offset):
            # To get price from N periods ago, we need N+1 items in the deque
            if len(prices) > offset:
                # prices[-(offset + 1)] gets the (N+1)th element from the end
                return prices[-(offset + 1)]
            return None

        price_5_periods_ago = get_past_price(5)
        price_10_periods_ago = get_past_price(10)
        price_20_periods_ago = get_past_price(20)

        # Calculate momentum; if historical price is not available, momentum is 0
        mom_5 = current_price - price_5_periods_ago if price_5_periods_ago is not None else 0.0
        mom_10 = current_price - price_10_periods_ago if price_10_periods_ago is not None else 0.0
        mom_20 = current_price - price_20_periods_ago if price_20_periods_ago is not None else 0.0
        
        return mom_5, mom_10, mom_20