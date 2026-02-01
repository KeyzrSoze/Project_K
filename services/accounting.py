from typing import Dict, Any

class PortfolioTracker:
    """
    Manages a virtual portfolio for paper trading, tracking cash, positions,
    and calculating Profit and Loss (PnL).
    """

    def __init__(self, starting_balance: float = 250.00):
        """
        Initializes the portfolio with a starting cash balance.
        """
        self.cash_balance: float = starting_balance
        # positions stores holdings: {ticker: {'yes': count, 'no': count, 'avg_price': float, 'market_value': float}}
        self.positions: Dict[str, Dict[str, Any]] = {}

    def record_transaction(self, ticker: str, side: str, count: int, price_cents: int):
        """
        Records a simulated trade, updating cash and position holdings.

        Args:
            ticker: The market ticker for the trade.
            side: 'yes' or 'no'.
            count: The number of contracts.
            price_cents: The price per contract in cents.
        """
        price_dollars = price_cents / 100.0
        trade_cost = count * price_dollars

        if self.cash_balance < trade_cost:
            print(f"PAPER_TRADE_REJECTED: Insufficient funds for {ticker} trade. Need ${trade_cost:.2f}, have ${self.cash_balance:.2f}")
            return

        self.cash_balance -= trade_cost

        # Initialize position if it's new
        if ticker not in self.positions:
            self.positions[ticker] = {'yes': 0, 'no': 0, 'avg_price': 0, 'market_value': 0}

        # Update position
        current_count = self.positions[ticker][side]
        current_avg_price = self.positions[ticker]['avg_price']

        # Update average price (simple weighted average)
        total_count = current_count + count
        self.positions[ticker]['avg_price'] = ((current_count * current_avg_price) + (count * price_dollars)) / total_count
        self.positions[ticker][side] = total_count
        
        print(f"PAPER_TRADE_EXECUTED: BOUGHT {count} {side} contracts of {ticker} @ ${price_dollars:.2f}. New Balance: ${self.cash_balance:.2f}")

    def update_valuation(self, ticker: str, current_bid_cents: int):
        """
        Updates the mark-to-market value of a position based on the current bid.

        Args:
            ticker: The market ticker to update.
            current_bid_cents: The current best bid price for a 'yes' contract in cents.
        """
        if ticker in self.positions:
            position = self.positions[ticker]
            yes_value = position['yes'] * (current_bid_cents / 100.0)
            
            # Assuming the value of a 'no' contract is (1 - bid)
            no_value = position['no'] * ((100 - current_bid_cents) / 100.0)

            position['market_value'] = yes_value + no_value

    def get_portfolio_summary(self) -> str:
        """
        Calculates and returns a formatted string of the portfolio's PnL.
        """
        total_investment = 0
        total_market_value = 0

        for ticker, position in self.positions.items():
            total_investment += position['yes'] * position['avg_price']
            total_investment += position['no'] * position['avg_price'] # This is a simplification
            total_market_value += position.get('market_value', 0)
            
        unrealized_pnl = total_market_value - total_investment
        total_equity = self.cash_balance + total_market_value

        return f"[PnL] Cash: ${self.cash_balance:.2f} | Unrealized: ${unrealized_pnl:.2f} | Total Equity: ${total_equity:.2f}"
