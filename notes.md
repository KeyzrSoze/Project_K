# Target Markets:

## Cryptocurrency Markets (must contain at least one of the following keywords)
- **BTC/BITCOIN (Bitcoint)**
- **ETH (Ethereum)**
- **SOL (Solana)**
- **DOGE (Dogecoin)**
- **SHIB (Shiba Inu)**


## Economics Markets (must contain at least one of the following keywords)
- **FED (Federal Reserve)**
- **CPI (Consumer Price Index)**
- **GDP (Gross Domestic Product)**
- **UNEMPLOYMENT**
- **INFL (Inflation)**
- **RATE (Interest Rate)**
- **FOMC (Federal Open Market Committee)**
- **CUT (Rate Cut)**
- **HIKE (Rate Hike)**

## The Trading Pipeline: How K-Alpha Thinks
The bot operates in a loop of Discovery, Analysis, and Execution.

1. **Discovery (The Smart Feed)**: Instead of asking the API "What is happening?" (which is slow), the bot queries your local SQLite database.
    - The Query: "Show me the top 10 Non-Esports markets where status='active', sorted by volume."

2. **Analysis (The "Momentum Strategy")**: Once the Feed hands over a ticker (e.g., FED-RATE-DEC), the MomentumStrategy class (strategies/momentum.py) takes over. It watches the price like a hawk.

It uses two "Triggers" to decide if it should trade:
    - **The LIquidity Trigger (Safe Check)**:
       - Logic: Is spread <= $0.05?
    - **The Velocity Trigger (Go Signal):
         - Logic: Has the price moved >= than $.03 in the last 5 ticks?
         - The math: CurrentBid - Bid{t-5}.

3. **Execution**: If both Triggers are present, the OrderExecutor places a limit order. 
    - Places order at the current Ask price (aggressive entry).
    - It logs a Papar Trade entry so it can be tracked without using real money.