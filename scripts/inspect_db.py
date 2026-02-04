import pandas as pd
import time
from services.db import DatabaseManager

db = DatabaseManager()

with db.get_connection() as conn:
    # 2. Inspect the "Broad" Discovery Data
    print("\n--- TOP 5 DISCOVERY TARGETS (High Volume) ---")
    df_metrics = pd.read_sql_query("""
        SELECT ticker, volume, spread, best_bid, best_ask 
        FROM market_metrics 
        ORDER BY volume DESC 
        LIMIT 5
    """, conn)
    print(df_metrics)

    # 3. Inspect the "Deep" OBI Data
    print("\n--- LATEST OBI DATA (Tracking Loop) ---")
    df_obi = pd.read_sql_query("""
        SELECT ticker, bid_count, ask_count, best_bid, best_ask, 
               (bid_count - ask_count) as net_imbalance
        FROM market_obi 
        ORDER BY timestamp DESC 
        LIMIT 10
    """, conn)
    print(df_obi)

# 4. Parquet Check(If you have recorders running)
print("\n--- CHECKING PARQUET FILES ---")
try:
    df_parquet = pd.read_parquet("data/training/")  # Adjust path
    print(df_parquet.head())
except Exception as e:
    print(f"No parquet files generated yet: {e}")
