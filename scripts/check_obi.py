import pandas as pd
from services.db import DatabaseManager, DB_PATH


def check_obi_data():
    db = DatabaseManager()
    print(f"[check_obi] Using DB: {DB_PATH}")

    # Query to see the latest OBI captures
    query = """
    SELECT 
        ticker, 
        datetime(timestamp, 'unixepoch', 'localtime') as last_capture,
        bid_count, 
        ask_count, 
        best_bid, 
        best_ask 
    FROM market_obi 
    ORDER BY timestamp DESC 
    LIMIT 20;
    """

    try:
        with db.get_connection() as conn:
            df = pd.read_sql_query(query, conn)

        if df.empty:
            print("[!] The market_obi table is still empty.")
        else:
            print("--- LATEST OBI CAPTURES ---")
            print(df.to_string(index=False))

            # Check for non-zero data
            valid_rows = df[(df['bid_count'] > 0) | (df['ask_count'] > 0)]
            print(
                f"\n[*] Found {len(valid_rows)} records with non-zero OBI data.")

    except Exception as e:
        print(f"[ERROR] Could not read OBI data: {e}")


if __name__ == "__main__":
    check_obi_data()
