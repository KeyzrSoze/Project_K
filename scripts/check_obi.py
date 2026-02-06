import argparse
import time
import pandas as pd
from services.db import DatabaseManager, OBI_DB_PATH


def check_obi_data(show_count: bool = False):
    db = DatabaseManager(db_path=OBI_DB_PATH, schema="obi")
    print(f"[check_obi] Using OBI DB: {OBI_DB_PATH}")

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
        with db.get_connection(role="read") as conn:
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
        return

    if show_count:
        try:
            now = time.time()
            with db.get_connection(role="read") as conn:
                total = conn.execute(
                    "SELECT COUNT(*) as total FROM market_obi"
                ).fetchone()["total"]
                recent = conn.execute(
                    "SELECT COUNT(DISTINCT ticker) as recent FROM market_obi WHERE timestamp >= ?",
                    (now - 60,)
                ).fetchone()["recent"]
            print(f"[check_obi] Counts: total_rows={total}, tickers_updated_60s={recent}")
        except Exception as e:
            print(f"[ERROR] Could not read OBI counts: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect recent OBI rows.")
    parser.add_argument(
        "--count",
        action="store_true",
        help="Print total market_obi rows and tickers updated in the last 60 seconds.",
    )
    args = parser.parse_args()
    check_obi_data(show_count=args.count)
