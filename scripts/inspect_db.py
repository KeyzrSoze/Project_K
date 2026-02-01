from services.db import DatabaseManager
import sys
import os
import sqlite3
import time

# -----------------------------------------------------------------------------
# 1. PATH FIX (Must come BEFORE importing from services)
# -----------------------------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# -----------------------------------------------------------------------------
# 2. IMPORTS (Now safe to import from project modules)
# -----------------------------------------------------------------------------

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


def health_check(db_manager):
    """Prints the total row count for each major table."""
    print("\n--- Database Health Check ---")
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()

            tables = ['series_registry', 'market_registry', 'market_snapshots']
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    print(f"[*] Rows in '{table}': {count}")
                except sqlite3.OperationalError:
                    print(f"[!] Table '{table}' not found.")

    except sqlite3.Error as e:
        print(f"[!] Health check failed due to a database error: {e}")


def verify_data_freshness(db_manager):
    """Checks the timestamp of the most recent snapshot."""
    print("\n--- Data Freshness Verification ---")
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(timestamp) FROM market_snapshots")
            result = cursor.fetchone()

            if result and result[0] is not None:
                last_ts = result[0]
                seconds_ago = time.time() - last_ts
                print(
                    f"[*] Most recent snapshot was {seconds_ago:.2f} seconds ago.")

                if seconds_ago > 120:
                    print(
                        f"[!] WARNING: Data is stale! Last update was more than 2 minutes ago.")
            else:
                print("[!] No snapshots found in the database.")

    except sqlite3.Error as e:
        print(f"[!] Freshness check failed due to a database error: {e}")


def show_top_targets(db_manager):
    """Prints the output of the 'get_top_active_tickers' method."""
    print("\n--- Top Target Candidates (Strict Filter) ---")
    try:
        # This calls the method in db.py which strictly filters for CRYPTO/ECON
        top_tickers = db_manager.get_top_active_tickers(limit=5)
        if top_tickers:
            print("[*] The Smart Feed found these high-priority targets:")
            for ticker in top_tickers:
                print(f"    - {ticker}")
        else:
            print("[!] 'get_top_active_tickers' returned no candidates.")
            print(
                "    (This is normal if the Harvester is still scanning through 'MISC' markets)")

    except Exception as e:
        print(f"[!] Failed to get top active tickers: {e}")


def sample_data_dump(db_manager):
    """Fetches and displays the last 5 rows from the snapshots table."""
    print("\n--- Sample Data Dump (Last 5 Snapshots) ---")
    try:
        with db_manager.get_connection() as conn:
            query = "SELECT * FROM market_snapshots ORDER BY id DESC LIMIT 5"

            if PANDAS_AVAILABLE:
                df = pd.read_sql_query(query, conn)
                print(df.to_string())
            else:
                print("[!] Pandas not found. Printing raw rows:")
                cursor = conn.cursor()
                cursor.execute(query)
                rows = cursor.fetchall()
                # Get column names for header
                col_names = [description[0]
                             for description in cursor.description]
                print(col_names)
                for row in rows:
                    print(dict(row))

    except (sqlite3.Error, Exception) as e:
        print(f"[!] Data dump failed: {e}")


if __name__ == "__main__":
    print("Initializing Database Inspector...")

    # Initialize without arguments because db.py handles the path internally
    db = DatabaseManager()

    health_check(db)
    verify_data_freshness(db)
    show_top_targets(db)
    sample_data_dump(db)

    print("\nInspection complete.")
