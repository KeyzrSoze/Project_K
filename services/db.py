import sqlite3
import os
from typing import List, Dict, Any, Tuple
from contextlib import contextmanager
DB_PATH = "data/market_master.db"


class DatabaseManager:
    def __init__(self):
        self._ensure_db_exists()

    def _ensure_db_exists(self):
        """Creates the schema if it doesn't exist."""
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 1. Series Registry: Metadata about the event series
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS series_registry (
                    series_ticker TEXT PRIMARY KEY,
                    title TEXT,
                    category TEXT,
                    frequency TEXT
                )
            """)
            # 2. Market Registry: Static details for specific contracts
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS market_registry (
                    ticker TEXT PRIMARY KEY,
                    series_ticker TEXT,
                    expiration_time TEXT,
                    FOREIGN KEY(series_ticker) REFERENCES series_registry(series_ticker)
                )
            """)
            # 3. Market Snapshots: High-frequency data (The "Hot" Table)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS market_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT,
                    timestamp REAL,
                    bid INTEGER,
                    ask INTEGER,
                    spread INTEGER,
                    volume INTEGER,
                    open_interest INTEGER,
                    status TEXT,
                    FOREIGN KEY(ticker) REFERENCES market_registry(ticker)
                )
            """)

            # Indexes for performance
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_snapshot_time ON market_snapshots(timestamp)")
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_snapshot_vol ON market_snapshots(volume)")
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_snapshot_ticker ON market_snapshots(ticker)")

            conn.commit()

    @contextmanager
    def get_connection(self):
        """Yields a connection context."""
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row  # Access columns by name
        try:
            yield conn
        finally:
            conn.close()

    def bulk_upsert_markets(self, market_data: List[Tuple]):
        """
        Efficiently inserts market snapshot data.
        market_data expected format: (ticker, timestamp, bid, ask, spread, vol, oi, status)
        """
        if not market_data:
            return

        with self.get_connection() as conn:
            conn.executemany("""
                INSERT INTO market_snapshots 
                (ticker, timestamp, bid, ask, spread, volume, open_interest, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, market_data)
            conn.commit()

    def upsert_metadata(self, series_data: List[Tuple], market_reg_data: List[Tuple]):
        """
        Updates the static registries (Series and Market).
        """
        with self.get_connection() as conn:
            # Upsert Series
            if series_data:
                conn.executemany("""
                    INSERT OR IGNORE INTO series_registry (series_ticker, title, category, frequency)
                    VALUES (?, ?, ?, ?)
                """, series_data)

            # Upsert Market Registry
            if market_reg_data:
                conn.executemany("""
                    INSERT OR IGNORE INTO market_registry (ticker, series_ticker, expiration_time)
                    VALUES (?, ?, ?)
                """, market_reg_data)
            conn.commit()

    def get_top_active_tickers(self, limit=10) -> List[str]:
        """
        Returns active CRYPTO, ECON, and MISC tickers with highest volume.
        Strictly filters out ESPORTS.
        """
        query = """
            SELECT s.ticker 
            FROM market_snapshots s
            JOIN market_registry m ON s.ticker = m.ticker
            JOIN series_registry sr ON m.series_ticker = sr.series_ticker
            WHERE sr.category IN ('CRYPTO', 'ECON', 'MISC') 
              AND s.status = 'active'
              AND s.timestamp > (strftime('%s', 'now') - 3600)
            GROUP BY s.ticker
            ORDER BY s.volume DESC
            LIMIT ?
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            rows = cursor.execute(query, (limit,)).fetchall()
            return [row['ticker'] for row in rows]
