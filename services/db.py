import sqlite3
import os
import time
from typing import List, Tuple
from contextlib import contextmanager

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(REPO_ROOT, "data", "kalshi.db")


class DatabaseManager:
    def __init__(self):
        self._ensure_db_exists()

    def _ensure_db_exists(self):
        """Creates the new two-table schema if it doesn't exist."""
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

        with self.get_connection() as conn:
            cursor = conn.cursor()

            # --- Table A: market_metrics (Broad View) ---
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS market_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    volume INTEGER,
                    open_interest INTEGER,
                    spread INTEGER,
                    best_bid INTEGER,
                    best_ask INTEGER,
                    status TEXT,
                    UNIQUE(ticker)
                )
            """)
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON market_metrics(timestamp)")
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_metrics_volume ON market_metrics(volume)")

            # --- Table B: market_obi (Deep View) ---
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS market_obi (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    bid_count INTEGER,
                    ask_count INTEGER,
                    best_bid INTEGER,
                    best_ask INTEGER,
                    UNIQUE(ticker)
                )
            """)
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_obi_ticker ON market_obi(ticker)")

            # --- Metadata Tables (Unchanged) ---
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS series_registry (
                    series_ticker TEXT PRIMARY KEY,
                    title TEXT,
                    category TEXT,
                    frequency TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS market_registry (
                    ticker TEXT PRIMARY KEY,
                    series_ticker TEXT,
                    expiration_time TEXT,
                    FOREIGN KEY(series_ticker) REFERENCES series_registry(series_ticker)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS rate_limit_state (
                    key TEXT PRIMARY KEY,
                    value REAL
                )
            """)
            conn.commit()

    @contextmanager
    def get_connection(self):
        """Yields a connection context with WAL mode enabled."""
        conn = sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=5000;")
            yield conn
        finally:
            conn.close()

    def bulk_upsert_metrics(self, market_data: List[Tuple]):
        """Upserts market metrics data from the Discovery Loop."""
        if not market_data:
            return

        with self.get_connection() as conn:
            conn.executemany("""
                INSERT INTO market_metrics (ticker, timestamp, volume, open_interest, spread, best_bid, best_ask, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ticker) DO UPDATE SET
                    timestamp=excluded.timestamp,
                    volume=excluded.volume,
                    open_interest=excluded.open_interest,
                    spread=excluded.spread,
                    best_bid=excluded.best_bid,
                    best_ask=excluded.best_ask,
                    status=excluded.status
            """, market_data)
            conn.commit()

    def bulk_upsert_obi(self, obi_data: List[Tuple]):
        """Upserts order book imbalance data from the Tracking Loop."""
        if not obi_data:
            return

        with self.get_connection() as conn:
            conn.executemany("""
                INSERT INTO market_obi (ticker, timestamp, bid_count, ask_count, best_bid, best_ask)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(ticker) DO UPDATE SET
                    timestamp=excluded.timestamp,
                    bid_count=excluded.bid_count,
                    ask_count=excluded.ask_count,
                    best_bid=excluded.best_bid,
                    best_ask=excluded.best_ask
            """, obi_data)
            conn.commit()

    def upsert_metadata(self, series_data: List[Tuple], market_reg_data: List[Tuple]):
        """Updates the static registries."""
        with self.get_connection() as conn:
            if series_data:
                conn.executemany("""
                    INSERT OR IGNORE INTO series_registry (series_ticker, title, category, frequency)
                    VALUES (?, ?, ?, ?)
                """, series_data)

            if market_reg_data:
                conn.executemany("""
                    INSERT OR IGNORE INTO market_registry (ticker, series_ticker, expiration_time)
                    VALUES (?, ?, ?)
                """, market_reg_data)
            conn.commit()

    def get_top_active_tickers(self, limit=50) -> List[str]:
        """
        Returns top active tickers by volume, aggregated over a 5-minute window.
        """
        cutoff_time = time.time() - 300  # 5-minute window

        query = """
            SELECT ticker
            FROM market_metrics
            WHERE timestamp > ? AND volume > 100
            GROUP BY ticker
            HAVING MIN(spread) BETWEEN 1 AND 50
            ORDER BY MAX(volume) DESC
            LIMIT ?
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            rows = cursor.execute(query, (cutoff_time, limit)).fetchall()
            return [row['ticker'] for row in rows]

    def get_market_snapshot(self, ticker: str):
        """Retrieves the latest snapshot for a ticker from the broad metrics table."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            row = cursor.execute(
                "SELECT * FROM market_metrics WHERE ticker = ?", (ticker,)).fetchone()
            return dict(row) if row else None

    def get_market_obi(self, ticker: str):
        """Retrieves the latest OBI data for a ticker."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            row = cursor.execute(
                "SELECT * FROM market_obi WHERE ticker = ?", (ticker,)).fetchone()
            return dict(row) if row else None

    def get_combined_market_view(self, ticker: str):
        """
        Joins metrics and OBI data for a comprehensive view of a single ticker.
        """
        query = """
            SELECT
                m.ticker,
                m.timestamp as metrics_ts,
                o.timestamp as obi_ts,
                m.volume,
                m.open_interest,
                m.spread,
                COALESCE(o.bid_count, 0) as bid_count,
                COALESCE(o.ask_count, 0) as ask_count,
                COALESCE(o.best_bid, m.best_bid) as best_bid,
                COALESCE(o.best_ask, m.best_ask) as best_ask
            FROM market_metrics m
            LEFT JOIN market_obi o ON o.ticker = m.ticker
            WHERE m.ticker = ?
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            row = cursor.execute(query, (ticker,)).fetchone()
            return dict(row) if row else None

    def get_rate_limit_cooldown_until(self) -> float:
        with self.get_connection() as conn:
            row = conn.execute(
                "SELECT value FROM rate_limit_state WHERE key = ?",
                ("kalshi_global_cooldown_until",)
            ).fetchone()
            return float(row["value"]) if row and row["value"] is not None else 0.0

    def set_rate_limit_cooldown_until(self, ts: float) -> None:
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO rate_limit_state (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """, ("kalshi_global_cooldown_until", float(ts)))
            conn.commit()
