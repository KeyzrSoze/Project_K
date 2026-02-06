import atexit
import os
import random
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Sequence, Tuple, Callable, Optional

from utils.logger import logger

def _env_flag(name: str) -> bool:
    value = (os.getenv(name) or "").strip().lower()
    return value in ("1", "true", "yes", "y", "on")


def _resolve_path(value: str, *, base: Optional[Path] = None) -> Path:
    p = Path(os.path.expanduser(value))
    if not p.is_absolute():
        p = (base or Path.cwd()) / p
    return p.resolve(strict=False)


def _default_repo_root() -> Path:
    # Canonical "production" location (internal SSD / APFS).
    return (Path.home() / "ev" / "Project_K").resolve(strict=False)


def resolve_repo_root() -> Path:
    env_root = os.getenv("PROJECT_K_ROOT")
    if env_root:
        return _resolve_path(env_root)
    return _default_repo_root()


REPO_ROOT = resolve_repo_root()

ENV_DATA_DIR = os.getenv("PROJECT_K_DATA_DIR")
if ENV_DATA_DIR:
    DATA_DIR = _resolve_path(ENV_DATA_DIR, base=REPO_ROOT)
else:
    DATA_DIR = (REPO_ROOT / "data").resolve(strict=False)

ENV_DB_PATH = os.getenv("PROJECT_K_DB_PATH")
if ENV_DB_PATH:
    DB_PATH = _resolve_path(ENV_DB_PATH, base=REPO_ROOT)
else:
    DB_PATH = (DATA_DIR / "kalshi.db").resolve(strict=False)

ENV_OBI_DB_PATH = os.getenv("PROJECT_K_OBI_DB_PATH")
if ENV_OBI_DB_PATH:
    OBI_DB_PATH = _resolve_path(ENV_OBI_DB_PATH, base=REPO_ROOT)
else:
    OBI_DB_PATH = DB_PATH

ENV_TRAINING_DIR = os.getenv("PROJECT_K_TRAINING_DIR")
if ENV_TRAINING_DIR:
    TRAINING_DIR = _resolve_path(ENV_TRAINING_DIR, base=REPO_ROOT)
else:
    # Large append-only parquet output goes on the external volume by default.
    TRAINING_DIR = Path("/Volumes/external/ev/Project_K/data/training").resolve(strict=False)

ALLOW_EXTERNAL_DB = _env_flag("ALLOW_EXTERNAL_DB")
TRAINING_FALLBACK_LOCAL = _env_flag("TRAINING_FALLBACK_LOCAL")


def _is_under(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except Exception:
        return False


def validate_db_path_or_raise(db_path: Path, *, label: str) -> None:
    db_path = Path(db_path).resolve(strict=False)

    home = Path.home().resolve(strict=False)
    if not _is_under(db_path, home):
        if ALLOW_EXTERNAL_DB:
            logger.log_warn(
                f"[DB] ALLOW_EXTERNAL_DB=1 set; using external {label} path: {db_path}"
            )
            return
        logger.log_error(f"[DB] Refusing to use external {label} path: {db_path}")
        raise RuntimeError(
            f"Refusing to use external {label} path: {db_path}. "
            "Set PROJECT_K_DB_PATH / PROJECT_K_OBI_DB_PATH to a path under your home directory, "
            "or set ALLOW_EXTERNAL_DB=1 to override."
        )

    if str(db_path).startswith("/Volumes/"):
        if ALLOW_EXTERNAL_DB:
            logger.log_warn(
                f"[DB] ALLOW_EXTERNAL_DB=1 set; using {label} under /Volumes: {db_path}"
            )
            return
        logger.log_error(f"[DB] Refusing to use {label} under /Volumes: {db_path}")
        raise RuntimeError(
            f"Refusing to use {label} under /Volumes: {db_path}. "
            "Move the SQLite DB to local disk (e.g., ~/ev/Project_K/data/kalshi.db) "
            "or set ALLOW_EXTERNAL_DB=1 to override."
        )


def ensure_training_dir() -> Path:
    training_dir = Path(TRAINING_DIR)
    external_root = Path("/Volumes/external")

    if _is_under(training_dir, external_root) and not external_root.exists():
        if TRAINING_FALLBACK_LOCAL:
            fallback = (REPO_ROOT / "data" / "training_staging").resolve(strict=False)
            fallback.mkdir(parents=True, exist_ok=True)
            logger.log_warn(
                f"[Training] External volume not mounted. Falling back to local staging: {fallback} "
                "(set TRAINING_FALLBACK_LOCAL=0 to fail fast)."
            )
            return fallback
        raise RuntimeError(
            "Training directory is on /Volumes/external but the volume is not mounted. "
            "Mount /Volumes/external or set TRAINING_FALLBACK_LOCAL=1 to write to "
            "~/ev/Project_K/data/training_staging."
        )

    try:
        training_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        if TRAINING_FALLBACK_LOCAL:
            fallback = (REPO_ROOT / "data" / "training_staging").resolve(strict=False)
            fallback.mkdir(parents=True, exist_ok=True)
            logger.log_warn(
                f"[Training] Failed to create training dir ({training_dir}): {e}. "
                f"Falling back to local staging: {fallback}"
            )
            return fallback
        raise RuntimeError(
            f"Failed to create training directory: {training_dir}: {e}"
        ) from e

    return training_dir


# Validate configuration at import time so all entrypoints agree early.
validate_db_path_or_raise(DB_PATH, label="primary DB")
if Path(OBI_DB_PATH) != Path(DB_PATH):
    validate_db_path_or_raise(OBI_DB_PATH, label="OBI DB")

_LOCKED_SNIPPETS = (
    "database is locked",
    "database table is locked",
    "database schema is locked",
)


@dataclass
class _ConnEntry:
    conn: sqlite3.Connection
    version: int
    db_path: Path
    role: str
    closed: bool = False


def _iter_chunks(rows: Sequence[Tuple], chunk_size: int) -> Iterator[Sequence[Tuple]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    for i in range(0, len(rows), chunk_size):
        yield rows[i : i + chunk_size]


class DatabaseManager:
    _tls = threading.local()
    _global_lock = threading.Lock()
    _global_entries: Dict[Tuple[int, str, str], _ConnEntry] = {}
    _cache_version: int = 0
    _atexit_registered: bool = False

    def __init__(self, db_path: Optional[Path] = None, *, schema: str = "primary"):
        self.db_path = Path(db_path or DB_PATH).resolve(strict=False)
        self.schema = schema

        self._ensure_db_exists()
        self._register_atexit_once()

    @classmethod
    def _register_atexit_once(cls) -> None:
        if cls._atexit_registered:
            return
        cls._atexit_registered = True
        atexit.register(cls.close_all_cached_connections)

    @classmethod
    def reset_connection_cache(cls) -> None:
        with cls._global_lock:
            cls._cache_version += 1
        cls.close_all_cached_connections()

    @classmethod
    def close_all_cached_connections(cls) -> None:
        with cls._global_lock:
            entries = list(cls._global_entries.values())
            cls._global_entries.clear()

        for entry in entries:
            cls._close_entry(entry)

    @classmethod
    def _close_entry(cls, entry: _ConnEntry) -> None:
        if entry.closed:
            return
        entry.closed = True
        try:
            entry.conn.close()
        except Exception:
            pass

    def _normalize_role(self, role: str) -> str:
        r = (role or "").strip().lower()
        if r in ("w", "write", "writer"):
            return "write"
        if r in ("r", "read", "reader"):
            return "read"
        if r in ("h", "health"):
            return "health"
        raise ValueError(f"Unknown DB role: {role!r}")

    def _connection_params(self, role: str) -> Tuple[float, int, bool]:
        role = self._normalize_role(role)
        if role == "write":
            return 30.0, 30000, False
        if role == "read":
            return 3.0, 2000, True
        return 2.0, 1000, True

    def _open_connection(self, role: str) -> sqlite3.Connection:
        connect_timeout_s, busy_timeout_ms, query_only = self._connection_params(role)
        conn = sqlite3.connect(
            str(self.db_path),
            timeout=connect_timeout_s,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row

        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)};")
        conn.execute(f"PRAGMA query_only={1 if query_only else 0};")
        return conn

    def _get_thread_cache(self) -> Dict[Tuple[str, str], _ConnEntry]:
        cache = getattr(self._tls, "cache", None)
        if cache is None:
            cache = {}
            setattr(self._tls, "cache", cache)
        return cache

    def _get_cached_connection(self, role: str) -> sqlite3.Connection:
        role = self._normalize_role(role)
        cache = self._get_thread_cache()
        key = (str(self.db_path), role)

        entry = cache.get(key)
        if entry and (not entry.closed) and entry.version == self._cache_version:
            return entry.conn

        if entry:
            self._close_entry(entry)

        conn = self._open_connection(role)
        entry = _ConnEntry(
            conn=conn,
            version=self._cache_version,
            db_path=self.db_path,
            role=role,
            closed=False,
        )
        cache[key] = entry

        with self._global_lock:
            gkey = (threading.get_ident(), str(self.db_path), role)
            old = self._global_entries.get(gkey)
            if old and old is not entry:
                self._close_entry(old)
            self._global_entries[gkey] = entry

        return conn

    def _is_locked_error(self, exc: Exception) -> bool:
        msg = str(exc).lower()
        return any(snippet in msg for snippet in _LOCKED_SNIPPETS)

    def _run_with_retry(
        self,
        op: Callable[[], None],
        *,
        total_timeout_s: float = 5.0,
        base_delay_s: float = 0.1,
        max_delay_s: float = 1.0,
    ) -> bool:
        start = time.time()
        delay = base_delay_s

        while True:
            try:
                op()
                return True
            except sqlite3.OperationalError as e:
                if not self._is_locked_error(e):
                    raise

                elapsed = time.time() - start
                if elapsed >= total_timeout_s:
                    logger.log_error(
                        f"[DB] database is locked for {elapsed:.1f}s (db={self.db_path}). Giving up."
                    )
                    return False

                # Exponential backoff + small jitter to reduce synchronized retries.
                sleep_for = min(delay, max(0.0, total_timeout_s - elapsed))
                sleep_for += random.uniform(0.0, min(0.2, sleep_for * 0.2))
                time.sleep(max(0.01, sleep_for))
                delay = min(max_delay_s, delay * 2.0)

    def _ensure_db_exists(self):
        """Creates the schema if it doesn't exist."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        def _op() -> None:
            with self.get_connection(role="write") as conn:
                cursor = conn.cursor()

                if self.schema not in ("primary", "obi"):
                    raise ValueError(f"Unknown DB schema: {self.schema!r}")

                if self.schema == "primary":
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
                conn.commit()

        self._run_with_retry(_op, total_timeout_s=5.0)

    @contextmanager
    def get_connection(self, *, role: str = "write"):
        conn = self._get_cached_connection(role)
        try:
            yield conn
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise

    def bulk_upsert_metrics(self, market_data: List[Tuple]) -> bool:
        """Upserts market metrics data from the Discovery Loop."""
        if not market_data:
            return False

        chunk_size = 250
        def _op() -> None:
            with self.get_connection(role="write") as conn:
                for chunk in _iter_chunks(market_data, chunk_size):
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
                    """, chunk)
                    conn.commit()

        return self._run_with_retry(_op, total_timeout_s=5.0)

    def bulk_upsert_obi(self, obi_data: List[Tuple]) -> bool:
        """Upserts order book imbalance data from the Tracking Loop."""
        if not obi_data:
            return False

        chunk_size = 500
        def _op() -> None:
            with self.get_connection(role="write") as conn:
                for chunk in _iter_chunks(obi_data, chunk_size):
                    conn.executemany("""
                        INSERT INTO market_obi (ticker, timestamp, bid_count, ask_count, best_bid, best_ask)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(ticker) DO UPDATE SET
                            timestamp=excluded.timestamp,
                            bid_count=excluded.bid_count,
                            ask_count=excluded.ask_count,
                            best_bid=excluded.best_bid,
                            best_ask=excluded.best_ask
                    """, chunk)
                    conn.commit()

        return self._run_with_retry(_op, total_timeout_s=5.0)

    def upsert_metadata(self, series_data: List[Tuple], market_reg_data: List[Tuple]) -> bool:
        """Updates the static registries."""

        chunk_size = 500
        def _op() -> None:
            with self.get_connection(role="write") as conn:
                if series_data:
                    for chunk in _iter_chunks(series_data, chunk_size):
                        conn.executemany("""
                            INSERT OR IGNORE INTO series_registry (series_ticker, title, category, frequency)
                            VALUES (?, ?, ?, ?)
                        """, chunk)
                        conn.commit()

                if market_reg_data:
                    for chunk in _iter_chunks(market_reg_data, chunk_size):
                        conn.executemany("""
                            INSERT OR IGNORE INTO market_registry (ticker, series_ticker, expiration_time)
                            VALUES (?, ?, ?)
                        """, chunk)
                        conn.commit()

        if not series_data and not market_reg_data:
            return False
        return self._run_with_retry(_op, total_timeout_s=5.0)

    def get_top_active_tickers(self, limit=50) -> List[str]:
        """Returns top active tickers by volume, aggregated over a 5-minute window."""
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
        with self.get_connection(role="read") as conn:
            rows = conn.execute(query, (cutoff_time, limit)).fetchall()
            return [row['ticker'] for row in rows]

    def get_market_snapshot(self, ticker: str):
        """Retrieves the latest snapshot for a ticker from the broad metrics table."""
        with self.get_connection(role="read") as conn:
            row = conn.execute(
                "SELECT * FROM market_metrics WHERE ticker = ?", (ticker,)).fetchone()
            return dict(row) if row else None

    def get_market_obi(self, ticker: str):
        """Retrieves the latest OBI data for a ticker."""
        with self.get_connection(role="read") as conn:
            row = conn.execute(
                "SELECT * FROM market_obi WHERE ticker = ?", (ticker,)).fetchone()
            return dict(row) if row else None

    def get_combined_market_view(self, ticker: str):
        """Joins metrics and OBI data for a comprehensive view of a single ticker."""
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
        with self.get_connection(role="read") as conn:
            row = conn.execute(query, (ticker,)).fetchone()
            return dict(row) if row else None

    def get_rate_limit_cooldown_until(self) -> float:
        with self.get_connection(role="read") as conn:
            row = conn.execute(
                "SELECT value FROM rate_limit_state WHERE key = ?",
                ("kalshi_global_cooldown_until",)
            ).fetchone()
            return float(row["value"]) if row and row["value"] is not None else 0.0

    def set_rate_limit_cooldown_until(self, ts: float) -> None:
        with self.get_connection(role="write") as conn:
            conn.execute("""
                INSERT INTO rate_limit_state (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """, ("kalshi_global_cooldown_until", float(ts)))
            conn.commit()

    def get_market_metrics_max_info(self, *, role: str = "health") -> Tuple[float, str]:
        with self.get_connection(role=role) as conn:
            row = conn.execute(
                "SELECT MAX(timestamp) as ts, "
                "datetime(MAX(timestamp),'unixepoch','localtime') as ts_local "
                "FROM market_metrics"
            ).fetchone()
            if not row or row["ts"] is None:
                return 0.0, "n/a"
            ts = float(row["ts"])
            ts_local = row["ts_local"] or "n/a"
            return ts, ts_local
