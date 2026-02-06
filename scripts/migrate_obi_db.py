import argparse
import os
import sqlite3
from pathlib import Path
from typing import List, Tuple

from services.db import DatabaseManager, DB_PATH, OBI_DB_PATH


def _resolve(p: str) -> Path:
    return Path(os.path.expanduser(p)).resolve(strict=False)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create (or migrate) a dedicated OBI DB from the primary DB."
    )
    parser.add_argument(
        "--source",
        default=str(DB_PATH),
        help="Primary DB path (default: PROJECT_K_DB_PATH / services.db:DB_PATH).",
    )
    parser.add_argument(
        "--dest",
        default=str(OBI_DB_PATH),
        help="OBI DB path (default: PROJECT_K_OBI_DB_PATH / services.db:OBI_DB_PATH).",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Create schema only; do not copy existing market_obi rows.",
    )
    parser.add_argument(
        "--wipe",
        action="store_true",
        help="Delete the destination DB file first (use with care).",
    )
    args = parser.parse_args()

    source_path = _resolve(args.source)
    dest_path = _resolve(args.dest)

    print(f"[migrate_obi_db] source={source_path}")
    print(f"[migrate_obi_db] dest={dest_path}")

    if source_path == dest_path:
        print("[migrate_obi_db] source == dest; nothing to do.")
        return 0

    if args.wipe and dest_path.exists():
        dest_path.unlink()
        for sidecar in (f"{dest_path}-wal", f"{dest_path}-shm"):
            try:
                Path(sidecar).unlink()
            except FileNotFoundError:
                pass

    dest_db = DatabaseManager(db_path=dest_path, schema="obi")
    if args.fresh:
        print("[migrate_obi_db] Created OBI schema (fresh).")
        return 0

    source_db = DatabaseManager(db_path=source_path, schema="primary")
    try:
        with source_db.get_connection(role="read") as conn:
            rows = conn.execute(
                "SELECT ticker, timestamp, bid_count, ask_count, best_bid, best_ask FROM market_obi"
            ).fetchall()
    except sqlite3.OperationalError as e:
        print(f"[migrate_obi_db] Failed to read from source market_obi: {e!r}")
        return 1

    obi_rows: List[Tuple] = []
    for row in rows:
        obi_rows.append(
            (
                row["ticker"],
                float(row["timestamp"]) if row["timestamp"] is not None else 0.0,
                int(row["bid_count"] or 0),
                int(row["ask_count"] or 0),
                row["best_bid"],
                row["best_ask"],
            )
        )

    if not obi_rows:
        print("[migrate_obi_db] Source market_obi is empty; destination schema is ready.")
        return 0

    ok = dest_db.bulk_upsert_obi(obi_rows)
    if not ok:
        print("[migrate_obi_db] Copy failed (DB locked/busy). Try again with writers stopped.")
        return 2

    print(f"[migrate_obi_db] Copied {len(obi_rows)} rows into dest market_obi.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

