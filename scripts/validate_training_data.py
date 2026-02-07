\
"""
Validate Project_K training parquet dataset.

Usage:
  python -m scripts.validate_training_data --training-dir "/path/to/training/data" --start-date 2026-02-01 --end-date 2026-02-06
"""
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

from services.ml.data import load_training_parquet, validate_schema


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--training-dir", required=True)
    ap.add_argument("--start-date", default=None, help="YYYY-MM-DD (optional)")
    ap.add_argument("--end-date", default=None, help="YYYY-MM-DD (optional)")
    ap.add_argument("--strict", action="store_true",
                    help="Fail if any required columns missing")
    ap.add_argument("--sample-rows", type=int, default=5)
    args = ap.parse_args()

    df = load_training_parquet(
        args.training_dir,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    ok, missing = validate_schema(df, strict=args.strict)
    if not ok:
        print("❌ Schema validation FAILED. Missing core columns:", missing)
        print("Columns present:", sorted(df.columns))
        return 2

    if missing:
        print("⚠️  Schema validation OK (core columns present), but metadata columns missing:", missing)
    else:
        print("✅ Schema validation OK.")

    # Basic diagnostics
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    ts_min = df["timestamp"].min()
    ts_max = df["timestamp"].max()

    print(f"Rows: {len(df):,}")
    print(
        f"Tickers: {df['ticker'].nunique() if 'ticker' in df.columns else 'n/a'}")
    print(f"Timestamp range: {ts_min} -> {ts_max}")

    # Metadata quality
    for c in ["category", "series_ticker", "status"]:
        if c in df.columns:
            vc = df[c].fillna("null").value_counts().head(10)
            print(f"\nTop {c} values:")
            print(vc.to_string())

    # Null counts for core columns
    core = ["bid", "ask", "spread", "volume", "bid_count", "ask_count", "obi"]
    present = [c for c in core if c in df.columns]
    if present:
        print("\nNull counts (core):")
        print(df[present].isna().sum().to_string())

    print("\nSample rows:")
    print(df.head(args.sample_rows).to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
