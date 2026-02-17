#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def audit_days(training_dir: str, ts_col: str = "timestamp"):
    import pyarrow.dataset as ds

    root = Path(training_dir)
    if not root.exists():
        raise FileNotFoundError(f"training_dir not found: {training_dir}")

    dset = ds.dataset(training_dir, format="parquet", partitioning="hive")

    # Auto-detect timestamp column if needed
    if ts_col not in dset.schema.names:
        candidates = ["timestamp", "ts", "time", "datetime", "event_time"]
        found = next((c for c in candidates if c in dset.schema.names), None)
        if not found:
            raise ValueError(
                f"Timestamp column '{ts_col}' not found and no candidates matched. "
                f"Schema columns include (first 60): {dset.schema.names[:60]}"
            )
        ts_col = found
        print(f"[info] Using detected timestamp column: {ts_col}")

    scanner = dset.scanner(columns=[ts_col])

    # Aggregate incrementally to avoid loading full dataset
    agg: dict[str, dict[str, object]] = {}  # day -> {count,min,max}
    overall_min = None
    overall_max = None

    for batch in scanner.to_batches():
        s = batch.column(0).to_pandas()
        s = pd.to_datetime(s, utc=True, errors="coerce").dropna()
        if s.empty:
            continue

        bmin = s.min()
        bmax = s.max()
        overall_min = bmin if overall_min is None else min(overall_min, bmin)
        overall_max = bmax if overall_max is None else max(overall_max, bmax)

        dfb = pd.DataFrame({"ts": s})
        dfb["day"] = dfb["ts"].dt.strftime("%Y-%m-%d")  # UTC day bucket
        g = dfb.groupby("day")["ts"].agg(["count", "min", "max"])

        for day, row in g.iterrows():
            if day not in agg:
                agg[day] = {"count": int(row["count"]), "min": row["min"], "max": row["max"]}
            else:
                agg[day]["count"] += int(row["count"])
                agg[day]["min"] = min(agg[day]["min"], row["min"])
                agg[day]["max"] = max(agg[day]["max"], row["max"])

    out = pd.DataFrame([{"day": d, **vals} for d, vals in sorted(agg.items())])

    if out.empty:
        print("[warn] No valid timestamps found.")
        return out

    print("\n=== DAYS PRESENT (UTC) ===")
    print(out[["day", "count"]].to_string(index=False))

    print("\n=== MIN/MAX PER DAY (UTC) ===")
    print(out[["day", "min", "max", "count"]].to_string(index=False))

    print("\n=== OVERALL RANGE ===")
    print("min_utc:", overall_min)
    print("max_utc:", overall_max)
    try:
        print("max_chicago:", overall_max.tz_convert("America/Chicago"))
    except Exception:
        pass

    return out


def main():
    ap = argparse.ArgumentParser(description="Audit training parquet: dates present + row counts per date (UTC).")
    ap.add_argument("--training-dir", required=True, help="Root of hive-partitioned parquet training data.")
    ap.add_argument("--ts-col", default="timestamp", help="Timestamp column name (default: timestamp).")
    args = ap.parse_args()

    audit_days(args.training_dir, ts_col=args.ts_col)


if __name__ == "__main__":
    main()

