\
"""
Project_K ML data utilities.

Design goals:
- Robustness to sparse / irregular sampling.
- Time-based bars (default 30s) so "steps" have consistent meaning.
- Segmenting on gaps so TFT does not learn false continuity across long idle periods.
- Explicit schema validation to detect writer drift early.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

try:
    import pyarrow.dataset as ds
except Exception:  # pragma: no cover
    ds = None


REQUIRED_COLUMNS = {
    "ticker",
    "timestamp",
    "bid",
    "ask",
    "spread",
    "volume",
    "bid_count",
    "ask_count",
    "obi",
    "spread_velocity",
    "momentum_5",
    "momentum_10",
    "momentum_20",
    # metadata written by recorder (may be "unknown"/default)
    "series_ticker",
    "status",
    "category",
}


@dataclass(frozen=True)
class BarSpec:
    freq: str = "30s"
    horizon_steps: int = 20        # 20 * 30s = 10 minutes
    encoder_length: int = 120      # 120 * 30s = 60 minutes
    # gap threshold to start a new segment (>= horizon scale)
    max_gap: str = "5min"


def _coerce_date(d: Optional[str | date]) -> Optional[date]:
    if d is None:
        return None
    if isinstance(d, date):
        return d
    return datetime.strptime(str(d), "%Y-%m-%d").date()


def load_training_parquet(
    training_dir: str | Path,
    *,
    start_date: Optional[str | date] = None,
    end_date: Optional[str | date] = None,
    columns: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """
    Load partitioned parquet dataset written by AsyncMarketRecorder.

    The dataset is hive-partitioned by:
      - date=YYYY-MM-DD
      - category=<...>

    We use pyarrow.dataset when available for efficient partition pruning.
    """
    training_dir = Path(training_dir).expanduser().resolve()
    if not training_dir.exists():
        raise FileNotFoundError(f"Training dir not found: {training_dir}")

    start = _coerce_date(start_date)
    end = _coerce_date(end_date)
    cols = list(columns) if columns else None

    if ds is None:
        # Fallback: pandas read_parquet (may be slower for large datasets)
        df = pd.read_parquet(training_dir, columns=cols)
        return df

    dataset = ds.dataset(training_dir, format="parquet", partitioning="hive")
    # Partition field "date" comes from hive dirs like date=YYYY-MM-DD and is typically a STRING.
    # Compare ISO strings for safe filtering.
    filt = None
    if start is not None:
        start_s = start.isoformat()
        filt = ds.field("date") >= start_s
    if end is not None:
        end_s = end.isoformat()
        end_expr = ds.field("date") <= end_s
        filt = end_expr if filt is None else (filt & end_expr)

    table = dataset.to_table(columns=cols, filter=filt)
    df = table.to_pandas()
    return df


def validate_schema(df: pd.DataFrame, *, strict: bool = False) -> Tuple[bool, Sequence[str]]:
    """
    Validate that the dataframe contains the columns we expect.
    If strict=False, we warn on missing optional metadata columns but require core numeric columns.
    """
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if not missing:
        return True, []

    core = [
        "ticker", "timestamp", "bid", "ask", "spread",
        "volume", "bid_count", "ask_count", "obi",
        "spread_velocity", "momentum_5", "momentum_10", "momentum_20",
    ]
    core_missing = [c for c in core if c not in df.columns]
    if core_missing:
        if strict:
            return False, core_missing
        return False, core_missing

    # Only metadata missing (category/series/status) -> acceptable for v1
    return True, missing


def _ensure_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if not np.issubdtype(out["timestamp"].dtype, np.datetime64):
        out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
    return out


def _compute_mid(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["mid"] = (out["bid"].astype("float64") +
                  out["ask"].astype("float64")) / 2.0
    return out


def _add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    ts = out["timestamp"]
    # Local time features (consistent for local-first training). If you move to cloud later, revisit TZ.
    out["tod_seconds"] = (ts.dt.hour * 3600 + ts.dt.minute *
                          60 + ts.dt.second).astype("int32")
    out["tod_sin"] = np.sin(2 * np.pi * out["tod_seconds"] / 86400.0)
    out["tod_cos"] = np.cos(2 * np.pi * out["tod_seconds"] / 86400.0)
    out["dow"] = ts.dt.dayofweek.astype("int16")
    return out


def build_bars(
    raw: pd.DataFrame,
    *,
    spec: BarSpec,
    min_bars_per_segment: int = 200,
) -> pd.DataFrame:
    """
    Convert raw snapshots into fixed-time bars per ticker, then segment on gaps.

    Output columns include:
      - series_id (ticker + segment_id)
      - segment_id
      - time_idx (0..n-1 within series_id)
      - timestamp (bar timestamp, aligned to spec.freq)
      - mid + existing features (last observation within bar)
    """
    if raw.empty:
        raise ValueError("No rows in training data after filtering.")

    df = raw.copy()
    df = _ensure_timestamp(df)

    # Drop rows with invalid timestamps
    df = df.dropna(subset=["timestamp"])

    # Ensure expected metadata exists (fill defaults if absent)
    for col, default in [("category", "MISC"), ("series_ticker", "unknown"), ("status", "active")]:
        if col not in df.columns:
            df[col] = default
        df[col] = df[col].fillna(default)

    # Drop obviously invalid books
    df = df[(df["bid"] > 0) & (df["ask"] > 0)]

    # Sort for stable resampling
    df = df.sort_values(["ticker", "timestamp"])

    # Resample per ticker
    numeric_last_cols = [
        "bid", "ask", "spread", "volume", "bid_count", "ask_count",
        "obi", "spread_velocity", "momentum_5", "momentum_10", "momentum_20",
    ]
    meta_cols = ["category", "series_ticker", "status"]
    keep_cols = ["ticker", "timestamp"] + meta_cols + numeric_last_cols
    df = df[keep_cols]

    bars = []
    freq = spec.freq
    gap_td = pd.Timedelta(spec.max_gap)

    for ticker, g in df.groupby("ticker", sort=False):
        g = g.set_index("timestamp")

        # last observation within each bar
        agg = {c: "last" for c in numeric_last_cols}
        # metadata: last known
        for c in meta_cols:
            agg[c] = "last"

        gr = g.resample(freq).agg(agg)

        # Keep bars where we have at least bid/ask
        gr = gr.dropna(subset=["bid", "ask"])

        if gr.empty:
            continue

        gr = gr.reset_index()
        gr["ticker"] = ticker

        # Segment on gaps in bar timestamps
        dt = gr["timestamp"].diff()
        new_seg = (dt.isna()) | (dt > gap_td)
        gr["segment_id"] = new_seg.cumsum().astype("int32") - 1  # start at 0

        # Build series_id and time_idx
        gr["series_id"] = gr["ticker"].astype(
            str) + ":" + gr["segment_id"].astype(str)

        # time_idx within segment
        gr["time_idx"] = gr.groupby("series_id").cumcount().astype("int32")

        # Compute mid after resampling (more stable than snapshot-based)
        gr = _compute_mid(gr)

        # Light transforms
        gr["volume_log1p"] = np.log1p(gr["volume"].astype("float64"))
        gr["bid_count_log1p"] = np.log1p(gr["bid_count"].astype("float64"))
        gr["ask_count_log1p"] = np.log1p(gr["ask_count"].astype("float64"))

        bars.append(gr)

    if not bars:
        raise ValueError(
            "No valid bars produced; check if bid/ask are mostly zero or timestamps invalid.")

    out = pd.concat(bars, ignore_index=True)

    # Drop tiny segments (noise / dead markets)
    counts = out.groupby("series_id")["time_idx"].max() + 1
    keep_series = counts[counts >= min_bars_per_segment].index
    out = out[out["series_id"].isin(keep_series)].copy()

    if out.empty:
        raise ValueError(
            f"All segments filtered out (<{min_bars_per_segment} bars). "
            "Collect more data or lower --min-bars-per-segment."
        )

    out = _add_time_features(out)

    # Final sort
    out = out.sort_values(["series_id", "time_idx"]).reset_index(drop=True)
    return out


def add_eval_labels(df: pd.DataFrame, *, horizon_steps: int) -> pd.DataFrame:
    """
    Add realized labels for evaluation (not used as model targets).

    For each row at decision time t:
      - mid_fwd: mid at t+h
      - dmid: mid_fwd - mid
      - pnl_long_proxy: (bid_fwd - ask_now)
      - pnl_short_proxy: (bid_now - ask_fwd)
    """
    out = df.copy()
    h = int(horizon_steps)

    out["mid_fwd_h"] = out.groupby("series_id")["mid"].shift(-h)
    out["dmid_h"] = out["mid_fwd_h"] - out["mid"]

    out["bid_fwd_h"] = out.groupby("series_id")["bid"].shift(-h)
    out["ask_fwd_h"] = out.groupby("series_id")["ask"].shift(-h)

    out["pnl_long_h"] = out["bid_fwd_h"] - out["ask"]
    out["pnl_short_h"] = out["bid"] - out["ask_fwd_h"]

    return out
