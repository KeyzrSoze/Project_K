"""
Project_K ML data utilities.

Design goals:
- Robustness to sparse / irregular sampling.
- Time-based bars (default 30s) so "steps" have consistent meaning.
- Segmenting on gaps so TFT does not learn false continuity across long idle periods.
- Explicit schema validation to detect writer drift early.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Sequence, Tuple

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
    "series_ticker",
    "status",
    "category",
}


@dataclass(frozen=True)
class BarSpec:
    freq: str = "30s"
    horizon_steps: int = 20
    encoder_length: int = 120
    max_gap: str = "5min"


def _data_debug_enabled() -> bool:
    return os.getenv("DATA_DEBUG", "0") == "1"


def _data_debug(msg: str) -> None:
    if _data_debug_enabled():
        print(msg)


def _coerce_date(d: Optional[str | date]) -> Optional[date]:
    if d is None:
        return None
    if isinstance(d, date):
        return d
    return datetime.strptime(str(d), "%Y-%m-%d").date()


def _iter_parquet_files(root: Path) -> list[str]:
    """
    Recursively collect parquet files while ignoring hidden and AppleDouble artifacts.
    """
    root = Path(root).expanduser().resolve()
    paths: list[str] = []
    for path in root.rglob("*.parquet"):
        if not path.is_file():
            continue
        if "__MACOSX" in path.parts:
            continue
        name = path.name
        if name.startswith("._") or name.startswith("."):
            continue
        paths.append(str(path))
    paths.sort()
    return paths


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
    """
    training_dir = Path(training_dir).expanduser().resolve()
    if not training_dir.exists():
        raise FileNotFoundError(f"Training dir not found: {training_dir}")

    start = _coerce_date(start_date)
    end = _coerce_date(end_date)
    cols = list(columns) if columns else None

    if ds is None:
        return pd.read_parquet(training_dir, columns=cols)

    try:
        dataset = ds.dataset(training_dir, format="parquet", partitioning="hive")
    except OSError:
        parquet_paths = _iter_parquet_files(training_dir)
        if not parquet_paths:
            raise
        _data_debug(
            "[DATA_DEBUG] load_training_parquet fallback: "
            f"using {len(parquet_paths)} filtered parquet files"
        )
        dataset = ds.dataset(
            parquet_paths,
            format="parquet",
            partitioning="hive",
            partition_base_dir=str(training_dir),
        )
    filt = None
    if start is not None:
        start_s = start.isoformat()
        filt = ds.field("date") >= start_s
    if end is not None:
        end_s = end.isoformat()
        end_expr = ds.field("date") <= end_s
        filt = end_expr if filt is None else (filt & end_expr)

    table = dataset.to_table(columns=cols, filter=filt)
    return table.to_pandas()


def validate_schema(df: pd.DataFrame, *, strict: bool = False) -> Tuple[bool, Sequence[str]]:
    """
    Validate that the dataframe contains the columns we expect.
    """
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if not missing:
        return True, []

    core = [
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
    ]
    core_missing = [c for c in core if c not in df.columns]
    if core_missing:
        if strict:
            return False, core_missing
        return False, core_missing

    return True, missing


def _ensure_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if not np.issubdtype(out["timestamp"].dtype, np.datetime64):
        out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
    return out


def _compute_mid(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["mid"] = (out["bid"].astype("float64") + out["ask"].astype("float64")) / 2.0
    return out


def _canonicalize_quotes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Canonicalize input quote columns into a consistent YES-side book.

    Produces:
      - bid_yes, ask_yes, spread_yes, mid_yes
      - bid_no, ask_no
    and overwrites downstream columns:
      - bid, ask, spread, mid
    """
    out = df.copy()
    out["bid"] = pd.to_numeric(out["bid"], errors="coerce")
    out["ask"] = pd.to_numeric(out["ask"], errors="coerce")
    out["spread"] = pd.to_numeric(out["spread"], errors="coerce")

    sample = out[["bid", "ask", "spread"]].dropna()
    if len(sample) > 200_000:
        sample = sample.sample(n=200_000, random_state=42)

    if sample.empty:
        median_sum = float("nan")
        median_diff = float("nan")
        median_spread = float("nan")
        infer_mode_2 = False
    else:
        median_sum = float((sample["bid"] + sample["ask"]).median())
        median_diff = float((sample["ask"] - sample["bid"]).median())
        median_spread = float(sample["spread"].median())
        infer_mode_2 = (
            (99.0 <= median_sum <= 101.0)
            and (median_diff > 40.0)
            and (median_spread < 20.0)
        )

    mode = "mode2_yes_bid_no_bid" if infer_mode_2 else "mode1_yes_bid_yes_ask"
    _data_debug(
        "[DATA_DEBUG] canonicalize_quotes "
        f"mode={mode} median_bid_plus_ask={median_sum:.4f} "
        f"median_ask_minus_bid={median_diff:.4f} median_spread={median_spread:.4f}"
    )

    if infer_mode_2:
        out["bid_yes"] = out["bid"]
        out["bid_no"] = out["ask"]
        out["ask_yes"] = 100.0 - out["bid_no"]
        out["ask_no"] = 100.0 - out["bid_yes"]
    else:
        out["bid_yes"] = out["bid"]
        out["ask_yes"] = out["ask"]
        out["bid_no"] = 100.0 - out["ask_yes"]
        out["ask_no"] = 100.0 - out["bid_yes"]

    out["spread_yes"] = out["ask_yes"] - out["bid_yes"]
    out["mid_yes"] = (out["bid_yes"] + out["ask_yes"]) / 2.0

    out["bid"] = out["bid_yes"]
    out["ask"] = out["ask_yes"]
    out["spread"] = out["spread_yes"]
    out["mid"] = out["mid_yes"]

    valid = (
        out["bid"].notna()
        & out["ask"].notna()
        & out["bid"].between(0.0, 100.0)
        & out["ask"].between(0.0, 100.0)
        & (out["ask"] >= out["bid"])
    )
    dropped = int((~valid).sum())
    if dropped > 0:
        _data_debug(
            f"[DATA_DEBUG] canonicalize_quotes dropped_rows={dropped} invalid_yes_book_rows"
        )
    out = out.loc[valid].copy()
    return out


def _add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    ts = out["timestamp"]
    out["tod_seconds"] = (ts.dt.hour * 3600 + ts.dt.minute * 60 + ts.dt.second).astype("int32")
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
    """
    if raw.empty:
        raise ValueError("No rows in training data after filtering.")

    df = raw.copy()
    df = _ensure_timestamp(df)
    df = df.dropna(subset=["timestamp"])

    for col, default in [("category", "MISC"), ("series_ticker", "unknown"), ("status", "active")]:
        if col not in df.columns:
            df[col] = default
        df[col] = df[col].fillna(default)

    df = _canonicalize_quotes(df)
    if df.empty:
        raise ValueError("No valid rows after quote canonicalization.")

    df = df.sort_values(["ticker", "timestamp"])

    state_cols = [
        "bid",
        "ask",
        "spread",
        "obi",
        "spread_velocity",
        "momentum_5",
        "momentum_10",
        "momentum_20",
    ]
    activity_cols = ["volume", "bid_count", "ask_count"]
    meta_cols = ["category", "series_ticker", "status"]
    keep_cols = [
        "ticker",
        "timestamp",
        "bid_no",
        "ask_no",
        "bid_yes",
        "ask_yes",
        "spread_yes",
        "mid_yes",
    ] + meta_cols + state_cols + activity_cols
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols]

    bars = []
    freq = spec.freq
    freq_td = pd.Timedelta(freq)
    max_gap_seconds = float(pd.Timedelta(spec.max_gap).total_seconds())

    for ticker, g in df.groupby("ticker", sort=False):
        g = g.sort_values("timestamp").set_index("timestamp")

        state_resampled = g[state_cols + meta_cols].resample(freq).last()
        activity_resampled = g[activity_cols].resample(freq).sum(min_count=1)

        obs_count = g["bid"].resample(freq).count().rename("obs_count")
        has_obs = (obs_count > 0).astype("int8").rename("has_obs")

        last_obs_ts = g.index.to_series().resample(freq).max().rename("last_obs_ts").ffill()

        gr = pd.concat(
            [state_resampled, activity_resampled, obs_count, has_obs, last_obs_ts],
            axis=1,
        )
        if gr.empty:
            continue

        gr[state_cols + meta_cols] = gr[state_cols + meta_cols].ffill()
        gr[activity_cols] = gr[activity_cols].fillna(0.0)
        gr["obs_count"] = gr["obs_count"].fillna(0.0)

        gr["spread"] = gr["ask"] - gr["bid"]
        bin_ts = pd.Series(gr.index, index=gr.index)
        gr["staleness_sec"] = (bin_ts - gr["last_obs_ts"]).dt.total_seconds().astype("float64")
        gr = gr.drop(columns=["last_obs_ts"])

        valid_quotes = (
            gr["bid"].notna()
            & gr["ask"].notna()
            & gr["bid"].between(0.0, 100.0)
            & gr["ask"].between(0.0, 100.0)
            & (gr["ask"] >= gr["bid"])
        )
        gr = gr.loc[valid_quotes]
        if gr.empty:
            continue

        stale_mask = gr["staleness_sec"] > max_gap_seconds
        if stale_mask.any():
            gr = gr.loc[~stale_mask].copy()
        if gr.empty:
            continue

        gr = gr.reset_index().rename(columns={"index": "timestamp"})
        gr["ticker"] = ticker

        dt = gr["timestamp"].diff()
        new_seg = dt.isna() | (dt > (freq_td * 1.5))
        gr["segment_id"] = new_seg.cumsum().astype("int32") - 1
        gr["series_id"] = gr["ticker"].astype(str) + ":" + gr["segment_id"].astype(str)
        gr["time_idx"] = gr.groupby("series_id").cumcount().astype("int32")

        gr = _compute_mid(gr)
        gr["spread"] = gr["ask"] - gr["bid"]

        for c in activity_cols:
            gr[c] = gr[c].astype("float64").clip(lower=0)
        gr["obs_count"] = gr["obs_count"].astype("float64").clip(lower=0)
        gr["has_obs"] = gr["has_obs"].astype("int8")
        gr["staleness_sec"] = gr["staleness_sec"].astype("float64").clip(lower=0)

        gr["volume_log1p"] = np.log1p(gr["volume"])
        gr["bid_count_log1p"] = np.log1p(gr["bid_count"])
        gr["ask_count_log1p"] = np.log1p(gr["ask_count"])

        bars.append(gr)

    if not bars:
        raise ValueError("No valid bars produced; check if quotes/timestamps are valid.")

    out = pd.concat(bars, ignore_index=True)

    counts = out.groupby("series_id")["time_idx"].max() + 1
    keep_series = counts[counts >= min_bars_per_segment].index
    out = out[out["series_id"].isin(keep_series)].copy()

    if out.empty:
        raise ValueError(
            f"All segments filtered out (<{min_bars_per_segment} bars). "
            "Collect more data or lower --min-bars-per-segment."
        )

    out = _add_time_features(out)
    out = out.sort_values(["series_id", "time_idx"]).reset_index(drop=True)
    return out


def add_eval_labels(df: pd.DataFrame, *, horizon_steps: int) -> pd.DataFrame:
    """
    Add realized labels for evaluation (not used as model targets).

    For each row at decision time t:
      - mid_fwd_h: mid at t+h
      - dmid_h: mid_fwd_h - mid
      - pnl_long_h: (bid_fwd_h - ask_now)
      - pnl_short_h: (bid_now - ask_fwd_h)
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
