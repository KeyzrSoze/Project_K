\
"""
Run TFT inference on the latest bars (live-style) and print top predicted movers.

This script:
- Loads the trained TFT checkpoint + dataset parameters.
- Loads recent parquet data, builds bars at the same frequency as training.
- For each ticker's latest segment, builds a prediction frame by appending
  horizon_steps "future" rows with NaNs for unknown covariates and target.
- Produces quantile predictions for mid at horizon end and computes
  spread-aware edges for both long and short.

Example:
  python -m scripts.infer_tft \
    --run-dir ~/ev/Project_K/artifacts/models/tft/<run_id> \
    --training-dir "$PROJECT_K_TRAINING_DIR" \
    --top 25
"""
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from services.ml.data import load_training_parquet, BarSpec, build_bars
from services.ml.tft import load_model_and_params, make_prediction_dataset


def _parse_freq_to_timedelta(freq: str) -> pd.Timedelta:
    return pd.Timedelta(freq)


def _append_future_rows(
    g: pd.DataFrame,
    *,
    freq: str,
    horizon_steps: int,
) -> pd.DataFrame:
    """
    Append horizon_steps future rows with known covariates set and unknown covariates NaN.
    """
    g = g.sort_values("time_idx").copy()
    last = g.iloc[-1].copy()

    dt = _parse_freq_to_timedelta(freq)

    future_rows = []
    for i in range(1, horizon_steps + 1):
        r = last.copy()
        r["time_idx"] = int(last["time_idx"]) + i
        r["timestamp"] = pd.Timestamp(last["timestamp"]) + i * dt

        # Unknown / observed reals in the future are not known -> NaN
        for c in [
            "bid", "ask", "spread",
            "volume_log1p", "bid_count_log1p", "ask_count_log1p",
            "obi", "spread_velocity",
            "momentum_5", "momentum_10", "momentum_20",
            "mid",
        ]:
            if c in r.index:
                r[c] = np.nan

        # Recompute time features for future
        ts = r["timestamp"]
        tod_seconds = int(ts.hour) * 3600 + int(ts.minute) * 60 + int(ts.second)
        r["tod_seconds"] = tod_seconds
        r["tod_sin"] = np.sin(2 * np.pi * tod_seconds / 86400.0)
        r["tod_cos"] = np.cos(2 * np.pi * tod_seconds / 86400.0)
        r["dow"] = int(ts.dayofweek)

        future_rows.append(r)

    fut = pd.DataFrame(future_rows)
    return pd.concat([g, fut], ignore_index=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--training-dir", required=True)
    ap.add_argument("--start-date", default=None, help="YYYY-MM-DD (optional)")
    ap.add_argument("--end-date", default=None, help="YYYY-MM-DD (optional)")
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--max-tickers", type=int, default=200, help="Limit tickers for quick sanity runs")
    args = ap.parse_args()

    model, params = load_model_and_params(args.run_dir)

    # Pull spec from spec.json if present to stay consistent
    spec_path = Path(args.run_dir).expanduser().resolve() / "spec.json"
    if spec_path.exists():
        spec = pd.read_json(spec_path)
        # safer: parse manually
        import json as _json
        d = _json.loads(spec_path.read_text())
        bar_cfg = d["bar_spec"]
        freq = bar_cfg["freq"]
        horizon_steps = int(bar_cfg["horizon_steps"])
        encoder_length = int(bar_cfg["encoder_length"])
        max_gap = bar_cfg["max_gap"]
    else:
        # fallback
        freq = "30s"
        horizon_steps = 20
        encoder_length = 120
        max_gap = "5min"

    bar_spec = BarSpec(freq=freq, horizon_steps=horizon_steps, encoder_length=encoder_length, max_gap=max_gap)

    df = load_training_parquet(args.training_dir, start_date=args.start_date, end_date=args.end_date)
    bars = build_bars(df, spec=bar_spec, min_bars_per_segment=200)

    # Take the latest segment per ticker
    latest = (
        bars.sort_values(["ticker", "timestamp"])
        .groupby("ticker", as_index=False)
        .tail(encoder_length)  # keep enough history
    )

    # limit tickers for speed if desired
    tickers = sorted(latest["ticker"].unique())[: args.max_tickers]
    latest = latest[latest["ticker"].isin(tickers)].copy()

    # Build prediction frame per series_id
    frames = []
    for series_id, g in latest.groupby("series_id", sort=False):
        # Ensure enough history
        if g["time_idx"].max() + 1 < max(20, encoder_length // 4):
            continue
        # Keep last encoder_length
        g2 = g.sort_values("time_idx").tail(encoder_length).copy()
        g3 = _append_future_rows(g2, freq=freq, horizon_steps=horizon_steps)
        frames.append(g3)

    if not frames:
        print("No eligible series to predict. Collect more data or reduce encoder_length.")
        return 2

    pred_df = pd.concat(frames, ignore_index=True).sort_values(["series_id", "time_idx"])

    # Build prediction dataset + dataloader
    ds_pred = make_prediction_dataset(params, pred_df, horizon_steps=horizon_steps)
    loader = ds_pred.to_dataloader(train=False, batch_size=256, num_workers=0)

    # Predict quantiles
    quantiles = [0.1, 0.5, 0.9]
    preds = model.predict(loader, mode="quantiles", quantiles=quantiles, return_index=True)

    # preds is DataFrame with quantiles per horizon step? In PF, quantile mode returns array; for simplicity,
    # run a second call for raw to get horizon-end explicitly.
    raw = model.predict(loader, mode="raw", return_x=True)
    yhat = raw.output.prediction.detach().cpu().numpy()  # [N, H, Q]
    q_idx = quantiles.index(0.5)
    mid_h_end = yhat[:, -1, q_idx]
    lo = yhat[:, -1, 0]
    hi = yhat[:, -1, -1]

    # Index alignment: use predict(return_index=True) for group/time keys
    idx = model.predict(loader, mode="prediction", return_index=True)
    idx = idx.copy()
    if "time_idx" not in idx.columns:
        raise RuntimeError(f"Unexpected predict index columns: {idx.columns}")

    # decision time is last encoder step => time_idx - 1 (fallback later if mismatch)
    idx["decision_time_idx"] = idx["time_idx"] - 1
    idx["pred_mid_h_end"] = mid_h_end
    idx["pred_lo"] = lo
    idx["pred_hi"] = hi
    idx["pred_uncertainty"] = hi - lo

    # Map to current mid/spread at decision time
    cur = pred_df.dropna(subset=["mid", "spread"]).copy()
    cur = cur.rename(columns={"time_idx": "decision_time_idx"})

    merged = cur.merge(
        idx[["series_id", "decision_time_idx", "pred_mid_h_end", "pred_uncertainty"]],
        on=["series_id", "decision_time_idx"],
        how="inner",
    )

    if merged.empty:
        # fallback: decision time equals idx time_idx
        cur2 = pred_df.dropna(subset=["mid", "spread"]).copy()
        merged = cur2.merge(
            idx[["series_id", "time_idx", "pred_mid_h_end", "pred_uncertainty"]],
            on=["series_id", "time_idx"],
            how="inner",
        )

    if merged.empty:
        print("Prediction alignment failed (0 merged rows). This can happen if PF index semantics changed.")
        return 2

    merged["pred_dmid_h"] = merged["pred_mid_h_end"] - merged["mid"]
    merged["edge_long_pred"] = merged["pred_dmid_h"] - merged["spread"] / 2.0
    merged["edge_short_pred"] = (-merged["pred_dmid_h"]) - merged["spread"] / 2.0

    # For reporting, collapse per ticker by taking the latest decision row
    merged = merged.sort_values(["ticker", "timestamp"]).groupby("ticker", as_index=False).tail(1)

    # Rank by max(|edge|)
    merged["best_edge"] = merged[["edge_long_pred", "edge_short_pred"]].max(axis=1)
    merged = merged.sort_values("best_edge", ascending=False).head(args.top)

    cols = [
        "ticker", "timestamp", "mid", "spread",
        "pred_mid_h_end", "pred_dmid_h", "edge_long_pred", "edge_short_pred",
        "pred_uncertainty",
    ]
    print(merged[cols].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
