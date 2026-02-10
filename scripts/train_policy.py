"""
Train and backtest a phase-1 Kalshi policy model using horizon PnL labels.
"""
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import List

from services.paths import ARTIFACTS_DIR

import pandas as pd
from rich.console import Console
from rich.table import Table

from services.ml.policy import (
    PolicySpec,
    backtest_policy,
    build_policy_frame,
    save_policy_run,
    split_by_time,
    tradable_mask,
    train_models,
)


def _default_run_root() -> Path:
    return (ARTIFACTS_DIR / "models" / "policy").resolve(strict=False)


def _tail_last_day(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    last_date = df["timestamp"].dt.date.max()
    return df[df["timestamp"].dt.date == last_date].copy()


def _feature_columns(df: pd.DataFrame, *, group_key: str) -> List[str]:
    exclude = {
        "timestamp",
        "ticker",
        "series_id",
        "time_idx",
        group_key,
        "y_long",
        "y_short",
        "pnl_long_h",
        "pnl_short_h",
        "mid_fwd_h",
        "dmid_h",
        "bid_fwd_h",
        "ask_fwd_h",
        "date",
        "segment_id",
        "category",
        "status",
        "series_ticker",
        "pred_long",
        "pred_short",
        "pred_edge",
        "pred_side",
        "is_tradable",
        "edge_ok",
        "is_candidate",
        "side",
        "realized_pnl",
    }
    numeric = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    return [c for c in numeric if c not in exclude]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--training-dir", required=True)
    ap.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end-date", required=True, help="YYYY-MM-DD")

    ap.add_argument("--freq", default="30s")
    ap.add_argument("--horizon-steps", type=int, default=20)
    ap.add_argument("--max-gap", default="2h")
    ap.add_argument("--min-bars-per-segment", type=int, default=150)

    ap.add_argument("--val-days", type=int, default=1)
    ap.add_argument("--test-days", type=int, default=1)

    ap.add_argument("--max-spread", type=float, default=5.0)
    ap.add_argument("--max-staleness-sec", type=float, default=60.0)
    ap.add_argument("--min-edge", type=float, default=1.0)
    ap.add_argument("--selection-mode", choices=("threshold", "topk"), default="threshold")
    ap.add_argument("--topk-per-day", type=int, default=3)

    ap.add_argument("--no-cooldown", action="store_true")
    ap.add_argument("--use-sample-weight", action="store_true")
    ap.add_argument("--run-dir", default=None)
    args = ap.parse_args()

    spec = PolicySpec(
        freq=args.freq,
        horizon_steps=args.horizon_steps,
        max_gap=args.max_gap,
        min_bars_per_segment=args.min_bars_per_segment,
        max_spread=args.max_spread,
        max_staleness_sec=args.max_staleness_sec,
        min_edge=args.min_edge,
        enforce_group_cooldown=not args.no_cooldown,
        selection_mode=args.selection_mode,
        topk_per_day=args.topk_per_day,
    )

    if args.run_dir:
        run_dir = Path(args.run_dir).expanduser().resolve()
    else:
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + f"_f{spec.freq}_h{spec.horizon_steps}"
        run_dir = _default_run_root() / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    print("Building policy frame...")
    df = build_policy_frame(
        args.training_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        spec=spec,
    )
    print(f"Policy frame rows: {len(df)}")

    splits = split_by_time(df, val_days=args.val_days, test_days=args.test_days)
    for split_name in ("train", "val", "test"):
        print(f"{split_name} rows: {len(splits[split_name])}")

    feature_cols = _feature_columns(df, group_key=spec.group_key)
    if not feature_cols:
        raise ValueError("No numeric feature columns available for policy model")
    print(f"Feature columns: {len(feature_cols)}")

    train_fit_df = splits["train"].copy()
    tradable_fit = tradable_mask(train_fit_df, spec)
    if tradable_fit.any():
        train_fit_df = train_fit_df.loc[tradable_fit].copy()
    print(f"Train rows used for fitting: {len(train_fit_df)}")

    models = train_models(
        train_fit_df,
        feature_cols,
        random_state=42,
        use_sample_weight=args.use_sample_weight,
    )

    # For runtime, train backtest is limited to last train day.
    train_backtest_df = _tail_last_day(splits["train"])

    split_inputs = {
        "train": train_backtest_df,
        "val": splits["val"],
        "test": splits["test"],
    }
    trade_logs: dict[str, pd.DataFrame] = {}
    split_metrics: dict[str, dict] = {}

    for split_name, split_df in split_inputs.items():
        trade_log, metrics = backtest_policy(split_df, models, feature_cols, spec=spec)
        trade_logs[split_name] = trade_log
        split_metrics[split_name] = metrics

    metrics_out = {
        "train_backtest_scope": "last_train_day",
        "feature_cols": feature_cols,
        "n_feature_cols": len(feature_cols),
        "n_train_fit_rows": int(len(train_fit_df)),
        "split_metrics": split_metrics,
    }
    save_policy_run(run_dir, spec, metrics_out, models, trade_logs)

    console = Console()
    table = Table(title="Policy Backtest Summary")
    table.add_column("split")
    table.add_column("n_rows", justify="right")
    table.add_column("n_candidates", justify="right")
    table.add_column("n_trades", justify="right")
    table.add_column("hit_rate", justify="right")
    table.add_column("avg_pnl", justify="right")
    table.add_column("sum_pnl", justify="right")
    table.add_column("avg_pred_edge", justify="right")
    table.add_column("capture_ratio_net", justify="right")

    for split_name in ("train", "val", "test"):
        metric = split_metrics.get(split_name, {})
        table.add_row(
            split_name,
            str(metric.get("n_rows", 0)),
            str(metric.get("n_candidates", 0)),
            str(metric.get("n_trades", 0)),
            f"{metric.get('hit_rate', float('nan')):.4f}",
            f"{metric.get('avg_realized_pnl', float('nan')):.4f}",
            f"{metric.get('sum_realized_pnl', float('nan')):.4f}",
            f"{metric.get('avg_pred_edge', float('nan')):.4f}",
            f"{metric.get('capture_ratio_net', float('nan')):.4f}",
        )
    console.print(table)
    print(f"Artifacts written to: {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
