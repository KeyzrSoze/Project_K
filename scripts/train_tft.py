\
"""
Train a Temporal Fusion Transformer (TFT) on Project_K's partitioned parquet dataset.

Design notes:
- Resamples snapshots into fixed bars (default 30s) so "steps" are consistent.
- Segments each ticker on gaps to avoid learning false continuity.
- Forecasts mid price over a multi-step horizon; trading evaluation is spread-aware and includes strict bid/ask pnl proxies.

Example:
  python -m scripts.train_tft \
    --training-dir "$PROJECT_K_TRAINING_DIR" \
    --freq 30s --horizon-steps 20 --encoder-length 120 \
    --val-days 2 --test-days 2 \
    --max-epochs 30 --batch-size 128
"""
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from services.ml.data import load_training_parquet, validate_schema, BarSpec, build_bars
from services.ml.tft import TFTSpec, make_datasets, train_tft, save_run_artifacts


def _default_run_root() -> Path:
    return (Path.home() / "ev" / "Project_K" / "artifacts" / "models" / "tft").expanduser()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--training-dir", required=True)
    ap.add_argument("--start-date", default=None, help="YYYY-MM-DD (optional)")
    ap.add_argument("--end-date", default=None, help="YYYY-MM-DD (optional)")

    ap.add_argument("--freq", default="30s")
    ap.add_argument("--horizon-steps", type=int, default=20)
    ap.add_argument("--encoder-length", type=int, default=120)
    ap.add_argument("--max-gap", default="5min")
    ap.add_argument("--min-bars-per-segment", type=int, default=200)

    ap.add_argument("--val-days", type=int, default=2)
    ap.add_argument("--test-days", type=int, default=2)

    ap.add_argument("--batch-size", type=int, default=128)
    ap.add_argument("--max-epochs", type=int, default=30)
    ap.add_argument("--lr", type=float, default=1e-3)

    ap.add_argument("--run-root", default=str(_default_run_root()))
    ap.add_argument("--seed", type=int, default=1337)
    args = ap.parse_args()

    bar_spec = BarSpec(
        freq=args.freq,
        horizon_steps=args.horizon_steps,
        encoder_length=args.encoder_length,
        max_gap=args.max_gap,
    )

    tft_spec = TFTSpec(
        freq=args.freq,
        horizon_steps=args.horizon_steps,
        encoder_length=args.encoder_length,
        max_gap=args.max_gap,
        batch_size=args.batch_size,
        max_epochs=args.max_epochs,
        learning_rate=args.lr,
        val_days=args.val_days,
        test_days=args.test_days,
        min_bars_per_segment=args.min_bars_per_segment,
    )

    # Load parquet
    print("📥 Loading parquet dataset...")
    df = load_training_parquet(
        args.training_dir,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    ok, missing = validate_schema(df, strict=False)
    if not ok:
        print("❌ Missing core columns:", missing)
        return 2
    if missing:
        print("⚠️  Missing metadata columns (OK for v1):", missing)

    # Prepare bars
    print("🧱 Building bars + segments...")
    bars = build_bars(df, spec=bar_spec, min_bars_per_segment=args.min_bars_per_segment)

    # Run directory
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + f"_f{args.freq}_h{args.horizon_steps}_e{args.encoder_length}"
    run_dir = Path(args.run_root).expanduser().resolve() / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    print(f"🏁 Run dir: {run_dir}")

    # Build datasets
    print("📦 Building TFT datasets...")
    datasets = make_datasets(bars, spec=tft_spec)

    # Train
    print("🧠 Training TFT...")
    best_model, metrics = train_tft(datasets, spec=tft_spec, run_dir=run_dir, seed=args.seed)

    # Save artifacts
    dataset_params = datasets["train"].get_parameters()
    save_run_artifacts(
        run_dir=run_dir,
        bar_spec=bar_spec,
        tft_spec=tft_spec,
        dataset_params=dataset_params,
        metrics=metrics,
    )

    print("\n✅ Training complete.")
    print("Metrics:")
    for k, v in metrics.items():
        print(f"  {k}: {v}")

    print(f"\nArtifacts written to: {run_dir}")
    print("Key files:")
    print(f"  {run_dir / 'best.ckpt'}")
    print(f"  {run_dir / 'dataset_params.pkl'}")
    print(f"  {run_dir / 'spec.json'}")
    print(f"  {run_dir / 'metrics.json'}")
    print(f"  {run_dir / 'tb'}  (TensorBoard logs)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
