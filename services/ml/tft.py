\
"""
Temporal Fusion Transformer training + inference helpers for Project_K.

This module is intentionally "production-ish":
- explicit dataset spec
- deterministic-ish seeding
- artifact emission (checkpoint + spec + metrics)
- evaluation includes spread-aware, trading-aligned sanity checks

We train the TFT to forecast mid price over a fixed horizon in bar-steps.
"""
from __future__ import annotations

import json
import os
import pickle
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

import torch
import pytorch_lightning as pl
from pytorch_lightning.callbacks import EarlyStopping, ModelCheckpoint, LearningRateMonitor
from pytorch_lightning.loggers import TensorBoardLogger

from pytorch_forecasting import TimeSeriesDataSet, TemporalFusionTransformer
from pytorch_forecasting.data import GroupNormalizer
from pytorch_forecasting.metrics import QuantileLoss

from services.ml.data import BarSpec, add_eval_labels


def _dataset_df(ds_obj: TimeSeriesDataSet) -> pd.DataFrame:
    """Best-effort access to the underlying dataframe used to build the dataset."""
    for attr in ("data", "dataframe", "df"):
        if hasattr(ds_obj, attr):
            val = getattr(ds_obj, attr)
            if isinstance(val, pd.DataFrame):
                return val
    raise AttributeError("Unable to access underlying dataframe from TimeSeriesDataSet")


@dataclass(frozen=True)
class TFTSpec:
    freq: str
    horizon_steps: int
    encoder_length: int
    max_gap: str

    batch_size: int = 128
    max_epochs: int = 30
    learning_rate: float = 1e-3

    hidden_size: int = 32
    attention_head_size: int = 4
    dropout: float = 0.1
    hidden_continuous_size: int = 16

    quantiles: Tuple[float, ...] = (0.1, 0.5, 0.9)

    # Split config (date-based by default)
    val_days: int = 2
    test_days: int = 2

    # Data filters
    min_bars_per_segment: int = 200

    # Dataloader workers (macOS: keep low to avoid multiprocessing overhead)
    num_workers: int = 0


def _ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def make_datasets(
    bars: pd.DataFrame,
    *,
    spec: TFTSpec,
) -> Dict[str, TimeSeriesDataSet]:
    """
    Build train/val/test TimeSeriesDataSet objects using date-based split.

    We split by bar timestamp date (local).
    """
    df = bars.copy()

    # Add evaluation labels (for later metrics + sanity checks)
    df = add_eval_labels(df, horizon_steps=spec.horizon_steps)

    df["date"] = df["timestamp"].dt.date

    max_date = df["date"].max()
    # Determine split boundaries
    # Train: all dates < (max_date - val_days - test_days)
    # Val: next val_days
    # Test: last test_days
    test_cut = max_date - pd.Timedelta(days=spec.test_days - 1)
    val_cut = test_cut - pd.Timedelta(days=spec.val_days)

    train_df = df[df["date"] < val_cut].copy()
    val_df = df[(df["date"] >= val_cut) & (df["date"] < test_cut)].copy()
    test_df = df[df["date"] >= test_cut].copy()

    if train_df.empty or val_df.empty or test_df.empty:
        raise ValueError(
            "Time split produced empty train/val/test. "
            f"Got counts: train={len(train_df)}, val={len(val_df)}, test={len(test_df)}. "
            "Collect more days of data or reduce --val-days/--test-days."
        )

    # Define covariates
    static_categoricals = ["ticker", "category", "series_ticker"]
    time_varying_known_reals = ["time_idx", "tod_sin", "tod_cos", "dow"]
    time_varying_unknown_reals = [
        "bid", "ask", "spread",
        "volume_log1p", "bid_count_log1p", "ask_count_log1p",
        "obi", "spread_velocity",
        "momentum_5", "momentum_10", "momentum_20",
    ]

    # Basic cleaning
    for c in static_categoricals:
        train_df[c] = train_df[c].fillna("unknown").astype(str)
        val_df[c] = val_df[c].fillna("unknown").astype(str)
        test_df[c] = test_df[c].fillna("unknown").astype(str)

    # Build dataset
    training = TimeSeriesDataSet(
        train_df,
        time_idx="time_idx",
        target="mid",
        group_ids=["series_id"],
        min_encoder_length=max(10, spec.encoder_length // 4),
        max_encoder_length=spec.encoder_length,
        min_prediction_length=spec.horizon_steps,
        max_prediction_length=spec.horizon_steps,
        static_categoricals=static_categoricals,
        time_varying_known_reals=time_varying_known_reals,
        time_varying_unknown_reals=time_varying_unknown_reals,
        target_normalizer=GroupNormalizer(groups=["series_id"]),
        add_relative_time_idx=True,
        add_target_scales=True,
        add_encoder_length=True,
        allow_missing_timesteps=True,
    )

    validation = TimeSeriesDataSet.from_dataset(training, val_df, predict=True, stop_randomization=True)
    testing = TimeSeriesDataSet.from_dataset(training, test_df, predict=True, stop_randomization=True)

    return {"train": training, "val": validation, "test": testing}


def train_tft(
    datasets: Dict[str, TimeSeriesDataSet],
    *,
    spec: TFTSpec,
    run_dir: Path,
    seed: int = 1337,
) -> Tuple[TemporalFusionTransformer, Dict[str, float]]:
    """
    Train TFT and return the best checkpoint-loaded model plus metrics summary.
    """
    _ensure_dir(run_dir)

    pl.seed_everything(seed, workers=False)
    torch.set_float32_matmul_precision("high")

    train_loader = datasets["train"].to_dataloader(train=True, batch_size=spec.batch_size, num_workers=spec.num_workers)
    val_loader = datasets["val"].to_dataloader(train=False, batch_size=spec.batch_size, num_workers=spec.num_workers)

    tb_dir = run_dir / "tb"
    logger = TensorBoardLogger(save_dir=str(tb_dir), name="", version="")

    callbacks = [
        EarlyStopping(monitor="val_loss", patience=5, mode="min"),
        LearningRateMonitor(logging_interval="epoch"),
        ModelCheckpoint(
            dirpath=str(run_dir),
            filename="best",
            monitor="val_loss",
            mode="min",
            save_top_k=1,
        ),
    ]

    trainer = pl.Trainer(
        max_epochs=spec.max_epochs,
        accelerator="auto",
        devices="auto",
        logger=logger,
        callbacks=callbacks,
        enable_checkpointing=True,
        log_every_n_steps=50,
        gradient_clip_val=0.1,
    )

    model = TemporalFusionTransformer.from_dataset(
        datasets["train"],
        learning_rate=spec.learning_rate,
        hidden_size=spec.hidden_size,
        attention_head_size=spec.attention_head_size,
        dropout=spec.dropout,
        hidden_continuous_size=spec.hidden_continuous_size,
        loss=QuantileLoss(list(spec.quantiles)),
        log_interval=10,
        reduce_on_plateau_patience=3,
    )

    trainer.fit(model, train_dataloaders=train_loader, val_dataloaders=val_loader)

    best_path = str(run_dir / "best.ckpt")
    if not Path(best_path).exists():
        # lightning may name it "best.ckpt" or "best-vX.ckpt"; pick from callback.
        ckpt_cb = next((c for c in callbacks if isinstance(c, ModelCheckpoint)), None)
        if ckpt_cb and ckpt_cb.best_model_path:
            best_path = ckpt_cb.best_model_path

    best = TemporalFusionTransformer.load_from_checkpoint(best_path)

    # Compute quick val metrics: val_loss is already logged; we also compute horizon-end MAE
    metrics = evaluate_model(best, datasets, spec=spec)
    return best, metrics


def evaluate_model(
    model: TemporalFusionTransformer,
    datasets: Dict[str, TimeSeriesDataSet],
    *,
    spec: TFTSpec,
) -> Dict[str, float]:
    """
    Evaluate on the test set with:
    - MAE/RMSE on horizon-end mid
    - direction accuracy
    - spread-aware "edge" win-rate proxy (using realized mid move vs current spread)
    - realized pnl proxy using bid/ask at horizon (strictest check)
    """
    test_loader = datasets["test"].to_dataloader(train=False, batch_size=spec.batch_size, num_workers=spec.num_workers)

    # raw predictions give us access to time indices to align with evaluation labels
    raw = model.predict(test_loader, mode="raw", return_x=True)

    # raw.output.prediction: [batch, horizon, quantiles]
    pred = raw.output.prediction  # torch.Tensor
    # find median quantile index
    q = list(spec.quantiles)
    if 0.5 in q:
        q_idx = q.index(0.5)
    else:
        # fallback: closest to median
        q_idx = int(np.argmin([abs(x - 0.5) for x in q]))

    pred_med = pred[:, :, q_idx].detach().cpu().numpy()
    pred_h_end = pred_med[:, -1]

    # Align with decision-time rows:
    # encoder_time_idx last value corresponds to current time t
    x = raw.x
    enc_time_idx = x["encoder_time_idx"][:, -1].detach().cpu().numpy()
    # series id is embedded in groups; decode categorical for groups[0] which is group id index
    # We'll instead use return_index=True in a second pass to get stable merge keys.
    pred_idx = model.predict(test_loader, mode="prediction", return_index=True)
    # pred_idx is a DataFrame with group_ids and time index for the prediction start (first decoder step).
    # We'll compute decision time as (time_idx - 1).
    idx_df = pred_idx.copy()
    # Depending on PF version, column names differ; normalize
    if "time_idx" not in idx_df.columns:
        # sometimes it's "decoder_time_idx" or similar; fail loudly
        raise RuntimeError(f"Unexpected index columns from predict(return_index=True): {idx_df.columns}")

    # decision time is the last encoder idx; in our datasets, predict index is first decoder time
    idx_df["decision_time_idx"] = idx_df["time_idx"] - 1
    # Attach horizon-end pred (1 row per sample)
    idx_df["pred_mid_h_end"] = pred_h_end

    # Build evaluation frame from underlying raw data accessible via dataset's dataframe
    # (datasets were constructed from a df with eval labels)
    base_df = _dataset_df(datasets["test"]).copy()

    # We only score where horizon labels exist (non-null mid_fwd_h)
    base_df = base_df.dropna(subset=["mid_fwd_h", "bid_fwd_h", "ask_fwd_h", "dmid_h"])

    # Merge predictions to base_df
    # Attempt merge on decision_time_idx (common case: idx time is first decoder step).
    eval_df = base_df.merge(
        idx_df[["series_id", "decision_time_idx", "pred_mid_h_end"]],
        left_on=["series_id", "time_idx"],
        right_on=["series_id", "decision_time_idx"],
        how="inner",
    )

    if eval_df.empty:
        # Fallback: some PF versions return time_idx aligned to the last encoder step already.
        eval_df = base_df.merge(
            idx_df[["series_id", "time_idx", "pred_mid_h_end"]],
            on=["series_id", "time_idx"],
            how="inner",
        )

    if eval_df.empty:
        raise RuntimeError(
            "Evaluation merge produced 0 rows. This usually indicates a mismatch in time_idx alignment "
            "between the dataset and predict(return_index=True)."
        )

        raise RuntimeError("Evaluation merge produced 0 rows. This usually indicates a mismatch in time_idx alignment.")

    # Actual horizon-end mid and predicted delta
    eval_df["pred_dmid_h"] = eval_df["pred_mid_h_end"] - eval_df["mid"]
    eval_df["abs_err_mid_h"] = (eval_df["pred_mid_h_end"] - eval_df["mid_fwd_h"]).abs()
    mae = float(eval_df["abs_err_mid_h"].mean())
    rmse = float(np.sqrt(np.mean((eval_df["pred_mid_h_end"] - eval_df["mid_fwd_h"]) ** 2)))

    # Direction accuracy
    eval_df["real_dmid_h"] = eval_df["dmid_h"]
    dir_acc = float((np.sign(eval_df["pred_dmid_h"]) == np.sign(eval_df["real_dmid_h"])).mean())

    # Spread-aware edges
    eval_df["edge_long_pred"] = eval_df["pred_dmid_h"] - (eval_df["spread"] / 2.0)
    eval_df["edge_short_pred"] = (-eval_df["pred_dmid_h"]) - (eval_df["spread"] / 2.0)

    # Realized edges using mid (proxy) and also strict bid/ask pnl proxies
    eval_df["edge_long_real_mid"] = eval_df["real_dmid_h"] - (eval_df["spread"] / 2.0)
    eval_df["edge_short_real_mid"] = (-eval_df["real_dmid_h"]) - (eval_df["spread"] / 2.0)

    # Strict realized pnl proxies (most realistic)
    eval_df["pnl_long_h"] = eval_df["pnl_long_h"]
    eval_df["pnl_short_h"] = eval_df["pnl_short_h"]

    # Policy: take long when predicted edge > 0, short when predicted edge > 0
    long_mask = eval_df["edge_long_pred"] > 0
    short_mask = eval_df["edge_short_pred"] > 0

    long_win_rate = float((eval_df.loc[long_mask, "pnl_long_h"] > 0).mean()) if long_mask.any() else float("nan")
    short_win_rate = float((eval_df.loc[short_mask, "pnl_short_h"] > 0).mean()) if short_mask.any() else float("nan")

    # Coverage (how often the model says there's edge after spread)
    long_coverage = float(long_mask.mean())
    short_coverage = float(short_mask.mean())

    return {
        "test_mae_mid_h_end": mae,
        "test_rmse_mid_h_end": rmse,
        "test_dir_acc": dir_acc,
        "policy_long_coverage": long_coverage,
        "policy_short_coverage": short_coverage,
        "policy_long_win_rate_strict": long_win_rate,
        "policy_short_win_rate_strict": short_win_rate,
        "n_eval_rows": float(len(eval_df)),
    }


def save_run_artifacts(
    *,
    run_dir: Path,
    bar_spec: BarSpec,
    tft_spec: TFTSpec,
    dataset_params: dict,
    metrics: Dict[str, float],
) -> None:
    _ensure_dir(run_dir)
    (run_dir / "spec.json").write_text(
        json.dumps(
            {
                "bar_spec": asdict(bar_spec),
                "tft_spec": asdict(tft_spec),
            },
            indent=2,
            sort_keys=True,
            default=str,
        )
    )
    (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2, sort_keys=True))
    with open(run_dir / "dataset_params.pkl", "wb") as f:
        pickle.dump(dataset_params, f, protocol=pickle.HIGHEST_PROTOCOL)


def load_model_and_params(run_dir: str | Path) -> Tuple[TemporalFusionTransformer, dict]:
    run_dir = Path(run_dir).expanduser().resolve()
    ckpt = run_dir / "best.ckpt"
    if not ckpt.exists():
        # allow lightning suffix
        cands = list(run_dir.glob("best*.ckpt"))
        if not cands:
            raise FileNotFoundError(f"No checkpoint found in {run_dir}")
        ckpt = cands[0]
    model = TemporalFusionTransformer.load_from_checkpoint(str(ckpt))
    with open(run_dir / "dataset_params.pkl", "rb") as f:
        params = pickle.load(f)
    return model, params


def make_prediction_dataset(
    dataset_params: dict,
    recent_bars: pd.DataFrame,
    *,
    horizon_steps: int,
) -> TimeSeriesDataSet:
    """
    Build a prediction dataset from recent bars.
    The dataset_params are saved from the training TimeSeriesDataSet.

    recent_bars must include enough history for encoder_length and columns used by the dataset.
    It should be in "bars" format (output of build_bars).
    """
    # Ensure we have time features already.
    needed = {"tod_sin", "tod_cos", "dow", "mid"}
    missing = needed - set(recent_bars.columns)
    if missing:
        raise ValueError(f"recent_bars missing required columns: {sorted(missing)}")

    ds_pred = TimeSeriesDataSet.from_parameters(
        dataset_params,
        recent_bars,
        predict=True,
        stop_randomization=True,
    )
    return ds_pred
