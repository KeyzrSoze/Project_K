"""
Temporal Fusion Transformer training + inference helpers for Project_K.
"""
from __future__ import annotations

import json
import os
import pickle
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import lightning.pytorch as pl
import numpy as np
import pandas as pd
import torch
from lightning.pytorch import Trainer, seed_everything
from lightning.pytorch.callbacks import EarlyStopping, LearningRateMonitor, ModelCheckpoint
from lightning.pytorch.loggers import TensorBoardLogger
from pytorch_forecasting import TemporalFusionTransformer, TimeSeriesDataSet
from pytorch_forecasting.data import GroupNormalizer
from pytorch_forecasting.data.encoders import NaNLabelEncoder
from pytorch_forecasting.metrics import QuantileLoss

from services.ml.data import BarSpec, add_eval_labels


def _dataset_df(ds_obj: TimeSeriesDataSet) -> pd.DataFrame:
    """Best-effort access to the backing dataframe used to build a dataset."""
    for attr in ("data", "dataframe", "df"):
        if hasattr(ds_obj, attr):
            value = getattr(ds_obj, attr)
            if isinstance(value, pd.DataFrame):
                return value
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

    val_days: int = 2
    test_days: int = 2

    min_bars_per_segment: int = 200
    num_workers: int = 0


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _print_split_diagnostics(name: str, df: pd.DataFrame, *, spec: TFTSpec) -> None:
    required = int(spec.encoder_length + spec.horizon_steps)
    print(f"\n{name} split diagnostics (required_len={required})")

    if df.empty:
        print("  rows: 0")
        return

    if "series_id" not in df.columns or "time_idx" not in df.columns:
        print(f"  rows: {len(df)}")
        print("  missing series_id/time_idx, cannot compute per-series spans")
        return

    grouped = df.groupby("series_id", sort=False)
    rows = grouped.size()
    span = grouped["time_idx"].agg(lambda x: int(x.max() - x.min() + 1))

    print(f"  rows: {len(df)}")
    print(f"  series_id: {int(rows.shape[0])}")
    print(f"  series_id with rows>=required: {int((rows >= required).sum())}")
    print(f"  series_id with span>=required: {int((span >= required).sum())}")


def _safe_from_dataset(
    name: str,
    base_ds: TimeSeriesDataSet,
    df: pd.DataFrame,
    **kwargs: Any,
) -> Tuple[Optional[TimeSeriesDataSet], Optional[str]]:
    try:
        ds = TimeSeriesDataSet.from_dataset(base_ds, df, **kwargs)
        if len(ds) == 0:
            return None, f"{name} dataset has 0 samples after filtering"
        return ds, None
    except (AssertionError, ValueError) as error:
        return None, f"{name} dataset build failed: {type(error).__name__}: {error}"


def make_datasets(
    bars: pd.DataFrame,
    *,
    spec: TFTSpec,
) -> Dict[str, Any]:
    """
    Build train/val/test TimeSeriesDataSet objects using date split.
    """
    df = bars.copy()
    df = add_eval_labels(df, horizon_steps=spec.horizon_steps)
    df["date"] = df["timestamp"].dt.date

    max_date = df["date"].max()
    test_cut = max_date - pd.Timedelta(days=spec.test_days - 1)
    val_cut = test_cut - pd.Timedelta(days=spec.val_days)

    train_df = df[df["date"] < val_cut].copy()
    val_df = df[(df["date"] >= val_cut) & (df["date"] < test_cut)].copy()
    test_df = df[df["date"] >= test_cut].copy()

    train_counts = train_df.groupby("series_id", sort=False).size()
    tiny_train_series = train_counts[train_counts < 2].index
    if len(tiny_train_series) > 0:
        print(
            f"\nDropping {len(tiny_train_series)} series_id with <2 rows from train split "
            "(prevents NaN target_scale)."
        )
        train_df = train_df[~train_df["series_id"].isin(tiny_train_series)].copy()

    if train_df.empty:
        raise ValueError(
            "Time split produced empty training set. "
            f"Got counts: train={len(train_df)}, val={len(val_df)}, test={len(test_df)}. "
            "Collect more days of data or expand the date range."
        )

    static_categoricals = ["ticker", "category", "series_ticker"]
    time_varying_known_reals = ["time_idx", "tod_sin", "tod_cos", "dow"]
    time_varying_unknown_reals = [
        "bid",
        "ask",
        "spread",
        "has_obs",
        "staleness_sec",
        "obs_count",
        "volume_log1p",
        "bid_count_log1p",
        "ask_count_log1p",
        "obi",
        "spread_velocity",
        "momentum_5",
        "momentum_10",
        "momentum_20",
    ]

    for column in static_categoricals:
        train_df[column] = train_df[column].fillna("unknown").astype(str)
        val_df[column] = val_df[column].fillna("unknown").astype(str)
        test_df[column] = test_df[column].fillna("unknown").astype(str)

    all_series_id = pd.concat(
        [train_df["series_id"], val_df["series_id"], test_df["series_id"]],
        ignore_index=True,
    ).astype(str)
    all_ticker = pd.concat(
        [train_df["ticker"], val_df["ticker"], test_df["ticker"]],
        ignore_index=True,
    ).astype(str)
    all_category = pd.concat(
        [train_df["category"], val_df["category"], test_df["category"]],
        ignore_index=True,
    ).astype(str)
    all_series_ticker = pd.concat(
        [train_df["series_ticker"], val_df["series_ticker"], test_df["series_ticker"]],
        ignore_index=True,
    ).astype(str)

    categorical_encoders = {
        "series_id": NaNLabelEncoder(add_nan=True).fit(all_series_id, overwrite=True),
        "__group_id__series_id": NaNLabelEncoder(add_nan=True).fit(all_series_id, overwrite=True),
        "ticker": NaNLabelEncoder(add_nan=True).fit(all_ticker, overwrite=True),
        "category": NaNLabelEncoder(add_nan=True).fit(all_category, overwrite=True),
        "series_ticker": NaNLabelEncoder(add_nan=True).fit(all_series_ticker, overwrite=True),
    }

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
        categorical_encoders=categorical_encoders,
        allow_missing_timesteps=True,
    )

    validation = None
    testing = None
    validation_error = None
    test_error = None

    if val_df.empty:
        validation_error = "val split has 0 rows after date split"
    else:
        validation, validation_error = _safe_from_dataset(
            "val", training, val_df, predict=True, stop_randomization=True
        )

    if validation is None and validation_error:
        print(f"\nNo validation dataset: {validation_error}")
        _print_split_diagnostics("val", val_df, spec=spec)

    if test_df.empty:
        test_error = "test split has 0 rows after date split"
    else:
        testing, test_error = _safe_from_dataset(
            "test", training, test_df, predict=True, stop_randomization=True
        )

    if testing is None and test_error:
        print(f"\nNo test dataset: {test_error}")
        _print_split_diagnostics("test", test_df, spec=spec)

    return {
        "training": training,
        "validation": validation,
        "testing": testing,
        "errors": {"val": validation_error, "test": test_error},
        "test_df": test_df,
        "train": training,
        "val": validation,
        "test": testing,
    }


def _compute_eval_metrics(eval_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute the same core evaluation metrics as the pipeline previously used.
    """
    df = eval_df.copy()
    df["pred_dmid_h"] = df["pred_mid_h_end"] - df["mid"]
    df["abs_err_mid_h"] = (df["pred_mid_h_end"] - df["mid_fwd_h"]).abs()

    mae = float(df["abs_err_mid_h"].mean())
    rmse = float(np.sqrt(np.mean((df["pred_mid_h_end"] - df["mid_fwd_h"]) ** 2)))
    df["real_dmid_h"] = df["dmid_h"]
    dir_acc = float((np.sign(df["pred_dmid_h"]) == np.sign(df["real_dmid_h"])).mean())

    df["edge_long_pred"] = df["pred_dmid_h"] - (df["spread"] / 2.0)
    df["edge_short_pred"] = (-df["pred_dmid_h"]) - (df["spread"] / 2.0)

    long_mask = df["edge_long_pred"] > 0
    short_mask = df["edge_short_pred"] > 0

    long_win_rate = (
        float((df.loc[long_mask, "pnl_long_h"] > 0).mean()) if long_mask.any() else float("nan")
    )
    short_win_rate = (
        float((df.loc[short_mask, "pnl_short_h"] > 0).mean()) if short_mask.any() else float("nan")
    )

    return {
        "test_mae_mid_h_end": mae,
        "test_rmse_mid_h_end": rmse,
        "test_dir_acc": dir_acc,
        "policy_long_coverage": float(long_mask.mean()),
        "policy_short_coverage": float(short_mask.mean()),
        "policy_long_win_rate_strict": long_win_rate,
        "policy_short_win_rate_strict": short_win_rate,
        "n_eval_rows": float(len(df)),
    }


def _compute_baseline_metrics(eval_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Baseline: horizon-end mid stays equal to current mid (delta=0).
    """
    mae = float((eval_df["mid_fwd_h"] - eval_df["mid"]).abs().mean())
    rmse = float(np.sqrt(np.mean((eval_df["mid_fwd_h"] - eval_df["mid"]) ** 2)))
    dir_acc = float((np.sign(0.0) == np.sign(eval_df["dmid_h"])).mean())
    return {
        "baseline_mae_mid_h_end": mae,
        "baseline_rmse_mid_h_end": rmse,
        "baseline_dir_acc": dir_acc,
        "baseline_n_eval_rows": int(len(eval_df)),
    }


def _alignment_shift_diagnostics(
    eval_df_base: pd.DataFrame,
    *,
    pred_map: pd.DataFrame,
    join_key: str,
    shifts: List[int],
) -> pd.DataFrame:
    """
    Join-shift sensitivity:
    keep eval rows fixed and shift prediction join key by s bars.
    """
    required_eval_cols = {"series_id", "time_idx", "mid", "mid_fwd_h", "dmid_h"}
    missing_eval_cols = required_eval_cols - set(eval_df_base.columns)
    if missing_eval_cols:
        raise KeyError(
            f"eval_df_base missing required columns for diagnostics: {sorted(missing_eval_cols)}"
        )

    required_pred_cols = {"series_id", join_key, "pred_mid_h_end"}
    missing_pred_cols = required_pred_cols - set(pred_map.columns)
    if missing_pred_cols:
        raise KeyError(
            f"pred_map missing required columns for diagnostics: {sorted(missing_pred_cols)}"
        )

    base_view = eval_df_base.copy()
    base_view["time_idx"] = base_view["time_idx"].astype("int64")

    pred_view = pred_map[["series_id", join_key, "pred_mid_h_end"]].copy()
    pred_view[join_key] = pred_view[join_key].astype("int64")

    rows: List[Dict[str, Any]] = []
    for shift in shifts:
        shift = int(shift)
        shifted = pred_view.copy()
        shifted["join_time_idx"] = shifted[join_key] + shift

        merged = base_view.merge(
            shifted[["series_id", "join_time_idx", "pred_mid_h_end"]],
            left_on=["series_id", "time_idx"],
            right_on=["series_id", "join_time_idx"],
            how="inner",
        )

        if merged.empty:
            rows.append(
                {
                    "shift": shift,
                    "n_rows": 0,
                    "mae_mid_h_end": float("nan"),
                    "rmse_mid_h_end": float("nan"),
                    "dir_acc": float("nan"),
                }
            )
            continue

        mae = float((merged["pred_mid_h_end"] - merged["mid_fwd_h"]).abs().mean())
        rmse = float(np.sqrt(np.mean((merged["pred_mid_h_end"] - merged["mid_fwd_h"]) ** 2)))
        pred_dmid = merged["pred_mid_h_end"] - merged["mid"]
        dir_acc = float((np.sign(pred_dmid) == np.sign(merged["dmid_h"])).mean())

        rows.append(
            {
                "shift": shift,
                "n_rows": int(len(merged)),
                "mae_mid_h_end": mae,
                "rmse_mid_h_end": rmse,
                "dir_acc": dir_acc,
            }
        )

    return pd.DataFrame.from_records(rows).sort_values("shift").reset_index(drop=True)


def _json_safe(value: Any) -> Any:
    """
    Convert numpy/pandas objects into plain Python objects for json serialization.
    """
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, (pd.Timestamp, pd.Timedelta)):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def _maybe_write_eval_debug_sample(
    eval_df: pd.DataFrame,
    *,
    spec: TFTSpec,
    run_dir: Optional[Path],
    n: int = 20,
) -> None:
    if run_dir is None:
        print("TFT_EVAL_DEBUG=1 but run_dir is None, skipping eval_debug_sample dump")
        return

    if eval_df.empty:
        print("Evaluation frame is empty, skipping eval_debug_sample dump")
        return

    sample_size = min(int(n), int(len(eval_df)))
    if sample_size <= 0:
        return

    out_csv = Path(run_dir) / "eval_debug_sample.csv"
    out_parquet = Path(run_dir) / "eval_debug_sample.parquet"

    sample = eval_df.sample(n=sample_size, random_state=42).copy()
    sample["decision_time_idx"] = sample.get("decision_time_idx", sample["time_idx"]).astype("int64")
    sample["horizon_end_idx"] = sample["decision_time_idx"] + int(spec.horizon_steps)
    sample["mid_now"] = sample["mid"]
    sample["mid_h_end_true"] = sample["mid_fwd_h"]
    sample["mid_h_end_pred"] = sample["pred_mid_h_end"]
    sample["pred_delta_h"] = sample["pred_mid_h_end"] - sample["mid"]
    sample["real_delta_h"] = sample["dmid_h"]

    preferred_cols = [
        "series_id",
        "time_idx",
        "decision_time_idx",
        "pred_time_idx",
        "horizon_end_idx",
        "timestamp",
        "mid_now",
        "mid_h_end_true",
        "mid_h_end_pred",
        "pred_delta_h",
        "real_delta_h",
        "spread",
        "bid",
        "ask",
        "bid_fwd_h",
        "ask_fwd_h",
    ]
    columns = [column for column in preferred_cols if column in sample.columns]
    if not columns:
        columns = list(sample.columns)

    sample[columns].to_csv(out_csv, index=False)
    print(f"Wrote eval debug sample: {out_csv}")

    try:
        sample[columns].to_parquet(out_parquet, index=False)
        print(f"Wrote eval debug sample parquet: {out_parquet}")
    except Exception as error:
        print(f"Could not write eval_debug_sample.parquet: {type(error).__name__}: {error}")


def evaluate_model(
    model: TemporalFusionTransformer,
    datasets: Dict[str, Any],
    *,
    spec: TFTSpec,
    run_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Evaluate model on test split and emit alignment diagnostics.
    """
    test_ds = datasets.get("testing")
    if test_ds is None:
        test_ds = datasets.get("test")
    if test_ds is None:
        return {"test_skipped": True, "test_skip_reason": "no test dataset"}

    test_loader = test_ds.to_dataloader(
        train=False, batch_size=spec.batch_size, num_workers=spec.num_workers
    )

    raw = model.predict(test_loader, mode="raw", return_x=True)
    pred = raw.output.prediction

    quantiles = list(spec.quantiles)
    if 0.5 in quantiles:
        quantile_idx = quantiles.index(0.5)
    else:
        quantile_idx = int(np.argmin([abs(x - 0.5) for x in quantiles]))

    pred_h_end = pred[:, :, quantile_idx].detach().cpu().numpy()[:, -1]

    pred_idx = model.predict(test_loader, mode="prediction", return_index=True)
    if isinstance(pred_idx, pd.DataFrame):
        idx_df = pred_idx.copy()
    else:
        idx_df = getattr(pred_idx, "index", None)
        if not isinstance(idx_df, pd.DataFrame):
            raise RuntimeError(
                f"Unexpected predict(return_index=True) type: {type(pred_idx)}"
            )
        idx_df = idx_df.copy()

    if "time_idx" not in idx_df.columns:
        for candidate in ("decoder_time_idx",):
            if candidate in idx_df.columns:
                idx_df = idx_df.rename(columns={candidate: "time_idx"})
                break
    if "time_idx" not in idx_df.columns:
        raise RuntimeError(
            f"Unexpected index columns from predict(return_index=True): {idx_df.columns}"
        )

    if "series_id" not in idx_df.columns:
        for column in idx_df.columns:
            if column.endswith("series_id"):
                idx_df = idx_df.rename(columns={column: "series_id"})
                break
    if "series_id" not in idx_df.columns:
        raise RuntimeError(f"Missing series_id in predict(return_index=True): {idx_df.columns}")

    idx_df["pred_time_idx"] = idx_df["time_idx"].astype("int64")
    idx_df["decision_time_idx"] = (idx_df["pred_time_idx"] - 1).astype("int64")
    idx_df["pred_mid_h_end"] = pred_h_end

    pred_map = idx_df[["series_id", "decision_time_idx", "pred_time_idx", "pred_mid_h_end"]].copy()

    base_df_all = datasets.get("test_df")
    if isinstance(base_df_all, pd.DataFrame):
        base_df_all = base_df_all.copy()
    else:
        base_df_all = _dataset_df(test_ds).copy()

    base_df_eval = base_df_all.dropna(
        subset=["mid_fwd_h", "bid_fwd_h", "ask_fwd_h", "dmid_h"]
    ).copy()

    eval_df = base_df_eval.merge(
        pred_map,
        left_on=["series_id", "time_idx"],
        right_on=["series_id", "decision_time_idx"],
        how="inner",
    )
    eval_join_key = "decision_time_idx"

    if eval_df.empty:
        eval_df = base_df_eval.merge(
            pred_map,
            left_on=["series_id", "time_idx"],
            right_on=["series_id", "pred_time_idx"],
            how="inner",
        )
        eval_join_key = "pred_time_idx"

    if eval_df.empty:
        raise RuntimeError(
            "Evaluation merge produced 0 rows. This usually indicates a mismatch in time_idx "
            "alignment between dataset and predict(return_index=True)."
        )

    metrics: Dict[str, Any] = _compute_eval_metrics(eval_df)
    metrics.update(_compute_baseline_metrics(eval_df))
    metrics["eval_join_key"] = eval_join_key
    metrics["eval_n_rows_after_join"] = int(len(eval_df))

    shifts = [-2, -1, 0, 1, 2]
    eval_df_base = eval_df.drop(columns=["pred_mid_h_end"]).copy()
    diag_df = _alignment_shift_diagnostics(
        eval_df_base,
        pred_map=pred_map,
        join_key=eval_join_key,
        shifts=shifts,
    )
    print("\nAlignment join-shift diagnostics:")
    print(diag_df.to_string(index=False))
    metrics["alignment_shift_diagnostics"] = _json_safe(diag_df.to_dict(orient="records"))

    if os.getenv("TFT_EVAL_DEBUG", "0") == "1":
        best_row = diag_df.sort_values("dir_acc", ascending=False).head(1)
        shift0 = diag_df.loc[diag_df["shift"] == 0, "dir_acc"]
        if not best_row.empty and not shift0.empty:
            best_shift = int(best_row["shift"].iloc[0])
            best_dir = float(best_row["dir_acc"].iloc[0])
            shift0_dir = float(shift0.iloc[0])
            if (
                best_shift != 0
                and np.isfinite(best_dir)
                and np.isfinite(shift0_dir)
                and (best_dir - shift0_dir) >= 0.15
            ):
                print(
                    "Alignment warning: non-zero join shift materially improves direction accuracy. "
                    f"shift=0 dir_acc={shift0_dir:.3f}, best_shift={best_shift:+d} dir_acc={best_dir:.3f}"
                )

        _maybe_write_eval_debug_sample(eval_df, spec=spec, run_dir=run_dir, n=20)

    return _json_safe(metrics)


def train_tft(
    datasets: Dict[str, Any],
    *,
    spec: TFTSpec,
    run_dir: Path,
    seed: int = 1337,
) -> Tuple[TemporalFusionTransformer, Dict[str, Any]]:
    """
    Train TFT and return the best checkpoint-loaded model plus metrics.
    """
    _ensure_dir(run_dir)

    training = datasets.get("training")
    if training is None:
        training = datasets.get("train")
    validation = datasets.get("validation")
    if validation is None:
        validation = datasets.get("val")
    testing = datasets.get("testing")
    if testing is None:
        testing = datasets.get("test")

    if training is None:
        raise KeyError("datasets missing 'training' (or legacy 'train') TimeSeriesDataSet")

    seed_everything(seed, workers=False)
    torch.set_float32_matmul_precision("high")

    train_loader = training.to_dataloader(
        train=True, batch_size=spec.batch_size, num_workers=spec.num_workers
    )
    val_loader = (
        validation.to_dataloader(
            train=False, batch_size=spec.batch_size, num_workers=spec.num_workers
        )
        if validation is not None
        else None
    )

    tb_dir = run_dir / "tb"
    logger = TensorBoardLogger(save_dir=str(tb_dir), name="", version="")

    callbacks: List[Any] = [LearningRateMonitor(logging_interval="epoch")]
    if validation is not None:
        callbacks.append(EarlyStopping(monitor="val_loss", patience=5, mode="min"))
        ckpt_cb = ModelCheckpoint(
            dirpath=str(run_dir),
            filename="best",
            monitor="val_loss",
            mode="min",
            save_top_k=1,
        )
    else:
        ckpt_cb = ModelCheckpoint(
            dirpath=str(run_dir),
            filename="best",
            save_last=True,
            save_top_k=0,
        )
    callbacks.append(ckpt_cb)

    trainer = Trainer(
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
        training,
        learning_rate=spec.learning_rate,
        hidden_size=spec.hidden_size,
        attention_head_size=spec.attention_head_size,
        dropout=spec.dropout,
        hidden_continuous_size=spec.hidden_continuous_size,
        loss=QuantileLoss(list(spec.quantiles)),
        log_interval=10,
        reduce_on_plateau_patience=3 if validation is not None else None,
    )

    print(f"[Lightning] model_type={type(model)}")
    print(f"[Lightning] model_mro={model.__class__.mro()[:3]}")
    print(f"[Lightning] pl.LightningModule={pl.LightningModule}")

    if val_loader is not None:
        trainer.fit(model, train_dataloaders=train_loader, val_dataloaders=val_loader)
    else:
        trainer.fit(model, train_dataloaders=train_loader)

    best_path = str(run_dir / "best.ckpt")
    if not Path(best_path).exists():
        if ckpt_cb and ckpt_cb.best_model_path:
            best_path = ckpt_cb.best_model_path
        elif ckpt_cb and getattr(ckpt_cb, "last_model_path", None):
            best_path = ckpt_cb.last_model_path

    canonical_path = str(run_dir / "best.ckpt")
    if Path(best_path).exists():
        if str(Path(best_path).resolve()) != str(Path(canonical_path).resolve()):
            shutil.copyfile(best_path, canonical_path)
        best_path = canonical_path
    else:
        trainer.save_checkpoint(canonical_path)
        best_path = canonical_path

    best = TemporalFusionTransformer.load_from_checkpoint(best_path)

    if testing is None:
        test_error = (datasets.get("errors") or {}).get("test")
        metrics = {"test_skipped": True, "test_skip_reason": test_error or "no test dataset"}
    else:
        metrics = evaluate_model(best, datasets, spec=spec, run_dir=run_dir)
    return best, metrics


def save_run_artifacts(
    *,
    run_dir: Path,
    bar_spec: BarSpec,
    tft_spec: TFTSpec,
    dataset_params: dict,
    metrics: Dict[str, Any],
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
    (run_dir / "metrics.json").write_text(json.dumps(_json_safe(metrics), indent=2, sort_keys=True))
    with open(run_dir / "dataset_params.pkl", "wb") as handle:
        pickle.dump(dataset_params, handle, protocol=pickle.HIGHEST_PROTOCOL)


def load_model_and_params(run_dir: str | Path) -> Tuple[TemporalFusionTransformer, dict]:
    run_dir = Path(run_dir).expanduser().resolve()
    ckpt = run_dir / "best.ckpt"
    if not ckpt.exists():
        candidates = list(run_dir.glob("best*.ckpt"))
        if not candidates:
            raise FileNotFoundError(f"No checkpoint found in {run_dir}")
        ckpt = candidates[0]
    model = TemporalFusionTransformer.load_from_checkpoint(str(ckpt))
    with open(run_dir / "dataset_params.pkl", "rb") as handle:
        params = pickle.load(handle)
    return model, params


def make_prediction_dataset(
    dataset_params: dict,
    recent_bars: pd.DataFrame,
    *,
    horizon_steps: int,
) -> TimeSeriesDataSet:
    """
    Build prediction dataset from saved parameters and recent bars.
    """
    required = {"tod_sin", "tod_cos", "dow", "mid"}
    missing = required - set(recent_bars.columns)
    if missing:
        raise ValueError(f"recent_bars missing required columns: {sorted(missing)}")

    return TimeSeriesDataSet.from_parameters(
        dataset_params,
        recent_bars,
        predict=True,
        stop_randomization=True,
    )
