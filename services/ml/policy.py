"""
Phase-1 policy modeling for Kalshi contracts using fixed-horizon PnL labels.
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor

from services.ml.data import BarSpec, add_eval_labels, build_bars, load_training_parquet, validate_schema


@dataclass(frozen=True)
class PolicySpec:
    freq: str = "30s"
    horizon_steps: int = 20
    max_gap: str = "2h"
    min_bars_per_segment: int = 150

    max_lag_steps: int = 40

    max_spread: float = 5.0
    max_staleness_sec: float = 60.0
    require_has_obs: bool = True

    min_edge: float = 1.0

    group_key: str = "group_key"
    group_key_mode: str = "event3"  # options: "prefix", "event3"
    enforce_group_cooldown: bool = True
    selection_mode: str = "threshold"
    topk_per_day: int = 3
    fee_per_trade: float = 2.0


def _json_safe(value: Any) -> Any:
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, (pd.Timestamp, pd.Timedelta)):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def _policy_debug_enabled() -> bool:
    return os.getenv("POLICY_DEBUG", "0") == "1"


def _policy_debug(msg: str) -> None:
    if _policy_debug_enabled():
        print(msg)


def derive_series_ticker(ticker: str) -> str:
    if pd.isna(ticker):
        return "unknown"
    text = str(ticker).strip()
    if not text:
        return "unknown"
    prefix = text.split("-", 1)[0].strip()
    return prefix if prefix else "unknown"


def derive_group_key(ticker: str, mode: str = "event3") -> str:
    if pd.isna(ticker):
        return "unknown"
    text = str(ticker).strip()
    if not text:
        return "unknown"

    parts = [part.strip() for part in text.split("-") if part.strip()]
    if not parts:
        return "unknown"

    prefix = parts[0]
    mode_norm = str(mode).strip().lower()
    if mode_norm == "event3" and len(parts) >= 3:
        return "-".join(parts[:3])
    return prefix


def _normalize_string_series(series: pd.Series) -> pd.Series:
    out = series.copy()
    out = out.where(out.notna(), "")
    out = out.astype(str).str.strip()
    out = out.replace({"nan": "", "None": "", "NaT": ""})
    return out


def _resolve_group_key(df: pd.DataFrame, *, group_key: str) -> pd.Series:
    if group_key in df.columns:
        candidate = _normalize_string_series(df[group_key])
        candidate = candidate.mask(candidate.str.lower().eq("unknown"), np.nan)
        candidate = candidate.mask(candidate.eq(""), np.nan)
        if candidate.notna().any():
            out = candidate
        else:
            out = pd.Series(np.nan, index=df.index)
    elif "group_key" in df.columns:
        candidate = _normalize_string_series(df["group_key"])
        candidate = candidate.mask(candidate.str.lower().eq("unknown"), np.nan)
        candidate = candidate.mask(candidate.eq(""), np.nan)
        if candidate.notna().any():
            out = candidate
        else:
            out = pd.Series(np.nan, index=df.index)
    elif "series_ticker" in df.columns:
        candidate = _normalize_string_series(df["series_ticker"])
        candidate = candidate.mask(candidate.str.lower().eq("unknown"), np.nan)
        candidate = candidate.mask(candidate.eq(""), np.nan)
        if candidate.notna().any():
            out = candidate
        else:
            out = pd.Series(np.nan, index=df.index)
    elif "ticker" in df.columns:
        ticker = _normalize_string_series(df["ticker"])
        out = ticker.map(derive_series_ticker)
    else:
        out = pd.Series(np.nan, index=df.index)

    if "series_id" in df.columns:
        fallback = _normalize_string_series(df["series_id"]).replace("", "unknown")
    else:
        fallback = pd.Series("unknown", index=df.index)

    out = out.fillna(fallback)
    out = _normalize_string_series(out).replace("", "unknown")
    return out


def build_policy_frame(
    training_dir: str | Path,
    *,
    start_date: str,
    end_date: str,
    spec: PolicySpec,
) -> pd.DataFrame:
    """
    Build feature/label frame for long and short horizon PnL prediction.
    """
    raw = load_training_parquet(
        training_dir,
        start_date=start_date,
        end_date=end_date,
    )
    ok, missing = validate_schema(raw, strict=False)
    if not ok:
        raise ValueError(f"Training dataframe missing required columns: {missing}")

    bar_spec = BarSpec(
        freq=spec.freq,
        horizon_steps=spec.horizon_steps,
        encoder_length=max(120, spec.max_lag_steps + spec.horizon_steps + 10),
        max_gap=spec.max_gap,
    )
    bars = build_bars(raw, spec=bar_spec, min_bars_per_segment=spec.min_bars_per_segment)
    df = add_eval_labels(bars, horizon_steps=spec.horizon_steps)

    df["y_long"] = df["pnl_long_h"].astype("float64")
    df["y_short"] = df["pnl_short_h"].astype("float64")
    df = df.dropna(subset=["y_long", "y_short"]).copy()

    if "ticker" not in df.columns:
        if "series_id" in df.columns:
            derived_ticker = _normalize_string_series(df["series_id"]).str.split(":", n=1).str[0]
            df["ticker"] = derived_ticker.replace("", "unknown")
        else:
            df["ticker"] = "unknown"
    df["ticker"] = _normalize_string_series(df["ticker"])
    df["ticker"] = df["ticker"].replace("", "unknown")

    derived_series = df["ticker"].map(derive_series_ticker)
    if "series_ticker" not in df.columns:
        df["series_ticker"] = derived_series
    else:
        series_ticker = _normalize_string_series(df["series_ticker"])
        bad_series = series_ticker.eq("") | series_ticker.str.lower().eq("unknown")
        series_ticker = series_ticker.mask(bad_series, derived_series)
        df["series_ticker"] = series_ticker.replace("", "unknown")

    derived_group = df["ticker"].map(lambda value: derive_group_key(value, mode=spec.group_key_mode))
    if "group_key" not in df.columns:
        df["group_key"] = derived_group
    else:
        group_key = _normalize_string_series(df["group_key"])
        bad_group = group_key.eq("") | group_key.str.lower().eq("unknown")
        group_key = group_key.mask(bad_group, derived_group)
        df["group_key"] = group_key.replace("", "unknown")

    if "timestamp" in df.columns and not np.issubdtype(df["timestamp"].dtype, np.datetime64):
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).copy()

    if "series_id" in df.columns and "time_idx" in df.columns:
        df = df.sort_values(["series_id", "time_idx"]).copy()
    else:
        df = df.sort_values(["timestamp"]).copy()

    if "mid" in df.columns:
        grouped = df.groupby("series_id", sort=False)["mid"]
        for lag in (1, 2, 5, 10, 20):
            df[f"mid_lag_{lag}"] = grouped.shift(lag)
        for lag in (1, 5, 20):
            lag_col = f"mid_lag_{lag}"
            if lag_col in df.columns:
                df[f"mid_ret_{lag}"] = df["mid"] - df[lag_col]

    if "spread" in df.columns:
        df["spread_lag_1"] = df.groupby("series_id", sort=False)["spread"].shift(1)
    if "obi" in df.columns:
        df["obi_lag_1"] = df.groupby("series_id", sort=False)["obi"].shift(1)

    resolved_group = _resolve_group_key(df, group_key=spec.group_key)
    if spec.group_key == "group_key":
        df["group_key"] = resolved_group
    else:
        df[spec.group_key] = resolved_group

    if _policy_debug_enabled():
        ticker_nonempty = _normalize_string_series(df["ticker"]).ne("")
        unknown_series = _normalize_string_series(df["series_ticker"]).str.lower().eq("unknown")
        denom = int(ticker_nonempty.sum())
        if denom > 0:
            pct_unknown = float((unknown_series & ticker_nonempty).sum() / denom)
        else:
            pct_unknown = float("nan")
        _policy_debug(
            f"[POLICY_DEBUG] pct_unknown_series_ticker={pct_unknown:.6f} "
            f"nonempty_ticker_rows={denom}"
        )
        top_counts = df["series_ticker"].astype(str).value_counts().head(10)
        _policy_debug("[POLICY_DEBUG] top_series_ticker_counts:")
        for key, value in top_counts.items():
            _policy_debug(f"  {key}: {int(value)}")

    df = df.reset_index(drop=True)
    return df


def tradable_mask(df: pd.DataFrame, spec: PolicySpec) -> pd.Series:
    mask = pd.Series(True, index=df.index)

    if "spread" in df.columns:
        mask &= df["spread"].notna()
        mask &= df["spread"] <= float(spec.max_spread)
    if "staleness_sec" in df.columns:
        mask &= df["staleness_sec"].notna()
        mask &= df["staleness_sec"] <= float(spec.max_staleness_sec)
    if spec.require_has_obs and "has_obs" in df.columns:
        mask &= df["has_obs"].fillna(0).astype("int64") == 1
    return mask


def is_tradable(
    *,
    spread: float | int | None,
    staleness_sec: float | int | None,
    has_obs: float | int | None,
    max_spread: float,
    max_staleness_sec: float,
    require_has_obs: bool = True,
) -> bool:
    if pd.isna(spread) or pd.isna(staleness_sec):
        return False
    try:
        spread_v = float(spread)
        staleness_v = float(staleness_sec)
    except (TypeError, ValueError):
        return False
    if require_has_obs:
        if pd.isna(has_obs):
            return False
        try:
            has_obs_v = int(float(has_obs))
        except (TypeError, ValueError):
            return False
        if has_obs_v != 1:
            return False
    return (spread_v <= float(max_spread)) and (staleness_v <= float(max_staleness_sec))


def _row_tradable_with_thresholds(
    *,
    bid: float | int | None,
    ask: float | int | None,
    spread: float | int | None,
    staleness_sec: float | int | None,
    has_obs: float | int | None,
    max_spread: float,
    max_staleness_sec: float,
    require_has_obs: bool = True,
) -> bool:
    # Keep rule parity with entry screening (tradable_mask/is_tradable): spread + staleness + has_obs.
    _ = bid
    _ = ask
    return is_tradable(
        spread=spread,
        staleness_sec=staleness_sec,
        has_obs=has_obs,
        max_spread=max_spread,
        max_staleness_sec=max_staleness_sec,
        require_has_obs=require_has_obs,
    )


def _apply_fee_per_trade(gross_pnl: pd.Series, fee_per_trade: float) -> pd.Series:
    return gross_pnl.astype("float64") - float(fee_per_trade)


def _write_debug_top_winners(
    *,
    run_dir: str | Path | None,
    split_name: str,
    trade_log: pd.DataFrame,
) -> None:
    out_dir = Path(run_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    top_winners = trade_log.sort_values("realized_pnl_net", ascending=False, na_position="last").head(20)
    out_path = out_dir / f"debug_top_winners_{split_name}.csv"
    top_winners.to_csv(out_path, index=False)


def split_by_time(df: pd.DataFrame, *, val_days: int, test_days: int) -> dict[str, pd.DataFrame]:
    if df.empty:
        return {"train": df.copy(), "val": df.copy(), "test": df.copy()}

    out = df.copy()
    dates = out["timestamp"].dt.date
    unique_dates = sorted(dates.dropna().unique())
    if not unique_dates:
        return {"train": out.iloc[0:0].copy(), "val": out.iloc[0:0].copy(), "test": out.iloc[0:0].copy()}

    test_days = max(int(test_days), 0)
    val_days = max(int(val_days), 0)

    test_dates = set(unique_dates[-test_days:]) if test_days > 0 else set()
    val_end = max(0, len(unique_dates) - test_days)
    val_start = max(0, val_end - val_days)
    val_dates = set(unique_dates[val_start:val_end]) if val_days > 0 else set()

    test_mask = dates.isin(test_dates)
    val_mask = dates.isin(val_dates)
    train_mask = ~(test_mask | val_mask)

    return {
        "train": out.loc[train_mask].copy(),
        "val": out.loc[val_mask].copy(),
        "test": out.loc[test_mask].copy(),
    }


def train_models(
    train_df: pd.DataFrame,
    feature_cols: List[str],
    *,
    random_state: int = 42,
    use_sample_weight: bool = False,
) -> dict:
    if train_df.empty:
        raise ValueError("train_df is empty")
    if not feature_cols:
        raise ValueError("feature_cols is empty")

    fit_df = train_df.dropna(subset=["y_long", "y_short"]).copy()
    if fit_df.empty:
        raise ValueError("No rows with non-null y_long/y_short in training split")

    x_train = fit_df[feature_cols].astype("float64")
    y_long = fit_df["y_long"].astype("float64")
    y_short = fit_df["y_short"].astype("float64")

    params = dict(
        max_depth=6,
        learning_rate=0.05,
        max_iter=300,
        l2_regularization=1e-3,
        random_state=random_state,
    )

    model_long = HistGradientBoostingRegressor(**params)
    model_short = HistGradientBoostingRegressor(**params)

    if use_sample_weight:
        weight_long = (1.0 + y_long.abs()).clip(upper=10.0)
        weight_short = (1.0 + y_short.abs()).clip(upper=10.0)
        model_long.fit(x_train, y_long, sample_weight=weight_long)
        model_short.fit(x_train, y_short, sample_weight=weight_short)
    else:
        model_long.fit(x_train, y_long)
        model_short.fit(x_train, y_short)

    return {
        "model_long": model_long,
        "model_short": model_short,
        "feature_cols": list(feature_cols),
        "random_state": int(random_state),
        "use_sample_weight": bool(use_sample_weight),
    }


def _cooldown_select(
    candidates: pd.DataFrame,
    *,
    group_col: str,
    timestamp_col: str,
    score_col: str,
    cooldown_td: pd.Timedelta,
    enforce_cooldown: bool,
) -> pd.DataFrame:
    if candidates.empty:
        return candidates.copy()

    ordered = candidates.sort_values([timestamp_col, score_col], ascending=[True, False]).copy()
    ordered = ordered.drop_duplicates(subset=[timestamp_col, group_col], keep="first")

    if not enforce_cooldown:
        return ordered

    next_allowed: dict[str, pd.Timestamp] = {}
    keep_idx: List[int] = []
    for idx, row in ordered.iterrows():
        group = str(row[group_col])
        ts = row[timestamp_col]
        allow_after = next_allowed.get(group)
        if allow_after is not None and ts < allow_after:
            continue
        keep_idx.append(idx)
        next_allowed[group] = ts + cooldown_td
    return ordered.loc[keep_idx].copy()


def _cooldown_select_optimal(
    candidates: pd.DataFrame,
    *,
    group_col: str,
    timestamp_col: str,
    score_col: str,
    cooldown_td: pd.Timedelta,
    enforce_cooldown: bool,
) -> pd.DataFrame:
    """
    Optimal cooldown selector per group using weighted interval scheduling.
    """
    if candidates.empty:
        return candidates.copy()

    ordered = candidates.sort_values([timestamp_col, score_col], ascending=[True, False]).copy()
    ordered = ordered.drop_duplicates(subset=[timestamp_col, group_col], keep="first")

    if not enforce_cooldown:
        return ordered

    cooldown_ns = int(cooldown_td.value)
    chosen_idx: List[int] = []

    for _, group_df in ordered.groupby(group_col, sort=False):
        group_df = group_df.sort_values(timestamp_col).copy()
        if group_df.empty:
            continue
        if len(group_df) == 1:
            chosen_idx.append(int(group_df.index[0]))
            continue

        ts_ns = group_df[timestamp_col].astype("int64").to_numpy()
        weights = group_df[score_col].astype("float64").to_numpy()

        prev = np.searchsorted(ts_ns, ts_ns - cooldown_ns, side="right") - 1
        n = len(group_df)
        dp = np.zeros(n, dtype="float64")

        for i in range(n):
            take = weights[i] + (dp[prev[i]] if prev[i] >= 0 else 0.0)
            skip = dp[i - 1] if i > 0 else 0.0
            dp[i] = take if take > skip else skip

        chosen_local: List[int] = []
        i = n - 1
        while i >= 0:
            take = weights[i] + (dp[prev[i]] if prev[i] >= 0 else 0.0)
            skip = dp[i - 1] if i > 0 else 0.0
            if take > skip:
                chosen_local.append(i)
                i = prev[i]
            else:
                i -= 1

        chosen_local.reverse()
        idx_values = group_df.index.to_numpy()
        for loc in chosen_local:
            chosen_idx.append(int(idx_values[loc]))

    if not chosen_idx:
        return ordered.iloc[0:0].copy()
    return ordered.loc[chosen_idx].sort_values([timestamp_col, score_col], ascending=[True, False]).copy()


def _trade_metrics(
    *,
    n_rows: int,
    n_candidates: int,
    trades: pd.DataFrame,
) -> dict:
    n_trades = int(len(trades))
    if n_trades == 0:
        avg_realized_gross = float("nan")
        median_realized_gross = float("nan")
        sum_realized_gross = 0.0
        hit_rate_gross = float("nan")
        avg_realized_net = float("nan")
        median_realized_net = float("nan")
        sum_realized_net = 0.0
        hit_rate_net = float("nan")
        avg_pred_edge = float("nan")
    else:
        if "realized_pnl_gross" in trades.columns:
            realized_gross = trades["realized_pnl_gross"].astype("float64")
        elif "realized_pnl" in trades.columns:
            realized_gross = trades["realized_pnl"].astype("float64")
        else:
            raise ValueError("Trade log missing realized_pnl_gross/realized_pnl columns")

        if "realized_pnl_net" in trades.columns:
            realized_net = trades["realized_pnl_net"].astype("float64")
        elif "realized_pnl" in trades.columns:
            realized_net = trades["realized_pnl"].astype("float64")
        else:
            raise ValueError("Trade log missing realized_pnl_net/realized_pnl columns")

        avg_realized_gross = float(realized_gross.mean())
        median_realized_gross = float(realized_gross.median())
        sum_realized_gross = float(realized_gross.sum())
        hit_rate_gross = float((realized_gross > 0).mean())

        avg_realized_net = float(realized_net.mean())
        median_realized_net = float(realized_net.median())
        sum_realized_net = float(realized_net.sum())
        hit_rate_net = float((realized_net > 0).mean())

        if "pred_edge" in trades.columns:
            avg_pred_edge = float(trades["pred_edge"].mean())
        else:
            avg_pred_edge = float("nan")

    trade_rate = float(n_trades / n_rows) if n_rows > 0 else 0.0
    return {
        "n_rows": int(n_rows),
        "n_candidates": int(n_candidates),
        "n_trades": int(n_trades),
        "trade_rate": trade_rate,
        # Backward-compatible keys now represent NET values.
        "avg_realized_pnl": avg_realized_net,
        "median_realized_pnl": median_realized_net,
        "sum_realized_pnl": sum_realized_net,
        "hit_rate": hit_rate_net,
        "avg_realized_pnl_gross": avg_realized_gross,
        "median_realized_pnl_gross": median_realized_gross,
        "sum_realized_pnl_gross": sum_realized_gross,
        "hit_rate_gross": hit_rate_gross,
        "avg_realized_pnl_net": avg_realized_net,
        "median_realized_pnl_net": median_realized_net,
        "sum_realized_pnl_net": sum_realized_net,
        "hit_rate_net": hit_rate_net,
        "avg_pred_edge": avg_pred_edge,
    }


def _trade_log_columns(group_col: str) -> List[str]:
    return [
        "timestamp",
        "ticker",
        group_col,
        "series_id",
        "time_idx",
        "side",
        "pred_long",
        "pred_short",
        "pred_edge",
        "realized_pnl",
        "realized_pnl_gross",
        "realized_pnl_net",
        "pnl_conservative_gross",
        "pnl_conservative_net",
        "spread",
        "staleness_sec",
        "has_obs",
        "entry_timestamp",
        "entry_time_idx",
        "entry_bid",
        "entry_ask",
        "entry_mid",
        "entry_spread",
        "entry_staleness_sec",
        "entry_has_obs",
        "entry_price_used",
        "exit_timestamp",
        "exit_time_idx",
        "exit_bid",
        "exit_ask",
        "exit_mid",
        "exit_spread",
        "exit_staleness_sec",
        "exit_has_obs",
        "exit_price_used",
        "horizon_dt_sec",
        "exit_tradable",
    ]


def make_trade_log_row(row: pd.Series, *, group_col: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for column in _trade_log_columns(group_col):
        out[column] = row.get(column, np.nan)
    return out


def _build_trade_log_frame(trades: pd.DataFrame, *, group_col: str) -> pd.DataFrame:
    columns = _trade_log_columns(group_col)
    if trades.empty:
        return pd.DataFrame(columns=columns)

    rows = [make_trade_log_row(row, group_col=group_col) for _, row in trades.iterrows()]
    return pd.DataFrame.from_records(rows, columns=columns)


def _resolve_quote_columns(frame: pd.DataFrame) -> tuple[str | None, str | None, str | None, str | None]:
    bid_col = "bid" if "bid" in frame.columns else ("bid_yes" if "bid_yes" in frame.columns else None)
    ask_col = "ask" if "ask" in frame.columns else ("ask_yes" if "ask_yes" in frame.columns else None)
    mid_col = "mid" if "mid" in frame.columns else ("mid_yes" if "mid_yes" in frame.columns else None)
    spread_col = "spread" if "spread" in frame.columns else ("spread_yes" if "spread_yes" in frame.columns else None)
    return bid_col, ask_col, mid_col, spread_col


def _series_or_default(frame: pd.DataFrame, column: str | None, *, default: Any = np.nan) -> pd.Series:
    if column is not None and column in frame.columns:
        return frame[column]
    return pd.Series(default, index=frame.index)


def _coalesce_mid_spread(
    *,
    bid: pd.Series,
    ask: pd.Series,
    mid_raw: pd.Series,
    spread_raw: pd.Series,
) -> tuple[pd.Series, pd.Series]:
    bid_num = pd.to_numeric(bid, errors="coerce")
    ask_num = pd.to_numeric(ask, errors="coerce")
    mid_num = pd.to_numeric(mid_raw, errors="coerce")
    spread_num = pd.to_numeric(spread_raw, errors="coerce")

    calc_mid = (bid_num + ask_num) / 2.0
    calc_spread = ask_num - bid_num
    mid = mid_num.where(mid_num.notna(), calc_mid)
    spread = spread_num.where(spread_num.notna(), calc_spread)
    return mid, spread


def _build_bars_lookup(bars_df: pd.DataFrame) -> pd.DataFrame:
    if ("series_id" not in bars_df.columns) or ("time_idx" not in bars_df.columns):
        return pd.DataFrame()

    lookup = bars_df.copy()
    lookup["_lookup_time_idx"] = pd.to_numeric(lookup["time_idx"], errors="coerce").astype("Int64")
    lookup = lookup.dropna(subset=["series_id", "_lookup_time_idx"]).copy()
    if lookup.empty:
        return lookup

    if "timestamp" in lookup.columns:
        lookup = lookup.sort_values("timestamp").copy()
    lookup = lookup.drop_duplicates(subset=["series_id", "_lookup_time_idx"], keep="last")
    lookup["_exit_row_exists"] = 1
    lookup = lookup.set_index(["series_id", "_lookup_time_idx"], drop=False)
    return lookup


def _populate_trade_log_audit_fields(
    trades: pd.DataFrame,
    *,
    bars_df: pd.DataFrame,
    spec: PolicySpec,
) -> pd.DataFrame:
    out = trades.copy()
    if out.empty:
        return out

    bid_col, ask_col, mid_col, spread_col = _resolve_quote_columns(out)
    entry_bid = _series_or_default(out, bid_col)
    entry_ask = _series_or_default(out, ask_col)
    entry_mid_raw = _series_or_default(out, mid_col)
    entry_spread_raw = _series_or_default(out, spread_col)
    entry_mid, entry_spread = _coalesce_mid_spread(
        bid=entry_bid,
        ask=entry_ask,
        mid_raw=entry_mid_raw,
        spread_raw=entry_spread_raw,
    )

    out["entry_timestamp"] = out["timestamp"] if "timestamp" in out.columns else pd.NaT
    if "time_idx" in out.columns:
        out["entry_time_idx"] = pd.to_numeric(out["time_idx"], errors="coerce").astype("Int64")
    else:
        out["entry_time_idx"] = pd.Series(pd.NA, index=out.index, dtype="Int64")
    out["entry_bid"] = pd.to_numeric(entry_bid, errors="coerce")
    out["entry_ask"] = pd.to_numeric(entry_ask, errors="coerce")
    out["entry_mid"] = entry_mid
    out["entry_spread"] = entry_spread
    out["entry_staleness_sec"] = pd.to_numeric(
        _series_or_default(out, "staleness_sec"),
        errors="coerce",
    )
    out["entry_has_obs"] = _series_or_default(out, "has_obs")
    out["entry_price_used"] = np.where(
        out["side"] == "LONG",
        out["entry_ask"],
        out["entry_bid"],
    )

    out["exit_time_idx"] = out["entry_time_idx"] + int(spec.horizon_steps)
    lookup = _build_bars_lookup(bars_df)
    series_id_values = _series_or_default(out, "series_id", default=np.nan)

    if not lookup.empty:
        exit_keys = pd.MultiIndex.from_arrays(
            [series_id_values, out["exit_time_idx"].astype("Int64")],
            names=["series_id", "_lookup_time_idx"],
        )
        exit_rows = lookup.reindex(exit_keys).copy()
        exit_rows.index = out.index
    else:
        exit_rows = pd.DataFrame(index=out.index)

    exit_bid_col, exit_ask_col, exit_mid_col, exit_spread_col = _resolve_quote_columns(
        lookup if not lookup.empty else bars_df
    )
    exit_bid = _series_or_default(exit_rows, exit_bid_col)
    exit_ask = _series_or_default(exit_rows, exit_ask_col)
    exit_mid_raw = _series_or_default(exit_rows, exit_mid_col)
    exit_spread_raw = _series_or_default(exit_rows, exit_spread_col)
    exit_mid, exit_spread = _coalesce_mid_spread(
        bid=exit_bid,
        ask=exit_ask,
        mid_raw=exit_mid_raw,
        spread_raw=exit_spread_raw,
    )

    has_exit_row = (
        _series_or_default(exit_rows, "_exit_row_exists", default=0).fillna(0).astype("int64") == 1
    )

    out["exit_timestamp"] = _series_or_default(exit_rows, "timestamp", default=pd.NaT)
    out["exit_bid"] = pd.to_numeric(exit_bid, errors="coerce")
    out["exit_ask"] = pd.to_numeric(exit_ask, errors="coerce")
    out["exit_mid"] = exit_mid
    out["exit_spread"] = exit_spread
    out["exit_staleness_sec"] = pd.to_numeric(
        _series_or_default(exit_rows, "staleness_sec"),
        errors="coerce",
    )
    exit_has_obs_raw = pd.to_numeric(
        _series_or_default(exit_rows, "has_obs"),
        errors="coerce",
    )
    out["exit_has_obs"] = exit_has_obs_raw.where(has_exit_row, 0)
    out["exit_price_used"] = np.where(
        out["side"] == "LONG",
        out["exit_bid"],
        out["exit_ask"],
    )

    horizon_dt_sec = (
        pd.to_datetime(out["exit_timestamp"], errors="coerce")
        - pd.to_datetime(out["entry_timestamp"], errors="coerce")
    ).dt.total_seconds()
    out["horizon_dt_sec"] = horizon_dt_sec.where(has_exit_row, None)

    exit_tradable: List[bool] = []
    for present, bid, ask, spread, staleness, has_obs in zip(
        has_exit_row.tolist(),
        out["exit_bid"],
        out["exit_ask"],
        out["exit_spread"],
        out["exit_staleness_sec"],
        out["exit_has_obs"],
    ):
        if not present:
            exit_tradable.append(False)
            continue
        exit_tradable.append(
            _row_tradable_with_thresholds(
                bid=bid,
                ask=ask,
                spread=spread,
                staleness_sec=staleness,
                has_obs=has_obs,
                max_spread=spec.max_spread,
                max_staleness_sec=spec.max_staleness_sec,
                require_has_obs=spec.require_has_obs,
            )
        )
    out["exit_tradable"] = exit_tradable
    return out


def backtest_policy(
    df: pd.DataFrame,
    models: dict,
    feature_cols: List[str],
    *,
    spec: PolicySpec,
    split_name: str | None = None,
    run_dir: str | Path | None = None,
) -> Tuple[pd.DataFrame, dict]:
    fee_per_trade = float(spec.fee_per_trade)
    split_label = split_name or str(df.attrs.get("split_name", "unknown"))
    if df.empty:
        empty_log = _build_trade_log_frame(pd.DataFrame(), group_col=spec.group_key)
        metrics = _trade_metrics(n_rows=0, n_candidates=0, trades=empty_log)
        metrics.update(
            {
                "oracle_n_trades": 0,
                "oracle_trade_rate": 0.0,
                "oracle_avg_realized_pnl": float("nan"),
                "oracle_median_realized_pnl": float("nan"),
                "oracle_sum_realized_pnl": 0.0,
                "oracle_hit_rate": float("nan"),
                "oracle_avg_realized_pnl_gross": float("nan"),
                "oracle_median_realized_pnl_gross": float("nan"),
                "oracle_sum_realized_pnl_gross": 0.0,
                "oracle_hit_rate_gross": float("nan"),
                "oracle_avg_realized_pnl_net": float("nan"),
                "oracle_median_realized_pnl_net": float("nan"),
                "oracle_sum_realized_pnl_net": 0.0,
                "oracle_hit_rate_net": float("nan"),
            }
        )
        return empty_log, metrics

    eval_df = df.copy()
    model_long = models["model_long"]
    model_short = models["model_short"]

    x_eval = eval_df[feature_cols].astype("float64")
    eval_df["pred_long"] = model_long.predict(x_eval)
    eval_df["pred_short"] = model_short.predict(x_eval)
    eval_df["pred_edge"] = np.maximum(eval_df["pred_long"], eval_df["pred_short"])
    eval_df["pred_side"] = np.where(eval_df["pred_long"] >= eval_df["pred_short"], "LONG", "SHORT")

    if spec.group_key in eval_df.columns:
        group_col = spec.group_key
    elif "group_key" in eval_df.columns:
        group_col = "group_key"
    else:
        group_col = "series_id"
    if group_col not in eval_df.columns:
        eval_df[group_col] = "unknown"

    eval_df["is_tradable"] = tradable_mask(eval_df, spec)
    selection_mode = str(spec.selection_mode).strip().lower()
    if selection_mode == "topk":
        k = max(int(spec.topk_per_day), 0)
        eval_df["edge_ok"] = True
        eval_df["is_candidate"] = False
        candidates = eval_df.loc[eval_df["is_tradable"]].copy()
        if k > 0 and not candidates.empty:
            candidates = (
                candidates.assign(_date=candidates["timestamp"].dt.date)
                .sort_values(["_date", "pred_edge"], ascending=[True, False])
                .groupby("_date", sort=False, group_keys=False)
                .head(k)
                .drop(columns=["_date"])
            )
            eval_df.loc[candidates.index, "is_candidate"] = True
        else:
            candidates = candidates.iloc[0:0].copy()
    else:
        eval_df["edge_ok"] = eval_df["pred_edge"] >= float(spec.min_edge)
        eval_df["is_candidate"] = eval_df["is_tradable"] & eval_df["edge_ok"]
        candidates = eval_df.loc[eval_df["is_candidate"]].copy()

    freq_seconds = float(pd.Timedelta(spec.freq).total_seconds())
    horizon_td = pd.Timedelta(seconds=int(spec.horizon_steps * freq_seconds))
    selected = _cooldown_select(
        candidates,
        group_col=group_col,
        timestamp_col="timestamp",
        score_col="pred_edge",
        cooldown_td=horizon_td,
        enforce_cooldown=spec.enforce_group_cooldown,
    )

    if selected.empty:
        trade_log = selected.copy()
    else:
        selected["side"] = selected["pred_side"]
        selected["realized_pnl_gross"] = np.where(
            selected["side"] == "LONG",
            selected["pnl_long_h"],
            selected["pnl_short_h"],
        )
        selected["realized_pnl_net"] = _apply_fee_per_trade(selected["realized_pnl_gross"], fee_per_trade)
        selected["realized_pnl"] = selected["realized_pnl_net"]
        selected = _populate_trade_log_audit_fields(selected, bars_df=eval_df, spec=spec)
        selected["pnl_conservative_gross"] = np.where(
            selected["side"] == "LONG",
            selected["exit_bid"] - selected["entry_ask"],
            selected["entry_bid"] - selected["exit_ask"],
        )
        selected["pnl_conservative_net"] = _apply_fee_per_trade(
            selected["pnl_conservative_gross"],
            fee_per_trade,
        )
        trade_log = selected.copy()

    trade_log = _build_trade_log_frame(trade_log, group_col=group_col)

    metrics = _trade_metrics(
        n_rows=len(eval_df),
        n_candidates=int(eval_df["is_candidate"].sum()),
        trades=trade_log,
    )

    oracle_base = eval_df.loc[eval_df["is_tradable"]].copy()
    if oracle_base.empty:
        oracle_log = oracle_base
    else:
        oracle_base["oracle_side_best"] = np.where(
            oracle_base["pnl_long_h"] >= oracle_base["pnl_short_h"], "LONG", "SHORT"
        )
        oracle_base["oracle_realized_best_gross"] = np.where(
            oracle_base["oracle_side_best"] == "LONG",
            oracle_base["pnl_long_h"],
            oracle_base["pnl_short_h"],
        )
        oracle_base["oracle_realized_best_net"] = (
            oracle_base["oracle_realized_best_gross"] - fee_per_trade
        )

        # Oracle includes HOLD by only considering strictly positive net realized value.
        oracle_candidates = oracle_base.loc[oracle_base["oracle_realized_best_net"] > 0].copy()

        oracle_selected = _cooldown_select_optimal(
            oracle_candidates,
            group_col=group_col,
            timestamp_col="timestamp",
            score_col="oracle_realized_best_net",
            cooldown_td=horizon_td,
            enforce_cooldown=spec.enforce_group_cooldown,
        )
        oracle_log = oracle_selected

    n_oracle = int(len(oracle_log))
    if n_oracle == 0:
        metrics.update(
            {
                "oracle_n_trades": 0,
                "oracle_trade_rate": 0.0,
                "oracle_avg_realized_pnl": float("nan"),
                "oracle_median_realized_pnl": float("nan"),
                "oracle_sum_realized_pnl": 0.0,
                "oracle_hit_rate": 1.0,
                "oracle_avg_realized_pnl_gross": float("nan"),
                "oracle_median_realized_pnl_gross": float("nan"),
                "oracle_sum_realized_pnl_gross": 0.0,
                "oracle_hit_rate_gross": 1.0,
                "oracle_avg_realized_pnl_net": float("nan"),
                "oracle_median_realized_pnl_net": float("nan"),
                "oracle_sum_realized_pnl_net": 0.0,
                "oracle_hit_rate_net": 1.0,
            }
        )
    else:
        oracle_avg_gross = float(oracle_log["oracle_realized_best_gross"].mean())
        oracle_med_gross = float(oracle_log["oracle_realized_best_gross"].median())
        oracle_sum_gross = float(oracle_log["oracle_realized_best_gross"].sum())
        oracle_hit_gross = float((oracle_log["oracle_realized_best_gross"] > 0).mean())
        oracle_avg_net = float(oracle_log["oracle_realized_best_net"].mean())
        oracle_med_net = float(oracle_log["oracle_realized_best_net"].median())
        oracle_sum_net = float(oracle_log["oracle_realized_best_net"].sum())
        oracle_hit_net = float((oracle_log["oracle_realized_best_net"] > 0).mean())
        metrics.update(
            {
                "oracle_n_trades": n_oracle,
                "oracle_trade_rate": float(n_oracle / len(eval_df)),
                # Backward-compatible keys now represent NET values.
                "oracle_avg_realized_pnl": oracle_avg_net,
                "oracle_median_realized_pnl": oracle_med_net,
                "oracle_sum_realized_pnl": oracle_sum_net,
                "oracle_hit_rate": oracle_hit_net,
                "oracle_avg_realized_pnl_gross": oracle_avg_gross,
                "oracle_median_realized_pnl_gross": oracle_med_gross,
                "oracle_sum_realized_pnl_gross": oracle_sum_gross,
                "oracle_hit_rate_gross": oracle_hit_gross,
                "oracle_avg_realized_pnl_net": oracle_avg_net,
                "oracle_median_realized_pnl_net": oracle_med_net,
                "oracle_sum_realized_pnl_net": oracle_sum_net,
                "oracle_hit_rate_net": oracle_hit_net,
            }
        )

    policy_sum_gross = float(metrics.get("sum_realized_pnl_gross", float("nan")))
    policy_sum_net = float(metrics.get("sum_realized_pnl_net", metrics.get("sum_realized_pnl", float("nan"))))
    oracle_sum_gross = float(metrics.get("oracle_sum_realized_pnl_gross", float("nan")))
    oracle_sum_net = float(
        metrics.get("oracle_sum_realized_pnl_net", metrics.get("oracle_sum_realized_pnl", float("nan")))
    )
    capture_ratio_gross = (
        float(policy_sum_gross / oracle_sum_gross)
        if np.isfinite(oracle_sum_gross) and oracle_sum_gross > 0
        else float("nan")
    )
    capture_ratio_net = (
        float(policy_sum_net / oracle_sum_net)
        if np.isfinite(oracle_sum_net) and oracle_sum_net > 0
        else float("nan")
    )
    metrics.update(
        {
            "capture_ratio_gross": capture_ratio_gross,
            "capture_ratio_net": capture_ratio_net,
        }
    )

    policy_sum = policy_sum_net
    oracle_sum = oracle_sum_net
    policy_hit = float(metrics.get("hit_rate_net", metrics.get("hit_rate", float("nan"))))
    oracle_hit = float(metrics.get("oracle_hit_rate_net", metrics.get("oracle_hit_rate", float("nan"))))

    if not np.isfinite(oracle_sum) or oracle_sum < (policy_sum - 1e-9):
        raise ValueError(
            "Oracle upper bound violated for split="
            f"{split_label}: oracle_sum_realized_pnl={oracle_sum:.6f}, "
            f"policy_sum_realized_pnl={policy_sum:.6f}, "
            f"oracle_n_trades={int(metrics.get('oracle_n_trades', 0))}, "
            f"policy_n_trades={int(metrics.get('n_trades', 0))}"
        )

    if np.isfinite(policy_hit):
        if not np.isfinite(oracle_hit) or oracle_hit < (policy_hit - 1e-9):
            raise ValueError(
                "Oracle upper bound violated for split="
                f"{split_label}: oracle_hit_rate={oracle_hit:.6f}, "
                f"policy_hit_rate={policy_hit:.6f}, "
                f"oracle_n_trades={int(metrics.get('oracle_n_trades', 0))}, "
                f"policy_n_trades={int(metrics.get('n_trades', 0))}"
            )

    return trade_log.reset_index(drop=True), metrics


def save_policy_run(
    run_dir: Path,
    spec: PolicySpec,
    metrics: dict,
    models: dict,
    trade_logs: dict[str, pd.DataFrame],
) -> None:
    run_dir = Path(run_dir).expanduser().resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    debug_enabled = os.getenv("POLICY_DEBUG") == "1"

    (run_dir / "spec.json").write_text(
        json.dumps(asdict(spec), indent=2, sort_keys=True, default=str)
    )
    (run_dir / "metrics.json").write_text(
        json.dumps(_json_safe(metrics), indent=2, sort_keys=True, default=str)
    )

    joblib.dump(models, run_dir / "models.joblib")

    for split_name, trade_log in trade_logs.items():
        out_path = run_dir / f"trade_logs_{split_name}.csv"
        trade_log.to_csv(out_path, index=False)
        if debug_enabled and not trade_log.empty:
            _write_debug_top_winners(
                run_dir=run_dir,
                split_name=split_name,
                trade_log=trade_log,
            )

    readme = (
        "Policy run artifacts\n"
        f"- min_edge: {spec.min_edge}\n"
        f"- group_key_mode: {spec.group_key_mode}\n"
        f"- selection_mode: {spec.selection_mode}\n"
        f"- topk_per_day: {spec.topk_per_day}\n"
        f"- fee_per_trade: {spec.fee_per_trade}\n"
        f"- max_spread: {spec.max_spread}\n"
        f"- max_staleness_sec: {spec.max_staleness_sec}\n"
        f"- require_has_obs: {spec.require_has_obs}\n"
        f"- enforce_group_cooldown: {spec.enforce_group_cooldown}\n"
        f"- group_key: {spec.group_key}\n"
        f"- horizon_steps: {spec.horizon_steps}\n"
        f"- freq: {spec.freq}\n"
    )
    (run_dir / "README.txt").write_text(readme)
