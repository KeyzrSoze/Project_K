import inspect
import pandas as pd
from services.ml.policy import PolicySpec, build_policy_frame

training_dir = "/Volumes/external/ev/Project_K/data/training"

spec = PolicySpec(
    freq="30s",
    horizon_steps=20,
    max_gap="2h",
    min_bars_per_segment=150,
    max_spread=5,
    max_staleness_sec=60,
    selection_mode="topk",
    topk_per_day=10,
    group_key_mode="event3",
    fee_per_trade=2.0,
)

sig = inspect.signature(build_policy_frame)
print("build_policy_frame signature:", sig)

# Build kwargs only from parameters the function actually accepts
kwargs = {}
for name in sig.parameters.keys():
    if name in ("training_dir", "training_path", "data_dir", "root_dir"):
        kwargs[name] = training_dir
    elif name in ("start_date",):
        kwargs[name] = "2026-02-06"
    elif name in ("end_date",):
        kwargs[name] = "2026-02-11"
    elif name in ("spec", "policy_spec"):
        kwargs[name] = spec
    # If your function supports any of these explicitly, pass them too
    elif name == "min_bars_per_segment":
        kwargs[name] = 150
    elif name == "max_gap":
        kwargs[name] = "2h"
    elif name == "horizon_steps":
        kwargs[name] = 20
    elif name == "freq":
        kwargs[name] = "30s"

df = build_policy_frame(**kwargs)

# Ensure timestamp is datetime
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
df = df.dropna(subset=["timestamp"])

# Per-day completeness signals
day = df["timestamp"].dt.date
print("\nRows per day:")
print(day.value_counts().sort_index())

print("\nMin/Max timestamp per day:")
print(df.groupby(day)["timestamp"].agg(["min", "max", "count"]).sort_index())
