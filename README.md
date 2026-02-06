# Project_K Runbook (DB Local, Training External)

This repo is set up for reliable live SQLite writes on the internal SSD while keeping large training output on an external volume.

## Canonical Paths

- Repo root (recommended): `~/ev/Project_K`
- SQLite DB (WAL, hot writes): `~/ev/Project_K/data/kalshi.db`
- OBI DB (optional, recommended): `~/ev/Project_K/data/obi.db`
- Training dataset output (parquet): `/Volumes/external/ev/Project_K/data/training`

These paths are centralized in `services/db.py` and can be overridden via environment variables.

## Migration Notes (Idempotent)

1) Stop all writers (Discovery + OBI tracker) so there are no open SQLite handles.

2) If you are copying a WAL-mode DB from another location, checkpoint first:

```bash
sqlite3 /path/to/source/kalshi.db "pragma wal_checkpoint(truncate);"
```

3) Copy the DB and sidecars together:

- `kalshi.db`
- `kalshi.db-wal`
- `kalshi.db-shm`

into `~/ev/Project_K/data/`.

Re-running these steps is safe as long as processes are stopped during the copy.

## Environment Variables

- `PROJECT_K_DB_PATH` (default: `~/ev/Project_K/data/kalshi.db`)
- `PROJECT_K_OBI_DB_PATH` (default: `PROJECT_K_DB_PATH`; recommended: `~/ev/Project_K/data/obi.db`)
- `PROJECT_K_TRAINING_DIR` (default: `/Volumes/external/ev/Project_K/data/training`)
- `PROJECT_K_DB_READ_WORKERS` (default: `4`)
- `PROJECT_K_ROOT` (optional; used for resolving relative key paths)

Safety flags:

- `ALLOW_EXTERNAL_DB=1` to allow a DB path under `/Volumes/...` (default: refuse)
- `TRAINING_FALLBACK_LOCAL=1` to write to `~/ev/Project_K/data/training_staging` if the external volume is not mounted (default: fail fast)

## Start (Two Terminals)

Terminal A (Discovery):

```bash
cd ~/ev/Project_K
export PROJECT_K_DB_PATH=~/ev/Project_K/data/kalshi.db
# Recommended to reduce cross-process writer contention:
export PROJECT_K_OBI_DB_PATH=~/ev/Project_K/data/obi.db
export PROJECT_K_TRAINING_DIR=/Volumes/external/ev/Project_K/data/training
export PROJECT_K_DB_READ_WORKERS=4
python main.py
```

Terminal B (OBI tracker):

```bash
cd ~/ev/Project_K
export PROJECT_K_DB_PATH=~/ev/Project_K/data/kalshi.db
export PROJECT_K_OBI_DB_PATH=~/ev/Project_K/data/obi.db
export PROJECT_K_TRAINING_DIR=/Volumes/external/ev/Project_K/data/training
python -m scripts.obi_tracker
```

At startup, both processes log PIDs and DB paths. If you split OBI, `db_primary=...` and `db_obi=...` should differ.

If you are enabling an OBI DB split on an existing primary DB, migrate existing OBI rows once (optional):

```bash
python -m scripts.migrate_obi_db --source "$PROJECT_K_DB_PATH" --dest "$PROJECT_K_OBI_DB_PATH"
```

## Supervisor (Optional)

`run_supervised.sh` exports the canonical path env vars and restarts `main.py` only when it exits with code `2` (watchdog).

```bash
./run_supervised.sh
```

## Verify DB Is Local + WAL

```bash
sqlite3 "$PROJECT_K_DB_PATH" "pragma journal_mode; pragma busy_timeout;"
```

Expected:

- `wal`
- `busy_timeout` is non-zero

Tables:

```bash
sqlite3 "$PROJECT_K_DB_PATH" ".tables"
```

Expected to include at least: `market_metrics`, `market_obi`

## Verify Freshness While Running

```bash
sqlite3 "$PROJECT_K_DB_PATH" "select datetime(max(timestamp),'unixepoch','localtime'), (strftime('%s','now')-max(timestamp)) as stale_s from market_metrics;"
sqlite3 "$PROJECT_K_OBI_DB_PATH" "select count(*) from market_obi;"
```

- `stale_s` should stay reasonably low during active runs (typically < 60-120s).
- `market_obi` count should be non-zero and generally increasing/stable while `scripts.obi_tracker` runs.

## Verify Training Writes Go To External

```bash
ls -la /Volumes/external/ev/Project_K/data/training | head
```

New partitions should appear under the external training directory when Discovery is running (parquet output is partitioned by date/category).

## Soak Test (60+ Minutes)

1) Run `python main.py` and `python -m scripts.obi_tracker` together for 60+ minutes.

2) While running, periodically check:

```bash
sqlite3 "$PROJECT_K_DB_PATH" "select (strftime('%s','now')-max(timestamp)) as stale_s from market_metrics;"
sqlite3 "$PROJECT_K_OBI_DB_PATH" "select count(*) from market_obi;"
```

3) Expected:

- Heartbeat continues to log non-zero `max_ts` and reasonable `stale_s`.
- If DB health checks fail repeatedly, the DB circuit breaker restarts the harvester and (if still unhealthy for >10 minutes after reinit attempts) exits with code `2` for supervisor restart.

## Debugging “Alive But Stalled” (Stack Dump)

`main.py` installs a SIGUSR1 handler that dumps stack traces for all threads. While the process is running:

```bash
kill -USR1 <pid>
```
