# Project_K Runbook (Portable Paths)

This repo is set up for:
- reliable live **SQLite WAL** writes on local disk
- large **training parquet** output that can live on an external volume
- **portable, repo-relative defaults** (no hard-coded absolute paths)

## Canonical Layout (defaults)

By default, paths resolve to:

- Repo root: `<repo>` (auto-detected from the code location; no need to be in `~/ev`)
- SQLite DB (WAL, hot writes): `<repo>/data/kalshi.db`
- OBI DB (optional): `<repo>/data/obi.db` (or same as primary DB if unset)
- Training dataset output (parquet): `<repo>/data/training`

These paths are centralized in `services/paths.py` and can be overridden via environment variables.

## Path 1 Storage Strategy (symlink training to external)

On the always-on headless host (your 2017 MBP), keep code + DB local, and point training output at the external drive via a symlink:

```bash
cd /path/to/Project_K

# Create the external directory (pick your volume name)
mkdir -p "/Volumes/<VOLUME_NAME>/Project_K/training"

# Replace the in-repo folder with a symlink
rm -rf data/training
ln -s "/Volumes/<VOLUME_NAME>/Project_K/training" data/training
```

Result:
- code continues writing to `<repo>/data/training` (portable)
- the external drive actually stores the bytes

If the external drive isn’t mounted:
- default behavior is **fail fast**
- set `TRAINING_FALLBACK_LOCAL=1` to write to `<repo>/data/training_staging` temporarily

## Environment Variables

- `PROJECT_K_ROOT` (optional; overrides repo auto-detection)
- `PROJECT_K_DATA_DIR` (optional; defaults to `<repo>/data`)
- `PROJECT_K_DB_PATH` (optional; defaults to `<repo>/data/kalshi.db`)
- `PROJECT_K_OBI_DB_PATH` (optional; defaults to `PROJECT_K_DB_PATH`)
- `PROJECT_K_TRAINING_DIR` (optional; defaults to `<repo>/data/training`)
- `PROJECT_K_ARTIFACTS_DIR` (optional; defaults to `<repo>/artifacts`)

Safety flags:
- `ALLOW_EXTERNAL_DB=1` to allow a DB path under `/Volumes/...` (default: refuse)
- `TRAINING_FALLBACK_LOCAL=1` to fall back to `<repo>/data/training_staging` if the external volume is not mounted

## Start (Two Terminals)

Terminal A (Discovery):

```bash
cd /path/to/Project_K
python main.py
```

Terminal B (OBI tracker):

```bash
cd /path/to/Project_K
python -m scripts.obi_tracker
```

If you split OBI into a separate DB file, set `PROJECT_K_OBI_DB_PATH` (see `.env.example`).

## Supervisor (Optional)

`run_supervised.sh` restarts `main.py` only when it exits with code `2` (watchdog).

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

## Verify Training Writes

```bash
# portable: always inspect via repo-relative path
ls -la data/training | head

# or, if you explicitly set PROJECT_K_TRAINING_DIR
ls -la "$PROJECT_K_TRAINING_DIR" | head
```

New partitions should appear under the training directory when Discovery is running (partitioned by date/category).
