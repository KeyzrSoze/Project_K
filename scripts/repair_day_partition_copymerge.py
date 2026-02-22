#!/usr/bin/env python3
import argparse
import hashlib
import os
from pathlib import Path
import shutil
import sys
from datetime import datetime

def sha256_file(p: Path, block_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        while True:
            b = f.read(block_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def iter_parquets(root: Path):
    # Recursively find parquet files; ignores hidden junk but keeps real data.
    for p in root.rglob("*.parquet"):
        if p.name.startswith("._"):  # macOS resource fork files (not real parquet)
            continue
        yield p

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dst", required=True, help="Destination day partition dir (will be rebuilt)")
    ap.add_argument("--src", required=True, nargs="+", help="One or more source day partition dirs")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-hash-dedup", action="store_true",
                    help="If set, copy everything (faster, but duplicates possible).")
    args = ap.parse_args()

    dst = Path(args.dst).expanduser().resolve()
    srcs = [Path(s).expanduser().resolve() for s in args.src]

    for s in srcs:
        if not s.exists():
            print(f"ERROR: source does not exist: {s}", file=sys.stderr)
            sys.exit(2)

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    parent = dst.parent
    tmp = parent / f"{dst.name}__REBUILD_TMP__{ts}"
    backup = parent / f"{dst.name}__BROKEN_BACKUP__{ts}"

    print(f"DST: {dst}")
    print("SRCS:")
    for s in srcs:
        print(f"  - {s}")
    print(f"TMP: {tmp}")
    print(f"BACKUP (if dst exists): {backup}")

    if args.dry_run:
        print("DRY RUN: no filesystem changes will be made.")

    # 1) Quarantine existing dst (broken)
    if dst.exists():
        if args.dry_run:
            print(f"[dry-run] Would rename existing dst -> {backup}")
        else:
            dst.rename(backup)
            print(f"Renamed existing dst -> {backup}")

    # 2) Create tmp
    if args.dry_run:
        print(f"[dry-run] Would create tmp dir {tmp}")
    else:
        tmp.mkdir(parents=True, exist_ok=False)

    # 3) Copy parquet files from sources into tmp with unique names
    seen_hash = {}  # hash -> dest_path
    copied = 0
    skipped_identical = 0

    for i, s in enumerate(srcs, start=1):
        tag = f"SRC{i}"
        files = list(iter_parquets(s))
        print(f"{tag}: found {len(files)} parquet files under {s}")

        for p in files:
            # Keep relative structure info to avoid collisions
            rel = p.relative_to(s).as_posix()
            rel_id = hashlib.md5(rel.encode("utf-8")).hexdigest()[:10]
            dest_name = f"{tag}__{rel_id}__{p.name}"
            dest_path = tmp / dest_name

            if args.dry_run:
                print(f"[dry-run] Would copy {p} -> {dest_path}")
                continue

            if not args.no_hash_dedup:
                h = sha256_file(p)
                if h in seen_hash:
                    skipped_identical += 1
                    continue
                seen_hash[h] = dest_path

            shutil.copy2(p, dest_path)
            copied += 1

    if args.dry_run:
        print("DRY RUN complete.")
        return

    print(f"Copied files: {copied}")
    if not args.no_hash_dedup:
        print(f"Skipped identical files (hash dedup): {skipped_identical}")

    # 4) Promote tmp to dst (atomic-ish rename)
    tmp.rename(dst)
    print(f"Rebuilt dst is now: {dst}")

    print("\nNext: run your audit script and confirm 2026-02-12 counts look normal.")
    print("Rollback: delete dst and rename the BROKEN_BACKUP back to dst if needed.")

if __name__ == "__main__":
    main()

