# -*- coding: utf-8 -*-
"""
Upload local dataset files to Hugging Face dataset repo.

Usage:
    python -X utf8 upload_hf.py
"""

import sys
from pathlib import Path

from huggingface_hub import HfApi

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

REPO_ID = "jscmp4/Moltbook"
DATA_DIR = Path("data")

FILES_TO_UPLOAD = [
    ("posts_all.jsonl", "posts_all.jsonl"),
    ("comments_all.jsonl", "comments_all.jsonl"),
    ("agents_seen.jsonl", "agents_seen.jsonl"),
    ("submolts.json", "submolts.json"),
    ("README.md", "README.md"),
]

FILES_TO_DELETE = [
    "comments_part1.jsonl",
    "comments_part2.jsonl",
]

api = HfApi()

# 1) Validate login
try:
    user = api.whoami()
    print(f"logged in as: {user['name']}")
except Exception as e:
    print(f"not logged in. run 'huggingface-cli login' first.\n{e}")
    sys.exit(1)

# 2) Delete old split files
print("\n[1/4] deleting obsolete files...")
for fname in FILES_TO_DELETE:
    try:
        api.delete_file(path_in_repo=fname, repo_id=REPO_ID, repo_type="dataset")
        print(f"  [ok] deleted {fname}")
    except Exception as e:
        msg = str(e).lower()
        if "404" in msg or "not found" in msg or "doesn't exist" in msg:
            print(f"  - {fname} not found, skip")
        else:
            print(f"  [!] failed deleting {fname}: {e}")

# 3) Upload primary files
print("\n[2/4] uploading primary files...")
for local_name, remote_name in FILES_TO_UPLOAD:
    local_path = Path(local_name) if local_name == "README.md" else DATA_DIR / local_name
    if not local_path.exists():
        print(f"  [!] {local_name} missing, skip")
        continue

    size_gb = local_path.stat().st_size / 1024**3
    print(f"  uploading {local_name} ({size_gb:.2f} GB)...")
    try:
        api.upload_file(
            path_or_fileobj=str(local_path),
            path_in_repo=remote_name,
            repo_id=REPO_ID,
            repo_type="dataset",
            commit_message=f"Update {remote_name}",
        )
        print(f"  [ok] uploaded {remote_name}")
    except Exception as e:
        print(f"  [!] upload failed for {remote_name}: {e}")
        raise

# 3b) Upload dataset-card figures (small PNGs; unchanged files are no-ops)
figures_dir = DATA_DIR / "figures"
if figures_dir.is_dir():
    print("\n[2b/4] uploading figures/ ...")
    try:
        api.upload_folder(
            folder_path=str(figures_dir),
            path_in_repo="figures",
            repo_id=REPO_ID,
            repo_type="dataset",
            commit_message="Update dataset-card figures",
            allow_patterns=["*.png"],
        )
        print("  [ok] uploaded figures/")
    except Exception as e:
        print(f"  [!] upload failed for figures/: {e}")
        raise

# 4) Upload agent snapshots folder (incremental: only new/changed files)
snapshot_dir = DATA_DIR / "agent_snapshots"
print(f"\n[3/4] uploading agent_snapshots/ ...")
if snapshot_dir.is_dir():
    snap_files = sorted(snapshot_dir.glob("*.jsonl"))
    total_mb = sum(p.stat().st_size for p in snap_files) / 1024**2
    print(f"  {len(snap_files)} snapshot files, {total_mb:.1f} MB total")
    try:
        api.upload_folder(
            folder_path=str(snapshot_dir),
            path_in_repo="agent_snapshots",
            repo_id=REPO_ID,
            repo_type="dataset",
            commit_message=f"Update agent_snapshots ({len(snap_files)} files)",
            allow_patterns=["*.jsonl"],
        )
        print(f"  [ok] uploaded agent_snapshots/")
    except Exception as e:
        print(f"  [!] upload failed for agent_snapshots/: {e}")
        raise
else:
    print(f"  [!] {snapshot_dir} not found, skip")

print("\n[4/4] done")
print(f"  dataset: https://huggingface.co/datasets/{REPO_ID}")
