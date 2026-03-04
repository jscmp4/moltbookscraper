# -*- coding: utf-8 -*-
"""
上传数据到 HuggingFace Dataset: jscmp4/Moltbook
用法: python -X utf8 upload_hf.py
"""

import sys
from pathlib import Path
from huggingface_hub import HfApi

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

REPO_ID   = "jscmp4/Moltbook"
DATA_DIR  = Path("data")

FILES_TO_UPLOAD = [
    ("posts_all.jsonl",    "posts_all.jsonl"),
    ("comments_all.jsonl", "comments_all.jsonl"),
    ("agents_seen.jsonl",  "agents_seen.jsonl"),
    ("submolts.json",      "submolts.json"),
    ("README.md",          "README.md"),
]

FILES_TO_DELETE = [
    "comments_part1.jsonl",
    "comments_part2.jsonl",
]

api = HfApi()

# 1. 验证登录
try:
    user = api.whoami()
    print(f"已登录: {user['name']}")
except Exception as e:
    print(f"未登录，请先运行 huggingface-cli login\n{e}")
    sys.exit(1)

# 2. 删除旧的分割文件
print("\n[1/3] 删除旧文件...")
for fname in FILES_TO_DELETE:
    try:
        api.delete_file(path_in_repo=fname, repo_id=REPO_ID, repo_type="dataset")
        print(f"  ✓ 删除 {fname}")
    except Exception as e:
        if "404" in str(e) or "not found" in str(e).lower() or "doesn't exist" in str(e).lower():
            print(f"  - {fname} 不存在，跳过")
        else:
            print(f"  ! 删除 {fname} 失败: {e}")

# 3. 上传数据文件
print("\n[2/3] 上传数据文件...")
for local_name, remote_name in FILES_TO_UPLOAD:
    local_path = Path(local_name) if local_name == "README.md" else DATA_DIR / local_name
    if not local_path.exists():
        print(f"  ! {local_name} 不存在，跳过")
        continue
    size_gb = local_path.stat().st_size / 1024**3
    print(f"  上传 {local_name} ({size_gb:.2f} GB)...")
    try:
        api.upload_file(
            path_or_fileobj=str(local_path),
            path_in_repo=remote_name,
            repo_id=REPO_ID,
            repo_type="dataset",
            commit_message=f"Update {remote_name}",
        )
        print(f"  ✓ {remote_name} 上传完成")
    except Exception as e:
        print(f"  ! {remote_name} 上传失败: {e}")
        raise

print("\n[3/3] 完成！")
print(f"  数据集地址: https://huggingface.co/datasets/{REPO_ID}")
