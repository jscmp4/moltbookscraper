# -*- coding: utf-8 -*-
"""
Moltbook 数据可视化
用法: python -X utf8 plot_stats.py
生成: data/plots/ 目录下的 PNG 图表
"""

import json
from collections import defaultdict
from datetime import date as date_type, timedelta
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
from matplotlib.patches import Patch
import datetime

DATA_DIR  = Path("data")
PLOTS_DIR = DATA_DIR / "plots"
PLOTS_DIR.mkdir(exist_ok=True)

POSTS_FILE    = DATA_DIR / "posts_all.jsonl"
COMMENTS_FILE = DATA_DIR / "comments_all.jsonl"

# ── 配色 ────────────────────────────────────────
C_NORMAL = "#4C9BE8"
C_SPAM   = "#E8694C"
C_EVENT1 = "#E8C34C"   # mbc-20 wave start
C_EVENT2 = "#6FBF73"   # platform intervention

plt.rcParams.update({
    "font.family": "sans-serif",
    "axes.spines.top":   False,
    "axes.spines.right": False,
    "figure.dpi": 150,
})


def load_daily_posts(path: Path):
    """返回 {date_str: {"normal": n, "spam": n}} """
    daily = defaultdict(lambda: {"normal": 0, "spam": 0})
    print("扫描帖子...", end="", flush=True)
    with open(path, encoding="utf-8") as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            try:
                p = json.loads(line)
                d = p.get("created_at", "")[:10]
                if not d:
                    continue
                if p.get("is_spam") or p.get("submolt", {}).get("name", "") in ("mbc20", "mbc-20"):
                    daily[d]["spam"] += 1
                else:
                    daily[d]["normal"] += 1
            except Exception:
                pass
            if (i + 1) % 500_000 == 0:
                print(f" {i+1//1000}k", end="", flush=True)
    print(" done")
    return daily


def load_daily_comments(path: Path):
    """返回 {date_str: count}"""
    daily = defaultdict(int)
    if not path.exists():
        return daily
    print("扫描评论...", end="", flush=True)
    with open(path, encoding="utf-8") as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            try:
                c = json.loads(line)
                d = c.get("created_at", "")[:10]
                if d:
                    daily[d] += 1
            except Exception:
                pass
            if (i + 1) % 1_000_000 == 0:
                print(f" {(i+1)//1000}k", end="", flush=True)
    print(" done")
    return daily


def date_range(daily: dict):
    dates = sorted(daily.keys())
    if not dates:
        return []
    d = date_type.fromisoformat(dates[0])
    end = date_type.fromisoformat(dates[-1])
    result = []
    while d <= end:
        result.append(d.isoformat())
        d += timedelta(days=1)
    return result


def plot_daily_posts(daily: dict, out: Path):
    dates = date_range(daily)
    xs = [datetime.date.fromisoformat(d) for d in dates]
    normal = [daily[d]["normal"] for d in dates]
    spam   = [daily[d]["spam"]   for d in dates]

    fig, ax = plt.subplots(figsize=(14, 5))
    ax.bar(xs, normal, label="Normal posts",   color=C_NORMAL, width=0.85)
    ax.bar(xs, spam,   label="Spam (mbc-20)",  color=C_SPAM,   width=0.85, bottom=normal)

    # 标注事件
    ax.axvline(datetime.date(2026, 2, 6),  color=C_EVENT1, lw=1.5, ls="--", alpha=0.8)
    ax.axvline(datetime.date(2026, 2, 18), color=C_EVENT2, lw=1.5, ls="--", alpha=0.8)
    ax.text(datetime.date(2026, 2, 6),  ax.get_ylim()[1] * 0.97, " mbc-20 wave start",
            color=C_EVENT1, fontsize=8, va="top")
    ax.text(datetime.date(2026, 2, 18), ax.get_ylim()[1] * 0.97, " anti-spam enforcement",
            color=C_EVENT2, fontsize=8, va="top")

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v/1000:.0f}k" if v >= 1000 else str(int(v))))
    ax.set_title("Moltbook — Daily Post Volume", fontsize=14, fontweight="bold", pad=12)
    ax.set_ylabel("Posts per day")
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)
    print(f"  saved → {out}")


def plot_daily_comments(daily: dict, out: Path):
    dates = date_range(daily)
    xs = [datetime.date.fromisoformat(d) for d in dates]
    ys = [daily[d] for d in dates]

    fig, ax = plt.subplots(figsize=(14, 4))
    ax.fill_between(xs, ys, color=C_NORMAL, alpha=0.35)
    ax.plot(xs, ys, color=C_NORMAL, lw=1.5)

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v/1000:.0f}k" if v >= 1000 else str(int(v))))
    ax.set_title("Moltbook — Daily Comments Collected", fontsize=14, fontweight="bold", pad=12)
    ax.set_ylabel("Comments per day")
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)
    print(f"  saved → {out}")


def plot_posts_by_hour(path: Path, out: Path):
    """全局每小时发帖分布（0-23）"""
    hours = defaultdict(int)
    print("扫描小时分布...", end="", flush=True)
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                p = json.loads(line)
                if p.get("is_spam"):
                    continue
                ts = p.get("created_at", "")
                if len(ts) >= 13:
                    hours[int(ts[11:13])] += 1
            except Exception:
                pass
    print(" done")

    xs = list(range(24))
    ys = [hours[h] for h in xs]
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(xs, ys, color=C_NORMAL, width=0.7)
    ax.set_xticks(xs)
    ax.set_xticklabels([f"{h:02d}:00" for h in xs], rotation=45, ha="right", fontsize=8)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v/1000:.0f}k" if v >= 1000 else str(int(v))))
    ax.set_title("Moltbook — Post Volume by Hour (UTC, spam excluded)", fontsize=13, fontweight="bold", pad=12)
    ax.set_ylabel("Total posts")
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)
    print(f"  saved → {out}")


def main():
    if not POSTS_FILE.exists():
        print(f"[!] {POSTS_FILE} 不存在")
        return

    print("\n=== Moltbook 数据可视化 ===\n")

    daily_posts = load_daily_posts(POSTS_FILE)
    daily_comments = load_daily_comments(COMMENTS_FILE)

    plot_daily_posts(daily_posts,       PLOTS_DIR / "daily_posts.png")
    plot_daily_comments(daily_comments, PLOTS_DIR / "daily_comments.png")
    plot_posts_by_hour(POSTS_FILE,      PLOTS_DIR / "posts_by_hour.png")

    print(f"\n完成！图表保存在 {PLOTS_DIR}/")
    print("  daily_posts.png    — 每日发帖量（normal vs spam）")
    print("  daily_comments.png — 每日评论收集量")
    print("  posts_by_hour.png  — 全天各小时发帖分布（UTC）")


if __name__ == "__main__":
    main()
