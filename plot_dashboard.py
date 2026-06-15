# -*- coding: utf-8 -*-
"""
Moltbook Scraper Dashboard - multi-panel overview figure.

Generates a single PNG with 6 panels showing the full project history.
Updated automatically after each scraper run.

Usage:
    python -X utf8 plot_dashboard.py
Output:
    data/plots/dashboard.png
"""

import json
import sys
from collections import defaultdict
from datetime import datetime, date as date_type, timedelta
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import numpy as np

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

DATA_DIR = Path(__file__).parent / "data"
PLOTS_DIR = DATA_DIR / "plots"
PLOTS_DIR.mkdir(exist_ok=True)
OUT_PATH = PLOTS_DIR / "dashboard.png"


def load_checkpoint():
    cp_path = DATA_DIR / "checkpoint.json"
    if cp_path.exists():
        with open(cp_path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def load_scheduler_history():
    hist_path = DATA_DIR / "auto_scheduler_history.jsonl"
    records = []
    if hist_path.exists():
        with open(hist_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except Exception:
                        pass
    return records


def aggregate_runs_daily(runs):
    """Aggregate checkpoint runs into daily buckets."""
    daily = defaultdict(lambda: {
        "posts": 0, "comments": 0, "duration_s": 0, "runs": 0
    })
    for r in runs:
        day = r["date"][:10]
        daily[day]["posts"] += r.get("new_posts", 0)
        daily[day]["comments"] += r.get("new_comments", 0)
        daily[day]["duration_s"] += r.get("duration_s", 0)
        daily[day]["runs"] += 1
    return dict(sorted(daily.items()))


def compute_cumulative(daily):
    """Build cumulative series from daily data."""
    days = sorted(daily.keys())
    cum_posts = []
    cum_comments = []
    total_p = 0
    total_c = 0
    for d in days:
        total_p += daily[d]["posts"]
        total_c += daily[d]["comments"]
        cum_posts.append(total_p)
        cum_comments.append(total_c)
    return days, cum_posts, cum_comments


def load_agent_snapshots():
    """Load agent snapshot files and extract summary stats per snapshot."""
    snap_dir = DATA_DIR / "agent_snapshots"
    if not snap_dir.exists():
        return []

    snapshots = []
    for f in sorted(snap_dir.glob("*.jsonl")):
        count = 0
        total_karma = 0
        total_followers = 0
        active = 0
        sampled_at = None
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    a = json.loads(line)
                    count += 1
                    total_karma += a.get("karma", 0)
                    total_followers += a.get("followerCount", 0)
                    if a.get("isActive"):
                        active += 1
                    if sampled_at is None:
                        sampled_at = a.get("sampled_at", "")
                except Exception:
                    pass
        if count > 0:
            snapshots.append({
                "file": f.name,
                "sampled_at": sampled_at,
                "count": count,
                "active": active,
                "avg_karma": total_karma / count,
                "avg_followers": total_followers / count,
                "total_karma": total_karma,
            })
    return snapshots


def parse_date(s):
    """Parse YYYY-MM-DD string to date object."""
    return datetime.strptime(s, "%Y-%m-%d").date()


def generate_dashboard():
    print("Generating dashboard...")

    cp = load_checkpoint()
    runs = cp.get("runs", [])
    hist = load_scheduler_history()
    snapshots = load_agent_snapshots()

    if not runs:
        print("  No runs found in checkpoint.json")
        return

    daily = aggregate_runs_daily(runs)
    days_str, cum_posts, cum_comments = compute_cumulative(daily)
    days = [parse_date(d) for d in days_str]

    daily_posts = [daily[d]["posts"] for d in days_str]
    daily_comments = [daily[d]["comments"] for d in days_str]
    daily_hours = [daily[d]["duration_s"] / 3600 for d in days_str]
    daily_runs = [daily[d]["runs"] for d in days_str]

    # --- Create figure ---
    fig, axes = plt.subplots(3, 2, figsize=(16, 14))
    fig.suptitle(
        f"Moltbook Scraper Dashboard\n"
        f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}  |  "
        f"Total: {cum_posts[-1]:,} posts, {cum_comments[-1]:,} comments, {len(runs)} runs",
        fontsize=14, fontweight="bold", y=0.98,
    )
    fig.subplots_adjust(hspace=0.35, wspace=0.25, top=0.91, bottom=0.06, left=0.08, right=0.95)

    # Colors
    C_POSTS = "#2196F3"
    C_COMMENTS = "#FF9800"
    C_HOURS = "#4CAF50"
    C_AGENTS = "#9C27B0"
    C_KARMA = "#E91E63"

    # ---- Panel 1: Daily Posts & Comments (bar chart) ----
    ax = axes[0, 0]
    x = np.arange(len(days))
    w = 0.4
    ax.bar(x - w/2, daily_posts, w, color=C_POSTS, alpha=0.8, label="Posts")
    ax.bar(x + w/2, daily_comments, w, color=C_COMMENTS, alpha=0.8, label="Comments")
    ax.set_ylabel("Count")
    ax.set_title("Daily Scraped Volume", fontweight="bold")
    ax.legend(loc="upper left", fontsize=9)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}k" if x >= 1000 else f"{x:.0f}"))
    # X ticks: show every few days
    tick_step = max(1, len(days) // 10)
    ax.set_xticks(x[::tick_step])
    ax.set_xticklabels([d.strftime("%m/%d") for d in days[::tick_step]], rotation=45, fontsize=8)
    ax.grid(axis="y", alpha=0.3)

    # ---- Panel 2: Cumulative Growth (line chart) ----
    ax = axes[0, 1]
    ax.plot(days, cum_posts, color=C_POSTS, linewidth=2, label="Posts (cum.)")
    ax.plot(days, cum_comments, color=C_COMMENTS, linewidth=2, label="Comments (cum.)")
    ax.set_ylabel("Cumulative Count")
    ax.set_title("Cumulative Data Growth", fontweight="bold")
    ax.legend(loc="upper left", fontsize=9)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M" if x >= 1e6 else f"{x/1000:.0f}k"))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(days)//8)))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, fontsize=8)
    ax.grid(alpha=0.3)

    # ---- Panel 3: Scrape Duration per Day ----
    ax = axes[1, 0]
    ax.bar(days, daily_hours, color=C_HOURS, alpha=0.8, width=0.8)
    ax.set_ylabel("Hours")
    ax.set_title("Daily Scrape Duration", fontweight="bold")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(days)//8)))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, fontsize=8)
    ax.axhline(y=10, color="red", linestyle="--", alpha=0.5, label="Budget (10h)")
    ax.legend(fontsize=9)
    ax.grid(axis="y", alpha=0.3)

    # ---- Panel 4: Comments per Post ratio ----
    ax = axes[1, 1]
    ratio = [c / max(1, p) for p, c in zip(daily_posts, daily_comments)]
    ax.plot(days, ratio, color=C_COMMENTS, linewidth=2, marker="o", markersize=3)
    ax.set_ylabel("Comments / Posts")
    ax.set_title("Daily Comments-to-Posts Ratio", fontweight="bold")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(days)//8)))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, fontsize=8)
    ax.grid(alpha=0.3)

    # ---- Panel 5: Agent Snapshots (if available) ----
    ax = axes[2, 0]
    if snapshots and len(snapshots) >= 2:
        snap_dates = []
        snap_counts = []
        snap_active = []
        snap_karma = []
        for s in snapshots:
            try:
                dt = datetime.strptime(s["sampled_at"][:10], "%Y-%m-%d").date()
                snap_dates.append(dt)
                snap_counts.append(s["count"])
                snap_active.append(s["active"])
                snap_karma.append(s["avg_karma"])
            except Exception:
                pass

        if snap_dates:
            ax.plot(snap_dates, snap_counts, color=C_AGENTS, linewidth=2, marker="s",
                    markersize=5, label="Total agents")
            ax.plot(snap_dates, snap_active, color=C_AGENTS, linewidth=2, marker="o",
                    markersize=5, linestyle="--", alpha=0.6, label="Active agents")
            ax.set_ylabel("Agent Count")
            ax.set_title("Agent Population (Snapshots)", fontweight="bold")
            ax.legend(fontsize=9)
            ax.yaxis.set_major_formatter(mticker.FuncFormatter(
                lambda x, _: f"{x/1000:.0f}k" if x >= 1000 else f"{x:.0f}"))
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, fontsize=8)

            ax2 = ax.twinx()
            ax2.plot(snap_dates, snap_karma, color=C_KARMA, linewidth=2, marker="D",
                     markersize=4, label="Avg karma")
            ax2.set_ylabel("Avg Karma", color=C_KARMA)
            ax2.tick_params(axis="y", labelcolor=C_KARMA)
            ax2.legend(loc="lower right", fontsize=9)
    else:
        ax.text(0.5, 0.5, "Agent snapshots\n(need 2+ snapshots)",
                ha="center", va="center", transform=ax.transAxes,
                fontsize=12, color="gray")
        ax.set_title("Agent Population (Snapshots)", fontweight="bold")
    ax.grid(alpha=0.3)

    # ---- Panel 6: Scheduler Performance ----
    ax = axes[2, 1]
    if hist and len(hist) >= 1:
        h_dates = []
        h_estimated = []
        h_actual = []
        h_threshold = []
        for h in hist:
            try:
                dt = datetime.strptime(h["timestamp"], "%Y-%m-%d %H:%M:%S").date()
                h_dates.append(dt)
                h_estimated.append(h.get("est_hours", 0))
                h_actual.append(h.get("actual_hours", 0))
                h_threshold.append(h.get("threshold", 0))
            except Exception:
                pass

        if h_dates:
            x = np.arange(len(h_dates))
            w = 0.35
            ax.bar(x - w/2, h_estimated, w, color="#64B5F6", alpha=0.8, label="Estimated")
            ax.bar(x + w/2, h_actual, w, color="#EF5350", alpha=0.8, label="Actual")
            ax.set_ylabel("Hours")
            ax.set_title("Scheduler: Estimated vs Actual", fontweight="bold")
            ax.legend(fontsize=9)

            # Show threshold as text on bars
            for i, t in enumerate(h_threshold):
                ax.text(i, max(h_estimated[i], h_actual[i]) + 0.2,
                        f">={t}", ha="center", fontsize=7, color="gray")

            ax.set_xticks(x)
            ax.set_xticklabels([d.strftime("%m/%d") for d in h_dates], rotation=45, fontsize=8)
    else:
        ax.text(0.5, 0.5, "Scheduler history\n(will appear after auto runs)",
                ha="center", va="center", transform=ax.transAxes,
                fontsize=12, color="gray")
        ax.set_title("Scheduler: Estimated vs Actual", fontweight="bold")
    ax.grid(axis="y", alpha=0.3)

    # Save
    fig.savefig(OUT_PATH, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"  Dashboard saved: {OUT_PATH} ({size_kb:.0f} KB)")
    return OUT_PATH


if __name__ == "__main__":
    generate_dashboard()
