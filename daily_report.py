# -*- coding: utf-8 -*-
"""
Moltbook Daily Report - quick overview of scraping status and growth trends.

Usage:
    python -X utf8 daily_report.py           # full report
    python -X utf8 daily_report.py --short   # one-screen summary
"""

import json
import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

DATA_DIR = Path(__file__).parent / "data"


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


def aggregate_daily(runs):
    """Aggregate runs into daily buckets."""
    daily = defaultdict(lambda: {"posts": 0, "comments": 0, "duration_s": 0, "runs": 0})
    for r in runs:
        day = r["date"][:10]
        daily[day]["posts"] += r.get("new_posts", 0)
        daily[day]["comments"] += r.get("new_comments", 0)
        daily[day]["duration_s"] += r.get("duration_s", 0)
        daily[day]["runs"] += 1
    return dict(sorted(daily.items()))


def file_stats():
    """Get current file sizes and line counts (fast, no full scan)."""
    stats = {}
    for name in ["posts_all.jsonl", "comments_all.jsonl", "agents_seen.jsonl"]:
        p = DATA_DIR / name
        if p.exists():
            size_mb = p.stat().st_size / 1024 / 1024
            stats[name] = {"size_mb": size_mb}
        else:
            stats[name] = {"size_mb": 0}
    return stats


def snapshot_count():
    snap_dir = DATA_DIR / "agent_snapshots"
    if snap_dir.exists():
        files = sorted(snap_dir.glob("*.jsonl"))
        return len(files), files[-1].name if files else "none"
    return 0, "none"


def print_report(short=False):
    cp = load_checkpoint()
    runs = cp.get("runs", [])
    hist = load_scheduler_history()
    fstats = file_stats()
    snap_n, snap_latest = snapshot_count()

    daily = aggregate_daily(runs)
    days_sorted = sorted(daily.keys())

    # Current totals
    total_posts = cp.get("total_posts", 0)
    total_comments = cp.get("total_comments", 0)
    newest = cp.get("newest_post_created_at", "?")[:19]
    n_runs = len(runs)

    # Last run
    last = runs[-1] if runs else {}
    last_date = last.get("date", "?")
    last_posts = last.get("new_posts", 0)
    last_comments = last.get("new_comments", 0)
    last_hours = last.get("duration_s", 0) / 3600

    print()
    print("=" * 62)
    print("  Moltbook Daily Report")
    print(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 62)

    # --- Current State ---
    print()
    print("  [Current State]")
    print(f"    Posts:          {fstats['posts_all.jsonl']['size_mb']:>8.0f} MB")
    print(f"    Comments:       {fstats['comments_all.jsonl']['size_mb']:>8.0f} MB")
    print(f"    Agents:         {fstats['agents_seen.jsonl']['size_mb']:>8.0f} MB")
    print(f"    Newest post:    {newest}")
    print(f"    Total runs:     {n_runs}")
    print(f"    Agent snapshots:{snap_n:>3} (latest: {snap_latest})")

    # --- Last Run ---
    print()
    print("  [Last Run]")
    print(f"    Date:           {last_date}")
    print(f"    Posts:          +{last_posts:,}")
    print(f"    Comments:       +{last_comments:,}")
    print(f"    Duration:       {last_hours:.1f}h")

    # --- Scheduler History ---
    if hist:
        latest_h = hist[-1]
        print()
        print("  [Auto Scheduler - Last Decision]")
        print(f"    Threshold:      >= {latest_h.get('threshold', '?')}")
        print(f"    Estimated:      {latest_h.get('est_hours', '?')}h")
        print(f"    Actual:         {latest_h.get('actual_hours', '?')}h")
        print(f"    Uncovered:      {latest_h.get('uncovered', '?'):,} posts")

    # --- Daily History Table ---
    print()
    print("  [Daily Growth]")
    print(f"    {'Date':>12}  {'Posts':>8}  {'Comments':>10}  {'Hours':>6}  {'Runs':>4}  {'Bar'}")
    print(f"    {'-'*12}  {'-'*8}  {'-'*10}  {'-'*6}  {'-'*4}  {'-'*20}")

    show_days = days_sorted[-14:] if not short else days_sorted[-7:]
    for day in show_days:
        d = daily[day]
        bar_len = min(20, d["comments"] // 5000)
        bar = "#" * bar_len
        print(
            f"    {day:>12}  {d['posts']:>8,}  {d['comments']:>10,}  "
            f"{d['duration_s']/3600:>5.1f}h  {d['runs']:>4}  {bar}"
        )

    # --- Averages & Prediction ---
    if len(days_sorted) >= 3:
        recent_7 = days_sorted[-7:]
        avg_posts = sum(daily[d]["posts"] for d in recent_7) / len(recent_7)
        avg_comments = sum(daily[d]["comments"] for d in recent_7) / len(recent_7)
        avg_hours = sum(daily[d]["duration_s"] for d in recent_7) / len(recent_7) / 3600

        # Trend: compare last 3 days vs previous 3 days
        if len(days_sorted) >= 6:
            last3 = days_sorted[-3:]
            prev3 = days_sorted[-6:-3]
            last3_posts = sum(daily[d]["posts"] for d in last3) / 3
            prev3_posts = sum(daily[d]["posts"] for d in prev3) / 3
            last3_comments = sum(daily[d]["comments"] for d in last3) / 3
            prev3_comments = sum(daily[d]["comments"] for d in prev3) / 3
            post_trend = ((last3_posts - prev3_posts) / max(1, prev3_posts)) * 100
            comment_trend = ((last3_comments - prev3_comments) / max(1, prev3_comments)) * 100
        else:
            post_trend = comment_trend = 0

        print()
        print("  [7-Day Averages]")
        print(f"    Posts/day:      {avg_posts:>10,.0f}")
        print(f"    Comments/day:   {avg_comments:>10,.0f}")
        print(f"    Scrape time:    {avg_hours:>9.1f}h")

        print()
        print("  [Trend (last 3d vs prev 3d)]")
        post_arrow = "^" if post_trend > 5 else ("v" if post_trend < -5 else "=")
        comment_arrow = "^" if comment_trend > 5 else ("v" if comment_trend < -5 else "=")
        print(f"    Posts:          {post_arrow} {post_trend:+.0f}%")
        print(f"    Comments:       {comment_arrow} {comment_trend:+.0f}%")

        print()
        print("  [Tomorrow Prediction]")
        # Simple: avg + trend adjustment
        pred_posts = avg_posts * (1 + post_trend / 200)  # damped
        pred_comments = avg_comments * (1 + comment_trend / 200)
        # Time prediction using scheduler calibration if available
        if hist and hist[-1].get("actual_hours") and hist[-1].get("est_hours"):
            calibration = hist[-1]["actual_hours"] / max(0.1, hist[-1]["est_hours"])
        else:
            calibration = 1.0
        pred_hours = avg_hours * calibration
        print(f"    Posts:          ~{pred_posts:,.0f}")
        print(f"    Comments:       ~{pred_comments:,.0f}")
        print(f"    Est. scrape:    ~{pred_hours:.1f}h (calibration: {calibration:.1f}x)")

    # --- Health Warnings ---
    warnings = []
    if last_hours > 20:
        warnings.append(f"Last run took {last_hours:.0f}h - consider raising min-comments threshold")
    if hist and hist[-1].get("actual_hours", 0) > hist[-1].get("budget_hours", 10) * 0.9:
        warnings.append("Last run nearly exceeded budget - auto-scheduler may raise threshold tomorrow")

    # Check for stale data
    if last_date != "?":
        last_dt = datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S")
        gap = (datetime.now() - last_dt).total_seconds() / 3600
        if gap > 48:
            warnings.append(f"Data is {gap:.0f}h old - scraper may not be running")

    if warnings:
        print()
        print("  [Warnings]")
        for w in warnings:
            print(f"    ! {w}")

    print()
    print("=" * 62)
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Moltbook Daily Report")
    parser.add_argument("--short", action="store_true", help="compact 7-day view")
    args = parser.parse_args()
    print_report(short=args.short)
