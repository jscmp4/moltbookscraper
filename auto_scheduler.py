# -*- coding: utf-8 -*-
"""
Moltbook Auto Scheduler - adaptive daily scraper driver.

Reads past run history from checkpoint.json, estimates current backlog,
and picks the best strategy for this run. If the estimated time exceeds
the budget, it automatically raises the min-comments threshold or caps
the work to fit within the time window.

Usage:
    python -X utf8 auto_scheduler.py              # run with adaptive defaults
    python -X utf8 auto_scheduler.py --budget 8   # max 8 hours this run
    python -X utf8 auto_scheduler.py --dry-run    # show plan without running
"""

import json
import sys
import os
import subprocess
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import Counter

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

DATA_DIR = Path(__file__).parent / "data"
CHECKPOINT = DATA_DIR / "checkpoint.json"
HISTORY_FILE = DATA_DIR / "auto_scheduler_history.jsonl"
LOCK_FILE = DATA_DIR / "scraper.lock"

# Thresholds to try, from preferred (broadest) to fallback (fastest)
THRESHOLD_LADDER = [3, 5, 10, 15, 20, 30]

# Default budget in hours
DEFAULT_BUDGET_HOURS = 10.0

# Single source of truth for the comment-endpoint rate, used by BOTH the
# backlog time estimate and the scraper argv. Platform limit verified
# 2026-06-09: plain X-RateLimit-Limit 200/min (+30/1s burst tier); 100 rpm
# keeps 50% headroom.
COMMENT_RPM = 100
READ_RPM = 100


def load_checkpoint():
    if CHECKPOINT.exists():
        with open(CHECKPOINT, encoding="utf-8") as f:
            return json.load(f)
    return {}


def load_history():
    """Load past auto_scheduler runs for trend analysis."""
    records = []
    if HISTORY_FILE.exists():
        with open(HISTORY_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except Exception:
                        pass
    return records


def save_history_record(record: dict):
    with open(HISTORY_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def estimate_backlog(cp: dict, threshold: int):
    """
    Estimate how many posts need comment backfill and how long it will take.
    Uses recent run speeds from checkpoint history.
    """
    posts_path = DATA_DIR / "posts_all.jsonl"
    comments_path = DATA_DIR / "comments_all.jsonl"

    if not posts_path.exists():
        return {"eligible": 0, "uncovered": 0, "gap_comments": 0, "est_hours": 0}

    # Count eligible posts and their expected comments
    post_expected = {}
    with open(posts_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                p = json.loads(line)
                pid = p.get("id")
                cc = p.get("comment_count", 0)
                if pid and isinstance(cc, int) and cc >= threshold:
                    post_expected[pid] = cc
            except Exception:
                pass

    # Count local comments per post
    comments_per_post = Counter()
    if comments_path.exists():
        with open(comments_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    c = json.loads(line)
                    pid = c.get("post_id", "")
                    if pid in post_expected:
                        comments_per_post[pid] += 1
                except Exception:
                    pass

    uncovered = 0
    gap_comments = 0
    for pid, expected in post_expected.items():
        local = comments_per_post.get(pid, 0)
        if local < expected:
            uncovered += 1
            gap_comments += expected - local

    # Estimate time: each uncovered post needs ~1 API call (small posts)
    # or more for large posts. Must match the rpm run_scraper actually passes.
    if uncovered == 0:
        est_hours = 0.0
    else:
        avg_pages = max(1.0, (gap_comments / max(1, uncovered)) / 100.0)
        api_calls = uncovered * avg_pages
        est_hours = api_calls / (COMMENT_RPM * 60)

    # Add time for new posts fetch (~0.5h typical)
    est_hours += 0.5

    return {
        "eligible": len(post_expected),
        "uncovered": uncovered,
        "gap_comments": gap_comments,
        "est_hours": round(est_hours, 1),
    }


def compute_recent_speed(cp: dict):
    """Compute average comments/hour from recent runs."""
    runs = cp.get("runs", [])
    recent = [r for r in runs[-15:] if r.get("new_comments", 0) > 100]
    if not recent:
        return 6000  # conservative default
    rates = []
    for r in recent:
        h = r["duration_s"] / 3600
        if h > 0.05:
            rates.append(r["new_comments"] / h)
    return sum(rates) / len(rates) if rates else 6000


def estimate_daily_growth(cp: dict, threshold_frac: float):
    """Estimate how many new eligible posts appear per day."""
    runs = cp.get("runs", [])
    if len(runs) < 2:
        return 4000  # conservative default

    # Use last 7 runs to estimate daily post growth
    recent = runs[-7:]
    total_posts = sum(r.get("new_posts", 0) for r in recent)
    first_dt = datetime.strptime(recent[0]["date"], "%Y-%m-%d %H:%M:%S")
    last_dt = datetime.strptime(recent[-1]["date"], "%Y-%m-%d %H:%M:%S")
    days = max(0.5, (last_dt - first_dt).total_seconds() / 86400)
    daily_posts = total_posts / days
    return daily_posts * threshold_frac


def pick_strategy(budget_hours: float, dry_run: bool = False):
    """
    Analyze current state and pick the best min-comments threshold
    that fits within the budget. Returns the chosen strategy dict.
    """
    cp = load_checkpoint()
    history = load_history()

    # Fraction of posts at each threshold (from past analysis)
    # These are approximate and stable enough to hardcode
    threshold_fracs = {
        3: 0.144, 5: 0.088, 10: 0.035, 15: 0.019, 20: 0.012, 30: 0.008
    }

    # Days since last run
    last_run_date = None
    runs = cp.get("runs", [])
    if runs:
        last_run_date = datetime.strptime(runs[-1]["date"], "%Y-%m-%d %H:%M:%S")
    days_since_last = 0
    if last_run_date:
        days_since_last = (datetime.now() - last_run_date).total_seconds() / 86400

    speed = compute_recent_speed(cp)

    print(f"\n{'='*60}")
    print(f"  Moltbook Auto Scheduler")
    print(f"{'='*60}")
    print(f"  Budget:          {budget_hours:.1f} hours")
    print(f"  Days since last: {days_since_last:.1f}")
    print(f"  Avg speed:       {speed:,.0f} comments/hour")
    print()

    # Full per-threshold table only in dry-run: each estimate_backlog call
    # re-reads the multi-GB JSONL files, and for real runs the lock is
    # already held — keep planning to a single scan.
    est_by_t = {}
    if dry_run:
        print(f"  {'Threshold':>10}  {'Eligible':>10}  {'Uncovered':>10}  {'Gap':>12}  {'Est.Hours':>10}  {'Fits?':>6}")
        print(f"  {'-'*10}  {'-'*10}  {'-'*10}  {'-'*12}  {'-'*10}  {'-'*6}")
        for t in THRESHOLD_LADDER:
            est = estimate_backlog(cp, t)
            est_by_t[t] = est
            fits = est["est_hours"] <= budget_hours
            print(
                f"  {'>= ' + str(t):>10}  {est['eligible']:>10,}  {est['uncovered']:>10,}  "
                f"{est['gap_comments']:>12,}  {est['est_hours']:>9.1f}h  "
                f"{'YES' if fits else 'no':>6}"
            )
    else:
        t0 = THRESHOLD_LADDER[0]
        est_by_t[t0] = estimate_backlog(cp, t0)
        e = est_by_t[t0]
        print(
            f"  >= {t0}: eligible {e['eligible']:,} | uncovered {e['uncovered']:,} | "
            f"gap {e['gap_comments']:,} | est {e['est_hours']:.1f}h (budget {budget_hours:.1f}h)"
        )

    # Always run the broadest threshold and cap the post count to the budget.
    # The old "pick the widest threshold whose FULL backlog fits" ratchets:
    # after a gap it settles on e.g. >=10 and the 3<=cc<10 layer grows forever,
    # so >=3 never fits again. Capping instead drains the full backlog over
    # successive nights.
    t = THRESHOLD_LADDER[0]
    est = est_by_t[t]
    chosen = {
        "threshold": t,
        "est_hours": min(est["est_hours"], budget_hours),
        "eligible": est["eligible"],
        "uncovered": est["uncovered"],
        "gap_comments": est["gap_comments"],
    }
    if est["est_hours"] > budget_hours:
        if est["uncovered"] > 0 and est["est_hours"] > 0:
            ratio = budget_hours / est["est_hours"]
            max_posts = max(100, int(est["uncovered"] * ratio * 0.8))
        else:
            max_posts = 5000
        chosen["max_comment_posts"] = max_posts
        print(f"\n  [cap] Full >= {t} backlog exceeds budget; capping at max-comment-posts={max_posts:,}")

    # Growth rate warning
    daily_growth = estimate_daily_growth(cp, threshold_fracs.get(chosen["threshold"], 0.1))
    daily_maintenance_h = daily_growth / max(1, speed) if speed else 999
    print(f"\n  Chosen:    >= {chosen['threshold']} comments")
    print(f"  Est. time: {chosen['est_hours']:.1f}h (budget: {budget_hours:.1f}h)")
    print(f"  Daily maintenance estimate: ~{daily_maintenance_h:.1f}h/day")

    if daily_maintenance_h > budget_hours * 0.8:
        print(f"  [WARN] Daily growth ({daily_maintenance_h:.1f}h) is approaching budget ({budget_hours:.1f}h)!")
        print(f"         Consider raising threshold or increasing budget.")

    # Trend analysis from history
    if len(history) >= 3:
        recent_h = history[-3:]
        avg_actual = sum(r.get("actual_hours", 0) for r in recent_h) / len(recent_h)
        avg_estimated = sum(r.get("est_hours", 0) for r in recent_h) / len(recent_h)
        if avg_estimated > 0:
            accuracy = avg_actual / avg_estimated
            print(f"  Estimate accuracy (last 3 runs): {accuracy:.1%}")
            if accuracy > 1.5:
                print(f"  [WARN] Estimates have been too optimistic! Actual runs took {accuracy:.1f}x longer.")
                # Adjust estimate
                chosen["est_hours"] = round(chosen["est_hours"] * accuracy, 1)
                print(f"  Adjusted est: {chosen['est_hours']:.1f}h")
                if chosen["est_hours"] > budget_hours:
                    # Tighten the post cap instead of raising the threshold:
                    # raising it would strand the 3<=cc<threshold layer forever
                    # (the ratchet this rewrite removed).
                    base = est_by_t[chosen["threshold"]]
                    if base["uncovered"] > 0 and base["est_hours"] > 0:
                        ratio = budget_hours / (base["est_hours"] * accuracy)
                        new_cap = max(100, int(base["uncovered"] * ratio * 0.8))
                        prev_cap = chosen.get("max_comment_posts")
                        chosen["max_comment_posts"] = min(prev_cap, new_cap) if prev_cap else new_cap
                        print(f"  [AUTO-ADJUST] Tightening max-comment-posts to {chosen['max_comment_posts']:,} "
                              f"(threshold stays >= {chosen['threshold']})")

    chosen["budget_hours"] = budget_hours
    chosen["days_since_last"] = round(days_since_last, 1)
    chosen["speed_comments_h"] = round(speed)
    chosen["daily_maintenance_h"] = round(daily_maintenance_h, 1)
    chosen["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"{'='*60}\n")
    return chosen


def acquire_lock():
    """
    Prevent concurrent scraper runs. Creates a lock file with PID.
    Returns True if lock acquired, False if another instance is running.
    """
    if LOCK_FILE.exists():
        try:
            lock_data = json.loads(LOCK_FILE.read_text(encoding="utf-8"))
            pid = lock_data.get("pid", 0)
            started = lock_data.get("started", "?")
            # A lock older than the max age is stale regardless of PID liveness:
            # after a crash (no cleanup) + OS PID reuse, the old PID can look
            # alive forever and wedge every future run. Longest real run is the
            # budget (~10h); 24h is safely beyond it.
            age_stale = False
            try:
                _t = datetime.strptime(started, "%Y-%m-%d %H:%M:%S")
                age_stale = (datetime.now() - _t).total_seconds() > 24 * 3600
            except Exception:
                age_stale = False
            # Check if the PID is still alive
            if sys.platform == "win32":
                import ctypes
                kernel32 = ctypes.windll.kernel32
                handle = kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
                if handle:
                    kernel32.CloseHandle(handle)
                    if not age_stale:
                        print(f"  [lock] Another scraper is running (PID {pid}, started {started}). Skipping.")
                        return False
                    print(f"  [lock] Lock older than 24h (PID {pid}, started {started}); treating as stale. Removing.")
                else:
                    # Process is dead, stale lock
                    print(f"  [lock] Stale lock found (PID {pid} no longer running). Removing.")
            else:
                import signal
                alive = True
                try:
                    os.kill(pid, 0)
                except OSError:
                    alive = False
                if alive and not age_stale:
                    print(f"  [lock] Another scraper is running (PID {pid}, started {started}). Skipping.")
                    return False
                if alive and age_stale:
                    print(f"  [lock] Lock older than 24h (PID {pid}, started {started}); treating as stale. Removing.")
                else:
                    print(f"  [lock] Stale lock found (PID {pid} no longer running). Removing.")
        except Exception:
            print(f"  [lock] Corrupt lock file. Removing.")
        try:
            LOCK_FILE.unlink()
        except OSError:
            return False

    # Atomic create ('x' mode): if another process won the race, refuse.
    lock_data = {
        "pid": os.getpid(),
        "started": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    try:
        with open(LOCK_FILE, "x", encoding="utf-8") as f:
            json.dump(lock_data, f)
    except FileExistsError:
        print("  [lock] Lost lock race to another process. Skipping.")
        return False
    return True


def release_lock():
    """Remove the lock file, but only if this process owns it."""
    try:
        if LOCK_FILE.exists():
            data = json.loads(LOCK_FILE.read_text(encoding="utf-8"))
            if int(data.get("pid", 0)) == os.getpid():
                LOCK_FILE.unlink()
    except Exception:
        pass


HF_UPLOAD_STAMP = DATA_DIR / "last_hf_upload.txt"


def _should_skip_upload() -> bool:
    if os.environ.get("MOLT_NO_HF_UPLOAD") == "1":
        return True
    return "--no-upload" in sys.argv


def _run_wrote_new_data() -> bool:
    """True if the most recent checkpoint run recorded new posts or comments."""
    try:
        cp = json.loads(CHECKPOINT.read_text(encoding="utf-8"))
        last = (cp.get("runs") or [])[-1]
        return (last.get("new_posts", 0) or 0) > 0 or (last.get("new_comments", 0) or 0) > 0
    except Exception:
        return True  # unknown -> err on the side of syncing


def _should_upload_monthly() -> bool:
    """
    HF sync cadence is MONTHLY (full re-upload is ~23GB; daily would be ~700GB/mo).
    Upload when we've never uploaded, or when the calendar month changed since the
    last successful upload. Stamp file holds the last upload date (YYYY-MM-DD).
    """
    try:
        stamp = HF_UPLOAD_STAMP.read_text(encoding="utf-8").strip()
        last_ym = stamp[:7]  # YYYY-MM
    except Exception:
        return True  # never uploaded
    return datetime.now().strftime("%Y-%m") != last_ym


def _record_hf_upload():
    try:
        HF_UPLOAD_STAMP.write_text(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), encoding="utf-8")
    except Exception:
        pass


def run_scraper(strategy: dict):
    """Execute scraper.py with the chosen strategy."""
    args = [
        sys.executable, "-X", "utf8", "scraper.py",
        "--workers", "1",
        "--read-rpm", str(READ_RPM),
        "--comment-rpm", str(COMMENT_RPM),
        "--min-comments", str(strategy["threshold"]),
        "--comment-queue-strategy", "layered",
        "--comment-id-cache", "sqlite",
        "--no-snapshot",
    ]
    if "max_comment_posts" in strategy:
        args.extend(["--max-comment-posts", str(strategy["max_comment_posts"])])

    print(f"[run] {' '.join(args)}\n")
    t0 = datetime.now()

    # We already hold scraper.lock; tell the child not to refuse it.
    child_env = dict(os.environ)
    child_env["MOLT_LOCK_INHERITED"] = "1"
    result = subprocess.run(args, cwd=str(Path(__file__).parent), env=child_env)

    elapsed = (datetime.now() - t0).total_seconds()
    strategy["actual_hours"] = round(elapsed / 3600, 2)
    strategy["exit_code"] = result.returncode

    save_history_record(strategy)

    print(f"\n[auto-scheduler] Done in {elapsed/3600:.1f}h (estimated {strategy['est_hours']:.1f}h)")
    if strategy["actual_hours"] > strategy["est_hours"] * 1.5:
        print(f"[auto-scheduler] Run took {strategy['actual_hours']/max(0.01, strategy['est_hours']):.1f}x "
              f"longer than estimated. Future runs will auto-calibrate.")

    # Data health check (fast mode with sample)
    try:
        check_result = subprocess.run(
            [sys.executable, "-X", "utf8", "scraper.py",
             "--check", "--check-fast", "--check-sample-posts", "100",
             "--min-comments", str(strategy["threshold"])],
            cwd=str(Path(__file__).parent),
            capture_output=True, text=True, timeout=300,
        )
        if check_result.stdout:
            # Extract key lines for the log
            for line in check_result.stdout.splitlines():
                if any(k in line.lower() for k in ["eligible", "coverage", "gap", "warn", "error", "mismatch", "unique"]):
                    print(f"  [check] {line.strip()}")
        if check_result.returncode != 0:
            print(f"  [check] WARNING: data check exited with code {check_result.returncode}")
    except Exception as e:
        print(f"  [check] data check failed: {e}")

    # Generate daily report after each run
    try:
        report_result = subprocess.run(
            [sys.executable, "-X", "utf8", "daily_report.py", "--short"],
            cwd=str(Path(__file__).parent),
            capture_output=True, text=True, timeout=60,
        )
        if report_result.stdout:
            print(report_result.stdout)
    except Exception as e:
        print(f"[auto-scheduler] daily report failed: {e}")

    # Update dashboard plot
    try:
        dash_result = subprocess.run(
            [sys.executable, "-X", "utf8", "plot_dashboard.py"],
            cwd=str(Path(__file__).parent),
            capture_output=True, text=True, timeout=120,
        )
        if dash_result.returncode == 0:
            print("[auto-scheduler] dashboard.png updated")
        else:
            print(f"[auto-scheduler] dashboard plot FAILED (exit {dash_result.returncode})")
            if dash_result.stderr:
                for line in dash_result.stderr.strip().splitlines()[-5:]:
                    print(f"  [dashboard] {line}")
    except Exception as e:
        print(f"[auto-scheduler] dashboard plot failed: {e}")

    # Sync to Hugging Face — MONTHLY cadence (once per calendar month), only
    # when the scrape succeeded and wrote new data. Skippable via
    # MOLT_NO_HF_UPLOAD=1 or --no-upload. NOTE: upload_hf.py re-uploads
    # posts_all.jsonl (~4.7GB) + comments_all.jsonl (~18GB) in FULL each time,
    # so each monthly sync pushes ~23GB. (Daily would be ~700GB/mo — hence
    # monthly. Monthly sharding could cut even this; see README.)
    if not _should_skip_upload():
        if result.returncode != 0:
            print("[auto-scheduler] skip HF upload: scrape exited non-zero.")
        elif not _should_upload_monthly():
            print("[auto-scheduler] skip HF upload: already synced this month "
                  f"(last: {HF_UPLOAD_STAMP.read_text(encoding='utf-8').strip() if HF_UPLOAD_STAMP.exists() else '?'}).")
        elif not _run_wrote_new_data():
            print("[auto-scheduler] skip HF upload: monthly window open but no new data this run.")
        else:
            # Clean duplicate rows before publishing the monthly snapshot.
            # We hold scraper.lock, so the child must inherit it (dedup is a
            # mutating subcommand that would otherwise refuse to start).
            inherited_env = dict(os.environ)
            inherited_env["MOLT_LOCK_INHERITED"] = "1"
            print("[auto-scheduler] monthly dedup before publish (rewrites comments_all.jsonl)...")
            try:
                dd = subprocess.run(
                    [sys.executable, "-X", "utf8", "scraper.py", "--dedup-comments"],
                    cwd=str(Path(__file__).parent), env=inherited_env,
                )
                print(f"[auto-scheduler] dedup {'OK' if dd.returncode == 0 else f'FAILED (exit {dd.returncode})'}")
            except Exception as e:
                print(f"[auto-scheduler] dedup failed: {e}")

            print("[auto-scheduler] monthly Hugging Face sync (large upload; no timeout)...")
            try:
                up = subprocess.run(
                    [sys.executable, "-X", "utf8", "upload_hf.py"],
                    cwd=str(Path(__file__).parent),
                )
                if up.returncode == 0:
                    _record_hf_upload()
                    print("[auto-scheduler] HF sync OK")
                else:
                    print(f"[auto-scheduler] HF sync FAILED (exit {up.returncode})")
            except Exception as e:
                print(f"[auto-scheduler] HF sync failed: {e}")

    return result.returncode


def main():
    parser = argparse.ArgumentParser(description="Moltbook Auto Scheduler")
    parser.add_argument("--budget", type=float, default=DEFAULT_BUDGET_HOURS,
                        help=f"max hours for this run (default {DEFAULT_BUDGET_HOURS})")
    parser.add_argument("--dry-run", action="store_true",
                        help="show plan without running scraper")
    parser.add_argument("--no-upload", action="store_true",
                        help="skip the Hugging Face sync at the end of the run")
    args = parser.parse_args()

    if args.dry_run:
        strategy = pick_strategy(args.budget, dry_run=True)
        print("[dry-run] Would run with:")
        print(f"  --min-comments {strategy['threshold']}")
        if "max_comment_posts" in strategy:
            print(f"  --max-comment-posts {strategy['max_comment_posts']}")
        return

    # Take the lock BEFORE the planning scan (pick_strategy reads ~20GB and
    # can take 20+ minutes; planning without the lock invites a second
    # instance to start mutating files mid-scan). Exit non-zero so Task
    # Scheduler history shows the overlap instead of a green "0".
    if not acquire_lock():
        print("[skip] Another scraper instance is already running. Exiting.")
        sys.exit(3)

    try:
        strategy = pick_strategy(args.budget)
        sys.exit(run_scraper(strategy))
    finally:
        release_lock()


if __name__ == "__main__":
    main()
