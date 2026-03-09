# -*- coding: utf-8 -*-
"""
Basic online community analysis for local Moltbook data.

This script is intentionally simple and stream-based:
- Reads JSONL files line by line (works on multi-GB files)
- Produces a compact markdown report + CSV exports
- Focuses on baseline sociology-relevant metrics

Usage examples:
    python -X utf8 community_basic_analysis.py
    python -X utf8 community_basic_analysis.py --start-date 2026-02-01 --end-date 2026-02-29
    python -X utf8 community_basic_analysis.py --max-posts-scan 200000 --max-comments-scan 300000

Outputs (default: data/analysis):
    - community_basic_report.md
    - daily_activity.csv
    - submolt_summary.csv
    - summary.json
"""

import argparse
import csv
import json
import math
import sqlite3
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


def _to_int(value, default=0):
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def _to_day(value: str) -> str:
    if not value:
        return ""
    text = str(value)
    return text[:10] if len(text) >= 10 else ""


def _in_date_range(day: str, start_day: str, end_day: str) -> bool:
    if not day:
        return False
    if start_day and day < start_day:
        return False
    if end_day and day > end_day:
        return False
    return True


def _post_submolt_name(post_obj: dict) -> str:
    submolt = post_obj.get("submolt")
    if isinstance(submolt, dict):
        return str(submolt.get("name") or "").strip() or "(unknown)"
    if isinstance(submolt, str):
        return submolt.strip() or "(unknown)"
    return "(unknown)"


def _comment_submolt_name(comment_obj: dict) -> str:
    submolt = comment_obj.get("submolt")
    if isinstance(submolt, dict):
        return str(submolt.get("name") or "").strip() or "(unknown)"
    if isinstance(submolt, str):
        return submolt.strip() or "(unknown)"
    return "(unknown)"


def _bucket_comment_count(n: int) -> str:
    if n <= 0:
        return "0"
    if n <= 2:
        return "1-2"
    if n <= 9:
        return "3-9"
    if n <= 29:
        return "10-29"
    if n <= 99:
        return "30-99"
    if n <= 299:
        return "100-299"
    return "300+"


def _fmt_pct(part: int, whole: int) -> str:
    if whole <= 0:
        return "0.0%"
    return f"{(part / whole * 100.0):.1f}%"


def _hhi(counter: Counter, total: int) -> float:
    if total <= 0:
        return 0.0
    s = 0.0
    for v in counter.values():
        p = v / total
        s += p * p
    return s


@dataclass
class PostsStats:
    scanned_rows: int = 0
    kept_rows: int = 0
    bad_json: int = 0
    spam_rows: int = 0
    nonspam_rows: int = 0
    expected_comments_sum: int = 0
    min_day: str = ""
    max_day: str = ""
    daily_posts: Counter = field(default_factory=Counter)
    daily_posts_nonspam: Counter = field(default_factory=Counter)
    daily_expected_comments: Counter = field(default_factory=Counter)
    submolt_posts: Counter = field(default_factory=Counter)
    submolt_posts_nonspam: Counter = field(default_factory=Counter)
    submolt_expected_comments: Counter = field(default_factory=Counter)
    comment_count_buckets: Counter = field(default_factory=Counter)


@dataclass
class CommentsStats:
    scanned_rows: int = 0
    kept_rows: int = 0
    bad_json: int = 0
    min_day: str = ""
    max_day: str = ""
    daily_comments: Counter = field(default_factory=Counter)
    submolt_comments: Counter = field(default_factory=Counter)
    depth_counter: Counter = field(default_factory=Counter)
    unique_total_sqlite: int = 0


@dataclass
class AgentsStats:
    scanned_rows: int = 0
    kept_rows: int = 0
    bad_json: int = 0
    claimed_rows: int = 0
    active_rows: int = 0
    follower_sum: int = 0
    top_followed: list = field(default_factory=list)  # tuples: (followers, name, id, karma)


def _scan_posts(
    path: Path,
    start_day: str,
    end_day: str,
    max_rows: int,
    heartbeat_every: int,
) -> PostsStats:
    stats = PostsStats()
    if not path.exists():
        return stats

    print(f"[scan posts] {path}")
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            stats.scanned_rows += 1
            if heartbeat_every > 0 and stats.scanned_rows % heartbeat_every == 0:
                print(f"  [heartbeat posts] scanned={stats.scanned_rows:,}, kept={stats.kept_rows:,}")
            if max_rows > 0 and stats.scanned_rows > max_rows:
                break
            try:
                p = json.loads(line)
            except Exception:
                stats.bad_json += 1
                continue

            day = _to_day(p.get("created_at", ""))
            if not _in_date_range(day, start_day, end_day):
                continue

            stats.kept_rows += 1
            if not stats.min_day or day < stats.min_day:
                stats.min_day = day
            if not stats.max_day or day > stats.max_day:
                stats.max_day = day

            is_spam = bool(p.get("is_spam", False))
            comment_count = max(0, _to_int(p.get("comment_count"), 0))
            submolt = _post_submolt_name(p)

            stats.daily_posts[day] += 1
            stats.daily_expected_comments[day] += comment_count
            stats.submolt_posts[submolt] += 1
            stats.submolt_expected_comments[submolt] += comment_count
            stats.comment_count_buckets[_bucket_comment_count(comment_count)] += 1
            stats.expected_comments_sum += comment_count

            if is_spam:
                stats.spam_rows += 1
            else:
                stats.nonspam_rows += 1
                stats.daily_posts_nonspam[day] += 1
                stats.submolt_posts_nonspam[submolt] += 1
    return stats


def _scan_comments(
    path: Path,
    start_day: str,
    end_day: str,
    max_rows: int,
    heartbeat_every: int,
) -> CommentsStats:
    stats = CommentsStats()
    if not path.exists():
        return stats

    print(f"[scan comments] {path}")
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            stats.scanned_rows += 1
            if heartbeat_every > 0 and stats.scanned_rows % heartbeat_every == 0:
                print(f"  [heartbeat comments] scanned={stats.scanned_rows:,}, kept={stats.kept_rows:,}")
            if max_rows > 0 and stats.scanned_rows > max_rows:
                break
            try:
                c = json.loads(line)
            except Exception:
                stats.bad_json += 1
                continue

            day = _to_day(c.get("created_at", ""))
            if not _in_date_range(day, start_day, end_day):
                continue

            stats.kept_rows += 1
            if not stats.min_day or day < stats.min_day:
                stats.min_day = day
            if not stats.max_day or day > stats.max_day:
                stats.max_day = day

            depth = max(0, _to_int(c.get("depth"), 0))
            submolt = _comment_submolt_name(c)
            stats.daily_comments[day] += 1
            stats.submolt_comments[submolt] += 1
            stats.depth_counter[depth] += 1
    return stats


def _scan_agents(path: Path, max_rows: int, top_n: int) -> AgentsStats:
    stats = AgentsStats()
    if not path.exists():
        return stats

    print(f"[scan agents] {path}")
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            stats.scanned_rows += 1
            if max_rows > 0 and stats.scanned_rows > max_rows:
                break
            try:
                a = json.loads(line)
            except Exception:
                stats.bad_json += 1
                continue

            stats.kept_rows += 1
            followers = max(0, _to_int(a.get("followerCount"), 0))
            karma = _to_int(a.get("karma"), 0)
            name = str(a.get("name") or "")
            aid = str(a.get("id") or "")
            stats.follower_sum += followers
            if bool(a.get("isClaimed", False)):
                stats.claimed_rows += 1
            if bool(a.get("isActive", False)):
                stats.active_rows += 1
            stats.top_followed.append((followers, name, aid, karma))

    stats.top_followed.sort(key=lambda x: (x[0], x[3]), reverse=True)
    stats.top_followed = stats.top_followed[: max(1, top_n)]
    return stats


def _read_unique_comments_from_sqlite(db_path: Path) -> int:
    """
    Return unique comment rows from sqlite id cache when available.
    This reads prebuilt dedup state from scraper.py (comment_ids table).
    """
    if not db_path.exists():
        return 0
    conn = None
    try:
        conn = sqlite3.connect(str(db_path), timeout=5.0)
        row = conn.execute("SELECT COUNT(*) FROM comment_ids").fetchone()
        return max(0, _to_int(row[0] if row else 0, 0))
    except Exception:
        return 0
    finally:
        if conn is not None:
            conn.close()


def _write_daily_csv(out_path: Path, posts: PostsStats, comments: CommentsStats):
    all_days = sorted(set(posts.daily_posts.keys()) | set(comments.daily_comments.keys()))
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "date",
                "posts_total",
                "posts_nonspam",
                "expected_comments_from_posts",
                "observed_comments_rows",
                "observed_comments_per_post",
                "expected_comments_per_post",
            ]
        )
        for d in all_days:
            p = posts.daily_posts.get(d, 0)
            p_nonspam = posts.daily_posts_nonspam.get(d, 0)
            exp_c = posts.daily_expected_comments.get(d, 0)
            obs_c = comments.daily_comments.get(d, 0)
            obs_cpp = (obs_c / p) if p > 0 else 0.0
            exp_cpp = (exp_c / p) if p > 0 else 0.0
            w.writerow([d, p, p_nonspam, exp_c, obs_c, f"{obs_cpp:.4f}", f"{exp_cpp:.4f}"])


def _write_submolt_csv(out_path: Path, posts: PostsStats, comments: CommentsStats, top_n: int):
    names = sorted(set(posts.submolt_posts.keys()) | set(comments.submolt_comments.keys()))
    rows = []
    for name in names:
        p = posts.submolt_posts.get(name, 0)
        p_nonspam = posts.submolt_posts_nonspam.get(name, 0)
        exp_c = posts.submolt_expected_comments.get(name, 0)
        obs_c = comments.submolt_comments.get(name, 0)
        avg_exp = exp_c / p if p > 0 else 0.0
        rows.append((name, p, p_nonspam, exp_c, obs_c, avg_exp))
    rows.sort(key=lambda x: (x[1], x[3]), reverse=True)
    rows = rows[: max(1, top_n)]

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "submolt",
                "posts_total",
                "posts_nonspam",
                "expected_comments_from_posts",
                "observed_comments_rows",
                "avg_expected_comments_per_post",
                "observed_to_expected_ratio",
            ]
        )
        for name, p, p_nonspam, exp_c, obs_c, avg_exp in rows:
            ratio = (obs_c / exp_c) if exp_c > 0 else 0.0
            w.writerow([name, p, p_nonspam, exp_c, obs_c, f"{avg_exp:.4f}", f"{ratio:.4f}"])
    return rows


def _write_report(
    out_path: Path,
    posts: PostsStats,
    comments: CommentsStats,
    agents: AgentsStats,
    submolt_rows: list,
    args,
):
    all_days = sorted(set(posts.daily_posts.keys()) | set(comments.daily_comments.keys()))
    active_days = len(all_days)
    avg_posts = (posts.kept_rows / active_days) if active_days > 0 else 0.0
    avg_expected_comments = (posts.expected_comments_sum / active_days) if active_days > 0 else 0.0
    avg_observed_comments = (comments.kept_rows / active_days) if active_days > 0 else 0.0

    top10_posts = sum(v for _, v in posts.submolt_posts.most_common(10))
    top10_share = (top10_posts / posts.kept_rows) if posts.kept_rows > 0 else 0.0
    hhi = _hhi(posts.submolt_posts, posts.kept_rows)
    effective_n = (1.0 / hhi) if hhi > 0 else 0.0

    depth_total = sum(comments.depth_counter.values())
    depth0 = comments.depth_counter.get(0, 0)
    depth_gt0 = depth_total - depth0

    bucket_order = ["0", "1-2", "3-9", "10-29", "30-99", "100-299", "300+"]
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    lines = []
    lines.append("# Community Basic Analysis")
    lines.append("")
    lines.append(f"- generated_at_utc: {now_utc}")
    lines.append(f"- date_filter: {args.start_date or 'min'} -> {args.end_date or 'max'}")
    lines.append("")
    lines.append("## 1) Scale")
    lines.append(f"- posts (kept): {posts.kept_rows:,}  |  spam: {posts.spam_rows:,} ({_fmt_pct(posts.spam_rows, posts.kept_rows)})")
    lines.append(f"- comments rows (kept): {comments.kept_rows:,}")
    if comments.unique_total_sqlite > 0:
        dup_rows = max(0, comments.kept_rows - comments.unique_total_sqlite)
        lines.append(
            f"- comments unique (sqlite cache): {comments.unique_total_sqlite:,}  "
            f"|  duplicate rows estimate: {dup_rows:,}"
        )
    lines.append(f"- expected comments from posts.comment_count: {posts.expected_comments_sum:,}")
    lines.append(f"- agents (kept): {agents.kept_rows:,}")
    lines.append("")
    lines.append("## 2) Time Window")
    lines.append(f"- posts window: {posts.min_day or '?'} -> {posts.max_day or '?'}")
    lines.append(f"- comments window: {comments.min_day or '?'} -> {comments.max_day or '?'}")
    lines.append(f"- active days: {active_days:,}")
    lines.append(f"- avg posts/day: {avg_posts:,.1f}")
    lines.append(f"- avg expected comments/day: {avg_expected_comments:,.1f}")
    lines.append(f"- avg observed comments/day: {avg_observed_comments:,.1f}")
    lines.append("")
    lines.append("## 3) Community Concentration (by posts)")
    lines.append(f"- active submolts: {len(posts.submolt_posts):,}")
    lines.append(f"- top 10 submolts share of posts: {top10_share * 100:.1f}%")
    lines.append(f"- HHI: {hhi:.4f}")
    lines.append(f"- effective community count (1/HHI): {effective_n:.1f}")
    lines.append("")
    lines.append("## 4) Engagement Structure (post comment_count)")
    lines.append("| bucket | posts | share |")
    lines.append("|---|---:|---:|")
    for b in bucket_order:
        cnt = posts.comment_count_buckets.get(b, 0)
        lines.append(f"| {b} | {cnt:,} | {_fmt_pct(cnt, posts.kept_rows)} |")
    lines.append("")
    lines.append("## 5) Comment Depth")
    lines.append(f"- depth==0: {depth0:,} ({_fmt_pct(depth0, depth_total)})")
    lines.append(f"- depth>0: {depth_gt0:,} ({_fmt_pct(depth_gt0, depth_total)})")
    lines.append("")
    lines.append(f"## 6) Top {len(submolt_rows):,} Submolts")
    lines.append("| submolt | posts | nonspam posts | expected comments | observed comments rows | avg expected comments/post |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for row in submolt_rows:
        name, p, p_nonspam, exp_c, obs_c, avg_exp = row
        lines.append(f"| {name} | {p:,} | {p_nonspam:,} | {exp_c:,} | {obs_c:,} | {avg_exp:.2f} |")
    lines.append("")
    lines.append(f"## 7) Top {len(agents.top_followed):,} Agents by Followers")
    lines.append("| followers | karma | name | agent_id |")
    lines.append("|---:|---:|---|---|")
    for followers, name, aid, karma in agents.top_followed:
        lines.append(f"| {followers:,} | {karma:,} | {name} | {aid} |")
    lines.append("")
    lines.append("## 8) Data Quality Notes")
    lines.append(f"- posts bad_json: {posts.bad_json:,}")
    lines.append(f"- comments bad_json: {comments.bad_json:,}")
    lines.append(f"- agents bad_json: {agents.bad_json:,}")
    lines.append("- observed comments are local rows from comments_all.jsonl; expected comments come from post.comment_count.")
    if comments.unique_total_sqlite > 0:
        lines.append("- unique comments are from data/comments_id_cache.sqlite (comment_ids table).")
    lines.append("")
    lines.append("## 9) Next Sociology Questions")
    lines.append("- Which submolts have high posting volume but low reply intensity (broadcast-like norms)?")
    lines.append("- Did spam enforcement periods change concentration (top10 share / HHI) over time?")
    lines.append("- Are high-follower agents concentrated in a few submolts or spread across communities?")
    lines.append("- Are deep-thread conversations (depth>0) clustered in specific communities?")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_summary_json(out_path: Path, posts: PostsStats, comments: CommentsStats, agents: AgentsStats):
    payload = {
        "posts_kept_rows": posts.kept_rows,
        "posts_spam_rows": posts.spam_rows,
        "posts_nonspam_rows": posts.nonspam_rows,
        "posts_expected_comments_sum": posts.expected_comments_sum,
        "comments_kept_rows": comments.kept_rows,
        "comments_unique_total_sqlite": comments.unique_total_sqlite,
        "agents_kept_rows": agents.kept_rows,
        "posts_window": [posts.min_day, posts.max_day],
        "comments_window": [comments.min_day, comments.max_day],
        "unique_submolts_in_posts": len(posts.submolt_posts),
        "depth_distribution": {str(k): int(v) for k, v in sorted(comments.depth_counter.items())},
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _validate_date(text: str, flag: str) -> str:
    if not text:
        return ""
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except Exception as e:
        raise SystemExit(f"{flag} must be YYYY-MM-DD: {e}")
    return text


def main():
    parser = argparse.ArgumentParser(description="Basic online community analysis for Moltbook JSONL files")
    parser.add_argument("--data-dir", default="data", help="directory containing posts/comments/agents JSONL files")
    parser.add_argument("--output-dir", default="data/analysis", help="analysis output directory")
    parser.add_argument("--top-n", type=int, default=20, help="top N communities/agents in report tables")
    parser.add_argument("--start-date", default="", help="optional start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default="", help="optional end date (YYYY-MM-DD)")
    parser.add_argument("--max-posts-scan", type=int, default=0, help="debug cap for scanned post rows (0=all)")
    parser.add_argument("--max-comments-scan", type=int, default=0, help="debug cap for scanned comment rows (0=all)")
    parser.add_argument("--max-agents-scan", type=int, default=0, help="debug cap for scanned agent rows (0=all)")
    parser.add_argument("--heartbeat-posts", type=int, default=500_000, help="heartbeat interval while scanning posts")
    parser.add_argument("--heartbeat-comments", type=int, default=1_000_000, help="heartbeat interval while scanning comments")
    args = parser.parse_args()

    args.start_date = _validate_date(args.start_date, "--start-date")
    args.end_date = _validate_date(args.end_date, "--end-date")
    if args.start_date and args.end_date and args.start_date > args.end_date:
        raise SystemExit("--start-date must be <= --end-date")

    data_dir = Path(args.data_dir)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    posts_path = data_dir / "posts_all.jsonl"
    comments_path = data_dir / "comments_all.jsonl"
    agents_path = data_dir / "agents_seen.jsonl"
    if not posts_path.exists():
        raise SystemExit(f"missing required file: {posts_path}")

    print("\n=== Community Basic Analysis ===")
    print(f"data_dir: {data_dir}")
    print(f"output_dir: {out_dir}")
    if args.start_date or args.end_date:
        print(f"date filter: {args.start_date or 'min'} -> {args.end_date or 'max'}")

    posts = _scan_posts(
        posts_path,
        start_day=args.start_date,
        end_day=args.end_date,
        max_rows=max(0, args.max_posts_scan),
        heartbeat_every=max(0, args.heartbeat_posts),
    )
    comments = _scan_comments(
        comments_path,
        start_day=args.start_date,
        end_day=args.end_date,
        max_rows=max(0, args.max_comments_scan),
        heartbeat_every=max(0, args.heartbeat_comments),
    )
    if (
        not args.start_date
        and not args.end_date
        and max(0, args.max_comments_scan) == 0
    ):
        comments.unique_total_sqlite = _read_unique_comments_from_sqlite(data_dir / "comments_id_cache.sqlite")
    agents = _scan_agents(
        agents_path,
        max_rows=max(0, args.max_agents_scan),
        top_n=max(1, args.top_n),
    )

    daily_csv = out_dir / "daily_activity.csv"
    submolt_csv = out_dir / "submolt_summary.csv"
    report_md = out_dir / "community_basic_report.md"
    summary_json = out_dir / "summary.json"

    _write_daily_csv(daily_csv, posts, comments)
    submolt_rows = _write_submolt_csv(submolt_csv, posts, comments, top_n=max(1, args.top_n))
    _write_report(report_md, posts, comments, agents, submolt_rows, args)
    _write_summary_json(summary_json, posts, comments, agents)

    print("\n[done]")
    print(f"  report: {report_md}")
    print(f"  daily csv: {daily_csv}")
    print(f"  submolt csv: {submolt_csv}")
    print(f"  summary json: {summary_json}")
    print(f"  posts kept: {posts.kept_rows:,} | comments kept: {comments.kept_rows:,} | agents kept: {agents.kept_rows:,}")


if __name__ == "__main__":
    main()
