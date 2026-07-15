# -*- coding: utf-8 -*-
"""
Generate the dataset-card figures for the HF dataset (jscmp4/Moltbook).

Aggregates posts_all.jsonl / comments_all.jsonl (optionally only a byte
prefix, to match exactly what a given HF push contained) and renders three
PNGs into data/figures/:

    daily_activity.png            posts+comments per day, event annotations
    top_communities.png           top-15 submolts by post count
    engagement_distributions.png  CCDF of posts/author and comments/post

Usage:
    python -X utf8 make_card_figures.py
    python -X utf8 make_card_figures.py --posts-bytes 5187185220 --comments-bytes 19087117057

The --*-bytes options limit each scan to the first N bytes, so the figures
describe a published snapshot (byte sizes of the files on HF) instead of the
live local files. Re-run and re-upload after each monthly HF refresh.
"""
import argparse
import collections
import json
from datetime import date
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker

DATA = Path(__file__).parent / "data"
OUTDIR = DATA / "figures"

# palette: validated light-mode set (see dataviz notes); baked light surface
# because HF renders the card PNG identically in light and dark themes
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
BASELINE = "#c3c2b7"
BLUE = "#2a78d6"
RED = "#e34948"


def iter_jsonl_prefix(path: Path, limit_bytes: int | None):
    """Yield parsed objects from the first limit_bytes of a JSONL file."""
    left = limit_bytes if limit_bytes else path.stat().st_size
    buf = b""
    with open(path, "rb") as f:
        while left > 0:
            chunk = f.read(min(1 << 25, left))
            if not chunk:
                break
            left -= len(chunk)
            buf += chunk
            *lines, buf = buf.split(b"\n")
            for ln in lines:
                if not ln.strip():
                    continue
                try:
                    yield json.loads(ln)
                except Exception:
                    continue


def aggregate_posts(limit_bytes):
    daily = collections.defaultdict(lambda: [0, 0])  # day -> [total, spam]
    by_submolt = collections.Counter()
    by_agent = collections.Counter()
    n = 0
    for p in iter_jsonl_prefix(DATA / "posts_all.jsonl", limit_bytes):
        n += 1
        d = (p.get("created_at") or "")[:10]
        if d:
            rec = daily[d]
            rec[0] += 1
            if p.get("is_spam") is True:
                rec[1] += 1
        sm = (p.get("submolt") or {}).get("name")
        if sm:
            by_submolt[sm] += 1
        aid = p.get("author_id")
        if aid:
            by_agent[aid] += 1
    return {
        "rows": n,
        "daily": dict(sorted(daily.items())),
        "top_submolts": by_submolt.most_common(15),
        "n_submolts": len(by_submolt),
        "n_agents": len(by_agent),
        "agent_post_dist": sorted(collections.Counter(by_agent.values()).items()),
    }


def aggregate_comments(limit_bytes):
    daily = collections.Counter()
    per_post = collections.Counter()
    n = 0
    for c in iter_jsonl_prefix(DATA / "comments_all.jsonl", limit_bytes):
        n += 1
        d = (c.get("created_at") or "")[:10]
        if d:
            daily[d] += 1
        pid = c.get("post_id")
        if pid:
            per_post[pid] += 1
    return {
        "rows": n,
        "daily": dict(sorted(daily.items())),
        "n_posts_with_comments": len(per_post),
        "comments_per_post_dist": sorted(collections.Counter(per_post.values()).items()),
    }


def despine(ax):
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.tick_params(length=0)


def ccdf(dist_pairs):
    total = sum(n for _, n in dist_pairs)
    xs, ys = [], []
    remaining = total
    for v, n in dist_pairs:
        xs.append(v)
        ys.append(remaining / total)
        remaining -= n
    return xs, ys


def render(posts, comments):
    OUTDIR.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({
        "font.family": ["Segoe UI", "DejaVu Sans", "sans-serif"],
        "figure.facecolor": SURFACE,
        "axes.facecolor": SURFACE,
        "savefig.facecolor": SURFACE,
        "axes.edgecolor": BASELINE,
        "axes.labelcolor": INK2,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "text.color": INK,
        "axes.grid": True,
        "grid.color": GRID,
        "grid.linewidth": 0.8,
        "axes.axisbelow": True,
        "axes.titlesize": 13,
        "axes.titlecolor": INK,
        "font.size": 10.5,
    })

    days = sorted(posts["daily"])
    first_day, last_day = days[0], days[-1]

    # ---------------------------------------------------- daily_activity.png
    xs = [date.fromisoformat(d) for d in days]
    tot = [posts["daily"][d][0] for d in days]
    spam = [posts["daily"][d][1] for d in days]
    nonspam = [t - s for t, s in zip(tot, spam)]
    cdays = sorted(comments["daily"])
    cxs = [date.fromisoformat(d) for d in cdays]
    cys = [comments["daily"][d] for d in cdays]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(9.6, 6.4), sharex=True,
                                   gridspec_kw={"hspace": 0.32})
    ax1.plot(xs, nonspam, color=BLUE, lw=2, solid_capstyle="round", label="not spam-flagged")
    spam_pts = [(x, s) for x, s in zip(xs, spam) if s > 0]
    ax1.plot([p[0] for p in spam_pts], [p[1] for p in spam_pts],
             color=RED, lw=2, solid_capstyle="round", label="spam-flagged")
    ax1.set_yscale("log")
    ax1.set_ylim(1, 6e5)
    ax1.set_title("Posts per day (log scale)", loc="left")
    ax1.legend(loc="upper right", frameon=False, fontsize=9.5, labelcolor=INK2)

    events = [
        (date(2026, 2, 6), "mbc-20 bot wave", "right", -4),
        (date(2026, 2, 17), "anti-spam\nintervention", "left", 4),
        (date(2026, 5, 6), "spam-flag\nregime change", "left", 4),
    ]
    for d0, label, ha, dx in events:
        ax1.axvline(d0, color=BASELINE, lw=1, ls=(0, (4, 3)), zorder=1)
        ax1.annotate(label, xy=(d0, 6e5), xytext=(dx, -2), textcoords="offset points",
                     fontsize=8.5, color=INK2, va="top", ha=ha)
    ax1.annotate("is_spam = 0\nfrom May 7 on", xy=(date(2026, 5, 9), 30),
                 fontsize=8.5, color=RED, va="center", ha="left")

    ax2.plot(cxs, cys, color=BLUE, lw=2, solid_capstyle="round")
    ax2.set_yscale("log")
    ax2.set_ylim(50, 8e6)
    ax2.set_title("Comments per day (log scale)", loc="left")
    for d0, *_ in events:
        ax2.axvline(d0, color=BASELINE, lw=1, ls=(0, (4, 3)), zorder=1)

    for ax in (ax1, ax2):
        despine(ax)
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
        ax.margins(x=0.01)

    fig.suptitle(f"Daily activity on Moltbook  ·  {first_day} → {last_day}",
                 x=0.045, ha="left", fontsize=14, fontweight="bold", color=INK)
    fig.text(0.045, 0.925,
             f"Published snapshot: {posts['rows']:,} posts, {comments['rows']:,} comments",
             fontsize=10, color=INK2)
    fig.subplots_adjust(top=0.86, bottom=0.07, left=0.075, right=0.97)
    fig.savefig(OUTDIR / "daily_activity.png", dpi=160)
    plt.close(fig)

    # -------------------------------------------------- top_communities.png
    top = posts["top_submolts"][::-1]
    names = [t[0] for t in top]
    vals = [t[1] for t in top]

    fig, ax = plt.subplots(figsize=(9.6, 5.6))
    bars = ax.barh(names, vals, color=BLUE, height=0.62, zorder=3)
    ax.set_xlim(0, max(vals) * 1.14)
    for v, bar in zip(vals, bars):
        ax.annotate(f"{v:,}", xy=(v, bar.get_y() + bar.get_height() / 2),
                    xytext=(5, 0), textcoords="offset points",
                    va="center", ha="left", fontsize=9, color=INK2)
    despine(ax)
    ax.grid(axis="y", visible=False)
    ax.xaxis.set_major_locator(mticker.MultipleLocator(5e5))
    ax.xaxis.set_major_formatter(lambda x, _: f"{x/1e6:.1f}M" if x else "0")
    ax.set_title("Top 15 communities (submolts) by post count", loc="left",
                 fontsize=14, fontweight="bold", pad=28)
    ax.text(0, 1.045,
            f"of {posts['n_submolts']:,} communities appearing in the published data  ·  "
            "mbc20 / mbc-20 are the Feb 2026 token-minting bot wave",
            transform=ax.transAxes, fontsize=10, color=INK2)
    fig.subplots_adjust(left=0.135, right=0.97, top=0.86, bottom=0.08)
    fig.savefig(OUTDIR / "top_communities.png", dpi=160)
    plt.close(fig)

    # ------------------------------------------ engagement_distributions.png
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(9.6, 4.2),
                                   gridspec_kw={"wspace": 0.28})
    x1, y1 = ccdf(posts["agent_post_dist"])
    ax1.plot(x1, y1, color=BLUE, lw=2)
    ax1.set_xscale("log"); ax1.set_yscale("log")
    ax1.set_title("Posts per author", loc="left")
    ax1.set_xlabel("posts by one author_id", color=INK2)
    ax1.set_ylabel("P(X ≥ x)", color=INK2)
    ax1.annotate(f"{posts['n_agents']:,} unique author_ids",
                 xy=(0.03, 0.06), xycoords="axes fraction", fontsize=9, color=INK2)

    x2, y2 = ccdf(comments["comments_per_post_dist"])
    ax2.plot(x2, y2, color=BLUE, lw=2)
    ax2.set_xscale("log"); ax2.set_yscale("log")
    ax2.set_title("Comments per post", loc="left")
    ax2.set_xlabel("collected comments on one post", color=INK2)
    ax2.annotate(f"{comments['n_posts_with_comments']:,} posts with collected comments\n"
                 "(comments fetched for posts with comment_count ≥ 3)",
                 xy=(0.03, 0.06), xycoords="axes fraction", fontsize=9, color=INK2)
    for ax in (ax1, ax2):
        despine(ax)

    fig.suptitle("Engagement distributions (CCDF, log-log)",
                 x=0.045, ha="left", fontsize=14, fontweight="bold", color=INK)
    fig.subplots_adjust(top=0.82, bottom=0.14, left=0.075, right=0.97)
    fig.savefig(OUTDIR / "engagement_distributions.png", dpi=160)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--posts-bytes", type=int, default=None,
                    help="scan only the first N bytes of posts_all.jsonl")
    ap.add_argument("--comments-bytes", type=int, default=None,
                    help="scan only the first N bytes of comments_all.jsonl")
    args = ap.parse_args()

    print("aggregating posts...")
    posts = aggregate_posts(args.posts_bytes)
    print(f"  {posts['rows']:,} posts, {posts['n_submolts']:,} submolts, "
          f"{posts['n_agents']:,} authors")
    print("aggregating comments...")
    comments = aggregate_comments(args.comments_bytes)
    print(f"  {comments['rows']:,} comments over {comments['n_posts_with_comments']:,} posts")
    render(posts, comments)
    print("figures written to", OUTDIR)


if __name__ == "__main__":
    main()
