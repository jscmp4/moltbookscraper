# -*- coding: utf-8 -*-
"""
Moltbook Data Scraper - v2 (incremental + checkpoint + progress bar)
Collects posts, comments, submolts, and agent profiles from moltbook.com API
for sociology research on AI agents.

Usage:
    python -X utf8 scraper.py                  # incremental run
    python -X utf8 scraper.py --full           # full historical crawl
    python -X utf8 scraper.py --no-comments    # posts only
    python -X utf8 scraper.py --max-posts 500  # limit new posts this run
    python -X utf8 scraper.py --reset          # reset checkpoint then run
    python -X utf8 scraper.py --clean-runs     # clean data/runs snapshots
    python -X utf8 scraper.py --check          # data health check

NOTE:
    On Windows, run with `python -X utf8` to avoid console encoding issues.

Data files:
    data/posts_all.jsonl
    data/comments_all.jsonl
    data/comments_done_posts.txt
    data/submolts.json
    data/agents_seen.jsonl
    data/checkpoint.json
    data/runs/YYYYMMDD_*.jsonl
"""

import requests
import json
import time
import argparse
import sys
import os
import atexit
import threading
import random
import hashlib
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, date as date_type, timedelta
from collections import Counter
from pathlib import Path
from tqdm import tqdm
from email.utils import parsedate_to_datetime

# Fix Windows terminal encoding for emoji/unicode
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

BASE_URL = "https://www.moltbook.com/api/v1"
HEADERS = {"User-Agent": "AcademicResearchBot/2.0 (computational social science)"}

# Safety cap on any single rate-limit / backoff sleep. Server-sent Retry-After
# or X-RateLimit-Reset can be a far-future HTTP-date or a millisecond-encoded
# epoch; without a ceiling one bad header would freeze every worker (via the
# shared global cooldown) for hours-to-years. We cap, then re-check and retry.
_MAX_RL_WAIT = 600  # seconds (10 min)

# Hard backstop on comment pages fetched per post, to bound an infinite
# pagination loop if the API returns a non-advancing / cyclic next_cursor.
# ~1M comments/post — far beyond any real post; the real guard is the
# next_cursor-did-not-advance check.
_MAX_COMMENT_PAGES = 10000

# A run lock older than this is treated as stale regardless of PID liveness,
# so a crash (atexit never fires) followed by OS PID reuse can't wedge the
# scheduler into refusing every future run. Longest real run is the scheduler
# budget (~10h); 24h is safely beyond it.
_MAX_LOCK_AGE_SECONDS = 24 * 3600

# Set when auth is dead platform-wide; checked by both stages so a dead key
# aborts the run instead of burning hours of futile per-post retries.
# 401 sets it immediately; 403 only after 3 CONSECUTIVE failures, because a
# single deleted/private post legitimately 403s without the key being dead.
_FATAL_AUTH_EVENT = threading.Event()
_auth_403_streak = 0
_auth_403_lock = threading.Lock()

# API key
# ?.env ?key
def _load_api_key() -> str:
    import os
    key = os.environ.get("MOLTBOOK_API_KEY", "")
    if not key:
        env_path = Path(__file__).parent / ".env"
        if env_path.exists():
            for line in env_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line.startswith("MOLTBOOK_API_KEY="):
                    key = line.split("=", 1)[1].strip()
                    break
    if key:
        HEADERS["Authorization"] = f"Bearer {key}"
    return key

_load_api_key()  # ?key?HEADERS["Authorization"]


#
# RATE LIMITER ()
#

class RateLimiter:
    """Thread-safe rate limiter shared across workers."""
    def __init__(self, max_per_minute=90):
        self._interval = 60.0 / max_per_minute  #
        self._lock = threading.Lock()
        self._last = 0.0

    def set_rate(self, max_per_minute):
        max_per_minute = max(1, int(max_per_minute))
        with self._lock:
            self._interval = 60.0 / max_per_minute

    def wait(self):
        with self._lock:
            elapsed = time.monotonic() - self._last
            gap = self._interval - elapsed
            if gap > 0:
                time.sleep(gap)
            self._last = time.monotonic()


_DEFAULT_READ_RPM = 40
_DEFAULT_COMMENT_RPM = 38
_DEFAULT_COMMENT_QUEUE_STRATEGY = "layered"
_DEFAULT_QUEUE_SMALL_MAX = 80
_DEFAULT_QUEUE_MEDIUM_MAX = 400
_DEFAULT_POST_ZERO_STREAK_GUARD = 300
_DEFAULT_POST_MAX_RECOVER_RETRIES = 6
_DEFAULT_COMMENT_ID_CACHE_MODE = "memory"
_rate_limiter          = RateLimiter(max_per_minute=_DEFAULT_READ_RPM)      # GET endpoints
_rate_limiter_comments = RateLimiter(max_per_minute=_DEFAULT_COMMENT_RPM)   # comments endpoints, slightly more conservative

# ?429
_global_cooldown_until = 0.0
_global_cooldown_lock  = threading.Lock()


#
# HTTP
#

def _countdown(seconds: float):
    """Print remaining wait time every 10s so progress is visible."""
    remaining = int(seconds)
    while remaining > 0:
        tqdm.write(f"  [wait] {remaining}s remaining...")
        chunk = min(10, remaining)
        time.sleep(chunk)
        remaining -= chunk


def _to_int(value, default=0):
    try:
        return int(float(value))
    except Exception:
        return default


def _id_fingerprint(value: str) -> int:
    """Stable 64-bit fingerprint used for in-memory dedup sets."""
    raw = str(value).encode("utf-8", errors="ignore")
    return int.from_bytes(hashlib.blake2b(raw, digest_size=8).digest(), "big")


def _pair_fingerprint(a: str, b: str) -> int:
    """Stable 64-bit fingerprint for (post_id, comment_id) pair."""
    raw = f"{a}|{b}".encode("utf-8", errors="ignore")
    return int.from_bytes(hashlib.blake2b(raw, digest_size=8).digest(), "big")


def _retry_after_seconds(resp, body=None):
    """
    Parse retry delay from response body/header.
    Retry-After may be either integer seconds or an HTTP-date string.
    """
    raw = None
    if isinstance(body, dict):
        raw = (
            body.get("retry_after_seconds")
            or body.get("retry_after")
            or body.get("retryAfter")
        )
    if raw is None:
        raw = resp.headers.get("Retry-After")
    if raw is None:
        return 0

    # Case 1: delta-seconds
    try:
        sec = int(float(raw))
        return max(0, sec)
    except Exception:
        pass

    # Case 2: HTTP-date
    try:
        dt = parsedate_to_datetime(str(raw))
        if dt is None:
            return 0
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        sec = int((dt - datetime.now(timezone.utc)).total_seconds())
        return max(0, sec)
    except Exception:
        return 0


def api_get(endpoint, params=None, retries=3, skip_on_ratelimit=False):
    global _global_cooldown_until, _auth_403_streak
    url = f"{BASE_URL}{endpoint}"
    limiter = _rate_limiter_comments if endpoint.endswith("/comments") else _rate_limiter
    for attempt in range(retries):
        # Global cooldown: if another worker hit 429, wait here.
        now = time.time()
        with _global_cooldown_lock:
            cooldown_remaining = _global_cooldown_until - now
        if cooldown_remaining > 0:
            if cooldown_remaining >= 1:
                tqdm.write(f"  [global cooldown] wait {cooldown_remaining:.0f}s...")
            time.sleep(cooldown_remaining)

        limiter.wait()  # thread-safe throttling
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=30)

            # Honor server rate-limit headers before hard 429.
            remaining = _to_int(r.headers.get("X-RateLimit-Remaining", 999), 999)
            reset_ts  = _to_int(r.headers.get("X-RateLimit-Reset", 0), 0)
            rl_limit  = _to_int(r.headers.get("X-RateLimit-Limit", 0), 0)
            window_s  = max(1, reset_ts - int(time.time()))
            if remaining <= 30 and r.status_code != 429:
                tqdm.write(f"  [rl] remaining={remaining}/{rl_limit}  reset_in={window_s}s")
            if remaining <= 15 and r.status_code != 429:
                wait = min(_MAX_RL_WAIT, max(65, reset_ts - time.time() + 2))
                tqdm.write(f"  [rate limit] remaining={remaining}, wait {wait:.0f}s for reset...")
                with _global_cooldown_lock:
                    _global_cooldown_until = max(_global_cooldown_until, time.time() + wait)
                time.sleep(wait)

            if r.status_code == 429:
                # Prefer server retry-after/reset, then exponential backoff + jitter.
                body = None
                try:
                    body = r.json()
                except Exception:
                    body = None

                retry_after = _retry_after_seconds(r, body)
                wait_to_reset = max(0, _to_int(r.headers.get("X-RateLimit-Reset", 0), 0) - int(time.time()))
                if retry_after > 0 or wait_to_reset > 0:
                    wait = min(_MAX_RL_WAIT, max(retry_after, wait_to_reset) + random.uniform(1, 6))
                else:
                    base = min(300, 20 * (2 ** attempt))
                    wait = base + random.uniform(0, max(5, base * 0.35))
                with _global_cooldown_lock:
                    _global_cooldown_until = max(_global_cooldown_until, time.time() + wait)
                if skip_on_ratelimit:
                    tqdm.write(f"  [rate limit] {endpoint} limited, skip (cooldown {wait:.0f}s)")
                    return None
                tqdm.write(f"  [rate limit 429] attempt={attempt+1}, retry in {wait:.0f}s...")
                _countdown(wait)
                continue
            r.raise_for_status()
            with _auth_403_lock:
                _auth_403_streak = 0
            return r.json()
        except requests.HTTPError as e:
            status = _to_int(getattr(r, "status_code", 0), 0)
            tqdm.write(f"  HTTP {status} on {url}: {e}")
            if status in (401, 403):
                if status == 401:
                    _FATAL_AUTH_EVENT.set()
                else:
                    with _auth_403_lock:
                        _auth_403_streak += 1
                        if _auth_403_streak >= 3:
                            _FATAL_AUTH_EVENT.set()
                return {"success": False, "_fatal_auth": status, "_error": str(e)}
            if 500 <= status < 600 and attempt < retries - 1:
                wait_s = min(30, 5 * (attempt + 1))
                time.sleep(wait_s)
                continue
            return None
        except Exception as e:
            tqdm.write(f"  Error ({attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(5)
    return None


#
# CHECKPOINT (?
#

class Checkpoint:
    """Persistent run metadata and cursor state stored in checkpoint.json."""

    def __init__(self, path: Path):
        self.path = path
        self.data = self._load()

    def _load(self):
        if self.path.exists():
            try:
                with open(self.path, encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError):
                print("  [!] checkpoint.json corrupted, ignore and restart.")
        return {
            "newest_post_created_at": None,
            "newest_post_id": None,
            "total_posts": 0,
            "total_comments": 0,
            "runs": [],
        }

    def save(self):
        """Atomic write: write .tmp then rename."""
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
        tmp.replace(self.path)

    def get_last_newest_time(self):
        return self.data.get("newest_post_created_at")

    def update_after_run(self, new_posts, new_comments, duration_s, newest_post=None,
                         clear_cursor=True, reached_end=False, advance_anchor=True):
        # advance_anchor must only be True when the run actually closed the
        # window down to the previous anchor (stopped_early/reached_end);
        # otherwise the next incremental would start above unfetched posts
        # and the gap would never be retried.
        if newest_post and advance_anchor:
            current = self.data.get("newest_post_created_at")
            candidate = newest_post.get("created_at")
            if not current or (candidate and candidate > current):
                self.data["newest_post_created_at"] = candidate
                self.data["newest_post_id"] = newest_post.get("id")

        self.data["total_posts"] = self.data.get("total_posts", 0) + new_posts
        self.data["total_comments"] = self.data.get("total_comments", 0) + new_comments
        self.data.setdefault("runs", []).append({
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "new_posts": new_posts,
            "new_comments": new_comments,
            "duration_s": round(duration_s, 1),
        })
        if clear_cursor:
            self.data.pop("_resume_cursor", None)
            self.data.pop("_resume_since", None)
            self.data.pop("_resume_newest_post", None)
        if reached_end:
            self.data.pop("_bottom_cursor", None)
        self.save()

    def save_resume_cursor(self, cursor, since_time, newest_post=None, bottom_date=""):
        self.data["_resume_cursor"] = cursor
        self.data["_resume_since"] = since_time
        if bottom_date:
            self.data["_bottom_cursor"] = {"cursor": cursor, "date": bottom_date}
            current_oldest = self.data.get("oldest_post_created_at", "")
            if not current_oldest or bottom_date < current_oldest:
                self.data["oldest_post_created_at"] = bottom_date
        if newest_post:
            current = self.data.get("_resume_newest_post")
            current_created = ""
            if isinstance(current, dict):
                current_created = current.get("created_at", "")
            candidate_created = newest_post.get("created_at", "")
            if not current_created or (candidate_created and candidate_created > current_created):
                self.data["_resume_newest_post"] = newest_post
        self.save()

    def get_resume_cursor(self):
        cursor = self.data.get("_resume_cursor")
        since = self.data.get("_resume_since")
        newest = self.data.get("_resume_newest_post")
        return cursor, since, newest

    def get_bottom_cursor(self):
        bc = self.data.get("_bottom_cursor")
        if not bc:
            return None, ""
        return bc.get("cursor"), bc.get("date", "")

    def clear_resume(self):
        self.data.pop("_resume_cursor", None)
        self.data.pop("_resume_since", None)
        self.data.pop("_resume_newest_post", None)
        self.save()


#
# DATA FILES ()
#

class JsonlStore:
    """Append-only JSONL storage with in-memory dedup by id."""

    def __init__(self, path: Path, load_ids: bool = True):
        self.path = path
        self.seen_ids = self._load_ids() if load_ids else set()

    def _load_ids(self):
        ids = set()
        if self.path.exists():
            with open(self.path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            ids.add(json.loads(line)["id"])
                        except Exception:
                            pass
        return ids

    def append_new(self, records):
        """Append non-duplicate records and return number written."""
        new = [r for r in records if r.get("id") not in self.seen_ids]
        if new:
            with open(self.path, "a", encoding="utf-8") as f:
                for r in new:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
                    self.seen_ids.add(r["id"])
        return len(new)

    def count(self):
        return len(self.seen_ids)


class CommentsDoneCache:
    """Track posts whose comments are already fully fetched."""

    def __init__(self, path: Path, comments_jsonl: Path = None):
        self.path = path
        self.done_ids: set = set()
        if path.exists():
            self._load()
        elif comments_jsonl and comments_jsonl.exists():
            self._migrate(comments_jsonl)

    def _load(self):
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                pid = line.strip()
                if pid:
                    self.done_ids.add(pid)

    def _migrate(self, jsonl: Path):
        """Build done-cache from comments_all.jsonl once on first run."""
        size_mb = jsonl.stat().st_size / 1024 / 1024
        print(f"  [init] building comments done-cache from comments_all.jsonl ({size_mb:.0f} MB), one-time...",
              end="", flush=True)
        with open(jsonl, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    pid = json.loads(line).get("post_id")
                    if pid:
                        self.done_ids.add(pid)
                except Exception:
                    pass
        with open(self.path, "w", encoding="utf-8") as f:
            for pid in self.done_ids:
                f.write(pid + "\n")
        print(f" {len(self.done_ids):,} posts done")

    def mark_done(self, post_id: str):
        """Mark one post as done and append to cache file."""
        if post_id not in self.done_ids:
            self.done_ids.add(post_id)
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(post_id + "\n")

    def is_done(self, post_id: str) -> bool:
        return post_id in self.done_ids

    def count(self) -> int:
        return len(self.done_ids)


#
# FETCHERS
#

class CommentsResumeCache:
    """Append-only cache for per-post comment resume cursors."""

    def __init__(self, path: Path):
        self.path = path
        self.cursors: dict = {}
        self._line_count = 0
        if self.path.exists():
            self._load()
            self.compact_if_needed()

    def _load(self):
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    self._line_count += 1
                    pid = rec.get("post_id")
                    if not pid:
                        continue
                    cursor = rec.get("cursor")
                    if cursor:
                        self.cursors[pid] = cursor
                    else:
                        self.cursors.pop(pid, None)
                except Exception:
                    pass

    def get(self, post_id: str):
        return self.cursors.get(post_id)

    def set(self, post_id: str, cursor):
        if cursor:
            self.cursors[post_id] = cursor
        else:
            self.cursors.pop(post_id, None)
        rec = {
            "post_id": post_id,
            "cursor": cursor or "",
            "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        self._line_count += 1
        self.compact_if_needed()

    def count(self) -> int:
        return len(self.cursors)

    def compact_if_needed(self, force: bool = False):
        target = max(10_000, len(self.cursors) * 8)
        if not force and self._line_count <= target:
            return
        tmp = self.path.with_suffix(".compact_tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            for pid, cursor in self.cursors.items():
                rec = {
                    "post_id": pid,
                    "cursor": cursor,
                    "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        tmp.replace(self.path)
        self._line_count = len(self.cursors)
        tqdm.write(f"  [compact] resume cache compacted: {self._line_count:,} rows")


class CommentsPostSyncState:
    """Per-post sync state for cooldown/backoff scheduling."""

    def __init__(self, path: Path):
        self.path = path
        self.states: dict = {}
        self._line_count = 0
        if self.path.exists():
            self._load()
            self.compact_if_needed()

    def _load(self):
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    self._line_count += 1
                    pid = rec.get("post_id")
                    if not pid:
                        continue
                    self.states[pid] = {
                        "last_unique_count": _to_int(rec.get("last_unique_count"), 0),
                        "no_gain_runs": _to_int(rec.get("no_gain_runs"), 0),
                        "next_retry_at": _to_int(rec.get("next_retry_at"), 0),
                        "last_error": str(rec.get("last_error", "") or ""),
                        "updated_at": rec.get("updated_at", ""),
                    }
                except Exception:
                    pass

    def get(self, post_id: str) -> dict:
        rec = self.states.get(post_id)
        if not rec:
            return {
                "last_unique_count": 0,
                "no_gain_runs": 0,
                "next_retry_at": 0,
                "last_error": "",
                "updated_at": "",
            }
        return dict(rec)

    def should_run(self, post_id: str, now_ts: int = None) -> bool:
        now_ts = int(now_ts or time.time())
        rec = self.states.get(post_id)
        if not rec:
            return True
        return _to_int(rec.get("next_retry_at"), 0) <= now_ts

    def _cooldown_seconds(self, no_gain_runs: int, expected: int) -> int:
        if expected >= 20_000:
            base = 6 * 3600
        elif expected >= 5_000:
            base = 3 * 3600
        elif expected >= 500:
            base = 3600
        else:
            base = 1800
        exp = min(5, max(0, no_gain_runs - 1))
        return int(min(7 * 24 * 3600, base * (2 ** exp)))

    def _write_one(self, post_id: str, rec: dict):
        payload = {
            "post_id": post_id,
            "last_unique_count": int(rec.get("last_unique_count", 0)),
            "no_gain_runs": int(rec.get("no_gain_runs", 0)),
            "next_retry_at": int(rec.get("next_retry_at", 0)),
            "last_error": str(rec.get("last_error", "") or ""),
            "updated_at": rec.get("updated_at") or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        self._line_count += 1

    def update_after_fetch(self, post_id: str, expected: int, local_unique: int, unique_added: int,
                           success: bool, resume_cursor: str, error: str = ""):
        now_ts = int(time.time())
        prev = self.get(post_id)
        prev_unique = _to_int(prev.get("last_unique_count"), 0)
        final_unique = max(prev_unique, _to_int(local_unique, 0) + max(0, _to_int(unique_added, 0)))
        no_gain_runs = _to_int(prev.get("no_gain_runs"), 0)
        last_error = ""
        next_retry_at = 0

        aligned = expected > 0 and final_unique >= expected
        if aligned:
            no_gain_runs = 0
        elif success:
            # End-of-thread underfill tends to be non-convergent; cool it down.
            no_gain_runs = max(1, no_gain_runs + 1)
            next_retry_at = now_ts + self._cooldown_seconds(no_gain_runs, expected)
            last_error = "underfilled_at_thread_end"
        elif resume_cursor:
            if unique_added > 0:
                no_gain_runs = 0
            else:
                no_gain_runs = no_gain_runs + 1
                next_retry_at = now_ts + self._cooldown_seconds(no_gain_runs, expected)
                last_error = error or "no_gain_with_resume"
        else:
            no_gain_runs = no_gain_runs + 1
            next_retry_at = now_ts + self._cooldown_seconds(no_gain_runs, expected)
            last_error = error or "fetch_failed"

        rec = {
            "last_unique_count": final_unique,
            "no_gain_runs": no_gain_runs,
            "next_retry_at": next_retry_at,
            "last_error": last_error,
            "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        self.states[post_id] = rec
        self._write_one(post_id, rec)
        self.compact_if_needed()

    def force_retry(self, post_ids):
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        changed = 0
        for pid in post_ids:
            rec = self.get(pid)
            rec["no_gain_runs"] = 0
            rec["next_retry_at"] = 0
            rec["last_error"] = "forced_refetch"
            rec["updated_at"] = now_str
            self.states[pid] = rec
            self._write_one(pid, rec)
            changed += 1
        if changed:
            self.compact_if_needed()
        return changed

    def compact_if_needed(self, force: bool = False):
        target = max(20_000, len(self.states) * 8)
        if not force and self._line_count <= target:
            return
        tmp = self.path.with_suffix(".compact_tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            for pid, rec in self.states.items():
                payload = {"post_id": pid, **rec}
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        tmp.replace(self.path)
        self._line_count = len(self.states)
        tqdm.write(f"  [compact] post sync state compacted: {self._line_count:,} rows")


class CommentsIdStore:
    """Dedup store for (post_id, comment_id), in memory or sqlite."""

    def __init__(self, mode: str = _DEFAULT_COMMENT_ID_CACHE_MODE, sqlite_path: Path = None):
        m = (mode or _DEFAULT_COMMENT_ID_CACHE_MODE).strip().lower()
        self.mode = m if m in ("memory", "sqlite") else _DEFAULT_COMMENT_ID_CACHE_MODE
        self._memory = {}
        self._conn = None
        self._pending = 0
        # add_if_new uses connection-level total_changes (sqlite) and
        # check-then-add (memory); both race under workers > 1 without this.
        self._add_lock = threading.Lock()
        if self.mode == "sqlite":
            if not sqlite_path:
                raise ValueError("sqlite_path is required when comment id cache mode is sqlite")
            sqlite_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(str(sqlite_path), check_same_thread=False, timeout=30.0)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA busy_timeout=30000")
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS comment_ids ("
                "post_id TEXT NOT NULL, "
                "comment_id TEXT NOT NULL, "
                "PRIMARY KEY(post_id, comment_id)"
                ")"
            )
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_comment_ids_post ON comment_ids(post_id)")
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS comment_ids_meta ("
                "k TEXT PRIMARY KEY, "
                "v TEXT NOT NULL"
                ")"
            )
            self._conn.commit()

    def _meta_get(self, key: str, default: str = "") -> str:
        if self.mode != "sqlite":
            return default
        row = self._conn.execute("SELECT v FROM comment_ids_meta WHERE k=?", (key,)).fetchone()
        if not row:
            return default
        return str(row[0] if row[0] is not None else default)

    def _meta_set(self, key: str, value):
        if self.mode != "sqlite":
            return
        self._conn.execute(
            "INSERT INTO comment_ids_meta(k, v) VALUES(?, ?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (key, str(value)),
        )

    def _sqlite_sync_from_file(self, comments_path: Path):
        if self.mode != "sqlite" or not comments_path.exists():
            return
        size = comments_path.stat().st_size
        mtime = int(comments_path.stat().st_mtime)
        saved_size = _to_int(self._meta_get("source_size", "0"), 0)
        saved_mtime = _to_int(self._meta_get("source_mtime", "0"), 0)
        saved_offset = _to_int(self._meta_get("indexed_offset", "0"), 0)
        has_meta = bool(self._meta_get("indexed_offset", ""))

        need_rebuild = (not has_meta) or (saved_offset > size) or (size < saved_size) or (mtime < saved_mtime)
        start_offset = 0 if need_rebuild else saved_offset
        if need_rebuild:
            tqdm.write("  [id-store] sqlite index rebuild needed; rebuilding comment id index...")
            self._conn.execute("DELETE FROM comment_ids")
            self._conn.execute("DELETE FROM comment_ids_meta")
            self._conn.commit()

        scanned = 0
        inserted = 0
        batch = []
        t0 = time.monotonic()
        hb_last = t0

        with open(comments_path, "rb") as fb:
            if start_offset > 0:
                fb.seek(start_offset)
            while True:
                line_b = fb.readline()
                if not line_b:
                    break
                scanned += 1
                try:
                    line = line_b.decode("utf-8", errors="ignore").strip()
                    if not line:
                        continue
                    obj = json.loads(line)
                    pid = obj.get("post_id")
                    cid = obj.get("id")
                    if not pid or not cid:
                        continue
                    batch.append((str(pid), str(cid)))
                    if len(batch) >= 4000:
                        before = self._conn.total_changes
                        self._conn.executemany(
                            "INSERT OR IGNORE INTO comment_ids(post_id, comment_id) VALUES(?, ?)", batch
                        )
                        inserted += max(0, self._conn.total_changes - before)
                        batch.clear()
                except Exception:
                    pass

                now = time.monotonic()
                if now - hb_last >= 15:
                    speed = scanned / max(1e-6, (now - t0))
                    tqdm.write(
                        f"  [heartbeat id-store] mode=sqlite, indexed_lines={scanned:,}, "
                        f"inserted={inserted:,}, speed~{speed:,.0f} lines/s"
                    )
                    hb_last = now

            if batch:
                before = self._conn.total_changes
                self._conn.executemany("INSERT OR IGNORE INTO comment_ids(post_id, comment_id) VALUES(?, ?)", batch)
                inserted += max(0, self._conn.total_changes - before)
            end_offset = fb.tell()

        self._meta_set("source_size", size)
        self._meta_set("source_mtime", mtime)
        self._meta_set("indexed_offset", end_offset)
        self._meta_set("indexed_at", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
        self._conn.commit()
        tqdm.write(
            f"  [id-store done] mode=sqlite, start_offset={start_offset:,}, end_offset={end_offset:,}, "
            f"indexed_lines={scanned:,}, inserted={inserted:,}"
        )

    def seed_from_comments_file(self, comments_path: Path, target_ids: set):
        if not comments_path.exists():
            return
        if self.mode == "sqlite":
            self._sqlite_sync_from_file(comments_path)
            return
        if not target_ids:
            return
        scanned = 0
        matched = 0
        t0 = time.monotonic()
        hb_last = t0
        with open(comments_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                scanned += 1
                try:
                    obj = json.loads(line)
                    pid = obj.get("post_id")
                    if pid not in target_ids:
                        continue
                    cid = obj.get("id")
                    if not cid:
                        continue
                    matched += 1
                    if self.mode == "memory":
                        bucket = self._memory.get(pid)
                        if bucket is None:
                            bucket = set()
                            self._memory[pid] = bucket
                        bucket.add(_id_fingerprint(cid))
                except Exception:
                    pass

                now = time.monotonic()
                if now - hb_last >= 15:
                    speed = scanned / max(1e-6, (now - t0))
                    tqdm.write(
                        f"  [heartbeat id-store] mode={self.mode}, scanned={scanned:,}, matched={matched:,}, "
                        f"posts={len(self._memory):,}, speed~{speed:,.0f} rows/s"
                    )
                    hb_last = now
        tqdm.write(f"  [id-store done] mode={self.mode}, scanned={scanned:,}, matched={matched:,}")

    def count_for_posts(self, post_ids):
        out = {}
        if not post_ids:
            return out
        ids = [str(pid) for pid in post_ids if pid]
        if not ids:
            return out

        if self.mode == "memory":
            for pid in ids:
                out[pid] = len(self._memory.get(pid, set()))
            return out

        # Chunk IN (...) queries to stay under sqlite parameter limits.
        out = {pid: 0 for pid in ids}
        chunk = 800
        for i in range(0, len(ids), chunk):
            sub = ids[i:i + chunk]
            qs = ",".join("?" for _ in sub)
            rows = self._conn.execute(
                f"SELECT post_id, COUNT(*) FROM comment_ids WHERE post_id IN ({qs}) GROUP BY post_id",
                sub,
            ).fetchall()
            for row in rows:
                pid = str(row[0])
                out[pid] = _to_int(row[1], 0)
        return out

    def add_if_new(self, post_id: str, comment_id: str) -> bool:
        if not comment_id:
            return True
        with self._add_lock:
            if self.mode == "memory":
                bucket = self._memory.get(post_id)
                if bucket is None:
                    bucket = set()
                    self._memory[post_id] = bucket
                fp = _id_fingerprint(comment_id)
                if fp in bucket:
                    return False
                bucket.add(fp)
                return True

            before = self._conn.total_changes
            self._conn.execute(
                "INSERT OR IGNORE INTO comment_ids(post_id, comment_id) VALUES(?, ?)",
                (post_id, str(comment_id)),
            )
            inserted = self._conn.total_changes > before
            self._pending += 1
            # NOTE: we deliberately do NOT commit here. The id must not become
            # durable before its comment row is flushed to JSONL, or a crash
            # between the two leaves a phantom id (counted as present, never
            # re-fetched) => permanent silent comment loss. The writer calls
            # commit() below AFTER flushing the rows.
            return inserted

    def commit(self, min_pending: int = 1):
        """Durably persist buffered id inserts. The comment writer calls this
        AFTER the corresponding rows are flushed to JSONL, so a crash can never
        leave an id committed without its row on disk (rows are re-indexed from
        the file on the next run via _sqlite_sync_from_file). Batches by
        min_pending to bound commit frequency. Assumes a single writer thread
        (production runs with --workers 1); with more workers the commit is
        connection-global and the ordering guarantee is best-effort."""
        if self.mode == "memory" or self._conn is None:
            return
        with self._add_lock:
            if self._pending >= min_pending:
                self._conn.commit()
                self._pending = 0

    def close(self):
        if self._conn is not None:
            self._conn.commit()
            self._conn.close()
            self._conn = None


def fetch_posts_incremental(checkpoint: Checkpoint, posts_store: "JsonlStore",
                            run_file, since_time=None, max_posts=None,
                            platform_total=0):
    """Fetch posts incrementally and stream writes to disk."""
    total_new = 0
    stopped_early = False
    api_error = False
    reached_end = False
    keep_cursor = False  # True when stopping mid-window with a valid cursor on disk
    newest_post = None
    pages_seen = 0
    api_rows_seen = 0
    zero_write_streak = 0
    initial_local = posts_store.count()
    pbar_total = max(0, platform_total - initial_local) if platform_total else None
    seen_next_cursors = set()
    last_page_sig = None
    same_page_sig_streak = 0

    # Resume from saved cursor — ONLY in full-history mode (since_time is None).
    # In incremental mode a saved cursor points deep into a window; if the run
    # that saved it was interrupted (common — the box gets shut/slept), posts
    # that arrived at the TOP since then would be skipped because we'd continue
    # paging older. So incremental always re-pages from the newest and relies on
    # id-dedup + stop-at-known to handle the overlap cheaply.
    resume_cursor, resume_since, resume_newest = checkpoint.get_resume_cursor()
    if since_time is None and resume_cursor and resume_since == since_time:
        tqdm.write("  [resume posts] continue from saved cursor (full mode)...")
        cursor = resume_cursor
        if resume_newest:
            newest_post = resume_newest
    else:
        cursor = None
        if resume_cursor:
            tqdm.write("  [resume posts] incremental run: ignoring saved cursor, paging from newest.")
            checkpoint.clear_resume()

    pbar = tqdm(desc="fetch posts", unit="row", dynamic_ncols=True, total=pbar_total)

    try:
        hb_last = time.monotonic()
        while True:
            params = {"sort": "new", "limit": 100}
            if cursor:
                params["cursor"] = cursor

            data = api_get("/posts", params)

            if not data or not data.get("success"):
                auth_code = _to_int((data or {}).get("_fatal_auth"), 0) if isinstance(data, dict) else 0
                if auth_code in (401, 403):
                    api_error = True
                    tqdm.write(f"  [fatal] posts auth error {auth_code}; stop posts stage and keep cursor.")
                    break

                api_error = True  # keep checkpoint cursor for next run
                retry_delay = 120
                retry_n = 0
                while retry_n < _DEFAULT_POST_MAX_RECOVER_RETRIES:
                    retry_n += 1
                    tqdm.write(
                        f"  [!] server error (retry #{retry_n}); "
                        f"auto retry in {retry_delay // 60} min... press Ctrl+C to stop."
                    )
                    time.sleep(retry_delay)
                    data = api_get("/posts", params)
                    auth_code = _to_int((data or {}).get("_fatal_auth"), 0) if isinstance(data, dict) else 0
                    if auth_code in (401, 403):
                        tqdm.write(f"  [fatal] posts auth error {auth_code}; stop posts stage and keep cursor.")
                        break
                    if data and data.get("success"):
                        api_error = False
                        tqdm.write("  [+] retry success, continue.")
                        break
                    retry_delay = min(retry_delay * 2, 600)
                if not data or not data.get("success"):
                    tqdm.write("  [!] posts stage stopped after max recover retries; will continue next run from cursor.")
                    break

            batch = data.get("posts", [])
            if not batch:
                break
            page_sig = (
                batch[0].get("id", ""),
                batch[-1].get("id", ""),
                data.get("next_cursor", ""),
            )
            if page_sig == last_page_sig:
                same_page_sig_streak += 1
            else:
                same_page_sig_streak = 0
            last_page_sig = page_sig
            if same_page_sig_streak >= 3:
                tqdm.write("  [guard] same posts page repeated 4 times, stop to avoid cursor loop.")
                break
            pages_seen += 1
            api_rows_seen += len(batch)

            if batch:
                page_newest = batch[0]
                page_created = page_newest.get("created_at", "")
                current_created = newest_post.get("created_at", "") if isinstance(newest_post, dict) else ""
                if newest_post is None or (page_created and page_created > current_created):
                    newest_post = page_newest

            if since_time:
                # A blank/missing created_at must NOT be read as "older than the
                # anchor": `"" > since_time` is False, which would silently drop
                # the post AND fake an early stop (len(new_batch) < len(batch)),
                # advancing the anchor past not-yet-fetched newer posts and losing
                # them forever. Keep such posts; only a PRESENT timestamp that is
                # <= the anchor is a genuine boundary that should stop the pass.
                new_batch = [
                    p for p in batch
                    if (not p.get("created_at")) or p.get("created_at") > since_time
                ]
                if len(new_batch) < len(batch):
                    stopped_early = True
                batch = new_batch

            if batch:
                scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                for p in batch:
                    p.setdefault("_scraped_at", scraped_at)
                # Keep run snapshots as "new rows only"; overlap pages otherwise look like fake progress.
                new_for_run = [p for p in batch if p.get("id") not in posts_store.seen_ids]
                written = posts_store.append_new(batch)
                total_new += written
                for p in new_for_run:
                    run_file.write(json.dumps(p, ensure_ascii=False) + "\n")
                run_file.flush()
                pbar.update(len(batch))
                cur_date = batch[-1].get("created_at", "")[:10]
                if written == 0:
                    zero_write_streak += 1
                    if zero_write_streak % 25 == 0:
                        if since_time:
                            tqdm.write(
                                f"  [info] zero-write streak={zero_write_streak} pages (incremental overlap or cursor loop)."
                            )
                        else:
                            tqdm.write(
                                "  [info] 25 pages in a row with no new rows; likely overlap area, keep scanning older history."
                            )
                    if since_time and zero_write_streak >= _DEFAULT_POST_ZERO_STREAK_GUARD:
                        tqdm.write(
                            f"  [guard] zero-write streak reached {_DEFAULT_POST_ZERO_STREAK_GUARD} pages in incremental mode; "
                            "stop this posts pass to avoid empty replay (cursor kept; next run continues here)."
                        )
                        keep_cursor = True
                        break
                else:
                    zero_write_streak = 0
                pbar.set_postfix_str(
                    f"page+{written}/{len(batch)} | total+{total_new} | zero_pages:{zero_write_streak} | date:{cur_date}"
                )

            now_hb = time.monotonic()
            if now_hb - hb_last >= 20:
                tqdm.write(
                    f"  [heartbeat posts] pages={pages_seen}, api_rows={api_rows_seen}, "
                    f"new_rows={total_new}, zero_streak={zero_write_streak}"
                )
                hb_last = now_hb

            if stopped_early:
                break

            if max_posts and total_new >= max_posts:
                # Truncation, not completion: keep the per-page cursor so the
                # next run resumes inside the window instead of re-paging from
                # the top (where the zero-streak guard could fire first).
                keep_cursor = True
                break

            next_cursor = data.get("next_cursor")
            if not data.get("has_more") or not next_cursor:
                reached_end = True
                break
            if next_cursor in seen_next_cursors:
                tqdm.write("  [guard] repeated next_cursor detected, stop to avoid infinite loop.")
                break
            seen_next_cursors.add(next_cursor)

            cursor = next_cursor
            bottom_date = batch[-1].get("created_at", "") if (batch and since_time is None) else ""
            checkpoint.save_resume_cursor(cursor, since_time, newest_post, bottom_date=bottom_date)

    finally:
        pbar.close()
        tqdm.write(f"  [posts] pages={pages_seen}, api_rows={api_rows_seen}, new_rows={total_new}")

    return total_new, stopped_early, newest_post, api_error, reached_end, keep_cursor


def _fetch_one_post_comments(post, start_cursor=None, page_progress_cb=None, page_write_cb=None):
    """
    Fetch comments for one post using cursor pagination.
    Stream page results via page_write_cb when provided.
    Returns: (post, success, resume_cursor, raw_rows, written_rows, written_unique)
    """
    if _FATAL_AUTH_EVENT.is_set():
        # Auth is dead platform-wide; don't waste a request per queued post.
        return post, False, start_cursor, 0, 0, 0
    cursor = start_cursor
    page = 0
    raw_rows_total = 0
    written_rows_total = 0
    written_unique_total = 0
    hb_last = time.monotonic()
    if start_cursor:
        tqdm.write(f"  [resume post] {post['id'][:8]}... continue from saved cursor")
    while True:
        params = {"sort": "new", "limit": 100}
        if cursor:
            params["cursor"] = cursor

        data = None
        for attempt in range(3):
            data = api_get(f"/posts/{post['id']}/comments", params=params)
            if data and data.get("success"):
                break
            if isinstance(data, dict) and data.get("_fatal_auth"):
                # 401/403 is not transient — abort this post (and, via the
                # event, the whole stage) instead of retrying.
                return post, False, cursor, raw_rows_total, written_rows_total, written_unique_total
            if data is None:
                # Keep partial pages and current cursor for next run.
                return post, False, cursor, raw_rows_total, written_rows_total, written_unique_total
            if attempt < 2:
                tqdm.write(f"  [!] {post['id'][:8]}... p{page} server error, retry ({attempt+1}/2)...")
                time.sleep(10)
        else:
            return post, False, cursor, raw_rows_total, written_rows_total, written_unique_total

        # `.get("comments", [])` returns None when the key is present but JSON
        # null; `or []` keeps _flatten_comments from raising on NoneType.
        flat = _flatten_comments(data.get("comments") or [])
        for c in flat:
            c["post_id"] = post["id"]
            c["post_title"] = post.get("title", "")
            c["submolt"] = post.get("submolt", {}).get("name", "")
        raw_rows_total += len(flat)
        if page_write_cb:
            try:
                wr_rows, wr_unique = page_write_cb(post, flat, page + 1)
                written_rows_total += max(0, _to_int(wr_rows, 0))
                written_unique_total += max(0, _to_int(wr_unique, 0))
                page_written_rows = max(0, _to_int(wr_rows, 0))
            except Exception as e:
                # A page-write failure means this page's rows are NOT durably
                # stored. Swallowing it and continuing would return success=True,
                # clear the resume cursor, and (in sqlite mode, where ids were
                # recorded during the write) leave phantom ids that mask the gap
                # forever -> permanent silent comment loss. Abort this post,
                # keeping the current cursor so the page is retried next run.
                tqdm.write(f"  [!] {post['id'][:8]}... page write failed: {e}; keeping cursor for retry")
                return post, False, cursor, raw_rows_total, written_rows_total, written_unique_total
        else:
            written_rows_total += len(flat)
            written_unique_total += len(flat)
            page_written_rows = len(flat)
        page += 1
        if page_progress_cb:
            try:
                page_progress_cb(
                    post["id"], page, len(flat), page_written_rows, raw_rows_total, written_rows_total, written_unique_total
                )
            except Exception:
                pass

        now_hb = time.monotonic()
        if page % 20 == 0 or (now_hb - hb_last >= 20):
            tqdm.write(
                f"  [heartbeat comments] {post['id'][:8]}... pages={page}, raw~{raw_rows_total}, written~{written_rows_total}"
            )
            hb_last = now_hb

        if not data.get("has_more") or not data.get("next_cursor"):
            break
        next_cursor = data["next_cursor"]
        if next_cursor == cursor:
            # A cursor that does not advance (but still says has_more) would loop
            # forever, re-fetching the same page, wedging this worker and — via
            # as_completed — the whole comments stage. Stop instead.
            tqdm.write(f"  [!] {post['id'][:8]}... next_cursor did not advance (p{page}); stopping to avoid an infinite loop.")
            break
        if page >= _MAX_COMMENT_PAGES:
            tqdm.write(f"  [!] {post['id'][:8]}... hit page cap {_MAX_COMMENT_PAGES} (possible cursor cycle); stopping.")
            break
        cursor = next_cursor

    return post, True, None, raw_rows_total, written_rows_total, written_unique_total


def _count_comments_for_target_posts(comments_path: Path, target_ids: set, strict_unique: bool = True):
    """
    Count existing local comments for target post_id set.
    strict_unique=True deduplicates by (post_id, comment_id).
    Also prints heartbeat logs while scanning large comments files.
    """
    counts = Counter()
    if not target_ids or not comments_path.exists():
        return counts

    scanned = 0
    matched = 0
    deduped = 0
    seen_pairs = set() if strict_unique else None
    t0 = time.monotonic()
    hb_last = t0
    with open(comments_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            scanned += 1
            try:
                obj = json.loads(line)
                pid = obj.get("post_id")
                if pid in target_ids:
                    cid = obj.get("id")
                    if strict_unique and cid:
                        fp = _pair_fingerprint(pid, cid)
                        if fp in seen_pairs:
                            deduped += 1
                            continue
                        seen_pairs.add(fp)
                    counts[pid] += 1
                    matched += 1
            except Exception:
                pass

            now = time.monotonic()
            if now - hb_last >= 15:
                speed = scanned / max(1e-6, (now - t0))
                tqdm.write(
                    f"  [heartbeat count] scanned_comments={scanned:,}, matched={matched:,}, deduped={deduped:,}, speed~{speed:,.0f} rows/s"
                )
                hb_last = now

    mode = "unique" if strict_unique else "rows"
    tqdm.write(
        f"  [count done] mode={mode}, scanned_comments={scanned:,}, matched={matched:,}, "
        f"deduped={deduped:,}, target_posts={len(target_ids):,}"
    )
    return counts


def _build_comment_id_index_for_posts(comments_path: Path, target_ids: set):
    """Build per-post comment-id fingerprint sets for dedup before append."""
    idx = {}
    if not target_ids or not comments_path.exists():
        return idx
    scanned = 0
    matched = 0
    t0 = time.monotonic()
    hb_last = t0
    with open(comments_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            scanned += 1
            try:
                obj = json.loads(line)
                pid = obj.get("post_id")
                if pid not in target_ids:
                    continue
                cid = obj.get("id")
                if not cid:
                    continue
                bucket = idx.get(pid)
                if bucket is None:
                    bucket = set()
                    idx[pid] = bucket
                bucket.add(_id_fingerprint(cid))
                matched += 1
            except Exception:
                pass
            now = time.monotonic()
            if now - hb_last >= 15:
                speed = scanned / max(1e-6, (now - t0))
                tqdm.write(
                    f"  [heartbeat id-index] scanned={scanned:,}, matched={matched:,}, posts={len(idx):,}, speed~{speed:,.0f} rows/s"
                )
                hb_last = now
    tqdm.write(f"  [id-index done] scanned={scanned:,}, matched={matched:,}, indexed_posts={len(idx):,}")
    return idx


def _estimate_remaining_comments(post: dict) -> int:
    """Estimate remaining work for one post in queue scheduling."""
    gap = _to_int(post.get("gap"), 0)
    if gap > 0:
        return gap
    expected = max(0, _to_int(post.get("comment_count"), 0))
    local = max(0, _to_int(post.get("local_count"), 0))
    remaining = expected - local
    if remaining > 0:
        return remaining
    if post.get("is_resume"):
        # Resume jobs still have unfinished cursor pages even when numeric gap looks 0.
        return max(1, expected)
    return 0


def _sort_comment_layer(posts: list, *, large_first: bool):
    if large_first:
        posts.sort(
            key=lambda p: (
                0 if p.get("is_resume") else 1,
                -p.get("_remaining_est", 0),
                -_to_int(p.get("comment_count"), 0),
            )
        )
        return
    posts.sort(
        key=lambda p: (
            0 if p.get("is_resume") else 1,
            p.get("_remaining_est", 0),
            _to_int(p.get("comment_count"), 0),
        )
    )


def _schedule_comment_queue(posts: list, strategy: str, small_max: int, medium_max: int):
    """
    Build final comment queue order.
    - layered: small -> medium -> long
    - small-first: global ascending by remaining
    - large-first: legacy behavior, global descending by remaining
    Returns: (ordered_posts, layer_stats[(label, count, remaining_sum, resume_count)])
    """
    queue = list(posts)
    for p in queue:
        p["_remaining_est"] = max(0, _estimate_remaining_comments(p))

    def _stat(label: str, layer: list):
        return (
            label,
            len(layer),
            sum(_to_int(x.get("_remaining_est"), 0) for x in layer),
            sum(1 for x in layer if x.get("is_resume")),
        )

    if strategy == "small-first":
        _sort_comment_layer(queue, large_first=False)
        return queue, [_stat("all", queue)]

    if strategy == "large-first":
        _sort_comment_layer(queue, large_first=True)
        return queue, [_stat("all", queue)]

    # layered (default): converge quick wins first, then chew long threads
    small_layer = []
    medium_layer = []
    long_layer = []
    for p in queue:
        rem = p.get("_remaining_est", 0)
        if rem <= small_max:
            small_layer.append(p)
        elif rem <= medium_max:
            medium_layer.append(p)
        else:
            long_layer.append(p)

    _sort_comment_layer(small_layer, large_first=False)
    _sort_comment_layer(medium_layer, large_first=False)
    _sort_comment_layer(long_layer, large_first=False)

    ordered = small_layer + medium_layer + long_layer
    stats = [
        _stat(f"small<={small_max}", small_layer),
        _stat(f"medium<={medium_max}", medium_layer),
        _stat(f"long>{medium_max}", long_layer),
    ]
    return ordered, stats


def fetch_comments_for_posts(posts_store: "JsonlStore", comments_path: Path,
                             done_cache: "CommentsDoneCache",
                             run_file, workers=5, min_comments=1, max_posts=0,
                             resume_cache: "CommentsResumeCache" = None,
                             sync_state: "CommentsPostSyncState" = None,
                             comment_id_cache_mode: str = _DEFAULT_COMMENT_ID_CACHE_MODE,
                             comment_id_cache_path: Path = None,
                             queue_strategy: str = _DEFAULT_COMMENT_QUEUE_STRATEGY,
                             queue_small_max: int = _DEFAULT_QUEUE_SMALL_MAX,
                             queue_medium_max: int = _DEFAULT_QUEUE_MEDIUM_MAX):
    """
    Concurrently fetch comments with gap-based backfill.
    Strategy:
    1) Build eligible post set (comment_count >= min_comments)
    2) Count local comment rows for those posts
    3) Backfill only posts where local_count != comment_count, plus resume-cursor posts
    4) Schedule queue by strategy (default layered: small -> medium -> long)
    Returns: (new_comment_rows_written, total_eligible_posts)
    """
    if not posts_store.path.exists():
        return 0, 0

    # 1) collect eligible posts from posts_all
    eligible_posts = []
    scanned = 0
    scan_t0 = time.monotonic()
    scan_hb_last = scan_t0
    with open(posts_store.path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            scanned += 1
            if scanned % 200_000 == 0:
                print(f"\r  scanning posts... {scanned:,} rows", end="", flush=True)
            now_scan = time.monotonic()
            if now_scan - scan_hb_last >= 15:
                speed = scanned / max(1e-6, (now_scan - scan_t0))
                tqdm.write(f"  [heartbeat scan] scanned={scanned:,} rows, speed~{speed:,.0f} rows/s")
                scan_hb_last = now_scan
            try:
                p = json.loads(line)
                pid = p.get("id")
                cc = p.get("comment_count", 0)
                if pid and isinstance(cc, int) and cc >= min_comments:
                    eligible_posts.append({
                        "id": pid,
                        "title": p.get("title", ""),
                        "submolt": p.get("submolt", {}),
                        "comment_count": cc,
                    })
            except Exception:
                pass

    total_eligible = len(eligible_posts)
    if not eligible_posts:
        print(f"\r  eligible=0 (scanned {scanned:,})")
        return 0, total_eligible

    # 2) count local comments for eligible posts (prefer unique comment_id accounting)
    expected_by_id = {p["id"]: int(p.get("comment_count", 0) or 0) for p in eligible_posts}
    eligible_ids = set(expected_by_id.keys())
    id_store = None
    strict_unique = True
    if comment_id_cache_mode == "sqlite":
        id_store = CommentsIdStore(mode=comment_id_cache_mode, sqlite_path=comment_id_cache_path)
        id_store.seed_from_comments_file(comments_path, eligible_ids)
        local_counts = Counter(id_store.count_for_posts(eligible_ids))
        tqdm.write(
            f"  [count done] mode=unique(sqlite), counted_posts={len(local_counts):,}, "
            f"target_posts={len(eligible_ids):,}"
        )
    else:
        if len(eligible_ids) > 120_000:
            # Row-based counting treats duplicate rows as coverage: posts with
            # legacy dup rows look "complete" and get silently skipped forever.
            raise SystemExit(
                f"  [fatal] {len(eligible_ids):,} eligible posts is too many for the in-memory "
                "id cache; row-based fallback would silently skip posts that contain duplicate "
                "rows. Re-run with --comment-id-cache sqlite."
            )
        local_counts = _count_comments_for_target_posts(comments_path, eligible_ids, strict_unique=True)

    # 3) build mismatch queue (and preserve resume-cursor posts)
    resume_ids = set(resume_cache.cursors.keys()) if resume_cache else set()
    posts_needing_comments = []
    cooled_candidates = []
    cooled_skipped = 0
    matched_exact = 0
    local_gt_expected = 0
    total_gap = 0
    now_ts = int(time.time())
    for p in eligible_posts:
        pid = p["id"]
        exp = expected_by_id[pid]
        got = int(local_counts.get(pid, 0))
        gap = exp - got
        is_resume = pid in resume_ids
        if gap == 0 and not is_resume:
            matched_exact += 1
            continue
        if gap < 0 and not is_resume:
            local_gt_expected += 1
            continue
        total_gap += max(0, gap)
        post_rec = dict(p)
        post_rec["local_count"] = got
        post_rec["gap"] = gap
        post_rec["is_resume"] = is_resume
        if sync_state and not is_resume:
            st = sync_state.get(pid)
            post_rec["no_gain_runs"] = _to_int(st.get("no_gain_runs"), 0)
            post_rec["next_retry_at"] = _to_int(st.get("next_retry_at"), 0)
            if post_rec["next_retry_at"] > now_ts:
                cooled_skipped += 1
                cooled_candidates.append(post_rec)
                continue
        posts_needing_comments.append(post_rec)

    # Probe a tiny sample of cooled posts each run to avoid permanent starvation.
    if cooled_candidates:
        probe_n = min(5, max(1, len(cooled_candidates) // 200))
        cooled_candidates.sort(key=lambda x: x.get("next_retry_at", 0))
        probes = cooled_candidates[:probe_n]
        for p in probes:
            p["is_probe"] = True
            posts_needing_comments.append(p)

    print(
        f"\r  eligible={total_eligible:,} | exact={matched_exact:,} | local>expected={local_gt_expected:,} "
        f"| pending={len(posts_needing_comments):,} | total_gap~{total_gap:,}"
    )
    if cooled_skipped:
        print(f"  [cooldown] skipped {cooled_skipped:,} posts by sync-state cooldown")
        print(f"  [cooldown] probe retry this run: {min(5, max(1, len(cooled_candidates) // 200)):,} posts")

    if resume_cache and resume_cache.count():
        print(f"  [resume] {resume_cache.count():,} posts have saved cursors")
        preview = [
            (max(0, p["gap"]), p["comment_count"], p["local_count"], p["id"])
            for p in posts_needing_comments if p.get("is_resume")
        ]
        if preview:
            preview.sort(reverse=True)
            print("  [resume] top pending posts (remaining ~= expected - local):")
            for rem, exp, got, pid in preview[:5]:
                print(f"    {pid[:8]}... {got}/{exp} (remaining~{rem})")

    if not posts_needing_comments:
        print("  [ok] all eligible posts are already aligned with local comment rows")
        if id_store:
            id_store.close()
        return 0, total_eligible

    posts_needing_comments, layer_stats = _schedule_comment_queue(
        posts_needing_comments,
        strategy=queue_strategy,
        small_max=queue_small_max,
        medium_max=queue_medium_max,
    )
    if layer_stats:
        bits = []
        for label, count, rem_sum, resume_n in layer_stats:
            if count <= 0:
                continue
            bits.append(f"{label}: {count:,} posts (remaining~{rem_sum:,}, resume={resume_n:,})")
        if bits:
            print(f"  [queue] strategy={queue_strategy} | " + " | ".join(bits))

    if max_posts and len(posts_needing_comments) > max_posts:
        posts_needing_comments = posts_needing_comments[:max_posts]
        print(f"  [limit] this run will fetch top {max_posts:,} posts")

    queue_ids = {p["id"] for p in posts_needing_comments}
    if id_store is None:
        id_store = CommentsIdStore(mode=comment_id_cache_mode, sqlite_path=comment_id_cache_path)
        id_store.seed_from_comments_file(comments_path, queue_ids)

    rate = 60 / _rate_limiter_comments._interval
    eta_min = len(posts_needing_comments) / rate
    print(f"  ETA {eta_min/60:.1f} hours ({rate:.0f} req/min, {workers} workers)")

    total_new_rows = 0
    total_new_unique = 0
    posts_done = 0
    write_lock = threading.Lock()
    per_post_unique_est = Counter(local_counts)
    per_post_remaining = {
        p["id"]: max(0, int(p.get("comment_count", 0)) - int(p.get("local_count", 0)))
        for p in posts_needing_comments
    }
    gap_remaining = sum(per_post_remaining.values())

    pbar = tqdm(total=len(posts_needing_comments), desc="fetch comments",
                unit="post", dynamic_ncols=True, position=0)
    pages_bar = tqdm(total=None, desc="comment pages", unit="page",
                     dynamic_ncols=True, position=1, leave=False)
    pages_bar.set_postfix_str("raw~0 | written~0")

    pages_seen = 0
    rows_seen_raw = 0
    rows_seen_written = 0
    progress_lock = threading.Lock()

    comments_f = open(comments_path, "a", encoding="utf-8")

    def on_page_progress(post_id, page_no, page_rows, page_written_rows, post_rows_total, post_written_total, post_unique_total):
        nonlocal pages_seen, rows_seen_raw, rows_seen_written
        with progress_lock:
            pages_seen += 1
            rows_seen_raw += max(0, int(page_rows))
            rows_seen_written += max(0, int(page_written_rows))
            pages_bar.update(1)
            if workers == 1:
                pages_bar.set_description_str(f"reading {post_id[:8]}...")
                pages_bar.set_postfix_str(
                    f"post_p={page_no} | post_raw~{post_rows_total:,} | post_written~{post_written_total:,} | total_written~{rows_seen_written:,}"
                )
            else:
                pages_bar.set_postfix_str(
                    f"last={post_id[:8]} p={page_no} | total_written~{rows_seen_written:,}"
                )

    def on_page_write(post, flat, page_no):
        nonlocal total_new_rows, total_new_unique
        pid = post["id"]
        to_write = []
        unique_added = 0
        for c in flat:
            cid = c.get("id")
            if cid:
                if not id_store.add_if_new(pid, str(cid)):
                    continue
                unique_added += 1
            else:
                unique_added += 1
            to_write.append(c)

        if not to_write:
            return 0, 0

        payload = "".join(json.dumps(c, ensure_ascii=False) + "\n" for c in to_write)
        with write_lock:
            comments_f.write(payload)
            run_file.write(payload)
            comments_f.flush()
            run_file.flush()
            total_new_rows += len(to_write)
            total_new_unique += unique_added
        # Rows are now durable; only NOW let the ids recorded by add_if_new above
        # commit. A crash before this rolls the ids back (rows re-index from the
        # file next run) — never a phantom id. Batched to ~1000 to bound commits.
        id_store.commit(min_pending=1000)
        return len(to_write), unique_added

    try:
        chunk_size = workers * 20
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for i in range(0, len(posts_needing_comments), chunk_size):
                if _FATAL_AUTH_EVENT.is_set():
                    tqdm.write("  [fatal] 401/403 auth failure; aborting comments stage (cursors kept for resume).")
                    break
                chunk = posts_needing_comments[i:i + chunk_size]
                futures = {}
                for post in chunk:
                    start_cursor = resume_cache.get(post["id"]) if resume_cache else None
                    futures[executor.submit(
                        _fetch_one_post_comments, post, start_cursor, on_page_progress, on_page_write
                    )] = post

                for future in as_completed(futures):
                    post, success, resume_cursor, raw_rows, written_rows, written_unique = future.result()
                    pid = post["id"]
                    if written_unique:
                        per_post_unique_est[pid] += written_unique
                        consumed = min(per_post_remaining.get(pid, 0), written_unique)
                        per_post_remaining[pid] = max(0, per_post_remaining.get(pid, 0) - consumed)
                        gap_remaining = max(0, gap_remaining - consumed)

                    if success:
                        if resume_cache:
                            resume_cache.set(pid, None)
                        if per_post_unique_est.get(pid, 0) >= expected_by_id.get(pid, 0):
                            done_cache.mark_done(pid)
                    elif resume_cache and resume_cursor:
                        resume_cache.set(pid, resume_cursor)
                        exp = expected_by_id.get(pid, 0)
                        got = per_post_unique_est.get(pid, 0)
                        if exp > 0:
                            rem = max(0, exp - got)
                            tqdm.write(f"  [partial] {pid[:8]}... saved~{got}/{exp}, remaining~{rem}, cursor saved")
                        else:
                            tqdm.write(f"  [partial] {pid[:8]}... saved~{got}, cursor saved")

                    if sync_state:
                        err = "" if success else "partial_or_failed"
                        sync_state.update_after_fetch(
                            pid,
                            expected=expected_by_id.get(pid, 0),
                            local_unique=local_counts.get(pid, 0),
                            unique_added=written_unique,
                            success=bool(success),
                            resume_cursor=resume_cursor or "",
                            error=err,
                        )

                    posts_done += 1
                    pbar.update(1)
                    resume_n = resume_cache.count() if resume_cache else 0
                    pbar.set_postfix_str(
                        f"new rows {total_new_rows} | unique+{total_new_unique} | gap~{gap_remaining} | "
                        f"done {posts_done}/{len(posts_needing_comments)} | resume {resume_n}"
                    )
    finally:
        pbar.close()
        pages_bar.close()
        comments_f.close()
        id_store.close()

    resume_left = resume_cache.count() if resume_cache else 0
    print(f"  comment pages fetched: {pages_seen:,} | raw rows seen: {rows_seen_raw:,} | written rows: {rows_seen_written:,}")
    print(
        f"  comments newly written: rows={total_new_rows:,}, unique_est={total_new_unique:,}  |  "
        f"processed posts: {posts_done:,}  |  resume left: {resume_left:,}"
    )
    return total_new_rows, total_eligible


def _flatten_comments(comments, depth=0):
    flat = []
    for c in comments:
        c["depth"] = depth
        replies = c.pop("replies", [])
        flat.append(c)
        if replies:
            flat.extend(_flatten_comments(replies, depth + 1))
    return flat


def fetch_submolts():
    data = api_get("/submolts")
    if not data or not data.get("success"):
        return []
    return data.get("submolts", [])


def fetch_platform_stats():
    """Fetch /stats and return {} when unavailable or rate limited."""
    data = api_get("/stats", skip_on_ratelimit=True)
    if not data:
        return {}
    return data


#
# AGENT SNAPSHOTS (time series)
#

def take_agent_snapshot(out: Path):
    """
    Save a point-in-time snapshot of every agent's key metrics.
    Reads agents_seen.jsonl and writes a compact JSONL file to
    data/agent_snapshots/YYYYMMDD_HHMMSS.jsonl with one row per agent
    containing only the fields needed for longitudinal analysis.
    """
    agents_path = out / "agents_seen.jsonl"
    if not agents_path.exists():
        print("  [agent-snapshot] agents_seen.jsonl not found; skip.")
        return None

    snap_dir = out / "agent_snapshots"
    snap_dir.mkdir(exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    snap_path = snap_dir / f"{ts}.jsonl"
    sampled_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    count = 0
    with open(agents_path, encoding="utf-8") as fin, \
         open(snap_path, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                a = json.loads(line)
                record = {
                    "id":             a.get("id", ""),
                    "name":           a.get("name", ""),
                    "karma":          a.get("karma", 0),
                    "followerCount":  a.get("followerCount", 0),
                    "followingCount": a.get("followingCount", 0),
                    "isClaimed":      a.get("isClaimed", False),
                    "isActive":       a.get("isActive", False),
                    "createdAt":      a.get("createdAt", ""),
                    "lastActive":     a.get("lastActive", ""),
                    "sampled_at":     sampled_at,
                }
                fout.write(json.dumps(record, ensure_ascii=False) + "\n")
                count += 1
            except Exception:
                pass

    size_kb = snap_path.stat().st_size / 1024
    print(f"  [agent-snapshot] {count:,} agents -> {snap_path.name} ({size_kb:.0f} KB)")
    return snap_path


def _snap_record(p: dict, sampled_at: str, sort_source: str) -> dict:
    """Extract one snapshot record from a post object."""
    return {
        "post_id":       p.get("id", ""),
        "sampled_at":    sampled_at,
        "sort_source":   sort_source,
        "title":         p.get("title", ""),
        "submolt":       p.get("submolt", {}).get("name", "") if isinstance(p.get("submolt"), dict) else p.get("submolt", ""),
        "created_at":    p.get("created_at", ""),
        "upvotes":       p.get("upvotes", 0),
        "downvotes":     p.get("downvotes", 0),
        "score":         p.get("score", 0),
        "comment_count": p.get("comment_count", 0),
        "hot_score":     p.get("hot_score", 0),
        "is_spam":       p.get("is_spam", False),
    }


# ? ?
# hot 3?= ~300 ? ?
# rising 2?= ~200 ? ?
# top 20?= ~2000 ? ?
_SNAPSHOT_SORTS = [("hot", 3), ("rising", 2), ("top", 20)]


def fetch_hot_snapshot(out: Path, sorts_config=None):
    """Fetch hot/rising/top snapshots into post_snapshots.jsonl."""
    if sorts_config is None:
        sorts_config = _SNAPSHOT_SORTS
    snapshots_path = out / "post_snapshots.jsonl"
    sampled_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    seen_ids: set = set()
    total = 0

    for sort, pages in sorts_config:
        cursor = None
        for _ in range(pages):
            params = {"sort": sort, "limit": 100}
            if cursor:
                params["cursor"] = cursor
            data = api_get("/posts", params)
            if not data or not data.get("success"):
                break
            posts = data.get("posts", [])
            with open(snapshots_path, "a", encoding="utf-8") as f:
                for p in posts:
                    pid = p.get("id")
                    if not pid or pid in seen_ids:
                        continue
                    seen_ids.add(pid)
                    f.write(json.dumps(_snap_record(p, sampled_at, sort), ensure_ascii=False) + "\n")
                    total += 1
            if not data.get("has_more") or not data.get("next_cursor"):
                break
            cursor = data["next_cursor"]

    print(f"  [snapshot] {sampled_at} saved {total} rows -> {snapshots_path.name}")
    return total


def seed_snapshots(out: Path, min_score: int = 1):
    """Seed baseline snapshots from posts_all.jsonl using score threshold."""
    posts_path = out / "posts_all.jsonl"
    snapshots_path = out / "post_snapshots.jsonl"
    if not posts_path.exists():
        print("  [!] posts_all.jsonl missing")
        return

    # post_id?
    existing_ids: set = set()
    if snapshots_path.exists():
        with open(snapshots_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    existing_ids.add(json.loads(line)["post_id"])
                except Exception:
                    pass
        print(f"  existing snapshots: {len(existing_ids):,}, skip duplicates")

    written = skipped_score = 0
    scanned = 0
    print(f"  scanning posts_all.jsonl (score >= {min_score}) to seed T0 snapshot...")
    with open(posts_path, encoding="utf-8") as fin,\
         open(snapshots_path, "a", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            scanned += 1
            if scanned % 500_000 == 0:
                print(f"    scanned {scanned:,} rows, written {written:,} rows...")
            try:
                p = json.loads(line)
                if p.get("score", 0) < min_score:
                    skipped_score += 1
                    continue
                pid = p.get("id")
                if not pid or pid in existing_ids:
                    continue
                # T0 _scraped_at created_at
                t0 = p.get("_scraped_at") or p.get("created_at", "")
                snap = _snap_record(p, t0, "seed")
                fout.write(json.dumps(snap, ensure_ascii=False) + "\n")
                existing_ids.add(pid)
                written += 1
            except Exception:
                pass

    print(
        f"  [ok] T0 seed done: written {written:,} rows "
        f"(score<{min_score} skipped {skipped_score:,}, scanned {scanned:,})"
    )
    print(f"      -> {snapshots_path}")


def extract_agents(posts):
    agents = {}
    for p in posts:
        a = p.get("author")
        if a and a.get("id"):
            agents[a["id"]] = a
    return list(agents.values())


#
# STARTUP SUMMARY
#

def show_startup_summary(checkpoint, local_posts, local_oldest, local_newest,
                         done_posts, total_eligible,
                         platform_total, platform_comments, platform_agents,
                         out, since_time, args):
    """Print startup overview: local status, platform stats, and run plan."""
    SEP = "-" * 58
    auth_str = "AUTH" if "Authorization" in HEADERS else "ANON"
    print(f"\n{SEP}")
    print(f"  Moltbook Scraper v2  -  {datetime.now().strftime('%Y-%m-%d %H:%M')}  -  {auth_str}")
    print(SEP)

    #
    print("\n  Local Data")
    date_range = f"{local_oldest} -> {local_newest}" if local_oldest else "(no local data yet)"
    print(f"    posts   {local_posts:>12,} rows  |  {date_range}")

    comments_jsonl = out / "comments_all.jsonl"
    metrics_cache = checkpoint.data.get("local_metrics_cache", {}) if isinstance(checkpoint.data, dict) else {}
    if comments_jsonl.exists():
        size_mb = comments_jsonl.stat().st_size / 1024 / 1024
        if total_eligible and total_eligible >= done_posts:
            pct_done = done_posts / total_eligible * 100 if total_eligible else 0
            coverage = (
                f"eligible-post progress (done-cache): {done_posts:,} / {total_eligible:,} posts  "
                f"|  indicator {pct_done:.1f}%"
            )
        elif done_posts:
            coverage = f"eligible-post progress (done-cache): {done_posts:,} posts (total eligible pending refresh)"
        else:
            coverage = "cache not built yet (auto-built in comments stage)"
        print(f"    comments {size_mb:>9.0f} MB  |  {coverage}")
        print("              note: this is cache indicator, not full platform comment-row coverage")
        cm_rows = _to_int(metrics_cache.get("comments_rows"), 0)
        cm_unique = _to_int(metrics_cache.get("comments_unique"), 0)
        cm_at = str(metrics_cache.get("updated_at", "") or "")
        if cm_rows > 0 and cm_unique > 0:
            uid_pct = (cm_unique / platform_comments * 100.0) if platform_comments else 0.0
            print(
                f"              cached local comments rows/uid: {cm_rows:,}/{cm_unique:,}  "
                f"|  uid coverage~{uid_pct:.1f}%"
            )
            if cm_at:
                print(f"              cache timestamp: {cm_at[:19]}")
    else:
        print("    comments (no local comments data yet)")

    #
    print("\n  Platform Data (API)")
    if platform_total:
        gap = max(0, platform_total - local_posts)
        pct = local_posts / platform_total * 100
        print(f"    posts   {platform_total:>12,} rows  |  local coverage {pct:.1f}%  |  gap ~{gap:,}")
    else:
        print("    posts   (API stats unavailable, maybe rate-limited)")
    if platform_comments:
        print(f"    comments {platform_comments:>11,} rows")
    if platform_agents:
        print(f"    agents   {platform_agents:>11,}")

    #
    print("\n  Plan")
    if args.comments_only:
        print("  - posts: skip (--comments-only)")
    elif args.full:
        print("  - posts: full history")
    elif since_time:
        print(f"  - posts: incremental newer than {since_time[:19]}")
    else:
        print("  - posts: full history (no checkpoint)")

    if args.no_comments:
        print("  - comments: skip (--no-comments)")
    else:
        limit_str = f", top {args.max_comment_posts:,} posts" if args.max_comment_posts else ""
        print(
            f"  - comments: comment_count >= {args.min_comments}{limit_str}, "
            f"workers={args.workers}, queue={args.comment_queue_strategy}, id_cache={args.comment_id_cache}"
        )
        if args.comment_queue_strategy == "layered":
            print(
                f"    layers: small<={args.queue_small_max}, "
                f"medium<={args.queue_medium_max}, long>{args.queue_medium_max}"
            )
    print(f"  - rate: posts~{args.read_rpm}/min, comments~{args.comment_rpm}/min")
    if args.no_resume:
        print("  - resume: ignore/clear (--no-resume)")

    print(f"\n  data dir: {out}/")
    print(f"{SEP}\n")


#
# CLEAN RUNS
#

def clean_runs(out: Path, keep_last: int = 0):
    """Delete data/runs snapshots and optionally keep latest N files."""
    runs_dir = out / "runs"
    if not runs_dir.exists():
        print("  runs/ directory not found.")
        return
    files = sorted(runs_dir.glob("*.jsonl"))
    if not files:
        print("  runs/ directory is already empty.")
        return

    total_size_mb = sum(f.stat().st_size for f in files) / 1024 / 1024
    if keep_last > 0 and keep_last < len(files):
        to_delete = files[:-keep_last]
        kept = keep_last
    else:
        to_delete = files
        kept = 0

    del_size_mb = sum(f.stat().st_size for f in to_delete) / 1024 / 1024
    print(f"  runs/ dir: {len(files)} files, total {total_size_mb:.1f} MB")
    if not to_delete:
        print(f"  keep latest {kept}, nothing to delete.")
        return

    print(f"  deleting {len(to_delete)} files, freeing ~{del_size_mb:.1f} MB (keep latest {kept})...")
    for f in to_delete:
        f.unlink()
    print(f"  done. kept {kept} run snapshot files.")


#
# DATA CHECK / DEDUP / REFETCH
#

def dedup_comments(out: Path):
    """Deduplicate comments_all.jsonl by comment id in place."""
    comments_path = out / "comments_all.jsonl"
    if not comments_path.exists():
        print("  [!] comments_all.jsonl missing")
        return
    tmp_path = comments_path.with_suffix(".dedup_tmp")
    seen = set()
    total = kept = 0
    print("deduplicating comments_all.jsonl by comment id...")
    with open(comments_path, encoding="utf-8") as fin,\
         open(tmp_path, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                obj = json.loads(line)
                cid = obj.get("id")
                if not cid:
                    # The scrape path deliberately keeps id-less rows; dedup
                    # must not drop them.
                    fout.write(line + "\n")
                    kept += 1
                    continue
                key = (obj.get("post_id") or "", cid)
                if key not in seen:
                    seen.add(key)
                    fout.write(line + "\n")
                    kept += 1
            except Exception:
                fout.write(line + "\n")  # ?-check ?
                kept += 1
    removed = total - kept
    print(f"  rows before: {total:,}  ->  after dedup: {kept:,}  (removed {removed:,} duplicates)")
    tmp_path.replace(comments_path)
    print("  [ok] done; replaced comments_all.jsonl in-place")


def refetch_comments(out: Path, min_count: int):
    """Force re-sync for posts above threshold by resetting cache/cooldown/resume state."""
    done_cache_path = out / "comments_done_posts.txt"
    posts_path = out / "posts_all.jsonl"
    resume_cache_path = out / "comments_resume_cursor.jsonl"
    sync_state_path = out / "comments_post_sync_state.jsonl"
    if not done_cache_path.exists():
        print("  [!] comments_done_posts.txt not found; nothing to refetch-reset")
        done_ids = set()
    else:
        done_ids = set()
        with open(done_cache_path, encoding="utf-8") as f:
            for line in f:
                pid = line.strip()
                if pid:
                    done_ids.add(pid)
    if not posts_path.exists():
        print("  [!] posts_all.jsonl missing")
        return

    target_ids = set()
    with open(posts_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                p = json.loads(line)
                pid = p.get("id")
                if pid and p.get("comment_count", 0) >= min_count:
                    target_ids.add(pid)
            except Exception:
                pass
    if not target_ids:
        print(f"  [ok] no posts with comment_count >= {min_count}; nothing to reset")
        return

    # 1) done-cache reset for target posts
    removed_done = len(done_ids & target_ids)
    if done_cache_path.exists():
        new_done = done_ids - target_ids
        with open(done_cache_path, "w", encoding="utf-8") as f:
            for pid in new_done:
                f.write(pid + "\n")
    else:
        new_done = set()

    # 2) clear resume cursors for target posts
    resume_reset = 0
    if resume_cache_path.exists():
        rc = CommentsResumeCache(resume_cache_path)
        for pid in target_ids:
            if rc.get(pid):
                rc.set(pid, None)
                resume_reset += 1
        rc.compact_if_needed(force=True)

    # 3) clear cooldown / force retry for target posts
    sync_reset = 0
    if sync_state_path.exists():
        ss = CommentsPostSyncState(sync_state_path)
        sync_reset = ss.force_retry(target_ids)
        ss.compact_if_needed(force=True)

    print(f"  target posts (comment_count >= {min_count}): {len(target_ids):,}")
    print(f"  done-cache removed: {removed_done:,}  |  remaining done posts: {len(new_done):,}")
    print(f"  resume cursors cleared: {resume_reset:,}")
    print(f"  sync-state forced retry: {sync_reset:,}")
    print("  next: run comments backfill again (comments-only recommended)")


def check_data(out: Path, min_comments: int = 1, fast: bool = False, sample_posts: int = 0):
    """Check local files, integrity, and coverage using a configurable eligible threshold."""
    print(f"\n=== Data Check ({out}/) ===\n")

    threshold = max(int(min_comments), 0)
    comment_rows_by_post = Counter()
    comment_unique_by_post = Counter()
    seen_comment_pairs = set()
    eligible_done_pct = None
    scan_stats = {}
    cp_cache_path = out / "checkpoint.json"
    cached_metrics = {}
    if cp_cache_path.exists():
        try:
            with open(cp_cache_path, encoding="utf-8") as f:
                cp_cache_obj = json.load(f)
            cached_metrics = cp_cache_obj.get("local_metrics_cache", {}) if isinstance(cp_cache_obj, dict) else {}
        except Exception:
            cached_metrics = {}

    files = [
        ("posts_all.jsonl",    "posts"),
        ("comments_all.jsonl", "comments"),
        ("agents_seen.jsonl",  "agents"),
    ]

    for filename, label in files:
        path = out / filename
        if not path.exists():
            print(f"  [{label}] {filename}: missing\n")
            continue

        size_mb = path.stat().st_size / 1024 / 1024
        total_lines = bad_lines = 0
        ids = set()
        dates = []

        # While scanning comments, collect post_id for done-cache bootstrap.
        collect_post_ids = (filename == "comments_all.jsonl")
        seen_post_ids_for_cache = set() if collect_post_ids else None
        if collect_post_ids and fast:
            rows_cached = _to_int(cached_metrics.get("comments_rows"), 0)
            unique_cached = _to_int(cached_metrics.get("comments_unique"), 0)
            updated_at = str(cached_metrics.get("updated_at", "") or "")
            scan_stats[label] = {"rows": rows_cached, "unique": unique_cached}
            print(f"  [{label}] {filename}")
            print(
                f"    size: {size_mb:.1f} MB  |  rows: {rows_cached:,} (cached)  |  unique_id: {unique_cached:,} (cached)"
            )
            print("    [fast] skipped full comments file scan")
            if updated_at:
                print(f"    cache timestamp: {updated_at[:19]}")
            print()
            continue

        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                total_lines += 1
                try:
                    obj = json.loads(line)
                    if obj_id := obj.get("id"):
                        ids.add(obj_id)
                    if d := obj.get("created_at"):
                        dates.append(d)
                    if collect_post_ids and (pid := obj.get("post_id")):
                        seen_post_ids_for_cache.add(pid)
                        comment_rows_by_post[pid] += 1
                        cid = obj.get("id")
                        if cid:
                            pair_fp = _pair_fingerprint(pid, cid)
                            if pair_fp not in seen_comment_pairs:
                                seen_comment_pairs.add(pair_fp)
                                comment_unique_by_post[pid] += 1
                        else:
                            comment_unique_by_post[pid] += 1
                except Exception:
                    bad_lines += 1

        dupes = total_lines - len(ids)
        scan_stats[label] = {"rows": total_lines, "unique": len(ids)}
        print(f"  [{label}] {filename}")
        print(
            f"    size: {size_mb:.1f} MB  |  rows: {total_lines:,}  |  unique_id: {len(ids):,}  |  "
            f"dupes: {dupes}  |  bad_json: {bad_lines}"
        )
        if dates:
            d_min, d_max = min(dates), max(dates)
            print(f"    date range: {d_min[:19]} -> {d_max[:19]}")
            if filename == "posts_all.jsonl":
                daily = Counter(d[:10] for d in dates)
                d_start = date_type.fromisoformat(d_min[:10])
                d_end   = date_type.fromisoformat(d_max[:10])
                total_days = (d_end - d_start).days + 1
                avg_per_day = len(dates) / total_days
                missing, sparse = [], []
                cur = d_start
                while cur <= d_end:
                    ds = cur.isoformat()
                    cnt = daily[ds]
                    if cnt == 0:
                        missing.append(ds)
                    elif cnt < max(50, avg_per_day * 0.05):
                        sparse.append((ds, cnt))
                    cur += timedelta(days=1)
                if missing:
                    print(f"    [!] missing {len(missing)} day(s) with 0 posts: {missing}")
                else:
                    print(f"    [ok] date contiguous, no missing day ({total_days} days, avg {int(avg_per_day):,}/day)")
                if sparse:
                    print(f"    [?] sparse days (<5% avg): {[(d, f'{n:,}') for d, n in sparse]}")
        if bad_lines:
            print(f"    [!] {bad_lines} JSON parse errors (possibly truncated lines)")
        if dupes > 0:
            print(f"    [!] found {dupes} duplicate records")

        # comments -> build done-cache if absent
        if collect_post_ids and seen_post_ids_for_cache:
            done_cache_path = out / "comments_done_posts.txt"
            if not done_cache_path.exists():
                with open(done_cache_path, "w", encoding="utf-8") as f:
                    for pid in seen_post_ids_for_cache:
                        f.write(pid + "\n")
                print(f"    [+] built comments_done_posts cache ({len(seen_post_ids_for_cache):,} posts)")
        print()

    # comments progress cache (legacy indicator only)
    done_cache_path = out / "comments_done_posts.txt"
    done_ids = set()
    if done_cache_path.exists():
        done_ids = {line.strip() for line in open(done_cache_path, encoding="utf-8") if line.strip()}
        done_count = len(done_ids)
        print(f"  [comments cache] comments_done_posts.txt: {done_count:,} posts marked done")
    else:
        print("  [comments cache] comments_done_posts.txt not found yet")

    # Strict audit: compare post.comment_count vs real local comment rows per post.
    posts_path = out / "posts_all.jsonl"
    def _keep_top(items, score, pid, expected, got, limit=5):
        rec = (score, pid, expected, got)
        if len(items) < limit:
            items.append(rec)
            items.sort(key=lambda x: x[0], reverse=True)
            return
        if score > items[-1][0]:
            items[-1] = rec
            items.sort(key=lambda x: x[0], reverse=True)

    if fast:
        print("  [comment_count audit]")
        print("    fast mode enabled: skipped strict per-post expected vs local audit")
        if sample_posts > 0 and posts_path.exists():
            sample_n = max(1, _to_int(sample_posts, 0))
            print(f"    sampled strict audit enabled: target {sample_n:,} posts (comment_count >= {threshold})")

            rng = random.Random(42)
            sample_pairs = []
            eligible_seen = 0
            with open(posts_path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        p = json.loads(line)
                        pid = p.get("id")
                        if not pid:
                            continue
                        expected = max(0, _to_int(p.get("comment_count"), 0))
                    except Exception:
                        continue
                    if expected < threshold:
                        continue
                    eligible_seen += 1
                    rec = (pid, expected)
                    if len(sample_pairs) < sample_n:
                        sample_pairs.append(rec)
                    else:
                        j = rng.randint(1, eligible_seen)
                        if j <= sample_n:
                            sample_pairs[j - 1] = rec

            if sample_pairs:
                sample_ids = {pid for pid, _ in sample_pairs}
                sample_counts = Counter()
                comments_path = out / "comments_all.jsonl"
                used_sqlite = False
                if comments_path.exists():
                    cache_candidates = [
                        out / "comments_id_cache.sqlite",
                        out / "comments_id_cache_check.sqlite",
                    ]
                    # --check runs lock-exempt; if a live scraper holds the
                    # lock, don't write-seed its sqlite cache out from under it
                    # (WAL allows it silently, and contention surfaces as
                    # swallowed callback errors in the live run).
                    _live_lock = out / "scraper.lock"
                    if _live_lock.exists():
                        try:
                            _ld = json.loads(_live_lock.read_text(encoding="utf-8"))
                            if _pid_alive(_to_int(_ld.get("pid"), 0)):
                                print("    [sample] live scraper detected; using dedicated check cache only.")
                                cache_candidates = cache_candidates[1:]
                        except Exception:
                            pass
                    for idx, cache_path in enumerate(cache_candidates):
                        sample_store = None
                        try:
                            sample_store = CommentsIdStore(
                                mode="sqlite",
                                sqlite_path=cache_path,
                            )
                            sample_store.seed_from_comments_file(comments_path, sample_ids)
                            sample_counts = Counter(sample_store.count_for_posts(sample_ids))
                            used_sqlite = True
                            if idx == 1:
                                print(f"    [sample] using dedicated sqlite cache: {cache_path.name}")
                            break
                        except Exception as e:
                            err_txt = str(e)
                            if idx == 0 and "locked" in err_txt.lower():
                                print("    [sample] shared sqlite cache is locked; trying dedicated cache...")
                            else:
                                print(f"    [sample] sqlite cache unavailable ({cache_path.name}): {e}")
                        finally:
                            if sample_store:
                                sample_store.close()
                    if not used_sqlite:
                        print("    [sample] fallback to one-pass comments scan for sample posts")
                        sample_counts = _count_comments_for_target_posts(comments_path, sample_ids, strict_unique=True)

                sample_exact = sample_under = sample_over = 0
                sample_expected = sample_local = sample_gap = 0
                top_under = []
                top_over = []
                for pid, expected in sample_pairs:
                    got = int(sample_counts.get(pid, 0))
                    sample_expected += expected
                    sample_local += got
                    if got == expected:
                        sample_exact += 1
                    elif got < expected:
                        sample_under += 1
                        miss = expected - got
                        sample_gap += miss
                        _keep_top(top_under, miss, pid, expected, got)
                    else:
                        sample_over += 1
                        _keep_top(top_over, got - expected, pid, expected, got)

                source = "sqlite id cache" if used_sqlite else "comments scan"
                print(f"    sampled posts: {len(sample_pairs):,} / eligible~{eligible_seen:,}  |  source: {source}")
                print(
                    f"    sample exact={sample_exact:,}  |  under={sample_under:,}  |  over={sample_over:,}  "
                    f"|  expected~{sample_expected:,}  |  local unique~{sample_local:,}  |  gap~{sample_gap:,}"
                )
                if top_under:
                    print("    sample top underfilled:")
                    for miss, pid, exp, got in top_under:
                        print(f"      {pid[:8]}...  {exp:,}/{got:,}  missing~{miss:,}")
                if top_over:
                    print("    sample top overfilled:")
                    for extra, pid, exp, got in top_over:
                        print(f"      {pid[:8]}...  {exp:,}/{got:,}  extra~{extra:,}")
            else:
                print("    sampled strict audit skipped: no eligible posts found")
        elif sample_posts > 0 and not posts_path.exists():
            print("    sampled strict audit skipped: posts_all.jsonl missing")
        else:
            print("    tip: add --check-sample-posts N to run sampled strict audit in fast mode")
        print("    run without --check-fast for full audit")
        print()
    elif posts_path.exists():
        total_posts_audited = 0
        all_exact = all_under = all_over = 0
        total_expected_all = total_local_all_unique = total_local_all_rows = 0
        top_under = []
        top_over = []

        total_eligible = pending = done_eligible = exact = overfilled = 0
        total_expected = total_local_unique = total_local_rows = total_gap = 0
        done_by_cache = 0

        with open(posts_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    p = json.loads(line)
                    pid = p.get("id")
                    if not pid:
                        continue
                    expected = max(0, _to_int(p.get("comment_count"), 0))
                    got_unique = int(comment_unique_by_post.get(pid, 0))
                    got_rows = int(comment_rows_by_post.get(pid, 0))
                except Exception:
                    continue

                total_posts_audited += 1
                total_expected_all += expected
                total_local_all_unique += got_unique
                total_local_all_rows += got_rows

                if got_unique == expected:
                    all_exact += 1
                elif got_unique < expected:
                    all_under += 1
                    _keep_top(top_under, expected - got_unique, pid, expected, got_unique)
                else:
                    all_over += 1
                    _keep_top(top_over, got_unique - expected, pid, expected, got_unique)

                if expected >= threshold:
                    total_eligible += 1
                    total_expected += expected
                    total_local_unique += got_unique
                    total_local_rows += got_rows
                    if pid in done_ids:
                        done_by_cache += 1
                    if got_unique >= expected:
                        done_eligible += 1
                        if got_unique == expected:
                            exact += 1
                        else:
                            overfilled += 1
                    else:
                        pending += 1
                        total_gap += (expected - got_unique)

        delta_all = total_local_all_unique - total_expected_all
        print("  [comment_count audit]")
        print(
            f"    all posts audited: {total_posts_audited:,}  |  exact={all_exact:,}  "
            f"|  under={all_under:,}  |  over={all_over:,}"
        )
        print(
            f"    expected comments~{total_expected_all:,}  |  local unique~{total_local_all_unique:,}  "
            f"|  delta(unique-expected) {delta_all:+,}"
        )
        print(f"    local raw rows~{total_local_all_rows:,}")
        if top_under:
            print("    top underfilled posts (expected/local, missing):")
            for miss, pid, exp, got in top_under:
                print(f"      {pid[:8]}...  {exp:,}/{got:,}  missing~{miss:,}")
        if top_over:
            print("    top overfilled posts (expected/local, extra):")
            for extra, pid, exp, got in top_over:
                print(f"      {pid[:8]}...  {exp:,}/{got:,}  extra~{extra:,}")

        pct = f"{done_eligible / total_eligible * 100:.1f}%" if total_eligible else "?"
        eligible_done_pct = (done_eligible / total_eligible * 100.0) if total_eligible else None
        print(
            f"    eligible posts (>={threshold} comments): {total_eligible:,}  |  "
            f"done(aligned): {done_eligible:,}  |  pending: {pending:,}  |  coverage: {pct}"
        )
        print(
            f"    expected~{total_expected:,}  |  local unique~{total_local_unique:,}  |  gap~{total_gap:,}  "
            f"|  exact={exact:,} overfilled={overfilled:,}"
        )
        print(f"    local raw rows (eligible only)~{total_local_rows:,}")
        if done_ids:
            print(f"    cache-only view (legacy): done(in threshold by cache) {done_by_cache:,}")
    else:
        print("  [comment_count audit] posts_all.jsonl missing; skip strict comment audit")
    print()

    # Checkpoint
    cp_path = out / "checkpoint.json"
    if cp_path.exists():
        with open(cp_path, encoding="utf-8") as f:
            cp = json.load(f)
        print(f"  [Checkpoint]")
        print(f"    newest post time: {cp.get('newest_post_created_at') or 'none (full crawl not completed yet)'}")
        print(
            f"    runs: {len(cp.get('runs', []))}  |  "
            f"recorded posts: {cp.get('total_posts', 0):,}  |  recorded comments: {cp.get('total_comments', 0):,}"
        )
        if cp.get("_resume_cursor"):
            print("    [!] unfinished resume cursor exists (_resume_cursor). Run with --full to continue.")
        print()

    # runs/ snapshots
    runs_dir = out / "runs"
    if runs_dir.exists():
        run_files = sorted(runs_dir.glob("*.jsonl"))
        total_size = sum(f.stat().st_size for f in run_files) / 1024 / 1024
        print(f"  [runs snapshots] {len(run_files)} files | total size {total_size:.1f} MB")
        if run_files:
            print(f"    latest: {run_files[-1].name}")
        if total_size > 500:
            print("    [tip] runs snapshots are large; use --clean-runs")
    print()

    # platform comparison
    print("  [platform compare] fetching live platform stats...")
    platform_posts = platform_comments = platform_agents = 0

    # ?ids?
    def count_jsonl(path):
        if not path.exists():
            return 0
        n = 0
        with open(path, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    n += 1
        return n

    local_posts_rows = scan_stats.get("posts", {}).get("rows", count_jsonl(out / "posts_all.jsonl"))
    local_posts_unique = scan_stats.get("posts", {}).get("unique", local_posts_rows)
    local_comments_rows = scan_stats.get("comments", {}).get("rows", count_jsonl(out / "comments_all.jsonl"))
    local_comments_unique = scan_stats.get("comments", {}).get("unique", local_comments_rows)
    local_agents_rows = scan_stats.get("agents", {}).get("rows", count_jsonl(out / "agents_seen.jsonl"))
    # --check is intentionally lock-free, but this is a read-modify-write of
    # checkpoint.json. A live scraper also rewrites checkpoint.json continuously
    # (resume cursor, anchor) via a full-file replace; if we replace it here with
    # our stale snapshot we clobber its just-advanced crawl state (-> re-crawl or
    # silently skipped window) or, sharing the same tmp name, produce corrupt
    # JSON. So only persist the metrics cache when NO live scraper holds the
    # lock, and use a private tmp name to avoid any tmp collision.
    _live_lock = out / "scraper.lock"
    _scrape_running = False
    try:
        if _live_lock.exists():
            _ld = json.loads(_live_lock.read_text(encoding="utf-8"))
            _scrape_running = _pid_alive(_to_int(_ld.get("pid"), 0))
    except Exception:
        _scrape_running = False
    if cp_cache_path.exists() and _scrape_running:
        print("    [i] a scrape is running; skipping local_metrics_cache write to protect checkpoint.json.")
    elif cp_cache_path.exists():
        try:
            with open(cp_cache_path, encoding="utf-8") as f:
                cp_data = json.load(f)
            cp_data["local_metrics_cache"] = {
                "posts_rows": local_posts_rows,
                "posts_unique": local_posts_unique,
                "comments_rows": local_comments_rows,
                "comments_unique": local_comments_unique,
                "agents_rows": local_agents_rows,
                "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            tmp = cp_cache_path.with_suffix(f".metrics.{os.getpid()}.tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(cp_data, f, ensure_ascii=False, indent=2)
            tmp.replace(cp_cache_path)
        except Exception:
            pass

    stats = fetch_platform_stats()
    if not stats:
        print("    [!] cannot fetch platform stats (timeout or rate-limited), skip comparison.\n")
        return
    platform_posts    = int(stats.get("totalPosts",    0) or 0)
    platform_comments = int(stats.get("totalComments", 0) or 0)
    platform_agents   = int(stats.get("totalAgents",   0) or 0)

    def fmt_pct(local, total):
        if not total:
            return "  ?"
        pct = local / total * 100
        mark = "OK" if pct >= 99 else ("~" if pct >= 50 else "!")
        return f"{mark} {pct:5.1f}%"

    print(f"  {'type':<10} {'local':>12} {'platform':>12} {'coverage':>10}")
    print(f"  {'-'*48}")
    print(f"  {'posts':<10} {local_posts_rows:>12,} {platform_posts:>12,} {fmt_pct(local_posts_rows, platform_posts):>10}")
    print(f"  {'cmts(rows)':<10} {local_comments_rows:>12,} {platform_comments:>12,} {fmt_pct(local_comments_rows, platform_comments):>10}")
    print(f"  {'cmts(uid)':<10} {local_comments_unique:>12,} {platform_comments:>12,} {fmt_pct(local_comments_unique, platform_comments):>10}")
    print(f"  {'agents':<10} {local_agents_rows:>12,} {platform_agents:>12,} {fmt_pct(local_agents_rows, platform_agents):>10}")
    post_cov = (local_posts_unique / platform_posts * 100.0) if platform_posts else None
    comment_cov = (local_comments_unique / platform_comments * 100.0) if platform_comments else None
    if post_cov is not None and comment_cov is not None:
        delta = post_cov - comment_cov
        miss_posts = max(0, platform_posts - local_posts_unique)
        miss_comments = max(0, platform_comments - local_comments_unique)
        print("  [coverage diff]")
        print(
            f"    platform coverage (unique-first): posts {post_cov:.1f}% vs comments {comment_cov:.1f}%  |  delta {delta:+.1f} pp"
        )
        print(f"    missing unique rows: posts ~{miss_posts:,}  |  comments ~{miss_comments:,}")
        if eligible_done_pct is not None:
            delta_align = post_cov - eligible_done_pct
            print(
                f"    aligned eligible (>= {threshold} comments): {eligible_done_pct:.1f}%  |  "
                f"delta vs posts {delta_align:+.1f} pp"
            )
    print()


#
# MAIN
#

#
# CROSS-PROCESS LOCK
#
# Shared with auto_scheduler.py (same file, same format). auto_scheduler
# acquires it before launching scraper.py and passes MOLT_LOCK_INHERITED=1
# so the child does not refuse its own parent's lock.

_LOCK_INHERITED_ENV = "MOLT_LOCK_INHERITED"


def _lock_is_stale_by_age(started: str) -> bool:
    """A lock older than _MAX_LOCK_AGE_SECONDS is stale regardless of PID
    liveness. Without this, a crash (atexit never fires on SIGKILL / power loss)
    plus OS PID reuse makes a dead run's lock look alive forever, so every future
    run refuses to start and the corpus silently stops growing."""
    try:
        t = datetime.strptime(started, "%Y-%m-%d %H:%M:%S")
        return (datetime.now() - t).total_seconds() > _MAX_LOCK_AGE_SECONDS
    except Exception:
        return False


def _pid_alive(pid: int) -> bool:
    if not pid:
        return False
    if sys.platform == "win32":
        import ctypes
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
        if handle:
            kernel32.CloseHandle(handle)
            return True
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def acquire_scraper_lock(lock_path: Path) -> bool:
    """Take the cross-process lock. Returns False if another live run holds it."""
    if os.environ.get(_LOCK_INHERITED_ENV) == "1":
        # Only honor inheritance when a live parent actually holds the lock;
        # a lingering shell var must not silently disable locking.
        try:
            data = json.loads(lock_path.read_text(encoding="utf-8"))
            if _pid_alive(_to_int(data.get("pid"), 0)):
                return True
        except Exception:
            pass
        print(f"  [lock] {_LOCK_INHERITED_ENV} set but no live parent lock; acquiring normally.")
    for _ in range(2):
        try:
            with open(lock_path, "x", encoding="utf-8") as f:
                json.dump({
                    "pid": os.getpid(),
                    "started": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "argv": sys.argv[1:],
                }, f)
            atexit.register(release_scraper_lock, lock_path)
            return True
        except FileExistsError:
            try:
                lock_data = json.loads(lock_path.read_text(encoding="utf-8"))
                pid = _to_int(lock_data.get("pid"), 0)
                started = lock_data.get("started", "?")
            except Exception:
                pid, started = 0, "?"
            age_stale = _lock_is_stale_by_age(started)
            if _pid_alive(pid) and not age_stale:
                print(f"  [lock] another scraper is running (PID {pid}, started {started}); refusing to start.")
                return False
            if age_stale:
                print(f"  [lock] lock older than {_MAX_LOCK_AGE_SECONDS // 3600}h (started {started}); treating as stale, removing.")
            else:
                print(f"  [lock] stale lock (PID {pid} not running); removing.")
            try:
                lock_path.unlink()
            except OSError:
                return False
    return False


def release_scraper_lock(lock_path: Path):
    try:
        if lock_path.exists():
            data = json.loads(lock_path.read_text(encoding="utf-8"))
            if _to_int(data.get("pid"), 0) == os.getpid():
                lock_path.unlink()
    except Exception:
        pass


def main():
    parser = argparse.ArgumentParser(description="Moltbook data scraper v2")
    parser.add_argument("--check", action="store_true", help="run data health check only")
    parser.add_argument("--check-fast", action="store_true",
                        help="fast check mode: skip strict per-post comment audit and reuse cached comment metrics")
    parser.add_argument(
        "--check-sample-posts",
        type=int,
        default=0,
        help="when used with --check-fast, run strict audit on a random sample of eligible posts (0=disabled)",
    )
    parser.add_argument("--full", action="store_true", help="full mode: ignore checkpoint and crawl history")
    parser.add_argument("--no-resume", action="store_true",
                        help="ignore and clear saved resume cursors")
    parser.add_argument("--comments-only", action="store_true",
                        help="run comments backfill only; skip posts stage")
    parser.add_argument("--no-comments", action="store_true", help="skip comments stage")
    parser.add_argument("--max-posts", type=int, default=None, help="max newly written posts this run")
    parser.add_argument("--reset", action="store_true", help="reset checkpoint and start fresh")
    parser.add_argument("--workers", type=int, default=1, help="comment worker threads")
    parser.add_argument("--read-rpm", type=int, default=_DEFAULT_READ_RPM,
                        help=f"target read rate req/min (default {_DEFAULT_READ_RPM})")
    parser.add_argument("--comment-rpm", type=int, default=_DEFAULT_COMMENT_RPM,
                        help=f"target comment endpoint req/min (default {_DEFAULT_COMMENT_RPM})")
    parser.add_argument("--min-comments", type=int, default=1,
                        help="only fetch comments for posts with comment_count >= N")
    parser.add_argument("--max-comment-posts", type=int, default=0,
                        help="max posts to fetch comments for (0 = unlimited)")
    parser.add_argument(
        "--comment-id-cache",
        choices=("memory", "sqlite"),
        default=_DEFAULT_COMMENT_ID_CACHE_MODE,
        help="comment id dedup cache backend (memory or sqlite)",
    )
    parser.add_argument(
        "--comment-queue-strategy",
        choices=("layered", "small-first", "large-first"),
        default=_DEFAULT_COMMENT_QUEUE_STRATEGY,
        help=(
            "comment scheduling strategy: layered (small->medium->long), "
            "small-first, or large-first (legacy)"
        ),
    )
    parser.add_argument(
        "--queue-small-max",
        type=int,
        default=_DEFAULT_QUEUE_SMALL_MAX,
        help=f"layered queue cutoff for small posts (default {_DEFAULT_QUEUE_SMALL_MAX})",
    )
    parser.add_argument(
        "--queue-medium-max",
        type=int,
        default=_DEFAULT_QUEUE_MEDIUM_MAX,
        help=f"layered queue cutoff for medium posts (default {_DEFAULT_QUEUE_MEDIUM_MAX})",
    )
    parser.add_argument("--output-dir", default="data", help="output directory")
    parser.add_argument("--api-key", default="", help="override MOLTBOOK_API_KEY")
    parser.add_argument("--clean-runs", nargs="?", const=0, type=int, metavar="KEEP",
                        help="clean data/runs snapshots, optionally keep latest N")
    parser.add_argument("--dedup-comments", action="store_true", help="deduplicate comments_all.jsonl by id")
    parser.add_argument("--refetch-comments", type=int, metavar="N",
                        help="force retry comments for posts with comment_count >= N (reset done/resume/cooldown state)")
    parser.add_argument("--snapshot", action="store_true", help="collect one hot/rising/top snapshot")
    parser.add_argument("--no-snapshot", action="store_true", help="disable end-of-run auto snapshot")
    parser.add_argument("--seed-snapshots", action="store_true", help="seed snapshots from posts_all.jsonl")
    parser.add_argument("--agent-snapshot", action="store_true",
                        help="save agent metrics snapshot (karma, followers, etc.) for time-series analysis")
    parser.add_argument("--no-agent-snapshot", action="store_true",
                        help="disable auto agent snapshot at end of run")
    args = parser.parse_args()
    if args.check_fast:
        # --check-fast alone must mean "fast check", not "unlocked full scrape".
        args.check = True

    # ?--api-key .env
    if args.api_key:
        HEADERS["Authorization"] = f"Bearer {args.api_key}"

    if args.read_rpm <= 0 or args.comment_rpm <= 0:
        raise SystemExit("--read-rpm and --comment-rpm must be >= 1")
    if args.queue_small_max <= 0 or args.queue_medium_max <= 0:
        raise SystemExit("--queue-small-max and --queue-medium-max must be >= 1")
    if args.comment_queue_strategy == "layered" and args.queue_small_max >= args.queue_medium_max:
        raise SystemExit("--queue-small-max must be < --queue-medium-max")
    if args.comments_only and args.no_comments:
        raise SystemExit("--comments-only conflicts with --no-comments")
    _rate_limiter.set_rate(args.read_rpm)
    _rate_limiter_comments.set_rate(args.comment_rpm)

    out = Path(args.output_dir)
    out.mkdir(exist_ok=True)
    (out / "runs").mkdir(exist_ok=True)

    # --check is read-only and may run alongside a live scrape; everything
    # else mutates shared files and must hold the cross-process lock.
    if not (args.check or args.check_fast):
        if not acquire_scraper_lock(out / "scraper.lock"):
            sys.exit(3)

    if args.check:
        check_data(
            out,
            min_comments=args.min_comments,
            fast=args.check_fast,
            sample_posts=max(0, args.check_sample_posts),
        )
        return

    if args.clean_runs is not None:
        clean_runs(out, keep_last=args.clean_runs)
        return

    if args.dedup_comments:
        dedup_comments(out)
        return

    if args.refetch_comments is not None:
        refetch_comments(out, args.refetch_comments)
        return

    if args.snapshot:
        fetch_hot_snapshot(out)
        return

    if args.agent_snapshot:
        take_agent_snapshot(out)
        return

    if args.seed_snapshots:
        seed_snapshots(out)
        return

    checkpoint_path = out / "checkpoint.json"
    checkpoint = Checkpoint(checkpoint_path)

    if args.reset:
        print("resetting checkpoint...")
        checkpoint_path.unlink(missing_ok=True)
        checkpoint = Checkpoint(checkpoint_path)
        args.full = True

    comments_resume_path = out / "comments_resume_cursor.jsonl"
    comments_sync_state_path = out / "comments_post_sync_state.jsonl"
    if args.no_resume:
        had_post_resume = bool(checkpoint.data.get("_resume_cursor"))
        had_comment_resume = comments_resume_path.exists() and comments_resume_path.stat().st_size > 0
        had_sync_state = comments_sync_state_path.exists() and comments_sync_state_path.stat().st_size > 0
        checkpoint.clear_resume()
        comments_resume_path.unlink(missing_ok=True)
        comments_sync_state_path.unlink(missing_ok=True)
        if had_post_resume or had_comment_resume or had_sync_state:
            print("  [no-resume] cleared post/comment resume and sync-state cursors; re-evaluating gaps.")
        else:
            print("  [no-resume] no saved cursors found; re-evaluating gaps.")

    since_time = None

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # checkpoint
    posts_store   = JsonlStore(out / "posts_all.jsonl")   # ?
    comments_path = out / "comments_all.jsonl"

    local_oldest = local_newest = local_oldest_dt = ""
    cp_oldest = checkpoint.data.get("oldest_post_created_at", "")
    cp_newest = checkpoint.data.get("newest_post_created_at", "")
    if cp_oldest:
        local_oldest_dt = cp_oldest
        local_oldest = cp_oldest[:10]
    if cp_newest:
        local_newest = cp_newest[:10]

    # fallback/checkpoint calibration
    posts_path = out / "posts_all.jsonl"
    if (not local_oldest or not local_newest) and posts_path.exists() and posts_path.stat().st_size > 0:
        head_dates, tail_dates = [], []
        with open(posts_path, encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= 500:
                    break
                try:
                    d = json.loads(line.strip()).get("created_at", "")
                    if d:
                        head_dates.append(d)
                except Exception:
                    pass
        with open(posts_path, "rb") as fb:
            fb.seek(max(0, posts_path.stat().st_size - 5_000_000))
            tail_raw = fb.read().decode("utf-8", errors="replace")
        for line in tail_raw.splitlines():
            try:
                d = json.loads(line.strip()).get("created_at", "")
                if d:
                    tail_dates.append(d)
            except Exception:
                pass
        all_dates = head_dates + tail_dates
        if all_dates:
            if not local_newest:
                local_newest = max(all_dates)[:10]
            if not local_oldest:
                local_oldest_dt = min(all_dates)
                local_oldest = local_oldest_dt[:10]

    # Always sample tail to detect stale checkpoint newest anchor.
    sampled_newest = ""
    if posts_path.exists() and posts_path.stat().st_size > 0:
        tail_dates = []
        with open(posts_path, "rb") as fb:
            fb.seek(max(0, posts_path.stat().st_size - 5_000_000))
            tail_raw = fb.read().decode("utf-8", errors="replace")
        for line in tail_raw.splitlines():
            try:
                d = json.loads(line.strip()).get("created_at", "")
                if d:
                    tail_dates.append(d)
            except Exception:
                pass
        if tail_dates:
            sampled_newest = max(tail_dates)
            if not local_newest or sampled_newest[:10] > local_newest:
                local_newest = sampled_newest[:10]
            cp_newest = checkpoint.data.get("newest_post_created_at") or ""
            if checkpoint.data.get("_resume_cursor"):
                # An interrupted incremental window is pending. The file tail
                # holds posts NEWER than the unfetched window (we page newest
                # first), so fast-forwarding the anchor here would orphan the
                # window and silently lose it. Let the resume finish instead.
                if cp_newest and sampled_newest > cp_newest:
                    print(
                        f"  [anchor fix] skipped: pending resume cursor for an unfinished "
                        f"window (anchor {cp_newest[:19]}, tail {sampled_newest[:19]})."
                    )
            elif cp_newest and sampled_newest > cp_newest:
                print(
                    f"  [anchor fix] checkpoint newest {cp_newest[:19]} is older than local posts "
                    f"tail {sampled_newest[:19]}; fast-forward incremental anchor."
                )
                checkpoint.data["newest_post_created_at"] = sampled_newest
                checkpoint.save()

    since_time = None if args.full else checkpoint.get_last_newest_time()

    #
    done_cache_path = out / "comments_done_posts.txt"
    done_posts_count = 0
    if done_cache_path.exists():
        with open(done_cache_path, encoding="utf-8") as _f:
            done_posts_count = sum(1 for line in _f if line.strip())
    eligible_map = checkpoint.data.get("total_eligible_comment_posts_by_min", {})
    if not isinstance(eligible_map, dict):
        eligible_map = {}
    legacy_total_eligible = _to_int(checkpoint.data.get("total_eligible_comment_posts", 0), 0)
    if legacy_total_eligible > 0 and legacy_total_eligible >= max(1, done_posts_count // 2) and "30" not in eligible_map:
        eligible_map["30"] = legacy_total_eligible
        checkpoint.data["total_eligible_comment_posts_by_min"] = eligible_map
        checkpoint.save()
    total_eligible_cached = _to_int(eligible_map.get(str(args.min_comments), 0), 0)

    # PI
    print("fetching platform stats...")
    stats = fetch_platform_stats()
    submolts = fetch_submolts()
    platform_total    = int(stats.get("totalPosts",    0) or 0)
    platform_agents   = int(stats.get("totalAgents",   0) or 0)
    platform_comments = int(stats.get("totalComments", 0) or 0)
    platform_submolts = int(stats.get("totalSubmolts", 0) or 0)

    # ?+
    show_startup_summary(
        checkpoint, posts_store.count(), local_oldest, local_newest,
        done_posts_count, total_eligible_cached,
        platform_total, platform_comments, platform_agents,
        out, since_time, args
    )

    t_start = time.time()

    #
    if args.full and not args.no_resume:
        rc, _, _ = checkpoint.get_resume_cursor()
        if not rc:
            bc_cursor, bc_date = checkpoint.get_bottom_cursor()
            if bc_cursor and bc_date and local_oldest_dt and bc_date >= local_oldest_dt:
                print(
                    f"  [auto-skip] bottom cursor found ({bc_date[:10]}), skip already-covered zone and continue older gaps..."
                )
                checkpoint.data["_resume_cursor"] = bc_cursor
                checkpoint.data["_resume_since"] = None
                checkpoint.save()

    # 1.
    run_posts_path = out / "runs" / f"{run_ts}_posts.jsonl"
    new_post_count = 0
    stopped_early = False
    newest_post = None
    api_error = False
    reached_end = False
    keep_cursor = False
    if args.comments_only:
        print("[1/3] skip posts stage (--comments-only)...")
        run_posts_path.touch()
    else:
        print("[1/3] fetch posts...")
        with open(run_posts_path, "w", encoding="utf-8") as run_posts_f:
            new_post_count, stopped_early, newest_post, api_error, reached_end, keep_cursor = fetch_posts_incremental(
                checkpoint, posts_store, run_posts_f,
                since_time=since_time, max_posts=args.max_posts,
                platform_total=platform_total
            )

        if since_time and new_post_count == 0:
            print("  no new posts since last run.")
            if not api_error:
                checkpoint.clear_resume()
            if args.no_comments:
                if _FATAL_AUTH_EVENT.is_set():
                    print("  [!] FATAL: 401/403 auth failures during this run; exiting non-zero.")
                    sys.exit(2)
                return
            #
        if stopped_early:
            print(f"  reached older posts; incremental stop. new written: {new_post_count}  |  total: {posts_store.count()}")
        else:
            print(f"  new written: {new_post_count}  |  total: {posts_store.count()}")

    # 2.
    new_comment_count = 0
    if not args.no_comments and _FATAL_AUTH_EVENT.is_set():
        print("\n[2/3] skip comments: fatal auth failure in posts stage (saves the eligible scan + seed).")
    elif not args.no_comments:
        print(f"\n[2/3] fetch comments (comment_count >= {args.min_comments})...")

        # ?comments_all.jsonl ?
        done_cache = CommentsDoneCache(
            done_cache_path,
            comments_jsonl=comments_path if comments_path.exists() else None
        )
        resume_cache = CommentsResumeCache(comments_resume_path)
        sync_state = CommentsPostSyncState(comments_sync_state_path)

        run_comments_path = out / "runs" / f"{run_ts}_comments.jsonl"
        comments_id_cache_sqlite = out / "comments_id_cache.sqlite"
        with open(run_comments_path, "w", encoding="utf-8") as run_comments_f:
            new_comment_count, total_eligible = fetch_comments_for_posts(
                posts_store, comments_path, done_cache, run_comments_f,
                workers=args.workers, min_comments=args.min_comments,
                max_posts=args.max_comment_posts,
                resume_cache=resume_cache,
                sync_state=sync_state,
                comment_id_cache_mode=args.comment_id_cache,
                comment_id_cache_path=comments_id_cache_sqlite,
                queue_strategy=args.comment_queue_strategy,
                queue_small_max=args.queue_small_max,
                queue_medium_max=args.queue_medium_max,
            )
        print(f"  new comments written: {new_comment_count}")

        # total_eligible ?
        if total_eligible:
            emap = checkpoint.data.get("total_eligible_comment_posts_by_min", {})
            if not isinstance(emap, dict):
                emap = {}
            emap[str(args.min_comments)] = int(total_eligible)
            checkpoint.data["total_eligible_comment_posts_by_min"] = emap
            # Keep legacy key for backward compatibility (default threshold use-case).
            if args.min_comments == 30:
                checkpoint.data["total_eligible_comment_posts"] = int(total_eligible)
            checkpoint.save()
    else:
        print("\n[2/3] skip comments.")

    # 3. + Agent
    print("\n[3/3] save submolts list and extract agent profiles...")

    if submolts:
        with open(out / "submolts.json", "w", encoding="utf-8") as f:
            json.dump(submolts, f, ensure_ascii=False, indent=2)
        print(f"  submolts list: {len(submolts)} -> data/submolts.json")

    agents_store = JsonlStore(out / "agents_seen.jsonl")
    this_run_agents = {}
    if run_posts_path.exists():
        with open(run_posts_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        p = json.loads(line)
                        a = p.get("author")
                        if a and a.get("id"):
                            this_run_agents[a["id"]] = a
                    except Exception:
                        pass
    new_agent_count = agents_store.append_new(list(this_run_agents.values()))
    print(f"  agent profiles: +{new_agent_count} | total: {agents_store.count()}")

    # Agent snapshot (time series)
    if not args.no_agent_snapshot:
        take_agent_snapshot(out)

    #
    duration = time.time() - t_start
    checkpoint.update_after_run(
        new_posts=new_post_count,
        new_comments=new_comment_count,
        duration_s=duration,
        newest_post=newest_post,
        # Never touch posts resume state when the posts stage did not run
        # (--comments-only) or did not finish its window (api_error /
        # keep_cursor from truncation or the zero-streak guard).
        clear_cursor=(not api_error) and (not keep_cursor) and (not args.comments_only),
        reached_end=reached_end,
        advance_anchor=(stopped_early or reached_end) and not api_error,
    )
    if api_error:
        print("  [!] API error mid-run: cursor kept; next --full can resume.")
    if _FATAL_AUTH_EVENT.is_set():
        runs = checkpoint.data.get("runs") or []
        if runs:
            runs[-1]["auth_failed"] = True
            checkpoint.save()
        print("  [!] FATAL: 401/403 auth failures during this run; exiting non-zero so the scheduler can alert.")
        sys.exit(2)

    total_now = posts_store.count()
    pct_now = f"{total_now / platform_total * 100:.2f}%" if platform_total else "?"
    print(f"\n=== done! elapsed {duration:.0f}s ===")
    print(f"  cumulative posts: {total_now:,} / {platform_total:,} ({pct_now})")
    print(f"  runs: {len(checkpoint.data['runs'])}")
    print(f"  data dir: {out}/")

    # ?run --no-snapshot?
    if not args.no_snapshot:
        print("\n[4/4] fetch hot snapshot...")
        fetch_hot_snapshot(out)


if __name__ == "__main__":
    main()


