# -*- coding: utf-8 -*-
"""
Moltbook Data Scraper  鈥? v2 (incremental + checkpoint + progress bar)
Collects posts, comments, submolts, and agent profiles from moltbook.com API
for sociology research on AI agents.

Usage:
    python -X utf8 scraper.py                  # 澧為噺鎶撳彇锛堝彧鎶撲笂娆′互鏉ョ殑鏂板笘瀛愶級
    python -X utf8 scraper.py --full           # 鍏ㄩ噺鎶撳彇鎵€鏈夊巻鍙插笘瀛?
    python -X utf8 scraper.py --no-comments    # 璺宠繃璇勮锛堟洿蹇級
    python -X utf8 scraper.py --max-posts 500  # 闄愬埗甯栧瓙鏁伴噺
    python -X utf8 scraper.py --reset          # 娓呯┖ checkpoint锛岄噸鏂板叏閲忔姄
    python -X utf8 scraper.py --clean-runs     # 鍒犻櫎 data/runs/ 蹇収閲婃斁纾佺洏锛堝彲鍔犳暟瀛椾繚鐣欐渶杩慛涓級
    python -X utf8 scraper.py --check          # 鏁版嵁鑷

NOTE: 鍦?Windows 涓婂繀椤荤敤 `python -X utf8` 杩愯锛岄伩鍏?emoji 缂栫爜閿欒銆?

鏁版嵁淇濆瓨浣嶇疆:
    data/posts_all.jsonl            鈫?绱Н鐨勫叏閮ㄥ笘瀛愶紙姣忓ぉ杩藉姞锛屽幓閲嶏級
    data/comments_all.jsonl         鈫?绱Н鐨勫叏閮ㄨ瘎璁?
    data/comments_done_posts.txt    鈫?宸叉姄瀹岃瘎璁虹殑 post_id 鍒楄〃锛堝揩閫熸柇鐐圭画浼犵紦瀛橈級
    data/submolts.json              鈫?绀惧尯鍒楄〃锛堟瘡娆¤繍琛屾洿鏂帮級
    data/agents_seen.jsonl          鈫?瑙佽繃鐨?agent 妗ｆ锛堝幓閲嶏級
    data/checkpoint.json            鈫?鏂偣鐘舵€侊紙杩愯璁板綍 + cursor锛?
    data/runs/YYYYMMDD_*.jsonl      鈫?姣忔杩愯鐨勫閲忔暟鎹紙鍙敤 --clean-runs 娓呯悊锛?
"""

import requests
import json
import time
import argparse
import sys
import threading
import random
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
HEADERS = {"User-Agent": "MoltbookResearchScraper/2.0 (sociology research)"}

# 鈹€鈹€ API key锛堝彲閫夛級鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# 浠?.env 鏂囦欢鎴栫幆澧冨彉閲忚鍙栵紝鏈?key 鍒欑敤璁よ瘉璇锋眰
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

_load_api_key()  # 鍓綔鐢細濡傛灉鏈?key锛屾敞鍏?HEADERS["Authorization"]


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# RATE LIMITER  (绾跨▼瀹夊叏锛屽叏灞€鍏变韩)
# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

class RateLimiter:
    """
    Token bucket 閫熺巼闄愬埗鍣ㄣ€傚绾跨▼鍏辩敤鍚屼竴涓疄渚嬶紝淇濊瘉鍏ㄥ眬涓嶈秴杩?max_per_minute銆?
    姣忔 .wait() 璋冪敤浼氬湪蹇呰鏃堕樆濉烇紝纭繚璇锋眰闂撮殧鍚堣銆?
    """
    def __init__(self, max_per_minute=90):
        self._interval = 60.0 / max_per_minute  # 鏈€灏忚姹傞棿闅旓紙绉掞級
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
_rate_limiter          = RateLimiter(max_per_minute=_DEFAULT_READ_RPM)      # GET endpoints
_rate_limiter_comments = RateLimiter(max_per_minute=_DEFAULT_COMMENT_RPM)   # comments endpoints, slightly more conservative

# 鍏ㄥ眬鐔旀柇锛氫换浣曠嚎绋嬭Е鍙?429 鍚庤缃鏃堕棿鎴筹紝鍏朵粬绾跨▼绛夊埌璇ユ椂闂存墠缁х画
_global_cooldown_until = 0.0
_global_cooldown_lock  = threading.Lock()


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# HTTP
# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

def _countdown(seconds: float):
    """绛夊緟鏈熼棿姣?10 绉掓墦鍗颁竴娆″€掕鏃讹紝璁╃敤鎴风煡閬撶▼搴忚繕鍦ㄨ繍琛屻€?"""
    remaining = int(seconds)
    while remaining > 0:
        tqdm.write(f"  [绛夊緟] 杩樺墿 {remaining}s...")
        chunk = min(10, remaining)
        time.sleep(chunk)
        remaining -= chunk


def _to_int(value, default=0):
    try:
        return int(float(value))
    except Exception:
        return default


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
    global _global_cooldown_until
    url = f"{BASE_URL}{endpoint}"
    limiter = _rate_limiter_comments if endpoint.endswith("/comments") else _rate_limiter
    for attempt in range(retries):
        # 鍏ㄥ眬鐔旀柇锛氳嫢鍏朵粬绾跨▼瑙﹀彂浜?429锛岀瓑鍒板喎鍗存湡缁撴潫
        now = time.time()
        with _global_cooldown_lock:
            cooldown_remaining = _global_cooldown_until - now
        if cooldown_remaining > 0:
            if cooldown_remaining >= 1:
                tqdm.write(f"  [鍏ㄥ眬鍐峰嵈] 绛?{cooldown_remaining:.0f}s...")
            time.sleep(cooldown_remaining)

        limiter.wait()  # 绾跨▼瀹夊叏鑺傛祦锛岃瘎璁烘帴鍙ｇ敤鐙珛闄愰€熷櫒
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=15)

            # 璇绘湇鍔″櫒杩斿洖鐨勭湡瀹為檺閫熷ご锛屽揩鍒颁笂闄愭椂涓诲姩绛夊埌绐楀彛閲嶇疆
            remaining = _to_int(r.headers.get("X-RateLimit-Remaining", 999), 999)
            reset_ts  = _to_int(r.headers.get("X-RateLimit-Reset", 0), 0)
            rl_limit  = _to_int(r.headers.get("X-RateLimit-Limit", 0), 0)
            window_s  = max(1, reset_ts - int(time.time()))
            if remaining <= 30 and r.status_code != 429:
                tqdm.write(f"  [rl] remaining={remaining}/{rl_limit}  reset_in={window_s}s")
            if remaining <= 15 and r.status_code != 429:
                wait = max(65, reset_ts - time.time() + 2)
                tqdm.write(f"  [rate limit] 鍓╀綑 {remaining} 娆★紝绛?{wait:.0f}s 鍒扮獥鍙ｉ噸缃?..")
                with _global_cooldown_lock:
                    _global_cooldown_until = max(_global_cooldown_until, time.time() + wait)
                time.sleep(wait)

            if r.status_code == 429:
                # 浼樺厛浣跨敤鏈嶅姟绔彁渚涚殑 retry-after / reset 淇℃伅锛涢兘娌℃湁鏃跺啀鎸囨暟閫€閬?+ jitter
                body = None
                try:
                    body = r.json()
                except Exception:
                    body = None

                retry_after = _retry_after_seconds(r, body)
                wait_to_reset = max(0, _to_int(r.headers.get("X-RateLimit-Reset", 0), 0) - int(time.time()))
                if retry_after > 0 or wait_to_reset > 0:
                    wait = max(retry_after, wait_to_reset) + random.uniform(1, 6)
                else:
                    base = min(300, 20 * (2 ** attempt))
                    wait = base + random.uniform(0, max(5, base * 0.35))
                with _global_cooldown_lock:
                    _global_cooldown_until = max(_global_cooldown_until, time.time() + wait)
                if skip_on_ratelimit:
                    tqdm.write(f"  [rate limit] {endpoint} limited, skip (cooldown {wait:.0f}s)")
                    return None
                tqdm.write(f"  [rate limit 429] attempt={attempt+1}锛岀瓑 {wait:.0f}s 鍚庨噸璇?..")
                _countdown(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            tqdm.write(f"  HTTP {r.status_code} on {url}: {e}")
            return None
        except Exception as e:
            tqdm.write(f"  Error ({attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(5)
    return None


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# CHECKPOINT  (鏂偣缁紶鐘舵€佹枃浠?
# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

class Checkpoint:
    """
    checkpoint.json 缁撴瀯:
    {
      "newest_post_created_at": "2026-02-26T01:29:21Z",
      "newest_post_id": "uuid",
      "total_posts": 1234,
      "total_comments": 5678,
      "runs": [...],
      "_resume_cursor": "eyJ...",
      "_resume_since": "2026-02-25T..."
    }
    """

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
                         clear_cursor=True, reached_end=False):
        if newest_post:
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
        if newest_post and "_resume_newest_post" not in self.data:
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


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# DATA FILES  (绱Н杩藉姞)
# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

class JsonlStore:
    """杩藉姞鍐欏叆 .jsonl 鏂囦欢锛岀淮鎶や竴涓?ID set 鐢ㄤ簬鍘婚噸"""

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
        """杩藉姞涓嶉噸澶嶇殑璁板綍锛岃繑鍥炲疄闄呭啓鍏ユ暟閲?"""
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
    """
    杞婚噺缂撳瓨锛氳褰曞凡鎴愬姛鎶撳畬璇勮鐨?post_id锛堟瘡琛屼竴涓?UUID锛夈€?

    - 鍚姩鍙渶璇诲嚑鍗?KB锛屽交搴曢伩鍏嶆瘡娆℃壂鎻?1.3 GB comments_all.jsonl銆?
    - 棣栨杩愯鏃惰嚜鍔ㄤ粠 comments_all.jsonl 杩佺Щ锛堜竴娆℃€ф壂鎻忥紝涔嬪悗涓嶅啀闇€瑕侊級銆?
    - mark_done() 鍦ㄨ瘎璁哄啓鐩樻垚鍔熷悗璋冪敤锛屼繚璇佸師瀛愭€э紙鍐欏け璐ヤ笉鏍囪锛夈€?
    """

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
        """涓€娆℃€т粠 comments_all.jsonl 鎵弿 post_id 瀛楁锛屽缓绔嬬紦瀛橈紙棣栨杩愯鏃舵墽琛岋級銆?"""
        size_mb = jsonl.stat().st_size / 1024 / 1024
        print(f"  [鍒濆鍖朷 浠?comments_all.jsonl ({size_mb:.0f} MB) 寤虹珛杩涘害缂撳瓨锛堜粎姝や竴娆★級...",
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
        print(f" {len(self.done_ids):,} posts ✓")

    def mark_done(self, post_id: str):
        """鏍囪鏌愬笘璇勮宸插畬鎴愶紙杩藉姞鍐欏叆缂撳瓨鏂囦欢锛?"""
        if post_id not in self.done_ids:
            self.done_ids.add(post_id)
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(post_id + "\n")

    def is_done(self, post_id: str) -> bool:
        return post_id in self.done_ids

    def count(self) -> int:
        return len(self.done_ids)


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# FETCHERS
# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

class CommentsResumeCache:
    """
    璇勮鍒嗛〉鏂偣缂撳瓨锛坅ppend-only 鏃ュ織锛夛細
    - key: post_id
    - value: 涓嬩竴娆¤姹傝浣跨敤鐨?cursor锛圢one 琛ㄧず娓呴櫎鏂偣锛?
    """

    def __init__(self, path: Path):
        self.path = path
        self.cursors: dict = {}
        if self.path.exists():
            self._load()

    def _load(self):
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
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

    def count(self) -> int:
        return len(self.cursors)


def fetch_posts_incremental(checkpoint: Checkpoint, posts_store: "JsonlStore",
                            run_file, since_time=None, max_posts=None,
                            platform_total=0):
    """
    鎶撳笘瀛愶紝姣忛〉锛?00鏉★級绔嬪埢鍐欑洏锛屼笉鍦ㄥ唴瀛橀噷鍫嗙Н銆?
    宕╀簡閲嶅惎浼氫粠涓婃鐨?cursor 缁х画锛屽凡鍐欏叆鐨勬暟鎹笉浼氫涪澶变篃涓嶄細閲嶅銆?

    杩斿洖: (鏈鏂板啓鍏ユ潯鏁? 鏄惁鍥犵鍒版棫甯栧仠姝? 鏈€鏂板笘瀛?dict, api_error, reached_end)
    """
    total_new = 0
    stopped_early = False
    api_error = False
    reached_end = False
    newest_post = None
    pages_seen = 0
    api_rows_seen = 0
    zero_write_streak = 0
    initial_local = posts_store.count()
    pbar_total = max(0, platform_total - initial_local) if platform_total else None
    seen_next_cursors = set()
    last_page_sig = None
    same_page_sig_streak = 0

    # 鏂偣缁紶锛氬鏋滀笂娆′腑閫斿穿浜嗭紝浠庝繚瀛樼殑 cursor 缁х画
    resume_cursor, resume_since, resume_newest = checkpoint.get_resume_cursor()
    if resume_cursor and resume_since == since_time:
        tqdm.write("  [鏂偣缁紶] 浠庝笂娆′腑鏂綅缃户缁?..")
        cursor = resume_cursor
        if resume_newest:
            newest_post = resume_newest
    else:
        cursor = None
        if resume_cursor:
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
                api_error = True  # 淇濈暀 checkpoint cursor锛屼笅娆″彲鏂偣缁紶
                retry_delay = 120
                retry_n = 0
                while True:
                    retry_n += 1
                    tqdm.write(
                        f"  [!] 鏈嶅姟鍣ㄩ敊璇紙绗?{retry_n} 娆￠噸璇曪級锛?"
                        f"{retry_delay // 60} 鍒嗛挓鍚庤嚜鍔ㄩ噸璇?.. 鎸?Ctrl+C 鍙仠姝?"
                    )
                    time.sleep(retry_delay)
                    data = api_get("/posts", params)
                    if data and data.get("success"):
                        api_error = False
                        tqdm.write("  [+] retry success, continue.")
                        break
                    retry_delay = min(retry_delay * 2, 600)

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

            if newest_post is None and batch:
                newest_post = batch[0]

            if since_time:
                new_batch = [p for p in batch if p.get("created_at", "") > since_time]
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
                            tqdm.write("  [info] 杩炵画 25 椤垫棤鏂板锛屽綋鍓嶅湪宸叉姄閲嶅彔鍖猴紝缁х画鍚戞洿鏃╁巻鍙叉帹杩?..")
                else:
                    zero_write_streak = 0
                pbar.set_postfix_str(
                    f"鏈〉+{written}/{len(batch)} | 绱+{total_new} | 杩炵画0椤?{zero_write_streak} | 褰撳墠:{cur_date}"
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

    return total_new, stopped_early, newest_post, api_error, reached_end


def _fetch_one_post_comments(post, start_cursor=None, page_progress_cb=None):
    """
    Fetch comments for one post using cursor pagination.
    Returns: (post, flat_comments, success, resume_cursor)
    """
    all_flat = []
    cursor = start_cursor
    page = 0
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
            if data is None:
                # Keep partial pages and current cursor for next run.
                return post, all_flat, False, cursor
            if attempt < 2:
                tqdm.write(f"  [!] {post['id'][:8]}... p{page} server error, retry ({attempt+1}/2)...")
                time.sleep(10)
        else:
            return post, all_flat, False, cursor

        flat = _flatten_comments(data.get("comments", []))
        for c in flat:
            c["post_id"] = post["id"]
            c["post_title"] = post.get("title", "")
            c["submolt"] = post.get("submolt", {}).get("name", "")
        all_flat.extend(flat)
        page += 1
        if page_progress_cb:
            try:
                page_progress_cb(post["id"], page, len(flat), len(all_flat))
            except Exception:
                pass

        now_hb = time.monotonic()
        if page % 20 == 0 or (now_hb - hb_last >= 20):
            tqdm.write(
                f"  [heartbeat comments] {post['id'][:8]}... pages={page}, rows~{len(all_flat)}"
            )
            hb_last = now_hb

        if not data.get("has_more") or not data.get("next_cursor"):
            break
        cursor = data["next_cursor"]

    return post, all_flat, True, None


def _count_comments_for_target_posts(comments_path: Path, target_ids: set):
    """
    Count existing comment rows for a target post_id set.
    Also prints heartbeat logs while scanning large comments files.
    """
    counts = Counter()
    if not target_ids or not comments_path.exists():
        return counts

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
                pid = json.loads(line).get("post_id")
                if pid in target_ids:
                    counts[pid] += 1
                    matched += 1
            except Exception:
                pass

            now = time.monotonic()
            if now - hb_last >= 15:
                speed = scanned / max(1e-6, (now - t0))
                tqdm.write(
                    f"  [heartbeat count] scanned_comments={scanned:,}, matched={matched:,}, speed~{speed:,.0f} rows/s"
                )
                hb_last = now

    tqdm.write(f"  [count done] scanned_comments={scanned:,}, matched={matched:,}, target_posts={len(target_ids):,}")
    return counts


def fetch_comments_for_posts(posts_store: "JsonlStore", comments_path: Path,
                             done_cache: "CommentsDoneCache",
                             run_file, workers=5, min_comments=1, max_posts=0,
                             resume_cache: "CommentsResumeCache" = None):
    """
    Concurrently fetch comments with gap-based backfill.
    Strategy:
    1) Build eligible post set (comment_count >= min_comments)
    2) Count local comment rows for those posts
    3) Backfill only posts where local_count != comment_count, plus resume-cursor posts
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

    # 2) count local comments for eligible posts
    expected_by_id = {p["id"]: int(p.get("comment_count", 0) or 0) for p in eligible_posts}
    eligible_ids = set(expected_by_id.keys())
    local_counts = _count_comments_for_target_posts(comments_path, eligible_ids)

    # 3) build mismatch queue (and preserve resume-cursor posts)
    resume_ids = set(resume_cache.cursors.keys()) if resume_cache else set()
    posts_needing_comments = []
    matched_exact = 0
    local_gt_expected = 0
    total_gap = 0
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
        posts_needing_comments.append(post_rec)

    print(
        f"\r  eligible={total_eligible:,} | exact={matched_exact:,} | local>expected={local_gt_expected:,} "
        f"| pending={len(posts_needing_comments):,} | total_gap~{total_gap:,}"
    )

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
        return 0, total_eligible

    # prioritize resume posts first, then bigger gap, then bigger threads
    posts_needing_comments.sort(
        key=lambda p: (0 if p.get("is_resume") else 1, -max(0, p.get("gap", 0)), -p.get("comment_count", 0))
    )
    if max_posts and len(posts_needing_comments) > max_posts:
        posts_needing_comments = posts_needing_comments[:max_posts]
        print(f"  [limit] this run will fetch top {max_posts:,} posts")

    rate = 60 / _rate_limiter_comments._interval
    eta_min = len(posts_needing_comments) / rate
    print(f"  ETA {eta_min/60:.1f} hours ({rate:.0f} req/min, {workers} workers)")

    total_new = 0
    posts_done = 0
    write_lock = threading.Lock()
    per_post_written_est = Counter(local_counts)
    per_post_remaining = {
        p["id"]: max(0, int(p.get("comment_count", 0)) - int(p.get("local_count", 0)))
        for p in posts_needing_comments
    }
    gap_remaining = sum(per_post_remaining.values())

    pbar = tqdm(total=len(posts_needing_comments), desc="fetch comments",
                unit="post", dynamic_ncols=True, position=0)
    pages_bar = tqdm(total=None, desc="comment pages", unit="page",
                     dynamic_ncols=True, position=1, leave=False)
    pages_bar.set_postfix_str("rows~0")

    pages_seen = 0
    rows_seen = 0
    progress_lock = threading.Lock()

    def on_page_progress(post_id, page_no, page_rows, post_rows_total):
        nonlocal pages_seen, rows_seen
        with progress_lock:
            pages_seen += 1
            rows_seen += max(0, int(page_rows))
            pages_bar.update(1)
            if workers == 1:
                pages_bar.set_description_str(f"reading {post_id[:8]}...")
                pages_bar.set_postfix_str(
                    f"post_p={page_no} | post_rows~{post_rows_total:,} | total_rows~{rows_seen:,}"
                )
            else:
                pages_bar.set_postfix_str(
                    f"last={post_id[:8]} p={page_no} | total_rows~{rows_seen:,}"
                )

    try:
        chunk_size = workers * 20
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for i in range(0, len(posts_needing_comments), chunk_size):
                chunk = posts_needing_comments[i:i + chunk_size]
                futures = {}
                for post in chunk:
                    start_cursor = resume_cache.get(post["id"]) if resume_cache else None
                    futures[executor.submit(_fetch_one_post_comments, post, start_cursor, on_page_progress)] = post

                for future in as_completed(futures):
                    post, flat, success, resume_cursor = future.result()
                    pid = post["id"]
                    written_now = len(flat)

                    if flat:
                        with write_lock:
                            # Partial-save mode may create rare duplicates across retries.
                            with open(comments_path, "a", encoding="utf-8") as cf:
                                for c in flat:
                                    cf.write(json.dumps(c, ensure_ascii=False) + "\n")
                            for c in flat:
                                run_file.write(json.dumps(c, ensure_ascii=False) + "\n")
                            run_file.flush()
                            total_new += written_now

                    if written_now:
                        per_post_written_est[pid] += written_now
                        consumed = min(per_post_remaining.get(pid, 0), written_now)
                        per_post_remaining[pid] = max(0, per_post_remaining.get(pid, 0) - consumed)
                        gap_remaining = max(0, gap_remaining - consumed)

                    if success:
                        if resume_cache:
                            resume_cache.set(pid, None)
                        # Backward-compatible cache update: only mark "done" when local rows reach expected.
                        if per_post_written_est.get(pid, 0) >= expected_by_id.get(pid, 0):
                            done_cache.mark_done(pid)
                    elif resume_cache and resume_cursor:
                        resume_cache.set(pid, resume_cursor)
                        exp = expected_by_id.get(pid, 0)
                        got = per_post_written_est.get(pid, 0)
                        if exp > 0:
                            rem = max(0, exp - got)
                            tqdm.write(f"  [partial] {pid[:8]}... saved~{got}/{exp}, remaining~{rem}, cursor saved")
                        else:
                            tqdm.write(f"  [partial] {pid[:8]}... saved~{got}, cursor saved")

                    posts_done += 1
                    pbar.update(1)
                    resume_n = resume_cache.count() if resume_cache else 0
                    pbar.set_postfix_str(
                        f"new rows {total_new} | gap~{gap_remaining} | done {posts_done}/{len(posts_needing_comments)} | resume {resume_n}"
                    )
    finally:
        pbar.close()
        pages_bar.close()

    resume_left = resume_cache.count() if resume_cache else 0
    print(f"  comment pages fetched: {pages_seen:,} | observed rows: {rows_seen:,}")
    print(f"  comments newly written: {total_new:,} | processed posts: {posts_done:,} | resume left: {resume_left:,}")
    return total_new, total_eligible


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
    """浠?/stats 鑾峰彇骞冲彴鐪熷疄鎬婚噺銆傞檺閫熸椂璺宠繃涓嶉樆濉炪€?"""
    data = api_get("/stats", skip_on_ratelimit=True)
    if not data:
        return {}
    return data


def _snap_record(p: dict, sampled_at: str, sort_source: str) -> dict:
    """浠庡笘瀛愬璞℃彁鍙栧揩鐓у瓧娈点€?"""
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


# 姣忔蹇収鎶撳彇鐨勬帓搴?脳 椤垫暟閰嶇疆锛?
#   hot    3椤?=  ~300 鏉? 褰撳墠鐑棬锛堣繎鏈熺梾姣掑紡浼犳挱锛?
#   rising 2椤?=  ~200 鏉? 涓婂崌涓紙鏂板叴甯栧瓙锛?
#   top   20椤?= ~2000 鏉? 鍘嗗彶楂樺垎锛堢ǔ瀹氳拷韪叏骞冲彴鏈€浣冲笘锛?
_SNAPSHOT_SORTS = [("hot", 3), ("rising", 2), ("top", 20)]


def fetch_hot_snapshot(out: Path, sorts_config=None):
    """
    鎶?hot/rising/top 鎺掕姒滃綋鍓嶇姸鎬侊紝杩藉姞鍐欏叆 data/post_snapshots.jsonl銆?

    榛樿鎶?~2500 鏉★紙hot脳3椤?+ rising脳2椤?+ top脳20椤碉級锛岀害 25 涓?API 璇锋眰銆?
    澶氭杩愯鍚庡彲鎸?post_id 鑱氬悎鍑烘椂搴忔洸绾匡紝鐢ㄤ簬鐮旂┒甯栧瓙璧扮孩杩囩▼銆?
    """
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

    print(f"  [蹇収] {sampled_at}  淇濆瓨 {total} 鏉?鈫?{snapshots_path.name}")
    return total


def seed_snapshots(out: Path, min_score: int = 1):
    """
    涓€娆℃€у皢 posts_all.jsonl 閲?score >= min_score 鐨勫笘瀛愭壒閲忓啓鍏?
    post_snapshots.jsonl 浣滀负 T0 鍩虹嚎锛宻ort_source="seed"銆?

    涔嬪悗姣忔 fetch_hot_snapshot 杩藉姞鐨勯兘鏄?T1/T2/T3...锛?
    浠庤€屽彲浠ュ浠绘剰甯栧瓙鍋?score / comment_count 鐨勬椂搴忓垎鏋愩€?

    宸叉湁鏉＄洰鐨勫笘瀛愪細琚烦杩囷紙鎸?post_id 鍘婚噸锛夛紝鎵€浠ュ娆¤繍琛屽畨鍏ㄣ€?
    """
    posts_path = out / "posts_all.jsonl"
    snapshots_path = out / "post_snapshots.jsonl"
    if not posts_path.exists():
        print("  [!] posts_all.jsonl missing")
        return

    # 璇诲彇宸叉湁蹇収閲岀殑 post_id锛岃烦杩囧凡瀛樺湪鐨?
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
        print(f"  宸叉湁蹇収 {len(existing_ids):,} 鏉★紝璺宠繃閲嶅")

    written = skipped_score = 0
    scanned = 0
    print(f"  鎵弿 posts_all.jsonl锛坰core >= {min_score} 鐨勫笘瀛愪綔涓?T0锛?..")
    with open(posts_path, encoding="utf-8") as fin, \
         open(snapshots_path, "a", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            scanned += 1
            if scanned % 500_000 == 0:
                print(f"    宸叉壂鎻?{scanned:,} 琛岋紝宸插啓鍏?{written:,} 鏉?..")
            try:
                p = json.loads(line)
                if p.get("score", 0) < min_score:
                    skipped_score += 1
                    continue
                pid = p.get("id")
                if not pid or pid in existing_ids:
                    continue
                # T0 鏃堕棿锛氫紭鍏堢敤 _scraped_at锛屽叾娆＄敤 created_at
                t0 = p.get("_scraped_at") or p.get("created_at", "")
                snap = _snap_record(p, t0, "seed")
                fout.write(json.dumps(snap, ensure_ascii=False) + "\n")
                existing_ids.add(pid)
                written += 1
            except Exception:
                pass

    print(f"  [鈭歖 T0 seed 瀹屾垚锛氬啓鍏?{written:,} 鏉★紙score=0 璺宠繃 {skipped_score:,} 鏉★紝鍏辨壂鎻?{scanned:,} 琛岋級")
    print(f"      鈫?{snapshots_path}")


def extract_agents(posts):
    agents = {}
    for p in posts:
        a = p.get("author")
        if a and a.get("id"):
            agents[a["id"]] = a
    return list(agents.values())


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# STARTUP SUMMARY
# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

def show_startup_summary(checkpoint, local_posts, local_oldest, local_newest,
                         done_posts, total_eligible,
                         platform_total, platform_comments, platform_agents,
                         out, since_time, args):
    """
    姣忔杩愯寮€濮嬪墠鎵撳嵃锛氭湰鍦版暟鎹姸鎬?/ 骞冲彴鏁版嵁宸紓 / 鏈璁″垝銆?
    鍏ㄩ儴浣跨敤缂撳瓨鍊硷紝涓嶆壂鎻忓ぇ鏂囦欢锛屽嚑涔庣灛鏃跺畬鎴愩€?
    """
    SEP = "鈹€" * 58
    auth_str = "AUTH" if "Authorization" in HEADERS else "ANON"
    print(f"\n{SEP}")
    print(f"  Moltbook Scraper v2  路  {datetime.now().strftime('%Y-%m-%d %H:%M')}  路  {auth_str}")
    print(SEP)

    # 鈹€鈹€ 鏈湴鏁版嵁 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    print(f"\n  鏈湴鏁版嵁")
    date_range = f"{local_oldest} 鈫?{local_newest}" if local_oldest else "锛堟殏鏃犳暟鎹級"
    print(f"    甯栧瓙   {local_posts:>12,} 鏉? |  {date_range}")

    comments_jsonl = out / "comments_all.jsonl"
    if comments_jsonl.exists():
        size_mb = comments_jsonl.stat().st_size / 1024 / 1024
        if total_eligible:
            pct_done = done_posts / total_eligible * 100 if total_eligible else 0
            coverage = f"宸插畬鎴?{done_posts:,} / {total_eligible:,} 甯? |  瑕嗙洊鐜?{pct_done:.1f}%"
        elif done_posts:
            coverage = f"宸插畬鎴?{done_posts:,} 甯栵紙鎬婚噺寰呴娆℃壂鎻忕‘璁わ級"
        else:
            coverage = "缂撳瓨寰呭缓绔嬶紙棣栨杩愯璇勮鎶撳彇鏃惰嚜鍔ㄥ垱寤猴級"
        print(f"    璇勮   {size_mb:>11.0f} MB  |  {coverage}")
    else:
        print(f"    璇勮   锛堝皻鏃犺瘎璁烘暟鎹級")

    # 鈹€鈹€ 骞冲彴鏁版嵁 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    print(f"\n  骞冲彴鏁版嵁 (API)")
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

    # 鈹€鈹€ 鏈璁″垝 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
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
        print(f"  - comments: comment_count >= {args.min_comments}{limit_str}, workers={args.workers}")
    print(f"  - rate: posts~{args.read_rpm}/min, comments~{args.comment_rpm}/min")
    if args.no_resume:
        print("  - resume: ignore/clear (--no-resume)")

    print(f"\n  鏁版嵁鐩綍: {out}/")
    print(f"{SEP}\n")


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# CLEAN RUNS
# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

def clean_runs(out: Path, keep_last: int = 0):
    """鍒犻櫎 data/runs/ 杩愯蹇収鏂囦欢锛岄噴鏀剧鐩樼┖闂淬€?"""
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
    print(f"  runs/ 鐩綍: {len(files)} 涓枃浠讹紝鍏?{total_size_mb:.1f} MB")
    if not to_delete:
        print(f"  keep latest {kept}, nothing to delete.")
        return

    print(f"  鍒犻櫎 {len(to_delete)} 涓枃浠讹紝閲婃斁绾?{del_size_mb:.1f} MB锛堜繚鐣欐渶杩?{kept} 涓級...")
    for f in to_delete:
        f.unlink()
    print(f"  done. kept {kept} run snapshot files.")


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# DATA CHECK / DEDUP / REFETCH
# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

def dedup_comments(out: Path):
    """娴佸紡鍘婚噸 comments_all.jsonl锛堟寜 id 瀛楁锛夛紝鍘熷湴鏇挎崲銆?"""
    comments_path = out / "comments_all.jsonl"
    if not comments_path.exists():
        print("  [!] comments_all.jsonl missing")
        return
    tmp_path = comments_path.with_suffix(".dedup_tmp")
    seen = set()
    total = kept = 0
    print("鍘婚噸 comments_all.jsonl锛堟寜 id 瀛楁锛?..")
    with open(comments_path, encoding="utf-8") as fin, \
         open(tmp_path, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                obj = json.loads(line)
                cid = obj.get("id")
                if cid and cid not in seen:
                    seen.add(cid)
                    fout.write(line + "\n")
                    kept += 1
            except Exception:
                fout.write(line + "\n")  # 淇濈暀鍧忚锛?-check 浼氭姤鍛?
                kept += 1
    removed = total - kept
    print(f"  鍘熷琛屾暟: {total:,}  鈫? 鍘婚噸鍚? {kept:,}  锛堝垹闄?{removed:,} 鏉￠噸澶嶏級")
    tmp_path.replace(comments_path)
    print("  [鈭歖 瀹屾垚锛屽凡鍘熷湴鏇挎崲 comments_all.jsonl")


def refetch_comments(out: Path, min_count: int):
    """
    浠?done_cache 绉婚櫎 comment_count >= min_count 鐨勫笘瀛愶紝浣垮叾涓嬫閲嶆柊鍏ㄩ噺鎶撳彇銆?
    鐢ㄤ簬琛ユ晳鍘嗗彶涓婂彧鎶撲簡绗竴椤电殑甯栧瓙銆?
    閲嶆姄鍚庝細鏈夐噸澶嶇涓€椤电殑璇勮锛岄渶瑕佽窇 --dedup-comments 娓呯悊銆?
    """
    done_cache_path = out / "comments_done_posts.txt"
    posts_path = out / "posts_all.jsonl"
    if not done_cache_path.exists():
        print("  [!] comments_done_posts.txt 涓嶅瓨鍦紝鏃犻渶澶勭悊")
        return
    if not posts_path.exists():
        print("  [!] posts_all.jsonl missing")
        return
    done_ids = set()
    with open(done_cache_path, encoding="utf-8") as f:
        for line in f:
            pid = line.strip()
            if pid:
                done_ids.add(pid)
    to_remove = set()
    scanned = 0
    with open(posts_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            scanned += 1
            try:
                p = json.loads(line)
                if p.get("comment_count", 0) >= min_count and p["id"] in done_ids:
                    to_remove.add(p["id"])
            except Exception:
                pass
    if not to_remove:
        print(f"  [鈭歖 娌℃湁 comment_count >= {min_count} 鐨勫凡鎶撳笘瀛愶紝鏃犻渶閲嶇疆")
        return
    new_done = done_ids - to_remove
    with open(done_cache_path, "w", encoding="utf-8") as f:
        for pid in new_done:
            f.write(pid + "\n")
    print(f"  removed {len(to_remove):,} posts from done_cache (comment_count >= {min_count})")
    print(f"  remaining done posts: {len(new_done):,}")
    print("  next: python -X utf8 scraper.py --workers 5")
    print("  then: python -X utf8 scraper.py --dedup-comments")


def check_data(out: Path):
    """鑷鏁版嵁鐩綍锛氱粺璁℃枃浠跺畬鏁存€с€佸幓閲嶆儏鍐点€佹棩鏈熻寖鍥淬€乧heckpoint 鐘舵€併€?"""
    print(f"\n=== 鏁版嵁鑷  ({out}/) ===\n")

    files = [
        ("posts_all.jsonl",    "甯栧瓙"),
        ("comments_all.jsonl", "璇勮"),
        ("agents_seen.jsonl",  "Agent妗ｆ"),
    ]

    for filename, label in files:
        path = out / filename
        if not path.exists():
            print(f"  [{label}] {filename}: 鏂囦欢涓嶅瓨鍦╘n")
            continue

        size_mb = path.stat().st_size / 1024 / 1024
        total_lines = bad_lines = 0
        ids = set()
        dates = []

        # 鎵弿 comments_all.jsonl 鏃堕『渚挎敹闆?post_id锛堢敤浜庡缓绔嬭繘搴︾紦瀛橈級
        collect_post_ids = (filename == "comments_all.jsonl")
        seen_post_ids_for_cache = set() if collect_post_ids else None

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
                except Exception:
                    bad_lines += 1

        dupes = total_lines - len(ids)
        print(f"  [{label}] {filename}")
        print(f"    澶у皬: {size_mb:.1f} MB  |  鎬昏: {total_lines:,}  |  鍞竴ID: {len(ids):,}  |  閲嶅: {dupes}  |  鎹熷潖琛? {bad_lines}")
        if dates:
            d_min, d_max = min(dates), max(dates)
            print(f"    鏃ユ湡鑼冨洿: {d_min[:19]} 鈫?{d_max[:19]}")
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
                    print(f"    [!] 鏂。 {len(missing)} 澶╋紙0 鏉″笘瀛愶級: {missing}")
                else:
                    print(f"    [ok] date contiguous, no missing day ({total_days} days, avg {int(avg_per_day):,}/day)")
                if sparse:
                    print(f"    [?] 绋€鐤忓ぉ锛? 鍧囧€?5%锛? {[(d, f'{n:,}') for d, n in sparse]}")
        if bad_lines:
            print(f"    [!] {bad_lines} JSON parse errors (possibly truncated lines)")
        if dupes > 0:
            print(f"    [!] found {dupes} duplicate records")

        # comments 鎵畬鍚庨『鎵嬪缓杩涘害缂撳瓨
        if collect_post_ids and seen_post_ids_for_cache:
            done_cache_path = out / "comments_done_posts.txt"
            if not done_cache_path.exists():
                with open(done_cache_path, "w", encoding="utf-8") as f:
                    for pid in seen_post_ids_for_cache:
                        f.write(pid + "\n")
                print(f"    [+] 宸插缓绔嬭瘎璁鸿繘搴︾紦瀛橈紙{len(seen_post_ids_for_cache):,} 甯栵級")
        print()

    # 璇勮杩涘害缂撳瓨
    done_cache_path = out / "comments_done_posts.txt"
    if done_cache_path.exists():
        done_count = sum(1 for line in open(done_cache_path, encoding="utf-8") if line.strip())
        print(f"  [璇勮杩涘害缂撳瓨] comments_done_posts.txt: {done_count:,} 甯栧凡瀹屾垚")

        # 缁熻鏈夊灏戝笘瀛愮鍚?>=5 鏉′欢浣嗚繕娌℃姄
        posts_path = out / "posts_all.jsonl"
        if posts_path.exists():
            done_ids = {line.strip() for line in open(done_cache_path, encoding="utf-8") if line.strip()}
            total_eligible = pending = 0
            with open(posts_path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        p = json.loads(line)
                        if p.get("comment_count", 0) >= 5:
                            total_eligible += 1
                            if p["id"] not in done_ids:
                                pending += 1
                    except Exception:
                        pass
            pct = f"{done_count / total_eligible * 100:.1f}%" if total_eligible else "?"
            print(f"    绗﹀悎鏉′欢甯栵紙鈮?鏉¤瘎璁猴級: {total_eligible:,}  |  宸叉姄: {done_count:,}  |  寰呮姄: {pending:,}  |  瑕嗙洊鐜? {pct}")
    else:
        print("  [comments cache] comments_done_posts.txt not found yet")
    print()

    # Checkpoint
    cp_path = out / "checkpoint.json"
    if cp_path.exists():
        with open(cp_path, encoding="utf-8") as f:
            cp = json.load(f)
        print(f"  [Checkpoint]")
        print(f"    鏈€鏂板笘瀛愭椂闂? {cp.get('newest_post_created_at') or '鏃狅紙鏈畬鎴愬叏閲忥級'}")
        print(f"    杩愯娆℃暟: {len(cp.get('runs', []))}  |  宸茶褰曞笘瀛? {cp.get('total_posts', 0):,}  |  宸茶褰曡瘎璁? {cp.get('total_comments', 0):,}")
        if cp.get("_resume_cursor"):
            print(f"    [!] 鏈夋湭瀹屾垚鐨勪腑鏂换鍔★紙_resume_cursor 瀛樺湪锛夆啋 鐢?--full 缁х画")
        print()

    # Runs 鐩綍
    runs_dir = out / "runs"
    if runs_dir.exists():
        run_files = sorted(runs_dir.glob("*.jsonl"))
        total_size = sum(f.stat().st_size for f in run_files) / 1024 / 1024
        print(f"  [杩愯蹇収] {len(run_files)} 涓枃浠? |  鎬诲ぇ灏? {total_size:.1f} MB")
        if run_files:
            print(f"    鏈€鏂? {run_files[-1].name}")
        if total_size > 500:
            print("    [tip] runs snapshots are large; use --clean-runs")
    print()

    # 骞冲彴瀹炴椂鏁版嵁瀵规瘮
    print("  [骞冲彴瀵规瘮] 姝ｅ湪璇锋眰骞冲彴瀹炴椂鏁版嵁...")
    stats = fetch_platform_stats()
    if not stats:
        print("    [!] 鏃犳硶鑾峰彇骞冲彴鏁版嵁锛圓PI 瓒呮椂鎴栭檺閫燂級锛岃烦杩囧姣擻n")
        return

    platform_posts    = int(stats.get("totalPosts",    0) or 0)
    platform_comments = int(stats.get("totalComments", 0) or 0)
    platform_agents   = int(stats.get("totalAgents",   0) or 0)

    # 璇绘湰鍦拌鏁帮紙澶嶇敤宸叉壂鎻忚繃鐨?ids锛?
    def count_jsonl(path):
        if not path.exists():
            return 0
        n = 0
        with open(path, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    n += 1
        return n

    local_posts    = count_jsonl(out / "posts_all.jsonl")
    local_comments = count_jsonl(out / "comments_all.jsonl")
    local_agents   = count_jsonl(out / "agents_seen.jsonl")

    def fmt_pct(local, total):
        if not total:
            return "  ?"
        pct = local / total * 100
        mark = "OK" if pct >= 99 else ("~" if pct >= 50 else "!")
        return f"{mark} {pct:5.1f}%"

    print(f"  {'type':<10} {'local':>12} {'platform':>12} {'coverage':>10}")
    print(f"  {'-'*48}")
    print(f"  {'posts':<10} {local_posts:>12,} {platform_posts:>12,} {fmt_pct(local_posts, platform_posts):>10}")
    print(f"  {'comments':<10} {local_comments:>12,} {platform_comments:>12,} {fmt_pct(local_comments, platform_comments):>10}")
    print(f"  {'agents':<10} {local_agents:>12,} {platform_agents:>12,} {fmt_pct(local_agents, platform_agents):>10}")
    print()


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# MAIN
# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

def main():
    parser = argparse.ArgumentParser(description="Moltbook data scraper v2")
    parser.add_argument("--check", action="store_true", help="run data health check only")
    parser.add_argument("--full", action="store_true", help="full mode: ignore checkpoint and crawl history")
    parser.add_argument("--no-resume", action="store_true",
                        help="ignore and clear saved resume cursors")
    parser.add_argument("--comments-only", action="store_true",
                        help="run comments backfill only; skip posts stage")
    parser.add_argument("--no-comments", action="store_true", help="skip comments stage")
    parser.add_argument("--max-posts", type=int, default=None, help="max newly written posts this run")
    parser.add_argument("--reset", action="store_true", help="reset checkpoint and start fresh")
    parser.add_argument("--workers", type=int, default=5, help="comment worker threads")
    parser.add_argument("--read-rpm", type=int, default=_DEFAULT_READ_RPM,
                        help=f"target read rate req/min (default {_DEFAULT_READ_RPM})")
    parser.add_argument("--comment-rpm", type=int, default=_DEFAULT_COMMENT_RPM,
                        help=f"target comment endpoint req/min (default {_DEFAULT_COMMENT_RPM})")
    parser.add_argument("--min-comments", type=int, default=1,
                        help="only fetch comments for posts with comment_count >= N")
    parser.add_argument("--max-comment-posts", type=int, default=0,
                        help="max posts to fetch comments for (0 = unlimited)")
    parser.add_argument("--output-dir", default="data", help="output directory")
    parser.add_argument("--api-key", default="", help="override MOLTBOOK_API_KEY")
    parser.add_argument("--clean-runs", nargs="?", const=0, type=int, metavar="KEEP",
                        help="clean data/runs snapshots, optionally keep latest N")
    parser.add_argument("--dedup-comments", action="store_true", help="deduplicate comments_all.jsonl by id")
    parser.add_argument("--refetch-comments", type=int, metavar="N",
                        help="remove done-cache marks for posts with comment_count >= N")
    parser.add_argument("--snapshot", action="store_true", help="collect one hot/rising/top snapshot")
    parser.add_argument("--no-snapshot", action="store_true", help="disable end-of-run auto snapshot")
    parser.add_argument("--seed-snapshots", action="store_true", help="seed snapshots from posts_all.jsonl")
    args = parser.parse_args()

    # 鍛戒护琛?--api-key 浼樺厛绾ф渶楂橈紝瑕嗙洊 .env
    if args.api_key:
        HEADERS["Authorization"] = f"Bearer {args.api_key}"

    if args.read_rpm <= 0 or args.comment_rpm <= 0:
        raise SystemExit("--read-rpm and --comment-rpm must be >= 1")
    if args.comments_only and args.no_comments:
        raise SystemExit("--comments-only conflicts with --no-comments")
    _rate_limiter.set_rate(args.read_rpm)
    _rate_limiter_comments.set_rate(args.comment_rpm)

    out = Path(args.output_dir)
    out.mkdir(exist_ok=True)
    (out / "runs").mkdir(exist_ok=True)

    if args.check:
        check_data(out)
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

    if args.seed_snapshots:
        seed_snapshots(out)
        return

    checkpoint_path = out / "checkpoint.json"
    checkpoint = Checkpoint(checkpoint_path)

    if args.reset:
        print("閲嶇疆 checkpoint...")
        checkpoint_path.unlink(missing_ok=True)
        checkpoint = Checkpoint(checkpoint_path)
        args.full = True

    comments_resume_path = out / "comments_resume_cursor.jsonl"
    if args.no_resume:
        had_post_resume = bool(checkpoint.data.get("_resume_cursor"))
        had_comment_resume = comments_resume_path.exists() and comments_resume_path.stat().st_size > 0
        checkpoint.clear_resume()
        comments_resume_path.unlink(missing_ok=True)
        if had_post_resume or had_comment_resume:
            print("  [no-resume] cleared post/comment resume cursors; re-evaluating gaps from local data.")
        else:
            print("  [no-resume] no saved cursors found; re-evaluating gaps from local data.")

    since_time = None if args.full else checkpoint.get_last_newest_time()

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 鈹€鈹€ 鏈湴鏁版嵁锛堜粠 checkpoint 蹇€熻鍙栵紝鏃犻渶鎵弿澶ф枃浠讹級鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    posts_store   = JsonlStore(out / "posts_all.jsonl")   # 鍙姞杞戒竴娆?
    comments_path = out / "comments_all.jsonl"

    local_oldest = local_newest = local_oldest_dt = ""
    cp_oldest = checkpoint.data.get("oldest_post_created_at", "")
    cp_newest = checkpoint.data.get("newest_post_created_at", "")
    if cp_oldest:
        local_oldest_dt = cp_oldest
        local_oldest = cp_oldest[:10]
    if cp_newest:
        local_newest = cp_newest[:10]

    # fallback锛歝heckpoint 鏃犺褰曟椂閲囨牱鏂囦欢澶村熬锛堥娆¤繍琛岋級
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

    # 璇勮杩涘害缂撳瓨锛堝揩閫熻灏忔枃浠讹紝涓嶈Е鍙戣縼绉伙級
    done_cache_path = out / "comments_done_posts.txt"
    done_posts_count = 0
    if done_cache_path.exists():
        with open(done_cache_path, encoding="utf-8") as _f:
            done_posts_count = sum(1 for line in _f if line.strip())
    total_eligible_cached = checkpoint.data.get("total_eligible_comment_posts", 0)

    # 鈹€鈹€ 骞冲彴鏁版嵁锛圓PI锛夆攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    print("姝ｅ湪鑾峰彇骞冲彴鏁版嵁...")
    stats = fetch_platform_stats()
    submolts = fetch_submolts()
    platform_total    = int(stats.get("totalPosts",    0) or 0)
    platform_agents   = int(stats.get("totalAgents",   0) or 0)
    platform_comments = int(stats.get("totalComments", 0) or 0)
    platform_submolts = int(stats.get("totalSubmolts", 0) or 0)

    # 鈹€鈹€ 鍚姩鎽樿锛堢姸鎬?+ 璁″垝锛夆攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    show_startup_summary(
        checkpoint, posts_store.count(), local_oldest, local_newest,
        done_posts_count, total_eligible_cached,
        platform_total, platform_comments, platform_agents,
        out, since_time, args
    )

    t_start = time.time()

    # 鍏ㄩ噺妯″紡璺宠繃宸茬煡鍖哄煙
    if args.full and not args.no_resume:
        rc, _, _ = checkpoint.get_resume_cursor()
        if not rc:
            bc_cursor, bc_date = checkpoint.get_bottom_cursor()
            if bc_cursor and bc_date and local_oldest_dt and bc_date >= local_oldest_dt:
                print(f"  [鑷姩璺宠繃] 鍙戠幇瀛樻。鐐?({bc_date[:10]})锛岃烦杩囧凡鎶撳尯鍩熺洿鎺ョ画鎶撳巻鍙茬己鍙?..")
                checkpoint.data["_resume_cursor"] = bc_cursor
                checkpoint.data["_resume_since"] = None
                checkpoint.save()

    # 鈹€鈹€ 1. 甯栧瓙 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    run_posts_path = out / "runs" / f"{run_ts}_posts.jsonl"
    new_post_count = 0
    stopped_early = False
    newest_post = None
    api_error = False
    reached_end = False
    if args.comments_only:
        print("[1/3] 跳过帖子阶段（--comments-only）...")
        run_posts_path.touch()
    else:
        print("[1/3] 抓取帖子...")
        with open(run_posts_path, "w", encoding="utf-8") as run_posts_f:
            new_post_count, stopped_early, newest_post, api_error, reached_end = fetch_posts_incremental(
                checkpoint, posts_store, run_posts_f,
                since_time=since_time, max_posts=args.max_posts,
                platform_total=platform_total
            )

        if since_time and new_post_count == 0:
            print(f"  没有新帖子（上次运行后暂无更新）。")
            checkpoint.clear_resume()
            if args.no_comments:
                return
            # 否则继续跑评论阶段（历史评论还没抓完）
        if stopped_early:
            print(f"  遇到旧帖子，增量结束。新写入 {new_post_count} 条  |  累积: {posts_store.count()} 条")
        else:
            print(f"  新写入 {new_post_count} 条  |  累积: {posts_store.count()} 条")

    # 鈹€鈹€ 2. 璇勮 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    new_comment_count = 0
    if not args.no_comments:
        print(f"\n[2/3] 鎶撳彇璇勮锛坈omment_count >= {args.min_comments}锛?..")

        # 鍒濆鍖栬瘎璁鸿繘搴︾紦瀛橈紙棣栨杩愯鑷姩浠?comments_all.jsonl 杩佺Щ锛屼箣鍚庣寮€锛?
        done_cache = CommentsDoneCache(
            done_cache_path,
            comments_jsonl=comments_path if comments_path.exists() else None
        )
        resume_cache = CommentsResumeCache(comments_resume_path)

        run_comments_path = out / "runs" / f"{run_ts}_comments.jsonl"
        with open(run_comments_path, "w", encoding="utf-8") as run_comments_f:
            new_comment_count, total_eligible = fetch_comments_for_posts(
                posts_store, comments_path, done_cache, run_comments_f,
                workers=args.workers, min_comments=args.min_comments,
                max_posts=args.max_comment_posts,
                resume_cache=resume_cache
            )
        print(f"  new comments written: {new_comment_count}")

        # 淇濆瓨 total_eligible 渚涗笅娆″惎鍔ㄦ憳瑕佷娇鐢?
        if total_eligible:
            checkpoint.data["total_eligible_comment_posts"] = total_eligible
            checkpoint.save()
    else:
        print("\n[2/3] skip comments.")

    # 鈹€鈹€ 3. 绀惧尯鍒楄〃 + Agent 妗ｆ 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    print(f"\n[3/3] 淇濆瓨绀惧尯鍒楄〃 & 鎻愬彇 agent 妗ｆ...")

    if submolts:
        with open(out / "submolts.json", "w", encoding="utf-8") as f:
            json.dump(submolts, f, ensure_ascii=False, indent=2)
        print(f"  绀惧尯鍒楄〃: {len(submolts)} 涓?鈫?data/submolts.json")

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

    # 鈹€鈹€ 瀹屾垚 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    duration = time.time() - t_start
    checkpoint.update_after_run(
        new_posts=new_post_count,
        new_comments=new_comment_count,
        duration_s=duration,
        newest_post=newest_post,
        clear_cursor=not api_error,
        reached_end=reached_end,
    )
    if api_error:
        print("  [!] API error mid-run: cursor kept; next --full can resume.")

    total_now = posts_store.count()
    pct_now = f"{total_now / platform_total * 100:.2f}%" if platform_total else "?"
    print(f"\n=== 瀹屾垚锛佽€楁椂 {duration:.0f}s ===")
    print(f"  绱Н甯栧瓙: {total_now:,} / {platform_total:,}  ({pct_now})")
    print(f"  runs: {len(checkpoint.data['runs'])}")
    print(f"  鏁版嵁鐩綍: {out}/")

    # 鑷姩鐑棬蹇収锛堟瘡娆?run 缁撴潫鏃舵墦涓€娆★紝闄ら潪 --no-snapshot锛?
    if not args.no_snapshot:
        print("\n[4/4] 鐑棬蹇収...")
        fetch_hot_snapshot(out)


if __name__ == "__main__":
    main()


