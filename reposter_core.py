from atproto import Client
import os
import time
from datetime import datetime, timedelta, timezone

# === CONFIG (later uitbreiden) ===
FEED_URI = os.getenv("FEED_URI", "").strip()  # optioneel; als leeg -> script stopt netjes
HOURS_BACK = int(os.getenv("HOURS_BACK", "3"))
MAX_PER_RUN = int(os.getenv("MAX_PER_RUN", "100"))
MAX_PER_USER = int(os.getenv("MAX_PER_USER", "5"))

REPOST_LOG_FILE = os.getenv("REPOST_LOG_FILE", "reposted_beautyfan.txt")

# Replies uitzondering (optioneel)
EXCEPTION_HANDLES = {
    # "voorbeeld1.bsky.social",
}

def log(msg: str):
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{now}] {msg}")

def parse_time(record, post):
    for attr in ["createdAt", "indexedAt", "created_at", "timestamp"]:
        val = getattr(record, attr, None) or getattr(post, attr, None)
        if val:
            try:
                return datetime.fromisoformat(val.replace("Z", "+00:00"))
            except Exception:
                continue
    return None

def load_repost_log(path: str):
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}

def save_repost_log(path: str, uris: set):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for uri in sorted(uris):
            f.write(uri + "\n")
    os.replace(tmp, path)

def is_quote_post(record) -> bool:
    embed = getattr(record, "embed", None)
    if not embed:
        return False
    return bool(getattr(embed, "record", None) or getattr(embed, "recordWithMedia", None))

def has_media(record) -> bool:
    embed = getattr(record, "embed", None)
    if not embed:
        return False
    if getattr(embed, "images", None):
        return True
    if getattr(embed, "video", None):
        return True
    # external-only telt niet
    if getattr(embed, "external", None):
        return False
    return False

def main():
    username = os.getenv("BSKY_USERNAME", "").strip()
    password = os.getenv("BSKY_PASSWORD", "").strip()

    if not username or not password:
        log("❌ Missing env BSKY_USERNAME / BSKY_PASSWORD")
        return
    if not FEED_URI:
        log("ℹ️ FEED_URI is leeg; niets te doen. (Zet FEED_URI in workflow env)")
        return

    client = Client()
    client.login(username, password)
    log("✅ Logged in.")

    # Feed ophalen
    try:
        feed = client.app.bsky.feed.get_feed({"feed": FEED_URI, "limit": 100})
        items = feed.feed
        log(f"📊 {len(items)} feed items.")
    except Exception as e:
        log(f"⚠️ Feed fetch error: {e}")
        return

    done = load_repost_log(REPOST_LOG_FILE)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=HOURS_BACK)
    exception_lc = {h.lower() for h in EXCEPTION_HANDLES}

    candidates = []
    for item in items:
        post = item.post
        record = post.record
        uri = post.uri
        cid = post.cid

        # boosts/reposts overslaan
        if hasattr(item, "reason") and item.reason is not None:
            continue

        author_handle = (getattr(post.author, "handle", "") or "").lower()
        is_exception = author_handle in exception_lc

        # replies overslaan (behalve uitzonderingen)
        if getattr(record, "reply", None) and not is_exception:
            continue

        # quotes overslaan
        if is_quote_post(record):
            continue

        # alleen media
        if not has_media(record):
            continue

        # al gedaan?
        if uri in done:
            continue

        created_dt = parse_time(record, post)
        if not created_dt or created_dt < cutoff:
            continue

        candidates.append({
            "uri": uri,
            "cid": cid,
            "created": created_dt,
            "author_key": getattr(post.author, "did", None) or author_handle or uri,
        })

    candidates.sort(key=lambda x: x["created"])
    log(f"🧩 {len(candidates)} candidates.")

    reposted = 0
    liked = 0
    per_user_count = {}

    for p in candidates:
        if reposted >= MAX_PER_RUN:
            break

        author_key = p["author_key"]
        per_user_count.setdefault(author_key, 0)
        if per_user_count[author_key] >= MAX_PER_USER:
            continue

        uri = p["uri"]
        cid = p["cid"]

        try:
            client.app.bsky.feed.repost.create(
                repo=client.me.did,
                record={
                    "subject": {"uri": uri, "cid": cid},
                    "createdAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                },
            )
            reposted += 1
            per_user_count[author_key] += 1
            done.add(uri)

            try:
                client.app.bsky.feed.like.create(
                    repo=client.me.did,
                    record={
                        "subject": {"uri": uri, "cid": cid},
                        "createdAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    },
                )
                liked += 1
            except Exception as e_like:
                log(f"⚠️ Like error: {e_like}")

            time.sleep(2)

        except Exception as e:
            log(f"⚠️ Repost error: {e}")
            time.sleep(8)

    save_repost_log(REPOST_LOG_FILE, done)
    log(f"🔥 Done — {reposted} reposts ({liked} liked).")

if __name__ == "__main__":
    main()
