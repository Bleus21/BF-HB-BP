from atproto import Client
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Set, Tuple

# ============================================================
# CONFIG — vul hier je bronnen in (leeg = skip)
# ============================================================

# === FEEDS (max 10) ===
# link mag: at://.../app.bsky.feed.generator/...  OF  https://bsky.app/profile/<actor>/feed/<rkey>
# note = alleen voor jouw overzicht (optioneel; wordt alleen gelogd)
FEEDS = {
    "feed 1": {"link": "", "note": ""},
    "feed 2": {"link": "", "note": ""},
    "feed 3": {"link": "", "note": ""},
    "feed 4": {"link": "", "note": ""},
    "feed 5": {"link": "", "note": ""},
    "feed 6": {"link": "", "note": ""},
    "feed 7": {"link": "", "note": ""},
    "feed 8": {"link": "", "note": ""},
    "feed 9": {"link": "", "note": ""},
    "feed 10": {"link": "", "note": ""},
}

# === LIJSTEN (max 10) ===
# link mag: at://.../app.bsky.graph.list/...  OF  https://bsky.app/profile/<actor>/lists/<rkey>
LIJSTEN = {
    "lijst 1": {"link": "https://bsky.app/profile/did:plc:jaka644beit3x4vmmg6yysw7/lists/3m3iga6wnmz2p", "note": "Beautygrouplist"},
    "lijst 2": {"link": "", "note": ""},
    "lijst 3": {"link": "", "note": ""},
    "lijst 4": {"link": "", "note": ""},
    "lijst 5": {"link": "", "note": ""},
    "lijst 6": {"link": "", "note": ""},
    "lijst 7": {"link": "", "note": ""},
    "lijst 8": {"link": "", "note": ""},
    "lijst 9": {"link": "", "note": ""},
    "lijst 10": {"link": "", "note": ""},
}

# === UITZONDERING (max 10 handles) ===
# ============================================================
# UITZONDERING
# ------------------------------------------------------------
# De onderstaande accounts (handles) zijn UITGEZONDERD van
# één specifieke regel:
#
# 👉 Replies zijn voor deze accounts WEL toegestaan.
#
# Voor alle andere regels geldt GEEN uitzondering:
# - ❌ geen reposts / boosts
# - ❌ geen quote-posts
# - ❌ geen text-only posts
# - ❌ geen link-only (external) posts
# - ✅ media (images/video) blijft VERPLICHT
# ============================================================
EXCEPTION_HANDLES = {
    # "voorbeeld1.bsky.social",
}

# ============================================================
# RUNTIME CONFIG (via env, zodat workflows per account kunnen verschillen)
# ============================================================
HOURS_BACK = int(os.getenv("HOURS_BACK", "3"))
MAX_PER_RUN = int(os.getenv("MAX_PER_RUN", "100"))
MAX_PER_USER = int(os.getenv("MAX_PER_USER", "5"))

REPOST_LOG_FILE = os.getenv("REPOST_LOG_FILE", "reposted_beautyfan.txt")

# Caps voor lijsten (houd runs voorspelbaar)
LIST_MEMBER_LIMIT = int(os.getenv("LIST_MEMBER_LIMIT", "50"))          # max leden per lijst
AUTHOR_POSTS_PER_MEMBER = int(os.getenv("AUTHOR_POSTS_PER_MEMBER", "10"))  # posts per lid om te scannen

# ============================================================
# helpers
# ============================================================

FEED_URL_RE = re.compile(r"^https?://(www\.)?bsky\.app/profile/([^/]+)/feed/([^/?#]+)", re.I)
LIST_URL_RE = re.compile(r"^https?://(www\.)?bsky\.app/profile/([^/]+)/lists/([^/?#]+)", re.I)

def log(msg: str):
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{now}] {msg}")

def parse_time(record, post) -> Optional[datetime]:
    for attr in ["createdAt", "indexedAt", "created_at", "timestamp"]:
        val = getattr(record, attr, None) or getattr(post, attr, None)
        if val:
            try:
                return datetime.fromisoformat(val.replace("Z", "+00:00"))
            except Exception:
                continue
    return None

def load_repost_log(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}

def save_repost_log(path: str, uris: Set[str]) -> None:
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
    # external-only telt niet als media
    if getattr(embed, "external", None):
        return False
    # recordWithMedia media-check (quote skippen we elders)
    rwm = getattr(embed, "recordWithMedia", None)
    if rwm and getattr(rwm, "media", None):
        m = rwm.media
        if getattr(m, "images", None):
            return True
        if getattr(m, "video", None):
            return True
    return False

def resolve_handle_to_did(client: Client, actor: str) -> Optional[str]:
    if actor.startswith("did:"):
        return actor
    try:
        out = client.com.atproto.identity.resolve_handle({"handle": actor})
        return getattr(out, "did", None)
    except Exception:
        return None

def normalize_feed_uri(client: Client, s: str) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    if s.startswith("at://") and "/app.bsky.feed.generator/" in s:
        return s
    m = FEED_URL_RE.match(s)
    if not m:
        return None
    actor = m.group(2)
    rkey = m.group(3)
    did = resolve_handle_to_did(client, actor)
    if not did:
        return None
    return f"at://{did}/app.bsky.feed.generator/{rkey}"

def normalize_list_uri(client: Client, s: str) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    if s.startswith("at://") and "/app.bsky.graph.list/" in s:
        return s
    m = LIST_URL_RE.match(s)
    if not m:
        return None
    actor = m.group(2)
    rkey = m.group(3)
    did = resolve_handle_to_did(client, actor)
    if not did:
        return None
    return f"at://{did}/app.bsky.graph.list/{rkey}"

def fetch_feed_items(client: Client, feed_uri: str, max_items: int = 1000) -> List:
    """Paginated get_feed best-effort."""
    items: List = []
    cursor = None
    while True:
        params = {"feed": feed_uri, "limit": 100}
        if cursor:
            params["cursor"] = cursor
        out = client.app.bsky.feed.get_feed(params)
        batch = getattr(out, "feed", []) or []
        items.extend(batch)
        cursor = getattr(out, "cursor", None)
        if not cursor or len(items) >= max_items:
            break
    return items[:max_items]

def fetch_list_members(client: Client, list_uri: str, limit: int) -> List[Tuple[str, str]]:
    """Return (handle, did) tuples best-effort."""
    members: List[Tuple[str, str]] = []
    cursor = None
    while True:
        params = {"list": list_uri, "limit": 100}
        if cursor:
            params["cursor"] = cursor
        out = client.app.bsky.graph.get_list(params)
        items = getattr(out, "items", []) or []
        for it in items:
            subj = getattr(it, "subject", None)
            if not subj:
                continue
            h = (getattr(subj, "handle", "") or "").lower()
            d = getattr(subj, "did", None)
            if h or d:
                members.append((h, d))
            if len(members) >= limit:
                return members[:limit]
        cursor = getattr(out, "cursor", None)
        if not cursor:
            break
    return members[:limit]

def fetch_author_feed(client: Client, actor: str, limit: int) -> List:
    try:
        out = client.app.bsky.feed.get_author_feed({"actor": actor, "limit": limit})
        return getattr(out, "feed", []) or []
    except Exception:
        return []

def build_candidates_from_items(
    items: List,
    done: Set[str],
    cutoff: datetime,
    exception_handles_lc: Set[str],
) -> List[Dict]:
    candidates: List[Dict] = []
    for item in items:
        post = getattr(item, "post", None)
        if not post:
            continue
        record = getattr(post, "record", None)
        if not record:
            continue

        uri = getattr(post, "uri", None)
        cid = getattr(post, "cid", None)
        if not uri or not cid:
            continue

        # boosts/reposts overslaan
        if hasattr(item, "reason") and item.reason is not None:
            continue

        author = getattr(post, "author", None)
        author_handle = (getattr(author, "handle", "") or "").lower()
        author_did = getattr(author, "did", None)
        is_exception = author_handle in exception_handles_lc

        # replies overslaan (behalve uitzonderingen)
        if getattr(record, "reply", None) and not is_exception:
            continue

        # quotes overslaan
        if is_quote_post(record):
            continue

        # alleen media
        if not has_media(record):
            continue

        # al eens gedaan?
        if uri in done:
            continue

        created_dt = parse_time(record, post)
        if not created_dt or created_dt < cutoff:
            continue

        candidates.append({
            "uri": uri,
            "cid": cid,
            "created": created_dt,
            "author_key": author_did or author_handle or uri,
        })

    candidates.sort(key=lambda x: x["created"])
    return candidates

def dedupe_candidates(cands: List[Dict]) -> List[Dict]:
    seen: Set[str] = set()
    out: List[Dict] = []
    for c in cands:
        u = c["uri"]
        if u in seen:
            continue
        seen.add(u)
        out.append(c)
    out.sort(key=lambda x: x["created"])
    return out


# ============================================================
# main
# ============================================================

def main():
    username = os.getenv("BSKY_USERNAME", "").strip()
    password = os.getenv("BSKY_PASSWORD", "").strip()
    if not username or not password:
        log("❌ Missing env BSKY_USERNAME / BSKY_PASSWORD")
        return

    client = Client()
    client.login(username, password)
    log("✅ Logged in.")

    cutoff = datetime.now(timezone.utc) - timedelta(hours=HOURS_BACK)
    done = load_repost_log(REPOST_LOG_FILE)
    exception_handles_lc = {h.lower() for h in EXCEPTION_HANDLES if h.strip()}

    # ---- normalize feeds ----
    feed_uris: List[Tuple[str, str, str]] = []  # (key, note, at_uri)
    for key, obj in FEEDS.items():
        link = (obj.get("link") or "").strip()
        note = (obj.get("note") or "").strip()
        if not link:
            continue
        uri = normalize_feed_uri(client, link)
        if uri:
            feed_uris.append((key, note, uri))
        else:
            log(f"⚠️ Feed ongeldig (skip): {key}")

    # ---- normalize lists ----
    list_uris: List[Tuple[str, str, str]] = []
    for key, obj in LIJSTEN.items():
        link = (obj.get("link") or "").strip()
        note = (obj.get("note") or "").strip()
        if not link:
            continue
        uri = normalize_list_uri(client, link)
        if uri:
            list_uris.append((key, note, uri))
        else:
            log(f"⚠️ Lijst ongeldig (skip): {key}")

    if not feed_uris:
        log("ℹ️ Geen FEEDS ingevuld — feeds blok geskipt.")
    if not list_uris:
        log("ℹ️ Geen LIJSTEN ingevuld — lijsten blok geskipt.")

    # ---- collect candidates ----
    all_candidates: List[Dict] = []

    # Feeds
    for key, note, furi in feed_uris:
        label = f"{key}" + (f" ({note})" if note else "")
        log(f"📥 Feed verwerken: {label}")
        try:
            items = fetch_feed_items(client, furi, max_items=1000)
        except Exception as e:
            log(f"⚠️ Feed fetch error ({key}): {e}")
            items = []
        all_candidates.extend(build_candidates_from_items(items, done, cutoff, exception_handles_lc))

    # Lists
    for key, note, luri in list_uris:
        label = f"{key}" + (f" ({note})" if note else "")
        log(f"📋 Lijst verwerken: {label}")

        members = fetch_list_members(client, luri, limit=LIST_MEMBER_LIMIT)
        if not members:
            continue

        for (h, d) in members:
            actor = d or h
            if not actor:
                continue
            author_items = fetch_author_feed(client, actor, AUTHOR_POSTS_PER_MEMBER)
            all_candidates.extend(build_candidates_from_items(author_items, done, cutoff, exception_handles_lc))

    candidates = dedupe_candidates(all_candidates)
    log(f"🧩 Candidates totaal: {len(candidates)} (na dedupe)")

    # ---- execute ----
    reposted = 0
    liked = 0
    per_user_count: Dict[str, int] = {}

    for c in candidates:
        if reposted >= MAX_PER_RUN:
            break

        author_key = c["author_key"]
        per_user_count.setdefault(author_key, 0)
        if per_user_count[author_key] >= MAX_PER_USER:
            continue

        uri = c["uri"]
        cid = c["cid"]

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