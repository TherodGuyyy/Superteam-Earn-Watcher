"""
Superteam Earn Watcher — Stage 2 (the real bot)
--------------------------------------------------
What this does, every poll cycle (default every 20 minutes):
1. Fetches live agent-eligible listings from Superteam Earn.
2. Keeps only: type = bounty/project (skips hackathons), status OPEN,
   winners not yet announced, deadline still in the future.
3. Skips anything we've already alerted on before (tracked in seen_ids.json).
4. For listings whose TITLE doesn't obviously match our keyword list,
   fetches the full listing details and checks the description text too,
   so we don't miss things that are a fit but don't say "content" in the title.
5. Sends a formatted alert to your Telegram channel for every new match.

HONEST LIMITATION (read this before deploying):
Render's free tier wipes local disk on every restart/redeploy. That means
seen_ids.json (which stops duplicate alerts) will reset whenever Render
restarts the service — you may occasionally get a repeat alert for a
listing you already saw, right after a redeploy. This isn't a bug, it's
a free-tier storage limitation. If duplicate alerts become annoying, the
fix is to persist seen_ids somewhere outside Render's disk (e.g. a free
Google Sheet or a tiny external database) — we can add that later if needed.

HOW THIS STAYS ALIVE ON RENDER FREE TIER (same trick as your memecoin bot):
This runs a tiny Flask web server with a "/" health-check endpoint.
UptimeRobot pings that endpoint every 5 minutes to stop Render's free
tier from spinning the service down after 15 minutes of inactivity.
The actual polling loop runs in a background thread alongside it.

REQUIRED ENVIRONMENT VARIABLES (set these on Render, not in this file):
  SUPERTEAM_API_KEY     - your sk_... key from registration
  TELEGRAM_BOT_TOKEN    - from BotFather
  TELEGRAM_CHANNEL_ID   - your channel's numeric ID (e.g. -1001234567890)
  POLL_INTERVAL_MINUTES - optional, defaults to 20
"""

import os
import sys
import json
import time
import threading
import requests
from flask import Flask

# ---------- Configuration ----------

BASE_URL = "https://superteam.fun"
SUPERTEAM_API_KEY = os.environ.get("SUPERTEAM_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID")
POLL_INTERVAL_MINUTES = int(os.environ.get("POLL_INTERVAL_MINUTES", "20"))

SEEN_IDS_FILE = "seen_ids.json"

# First-pass keyword filter — matched against title (fast, free, no extra API call)
KEYWORDS = [
    "content", "writing", "write", "research", "community",
    "social", "translation", "translate", "copywriting",
    "moderation", "moderator", "documentation", "docs",
]

# ---------- Startup checks (fail loudly and clearly, not silently) ----------

def check_config():
    missing = []
    if not SUPERTEAM_API_KEY:
        missing.append("SUPERTEAM_API_KEY")
    if not TELEGRAM_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHANNEL_ID:
        missing.append("TELEGRAM_CHANNEL_ID")
    if missing:
        print(f"FATAL: missing required environment variable(s): {', '.join(missing)}")
        print("Set these in Render's Environment tab before deploying.")
        sys.exit(1)
    if not SUPERTEAM_API_KEY.startswith("sk_"):
        print(f"WARNING: SUPERTEAM_API_KEY doesn't start with 'sk_' — double-check it's correct.")

# ---------- Seen-IDs tracking (dedupe) ----------

def load_seen_ids():
    if os.path.exists(SEEN_IDS_FILE):
        try:
            with open(SEEN_IDS_FILE, "r") as f:
                return set(json.load(f))
        except (json.JSONDecodeError, IOError) as e:
            print(f"WARNING: couldn't read {SEEN_IDS_FILE} ({e}) — starting fresh.")
            return set()
    return set()

def save_seen_ids(seen_ids):
    try:
        with open(SEEN_IDS_FILE, "w") as f:
            json.dump(list(seen_ids), f)
    except IOError as e:
        print(f"WARNING: couldn't save {SEEN_IDS_FILE} ({e}) — dedupe may not persist.")

# ---------- Superteam Earn API calls ----------

def fetch_live_listings():
    url = f"{BASE_URL}/api/agents/listings/live"
    headers = {"Authorization": f"Bearer {SUPERTEAM_API_KEY}"}
    params = {"take": 50}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
    except requests.exceptions.RequestException as e:
        print(f"ERROR: network failure calling listings/live: {e}")
        return []

    if resp.status_code == 401:
        print("FATAL: 401 Unauthorized — SUPERTEAM_API_KEY is invalid. Stopping.")
        sys.exit(1)
    if resp.status_code == 429:
        print("WARNING: 429 rate limited on listings/live — will retry next cycle.")
        return []
    if resp.status_code != 200:
        print(f"ERROR: unexpected status {resp.status_code} from listings/live: {resp.text[:300]}")
        return []

    try:
        data = resp.json()
    except ValueError:
        print("ERROR: listings/live did not return valid JSON.")
        return []

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("listings") or data.get("data") or []
    return []

def fetch_listing_details(slug):
    url = f"{BASE_URL}/api/agents/listings/details/{slug}"
    headers = {"Authorization": f"Bearer {SUPERTEAM_API_KEY}"}
    try:
        resp = requests.get(url, headers=headers, timeout=30)
    except requests.exceptions.RequestException as e:
        print(f"WARNING: network failure fetching details for {slug}: {e}")
        return None
    if resp.status_code != 200:
        print(f"WARNING: status {resp.status_code} fetching details for {slug}")
        return None
    try:
        return resp.json()
    except ValueError:
        print(f"WARNING: details for {slug} wasn't valid JSON.")
        return None

# ---------- Filtering logic ----------

def is_open_and_eligible(listing):
    """Basic eligibility check — type, status, winners not announced, deadline future."""
    if listing.get("type") not in ("bounty", "project"):
        return False
    if listing.get("status") != "OPEN":
        return False
    if listing.get("isWinnersAnnounced") is True:
        return False
    deadline_str = listing.get("deadline")
    if deadline_str:
        try:
            # deadline looks like "2026-07-06T22:59:59.999Z"
            from datetime import datetime, timezone
            deadline = datetime.fromisoformat(deadline_str.replace("Z", "+00:00"))
            if deadline < datetime.now(timezone.utc):
                return False
        except (ValueError, AttributeError):
            print(f"WARNING: couldn't parse deadline '{deadline_str}' — including listing anyway.")
    return True

def title_matches_keywords(title):
    if not title:
        return False
    title_lower = title.lower()
    return any(kw in title_lower for kw in KEYWORDS)

def description_matches_keywords(details):
    if not details:
        return False
    # Best-effort: check a few plausible description fields
    text_parts = []
    for field in ("description", "skills", "requirements"):
        val = details.get(field)
        if isinstance(val, str):
            text_parts.append(val)
        elif isinstance(val, list):
            text_parts.append(" ".join(str(v) for v in val))
    combined = " ".join(text_parts).lower()
    return any(kw in combined for kw in KEYWORDS)

# ---------- Telegram ----------

def send_telegram_alert(listing):
    title = listing.get("title", "Untitled listing")
    reward = listing.get("rewardAmount")
    token = listing.get("token", "")
    deadline = listing.get("deadline", "unknown")
    slug = listing.get("slug", "")
    listing_type = listing.get("type", "listing")
    link = f"{BASE_URL}/listing/{slug}" if slug else BASE_URL

    reward_text = f"{reward} {token}" if reward else "reward not listed"

    message = (
        f"🟢 New {listing_type}: {title}\n"
        f"💰 {reward_text}\n"
        f"⏰ Deadline: {deadline}\n"
        f"🔗 {link}"
    )

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHANNEL_ID, "text": message}

    try:
        resp = requests.post(url, data=payload, timeout=15)
    except requests.exceptions.RequestException as e:
        print(f"ERROR: network failure sending Telegram alert: {e}")
        return False

    if resp.status_code != 200:
        print(f"ERROR: Telegram send failed ({resp.status_code}): {resp.text[:300]}")
        return False

    print(f"Sent alert: {title}")
    return True

# ---------- Main poll cycle ----------

def poll_once(seen_ids):
    listings = fetch_live_listings()
    print(f"Fetched {len(listings)} listing(s) from API.")

    new_matches = 0
    for listing in listings:
        listing_id = listing.get("id")
        title = listing.get("title", "(no title)")

        if not listing_id or listing_id in seen_ids:
            print(f"SKIP (already seen): {title}")
            continue

        if not is_open_and_eligible(listing):
            print(f"SKIP (not open/eligible - type={listing.get('type')}, "
                  f"status={listing.get('status')}, "
                  f"winnersAnnounced={listing.get('isWinnersAnnounced')}, "
                  f"deadline={listing.get('deadline')}): {title}")
            continue

        matched = title_matches_keywords(title)
        match_source = "title" if matched else None

        if not matched:
            slug = listing.get("slug")
            if slug:
                details = fetch_listing_details(slug)
                if details is None:
                    print(f"  -> details fetch failed for slug '{slug}', skipping description check")
                else:
                    print(f"  -> details keys for '{slug}': {list(details.keys())}")
                matched = description_matches_keywords(details)
                match_source = "description" if matched else None

        # Mark as seen regardless of match, so we don't re-check it every cycle
        seen_ids.add(listing_id)

        if matched:
            print(f"MATCH (via {match_source}): {title}")
            send_telegram_alert(listing)
            new_matches += 1
        else:
            print(f"NO MATCH (eligible but no keyword hit): {title}")

    save_seen_ids(seen_ids)
    print(f"Cycle complete. {new_matches} new alert(s) sent.")

def poll_loop():
    seen_ids = load_seen_ids()
    print(f"Loaded {len(seen_ids)} previously-seen listing ID(s).")
    while True:
        try:
            poll_once(seen_ids)
        except Exception as e:
            # Catch-all so one bad cycle never kills the whole bot
            print(f"ERROR: unexpected exception during poll cycle: {e}")
        time.sleep(POLL_INTERVAL_MINUTES * 60)

# ---------- Flask keep-alive server ----------

app = Flask(__name__)

@app.route("/")
def health():
    return "Superteam Earn watcher is running.", 200

if __name__ == "__main__":
    check_config()
    # Start the polling loop in the background, Flask serves the health check
    thread = threading.Thread(target=poll_loop, daemon=True)
    thread.start()
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
