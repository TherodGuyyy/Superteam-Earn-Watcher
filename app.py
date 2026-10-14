"""
Opportunity Watcher
-------------------
Watches Superteam Earn (bounties/projects) and the career boards of crypto
companies (Greenhouse, Lever, Ashby) and pushes an alert to Telegram (primary),
ntfy.sh (backup) and/or email the moment something new and relevant appears.

Built for first-come-first-served opportunities:
  * polls every POLL_SECONDS (default 120)
  * flags "first come first served" wording, fresh postings and closing deadlines
  * sends a reminder 24h and 3h before a deadline for things you were alerted on
  * never marks a listing as "seen" unless the alert was actually delivered,
    so a Telegram outage can't make you silently miss a listing
  * /status shows exactly what the bot is doing, so "no alerts" is debuggable

REQUIRED (set in Render's Environment tab, never in this file)
  TELEGRAM_BOT_TOKEN   token from @BotFather (use the NEW account's bot)
  TELEGRAM_CHAT_ID     your numeric chat id (run:  python app.py --find-chat-id)

OPTIONAL
  NTFY_TOPIC           backup push channel, e.g. ayo-jobs-x7k29qpm41 (ntfy app)
  NTFY_SERVER          default https://ntfy.sh
  EMAIL_USER           Gmail address to send from
  EMAIL_APP_PASSWORD   Gmail "app password" (not your normal password)
  EMAIL_TO             where to send (default: EMAIL_USER)
  GREENHOUSE_BOARDS    comma list of slugs, e.g. coinbase,ripple
  LEVER_BOARDS         comma list of slugs, e.g. binance
  ASHBY_BOARDS         comma list of slugs, e.g. kraken.com
  SUPERTEAM_API_KEY    sk_... key; adds Superteam's agent feed (projects)
  POLL_SECONDS         default 120 (minimum 30)
  STRICT_LOCATION      default 1: only remote/global/Africa/Nigeria roles. 0 = any "remote"
  ALLOW_SENIOR         default 0: skip senior/staff/lead/principal titles
  EXTRA_NOISE          extra words that mark junk bounties, comma list
  TZ_OFFSET_HOURS      default 1 (Lagos)
  STATE_FILE           default seen_ids.json
  DRY_RUN              1 = print alerts to the log instead of sending
"""

import html
import json
import logging
import os
import re
import smtplib
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage

import requests
from flask import Flask, jsonify

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
log = logging.getLogger("watcher")

# =====================================================================
# Config
# =====================================================================


def _env(name, default=""):
    return (os.environ.get(name) or default).strip()


def _env_list(name):
    return [x.strip() for x in _env(name).replace("\n", ",").split(",") if x.strip()]


def _env_int(name, default):
    raw = _env(name, str(default))
    try:
        return int(raw)
    except ValueError:
        log.warning("%s=%r is not a number, using %s", name, raw, default)
        return default


class Config:
    def __init__(self):
        self.telegram_token = _env("TELEGRAM_BOT_TOKEN")
        self.telegram_chat = _env("TELEGRAM_CHAT_ID") or _env("TELEGRAM_CHANNEL_ID")
        self.ntfy_topic = _env("NTFY_TOPIC")
        self.ntfy_server = _env("NTFY_SERVER", "https://ntfy.sh").rstrip("/")
        self.email_user = _env("EMAIL_USER")
        self.email_pass = _env("EMAIL_APP_PASSWORD").replace(" ", "")
        self.email_to = _env("EMAIL_TO") or self.email_user
        self.superteam_key = _env("SUPERTEAM_API_KEY")
        self.poll_seconds = max(30, _env_int("POLL_SECONDS", 120))
        self.greenhouse = _env_list("GREENHOUSE_BOARDS")
        self.lever = _env_list("LEVER_BOARDS")
        self.ashby = _env_list("ASHBY_BOARDS")
        self.strict_location = _env("STRICT_LOCATION", "1") != "0"
        self.allow_senior = _env("ALLOW_SENIOR", "0") == "1"
        self.extra_noise = [w.lower() for w in _env_list("EXTRA_NOISE")]
        self.tz_hours = _env_int("TZ_OFFSET_HOURS", 1)
        self.state_file = _env("STATE_FILE", "seen_ids.json")
        self.dry_run = _env("DRY_RUN", "0") == "1"
        self.max_alerts_per_cycle = 10

    @property
    def telegram_ok(self):
        return bool(self.telegram_token and self.telegram_chat)

    @property
    def ntfy_ok(self):
        return bool(self.ntfy_topic)

    @property
    def email_ok(self):
        return bool(self.email_user and self.email_pass)

    def errors(self):
        errs = []
        if self.telegram_token and not self.telegram_chat:
            errs.append("TELEGRAM_BOT_TOKEN is set but TELEGRAM_CHAT_ID is missing "
                        "(run: python app.py --find-chat-id)")
        if self.dry_run:
            return errs
        if not (self.telegram_ok or self.ntfy_ok or self.email_ok):
            errs.append("No alert channel configured. Set TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID, "
                        "or NTFY_TOPIC, or EMAIL_USER + EMAIL_APP_PASSWORD.")
        return errs


CFG = Config()

SUPERTEAM = "https://superteam.fun"
HEADERS = {"User-Agent": "opportunity-watcher/2.0 (personal job alerts)", "Accept": "application/json"}

# =====================================================================
# Status (what /status shows) and outbox for DRY_RUN / tests
# =====================================================================

STATUS = {
    "started_at": None,
    "last_cycle_at": None,
    "last_cycle_ok": None,
    "cycles": 0,
    "alerts_sent": 0,
    "last_alert": None,
    "last_notify_error": None,
    "config_error": None,
    "sources": {},
}
OUTBOX = []
WORKER = {"thread": None}


def utcnow():
    return datetime.now(timezone.utc)


def _scrub(text):
    """Never let the bot token or passwords reach the logs."""
    text = str(text)
    for secret in (CFG.telegram_token, CFG.email_pass, CFG.superteam_key):
        if secret:
            text = text.replace(secret, "<hidden>")
    return text


# =====================================================================
# Persistent state (dedupe). Written atomically.
# =====================================================================


class State:
    KEEP_DAYS = 120

    def __init__(self, path):
        self.path = path
        self.data = {"initialised": False, "seen": {}, "reminded": {}}
        self.failures = {}  # uid -> failed delivery attempts (memory only)
        self.load()

    def load(self):
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                self.data["initialised"] = bool(raw.get("initialised"))
                self.data["seen"] = dict(raw.get("seen") or {})
                self.data["reminded"] = dict(raw.get("reminded") or {})
            elif isinstance(raw, list):  # old seen_ids.json format
                now = time.time()
                self.data["seen"] = {str(x): now for x in raw}
                self.data["initialised"] = True
        except (ValueError, OSError) as e:
            log.warning("Could not read %s (%s) - starting fresh.", self.path, e)

    def save(self):
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f)
            os.replace(tmp, self.path)
        except OSError as e:
            log.warning("Could not save %s (%s) - dedupe will not survive a restart.", self.path, e)

    def seen(self, uid):
        return uid in self.data["seen"]

    def mark_seen(self, uid):
        self.data["seen"][uid] = time.time()

    def reminded(self, key):
        return key in self.data["reminded"]

    def mark_reminded(self, keys):
        for k in keys:
            self.data["reminded"][k] = time.time()

    def prune(self):
        cutoff = time.time() - self.KEEP_DAYS * 86400
        for bucket in ("seen", "reminded"):
            self.data[bucket] = {k: v for k, v in self.data[bucket].items() if v >= cutoff}


# =====================================================================
# Fetchers - every one returns a list of "job" dicts, or raises on failure
# =====================================================================


def parse_dt(value):
    if value in (None, ""):
        return None
    try:
        if isinstance(value, (int, float)):
            seconds = value / 1000.0 if value > 1e11 else float(value)
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        dt = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, OverflowError, OSError):
        return None


def make_job(uid, source, title, url, kind, company="", location="", reward="",
             deadline=None, posted=None, text="", remote=None, submissions=None):
    return {
        "uid": uid, "source": source, "title": (title or "").strip(), "url": url or "",
        "kind": kind, "company": company or "", "location": location or "",
        "reward": reward or "", "deadline": deadline, "posted": posted,
        "text": text or "", "remote": remote, "submissions": submissions,
    }


def get_json(url, params=None, headers=None):
    h = dict(HEADERS)
    if headers:
        h.update(headers)
    r = requests.get(url, params=params, headers=h, timeout=25)
    r.raise_for_status()
    return r.json()


def _items(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("listings", "data", "jobs", "results"):
            if isinstance(data.get(key), list):
                return data[key]
    return []


def _reward_text(it):
    token = it.get("token") or ""
    try:
        amount = float(it.get("rewardAmount"))
    except (TypeError, ValueError):
        amount = 0
    if amount:
        return f"{amount:,.0f} {token}".strip()
    lo, hi = it.get("minRewardAsk"), it.get("maxRewardAsk")
    if lo or hi:
        return f"{lo or '?'}-{hi or '?'} {token}".strip()
    return ""


def _superteam_job(it):
    uid, slug = it.get("id"), it.get("slug")
    if not uid or not slug:
        return None
    kind = it.get("type")
    if kind not in ("bounty", "project"):  # skip hackathons etc.
        return None
    if it.get("status") not in (None, "OPEN"):
        return None
    if it.get("isWinnersAnnounced") is True:
        return None
    deadline = parse_dt(it.get("deadline"))
    if deadline and deadline < utcnow():
        return None
    return make_job(
        uid=f"st:{uid}", source="Superteam Earn", title=it.get("title"),
        url=f"{SUPERTEAM}/earn/listing/{slug}", kind=kind,
        company=(it.get("sponsor") or {}).get("name", ""), reward=_reward_text(it),
        deadline=deadline, submissions=(it.get("_count") or {}).get("Submission"),
    )


def fetch_superteam():
    """Public feed (all open bounties, incl. human-only ones) + optional agent feed (projects)."""
    jobs, errors, succeeded = {}, [], 0
    feeds = [("public", f"{SUPERTEAM}/api/listings", {"take": 50}, None)]
    if CFG.superteam_key:
        feeds.append(("agent", f"{SUPERTEAM}/api/agents/listings/live", {"take": 50},
                      {"Authorization": f"Bearer {CFG.superteam_key}"}))
    for name, url, params, headers in feeds:
        try:
            for it in _items(get_json(url, params=params, headers=headers)):
                job = _superteam_job(it)
                if job:
                    jobs.setdefault(job["uid"], job)
            succeeded += 1
        except Exception as e:  # noqa: BLE001 - one feed failing must not kill the other
            errors.append(f"{name} feed: {_scrub(e)}")
            log.warning("Superteam %s feed failed: %s", name, _scrub(e))
    if not succeeded:
        raise RuntimeError("; ".join(errors) or "no Superteam feed worked")
    return list(jobs.values())


def fetch_greenhouse(slug):
    data = get_json(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs")
    out = []
    for j in _items(data):
        if not j.get("id") or not j.get("title"):
            continue
        out.append(make_job(
            uid=f"gh:{slug}:{j['id']}", source=f"{slug} (Greenhouse)", title=j["title"],
            url=j.get("absolute_url", ""), kind="job", company=j.get("company_name") or slug,
            location=(j.get("location") or {}).get("name", ""),
            posted=parse_dt(j.get("first_published")),
        ))
    return out


def fetch_lever(slug):
    data = get_json(f"https://api.lever.co/v0/postings/{slug}", params={"mode": "json"})
    if not isinstance(data, list):
        raise RuntimeError(f"unexpected Lever response for '{slug}': {str(data)[:120]}")
    out = []
    for p in data:
        if not p.get("id") or not p.get("text"):
            continue
        cats = p.get("categories") or {}
        locs = cats.get("allLocations") or ([cats["location"]] if cats.get("location") else [])
        out.append(make_job(
            uid=f"lv:{slug}:{p['id']}", source=f"{slug} (Lever)", title=p["text"],
            url=p.get("hostedUrl") or p.get("applyUrl", ""), kind="job", company=slug,
            location=" | ".join(str(x) for x in locs),
            remote=True if str(p.get("workplaceType", "")).lower() == "remote" else None,
            posted=parse_dt(p.get("createdAt")), text=p.get("descriptionPlain") or "",
        ))
    return out


def fetch_ashby(slug):
    data = get_json(f"https://api.ashbyhq.com/posting-api/job-board/{slug}")
    out = []
    for j in _items(data):
        if not j.get("id") or not j.get("title") or j.get("isListed") is False:
            continue
        locs = [j.get("location") or ""]
        for sec in j.get("secondaryLocations") or []:
            locs.append(sec.get("location", "") if isinstance(sec, dict) else str(sec))
        remote = bool(j.get("isRemote")) or str(j.get("workplaceType", "")).lower() == "remote"
        out.append(make_job(
            uid=f"ab:{slug}:{j['id']}", source=f"{slug} (Ashby)", title=j["title"],
            url=j.get("jobUrl") or j.get("applyUrl", ""), kind="job", company=slug,
            location=" | ".join(x for x in locs if x), remote=remote or None,
            posted=parse_dt(j.get("publishedAt")), text=j.get("descriptionPlain") or "",
        ))
    return out


def build_sources():
    sources = [("superteam", fetch_superteam)]
    for slug in CFG.greenhouse:
        sources.append((f"greenhouse:{slug}", lambda s=slug: fetch_greenhouse(s)))
    for slug in CFG.lever:
        sources.append((f"lever:{slug}", lambda s=slug: fetch_lever(s)))
    for slug in CFG.ashby:
        sources.append((f"ashby:{slug}", lambda s=slug: fetch_ashby(s)))
    return sources


# =====================================================================
# Matching: what is worth alerting on, and how urgently
# =====================================================================

STRONG = re.compile(
    r"\b(python|developer|dev|engineer|programmer|bots?|automation|scraper|scraping|backend|back-end|"
    r"full[- ]?stack|front[- ]?end|javascript|typescript|node(?:\.?js)?|rust|apis?|data|analyst|"
    r"analytics|dashboard|qa|tester|testing|junior|jr|intern|internship|entry[- ]level|trainee|"
    r"graduate|apprentice|on-?chain|smart contracts?|audit|security)\b", re.I)
BROAD = re.compile(
    r"\b(project manager|project coordinator|program coordinator|coordinator|community|moderator|"
    r"moderation|support|operations|ops|research(?:er)?|ambassador|documentation|technical writer|"
    r"translator|translation|customer success|onboarding)\b", re.I)
SENIOR = re.compile(
    r"\b(senior|sr|staff|principal|director|head of|vp|vice president|chief|lead|iii|iv|"
    r"distinguished|architect|engineering manager|general manager)\b", re.I)
YEARS = re.compile(r"\b(?:[5-9]|1[0-9])\s*\+?\s*(?:or more\s+)?years?\b", re.I)
NOISE = re.compile(
    r"\b(threads?|tweets?|retweets?|memes?|videos?|reels?|tiktok|shill|giveaway|viral|raid|"
    r"leaderboard|quests?|drive attention|bring your friends|predictions?|referral|posts?|"
    r"articles?|blogs?|podcast|infographic|try a)\b", re.I)
FCFS = re.compile(
    r"first[- ]come|fcfs|first[- ]served|first\s+\d+\s+(?:applicants|people|submissions|users|"
    r"participants|wallets)|limited\s+(?:spots|slots|seats|availability)|spots?\s+(?:are\s+)?limited|"
    r"rolling\s+(?:basis|review|applications?)|until\s+filled|apply\s+(?:early|asap)", re.I)
GLOBAL_LOC = re.compile(r"\b(worldwide|global|anywhere|africa|nigeria|emea)\b", re.I)
REMOTE_WORDS = re.compile(r"\b(remote|work from home|wfh|fully|first|friendly|only|100%)\b", re.I)


def _location_piece_ok(piece):
    p = piece.strip().lower()
    if not p:
        return False
    if GLOBAL_LOC.search(p):
        return True
    if "remote" not in p:  # a plain city / "hybrid" / "on-site"
        return False
    rest = re.sub(r"[^a-z]+", " ", REMOTE_WORDS.sub(" ", p)).strip()
    return rest == ""  # "Remote - USA" leaves "usa" -> restricted


def location_ok(location, remote_flag=None):
    pieces = [x for x in re.split(r"[|;]", location or "") if x.strip()]
    if not pieces:
        return remote_flag is True
    if not CFG.strict_location:
        return remote_flag is True or any("remote" in x.lower() or GLOBAL_LOC.search(x) for x in pieces)
    return any(_location_piece_ok(x) for x in pieces)


def evaluate(job, now=None):
    """Return None to drop the listing, otherwise a dict describing why/how urgently to alert."""
    now = now or utcnow()
    title, text, kind = job["title"], job["text"], job["kind"]
    if not title:
        return None
    if SENIOR.search(title) and not CFG.allow_senior:
        return None
    if text and YEARS.search(text):
        return None

    strong = bool(STRONG.search(title))
    broad = bool(BROAD.search(title))

    if kind == "job":
        if not location_ok(job["location"], job["remote"]):
            return None
        if not (strong or broad):
            return None
    elif kind == "bounty":
        extra = any(w in title.lower() for w in CFG.extra_noise)
        if (NOISE.search(title) or extra) and not strong:
            return None
    # projects (freelance gigs) are always kept

    fcfs = bool(FCFS.search(f"{title} {text}"))
    hours_left = None
    if job["deadline"]:
        hours_left = (job["deadline"] - now).total_seconds() / 3600.0
        if hours_left <= 0:
            return None
    fresh = bool(job["posted"] and 0 <= (now - job["posted"]).total_seconds() <= 3 * 3600)

    score = 3 if strong else 2 if (broad or kind == "project") else 1
    score += 2 if fcfs else 0
    score += 1 if fresh else 0
    score += 1 if (hours_left is not None and hours_left <= 48) else 0

    if fcfs or (hours_left is not None and hours_left <= 6) or (fresh and (strong or broad)):
        tier = "urgent"
    elif score >= 2:
        tier = "normal"
    else:
        tier = "low"
    return {"tier": tier, "score": score, "fcfs": fcfs, "fresh": fresh,
            "hours_left": hours_left, "strong": strong}


# =====================================================================
# Message formatting
# =====================================================================

TIER_RANK = {"urgent": 0, "normal": 1, "low": 2}
TIER_LABEL = {"urgent": "🚨 URGENT", "normal": "🟢 NEW", "low": "⚪ FYI"}


def human_delta(seconds):
    seconds = int(max(0, seconds))
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m = rem // 60
    if d:
        return f"{d}d {h}h"
    if h:
        return f"{h}h {m}m"
    return f"{max(m, 1)}m"


def local_time(dt):
    return dt.astimezone(timezone(timedelta(hours=CFG.tz_hours))).strftime("%a %d %b, %H:%M")


def build_alert(job, ev, now=None, reminder_hours=None):
    now = now or utcnow()
    e = html.escape
    kind = job["kind"].capitalize()
    if reminder_hours:
        label = f"⏰ CLOSING SOON ({kind})"
        prio = "urgent"
    else:
        label = f"{TIER_LABEL[ev['tier']]} · {kind}"
        prio = ev["tier"]

    lines_html = [f"<b>{e(label)}</b>", f"<b>{e(job['title'])}</b>"]
    lines_txt = [label, job["title"]]

    def add(icon, text):
        lines_html.append(f"{icon} {e(text)}")
        lines_txt.append(f"{icon} {text}")

    if job["company"]:
        add("🏢", job["company"] + ("" if job["source"].startswith(job["company"]) else f" · {job['source']}"))
    if job["reward"]:
        add("💰", job["reward"])
    if job["location"]:
        add("📍", job["location"])
    if ev["fcfs"]:
        add("⚡", "First-come-first-served wording - apply fast")
    if job["deadline"]:
        left = (job["deadline"] - now).total_seconds()
        add("⏳", f"Closes in {human_delta(left)} ({local_time(job['deadline'])})")
    if ev["fresh"] and job["posted"]:
        add("🆕", f"Posted {human_delta((now - job['posted']).total_seconds())} ago")
    if job["submissions"] is not None:
        add("👥", f"{job['submissions']} submissions so far")
    if job["url"]:
        lines_html.append(f'🔗 <a href="{html.escape(job["url"], quote=True)}">Open &amp; apply</a>')
        lines_txt.append(f"🔗 {job['url']}")
    return {"prio": prio, "title": f"{label}: {job['title']}", "html": "\n".join(lines_html),
            "plain": "\n".join(lines_txt), "url": job["url"]}


# =====================================================================
# Notification channels. Each returns True / False, or None if not configured.
# =====================================================================


def send_telegram(n):
    if not CFG.telegram_ok:
        return None
    url = f"https://api.telegram.org/bot{CFG.telegram_token}/sendMessage"
    payload = {"chat_id": CFG.telegram_chat, "text": n["html"][:4000], "parse_mode": "HTML",
               "disable_web_page_preview": True, "disable_notification": n["prio"] == "low"}
    for attempt in (1, 2):
        try:
            r = requests.post(url, json=payload, timeout=20)
        except requests.RequestException as e:
            log.error("Telegram network error: %s", _scrub(e))
            STATUS["last_notify_error"] = f"telegram: {_scrub(e)}"
            return False
        if r.status_code == 429 and attempt == 1:
            try:
                wait = int(r.json().get("parameters", {}).get("retry_after", 3))
            except (ValueError, AttributeError):
                wait = 3
            time.sleep(min(wait, 30))
            continue
        if r.ok:
            return True
        if r.status_code == 400 and "parse" in r.text.lower() and attempt == 1:
            # Formatting problem in one listing's text: resend it as plain text instead of losing it.
            payload = {"chat_id": CFG.telegram_chat, "text": n["plain"][:4000],
                       "disable_web_page_preview": True, "disable_notification": n["prio"] == "low"}
            continue
        log.error("Telegram rejected the message (%s): %s", r.status_code, _scrub(r.text[:200]))
        STATUS["last_notify_error"] = f"telegram {r.status_code}: {_scrub(r.text[:200])}"
        return False
    return False


def send_ntfy(n):
    if not CFG.ntfy_ok:
        return None
    priority = {"urgent": 5, "normal": 4, "low": 2}[n["prio"]]
    tags = {"urgent": ["rotating_light"], "normal": ["briefcase"], "low": ["page_facing_up"]}[n["prio"]]
    payload = {"topic": CFG.ntfy_topic, "title": n["title"][:200], "message": n["plain"][:3900],
               "priority": priority, "tags": tags}
    if n["url"]:
        payload["click"] = n["url"]
        payload["actions"] = [{"action": "view", "label": "Open", "url": n["url"]}]
    try:
        r = requests.post(CFG.ntfy_server, json=payload, timeout=15)
    except requests.RequestException as e:
        log.error("ntfy network error: %s", _scrub(e))
        STATUS["last_notify_error"] = f"ntfy: {_scrub(e)}"
        return False
    if r.ok:
        return True
    log.error("ntfy rejected the message (%s): %s", r.status_code, r.text[:200])
    STATUS["last_notify_error"] = f"ntfy {r.status_code}: {r.text[:200]}"
    return False


def send_email(n):
    if not CFG.email_ok or n["prio"] == "low":
        return None
    msg = EmailMessage()
    msg["Subject"] = n["title"][:150]
    msg["From"] = CFG.email_user
    msg["To"] = CFG.email_to
    msg.set_content(n["plain"])
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=20) as s:
            s.login(CFG.email_user, CFG.email_pass)
            s.send_message(msg)
        return True
    except Exception as e:  # noqa: BLE001
        log.error("Email failed: %s", _scrub(e))
        STATUS["last_notify_error"] = f"email: {_scrub(e)}"
        return False


def notify(n):
    """Deliver via every configured channel. True if at least one delivered."""
    if CFG.dry_run:
        OUTBOX.append(n)
        log.info("[DRY RUN] %s\n%s", n["title"], n["plain"])
        return True
    results = [r for r in (send_telegram(n), send_ntfy(n), send_email(n)) if r is not None]
    if not results:
        if CFG.telegram_ok or CFG.ntfy_ok or CFG.email_ok:
            return True  # configured, but nothing eligible for this priority (e.g. email + low)
        log.error("No alert channel configured - nothing was sent.")
        return False
    ok = any(results)
    if ok:
        STATUS["alerts_sent"] += 1
        STATUS["last_alert"] = n["title"]
    return ok


# =====================================================================
# One poll cycle
# =====================================================================


def _record_source(name, ok, count=0, error=None):
    s = STATUS["sources"].setdefault(name, {"fails": 0, "count": 0, "last_error": None,
                                            "last_ok": None, "warned": False})
    if ok:
        s.update(fails=0, count=count, last_error=None, last_ok=utcnow().isoformat(), warned=False)
        return False
    s["fails"] += 1
    s["last_error"] = error
    if s["fails"] >= 3 and not s["warned"]:
        s["warned"] = True
        return True  # caller should warn the user once
    return False


def send_digest(matches, sources_ok, now):
    e = html.escape
    ordered = sorted(matches, key=lambda m: (TIER_RANK[m[1]["tier"]], -m[1]["score"]))
    head_html = (f"✅ <b>Opportunity watcher is online</b>\n"
                 f"Watching {sources_ok} source(s). {len(matches)} open match(es) right now.\n"
                 f"From here on you only get NEW ones.\n")
    head_txt = (f"✅ Opportunity watcher is online\nWatching {sources_ok} source(s). "
                f"{len(matches)} open match(es) right now. From here on you only get NEW ones.\n")
    lines_html, lines_txt, total = [], [], len(head_html)
    for job, ev in ordered[:15]:
        left = ""
        if job["deadline"]:
            left = f" · closes in {human_delta((job['deadline'] - now).total_seconds())}"
        icon = {"urgent": "🚨", "normal": "🟢", "low": "⚪"}[ev["tier"]]
        line_html = f'{icon} <a href="{html.escape(job["url"], quote=True)}">{e(job["title"])}</a> - {e(job["company"] or job["source"])}{e(left)}'
        line_txt = f"{icon} {job['title']} - {job['company'] or job['source']}{left}\n   {job['url']}"
        if total + len(line_html) > 3500:
            break
        lines_html.append(line_html)
        lines_txt.append(line_txt)
        total += len(line_html) + 1
    return notify({"prio": "normal", "title": "Opportunity watcher online",
                   "html": head_html + "\n".join(lines_html), "plain": head_txt + "\n".join(lines_txt),
                   "url": ""})


def run_cycle(state):
    now = utcnow()
    jobs, sources_ok = {}, 0
    for name, fetch in build_sources():
        try:
            items = fetch()
            _record_source(name, True, count=len(items))
            sources_ok += 1
            for j in items:
                jobs.setdefault(j["uid"], j)
        except Exception as e:  # noqa: BLE001
            msg = _scrub(e)[:300]
            log.warning("Source %s failed: %s", name, msg)
            if _record_source(name, False, error=msg):
                notify({"prio": "normal", "title": f"Watcher: {name} keeps failing",
                        "html": f"⚠️ <b>{html.escape(name)}</b> failed 3 times in a row:\n{html.escape(msg)}",
                        "plain": f"⚠️ {name} failed 3 times in a row:\n{msg}", "url": ""})

    STATUS["cycles"] += 1
    STATUS["last_cycle_at"] = utcnow().isoformat()
    if sources_ok == 0:
        STATUS["last_cycle_ok"] = False
        log.error("Every source failed this cycle.")
        return
    STATUS["last_cycle_ok"] = True

    matches = []
    for job in jobs.values():
        ev = evaluate(job, now)
        if ev:
            matches.append((job, ev))
    log.info("Cycle: %d listings fetched, %d match filters, %d new.", len(jobs), len(matches),
             sum(1 for j, _ in matches if not state.seen(j["uid"])))

    # First ever run: one digest instead of a flood, and it doubles as a delivery test.
    if not state.data["initialised"]:
        if send_digest(matches, sources_ok, now):
            for job, _ in matches:
                state.mark_seen(job["uid"])
            state.data["initialised"] = True
            state.save()
        else:
            log.error("Startup message could not be delivered - will retry next cycle. "
                      "Check TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID (see /status).")
        return

    new = [(j, e) for j, e in matches if not state.seen(j["uid"])]
    new.sort(key=lambda m: (TIER_RANK[m[1]["tier"]], -m[1]["score"],
                            m[0]["deadline"] or datetime.max.replace(tzinfo=timezone.utc)))
    for job, ev in new[:CFG.max_alerts_per_cycle]:
        if not notify(build_alert(job, ev, now)):
            n_fail = state.failures[job["uid"]] = state.failures.get(job["uid"], 0) + 1
            if n_fail < 5:
                log.error("Alert not delivered (try %d/5) - leaving it unseen to retry next cycle.", n_fail)
                break
            log.error("Giving up on %r after 5 failed deliveries.", job["title"])
            state.mark_seen(job["uid"])  # a poisoned listing must not block every later alert
            continue
        state.mark_seen(job["uid"])
        if job["deadline"]:  # no point reminding about a window that has already passed
            left_h = (job["deadline"] - now).total_seconds() / 3600.0
            state.mark_reminded([f"{job['uid']}@{t}" for t in (3, 24) if left_h <= t])
        state.save()
        time.sleep(0.4)

    # Deadline reminders for things already alerted on (24h and 3h before)
    sent = 0
    for job, ev in matches:
        if sent >= 5 or ev["tier"] == "low" or not job["deadline"] or not state.seen(job["uid"]):
            continue
        left = (job["deadline"] - now).total_seconds()
        for thr in (3, 24):
            if 0 < left <= thr * 3600:
                keys = [f"{job['uid']}@{t}" for t in (3, 24) if t >= thr]
                if state.reminded(keys[0]):
                    break
                if notify(build_alert(job, ev, now, reminder_hours=thr)):
                    state.mark_reminded(keys)
                    state.save()
                    sent += 1
                    time.sleep(0.4)
                break

    state.prune()
    state.save()


# =====================================================================
# Worker + web server (keeps Render awake; /status explains what is going on)
# =====================================================================


def worker():
    state = State(CFG.state_file)
    log.info("Worker started. %d previously seen listing(s). Poll every %ss.",
             len(state.data["seen"]), CFG.poll_seconds)
    while True:
        try:
            run_cycle(state)
        except Exception:  # noqa: BLE001 - one bad cycle must never kill the bot
            log.exception("Unexpected error in poll cycle")
            STATUS["last_cycle_ok"] = False
            STATUS["last_cycle_at"] = utcnow().isoformat()
        time.sleep(CFG.poll_seconds)


_start_lock = threading.Lock()


def start_worker():
    with _start_lock:
        if WORKER["thread"] is not None:
            return
        STATUS["started_at"] = utcnow().isoformat()
        errs = CFG.errors()
        if errs:
            STATUS["config_error"] = errs
            for e in errs:
                log.error("CONFIG: %s", e)
            if any(e.startswith("No alert channel") for e in errs):
                WORKER["thread"] = False  # nothing useful to run; /status explains why
                return
        t = threading.Thread(target=worker, daemon=True, name="watcher")
        WORKER["thread"] = t
        t.start()


app = Flask(__name__)


@app.route("/")
def health():
    """200 while the poller is alive and recent; 503 otherwise (so UptimeRobot tells you)."""
    t = WORKER["thread"]
    if t is None or t is False:
        return "watcher is NOT running - open /status", 503
    last = STATUS["last_cycle_at"] or STATUS["started_at"]
    age = (utcnow() - parse_dt(last)).total_seconds() if last else 0
    if not t.is_alive() or age > max(600, CFG.poll_seconds * 5):
        return "watcher looks stuck - open /status", 503
    return "Opportunity watcher is running.", 200


@app.route("/status")
def status():
    return jsonify({
        **STATUS,
        "channels": {"telegram": CFG.telegram_ok, "ntfy": CFG.ntfy_ok, "email": CFG.email_ok},
        "poll_seconds": CFG.poll_seconds,
        "worker_alive": bool(WORKER["thread"]) and WORKER["thread"].is_alive(),
    })


def find_chat_id():
    if not CFG.telegram_token:
        print("Set TELEGRAM_BOT_TOKEN first, send your bot a message, then run this again.")
        return 1
    try:
        r = requests.get(f"https://api.telegram.org/bot{CFG.telegram_token}/getUpdates", timeout=20)
        data = r.json()
    except Exception as e:  # noqa: BLE001
        print("Could not reach Telegram:", _scrub(e))
        return 1
    if not data.get("ok"):
        print("Telegram said:", data.get("description", data))
        return 1
    seen = {}
    for u in data.get("result", []):
        m = u.get("message") or u.get("channel_post") or u.get("edited_message") or {}
        chat = m.get("chat")
        if chat:
            seen[chat["id"]] = chat.get("title") or chat.get("username") or chat.get("first_name") or ""
    if not seen:
        print("No messages found. Open your bot in Telegram, press Start, send 'hi', then run this again.")
        return 1
    for cid, label in seen.items():
        print(f"TELEGRAM_CHAT_ID = {cid}   ({label})")
    return 0


if __name__ != "__main__" and not os.environ.get("WATCHER_NO_AUTOSTART"):
    start_worker()  # works under `gunicorn app:app --workers 1` too

if __name__ == "__main__":
    if "--find-chat-id" in sys.argv:
        sys.exit(find_chat_id())
    if "--test" in sys.argv:
        ok = notify({"prio": "urgent", "title": "Watcher test", "html": "✅ <b>Watcher test</b>\nIf you see this, alerts work.",
                     "plain": "✅ Watcher test - if you see this, alerts work.", "url": ""})
        print("Delivered." if ok else f"FAILED: {STATUS['last_notify_error']}")
        sys.exit(0 if ok else 1)
    start_worker()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "10000")))
