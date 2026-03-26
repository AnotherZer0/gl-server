# =============================================================================
# CONFIG — edit these values only (junior-friendly)
# =============================================================================

SLACK_BOT_TOKEN = ""
#gli-testing, strike-team
CHANNEL_IDS = ["C0AM3SP0BGE","C08D9RKJAMB"]
SQLITE_DB_FILENAME = "slack_messages.db"
LOG_DIR_NAME = "logs"
REQUEST_TIMEOUT_SECONDS = 30
PAGE_LIMIT = 200
ROLLING_LOOKBACK_HOURS = 24
MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 1
BACKOFF_MAX_SECONDS = 30

# =============================================================================
# End of CONFIG
# =============================================================================

import json
import logging
import random
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

import requests

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / SQLITE_DB_FILENAME
LOG_DIR = BASE_DIR / LOG_DIR_NAME

SLACK_API_BASE = "https://slack.com/api"
# US Eastern for human_time column (uses DST → shows EST or EDT via tzname)
HUMAN_TIME_TZ = ZoneInfo("America/New_York")


def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    day = datetime.now(timezone.utc).date().isoformat()
    log_path = LOG_DIR / f"slack_messages_{day}.log"
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)
    root.addHandler(handler)


def slack_ts_from_unix(ts: float) -> str:
    """Format a Unix timestamp the way Slack uses for ts (string, fractional seconds)."""
    s = f"{ts:.6f}"
    if "." in s:
        head, tail = s.split(".", 1)
        tail = tail.rstrip("0")
        return f"{head}.{tail}" if tail else head
    return s


def parse_slack_ts(ts: str | None) -> float | None:
    if ts is None:
        return None
    try:
        return float(ts)
    except (TypeError, ValueError):
        return None


def slack_ts_to_human_time(ts: str | None) -> str | None:
    """Turn Slack message ts (Unix time string) into US Eastern wall time (EST or EDT)."""
    sec = parse_slack_ts(ts)
    if sec is None:
        return None
    dt = datetime.fromtimestamp(sec, tz=timezone.utc).astimezone(HUMAN_TIME_TZ)
    base = dt.strftime("%Y-%m-%d %H:%M:%S")
    if dt.microsecond:
        frac = f"{dt.microsecond:06d}".rstrip("0")
        if frac:
            base = f"{base}.{frac}"
    tzabbr = dt.tzname() or "ET"
    return f"{base} {tzabbr}"


def slack_ts_for_api(ts: Any) -> str:
    """Ensure message timestamps are JSON strings (Slack requires string `ts`, not numbers)."""
    if ts is None:
        return ""
    if isinstance(ts, (int, float)):
        return slack_ts_from_unix(float(ts))
    s = str(ts).strip()
    return s


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def _migrate_schema(conn: sqlite3.Connection) -> None:
    """Add new columns on existing databases (CREATE IF NOT EXISTS does not update old tables)."""
    if "channel_name" not in _table_columns(conn, "channel_state"):
        conn.execute("ALTER TABLE channel_state ADD COLUMN channel_name TEXT")
    if "channel_name" not in _table_columns(conn, "messages"):
        conn.execute("ALTER TABLE messages ADD COLUMN channel_name TEXT")
    if "user_name" not in _table_columns(conn, "messages"):
        conn.execute("ALTER TABLE messages ADD COLUMN user_name TEXT")
    if "human_time" not in _table_columns(conn, "messages"):
        conn.execute("ALTER TABLE messages ADD COLUMN human_time TEXT")


def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS channel_state (
            channel_id TEXT PRIMARY KEY,
            latest_ts TEXT NOT NULL,
            channel_name TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            channel_id TEXT NOT NULL,
            user TEXT,
            ts TEXT NOT NULL,
            client_msg_id TEXT,
            text TEXT,
            channel_name TEXT,
            user_name TEXT,
            human_time TEXT,
            PRIMARY KEY (channel_id, ts)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            name TEXT,
            email TEXT,
            status TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS channel_summaries (
            summary_date TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            channel_name TEXT,
            summary_kind TEXT NOT NULL,
            response TEXT NOT NULL,
            model TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY (summary_date, channel_id, summary_kind)
        )
        """
    )
    _migrate_schema(conn)
    conn.commit()
    return conn


UNKNOWN_USER_NAME = "Unknown Name"


def slack_user_status(member: dict[str, Any]) -> str:
    profile = member.get("profile") or {}
    if member.get("deleted"):
        return "deleted"
    if member.get("is_bot"):
        return "bot"
    if member.get("is_ultra_restricted"):
        return "single_channel_guest"
    if member.get("is_restricted"):
        return "guest"
    custom = (profile.get("status_text") or "").strip()
    if custom:
        return custom[:500]
    return "active"


def slack_user_display_name(member: dict[str, Any]) -> str:
    profile = member.get("profile") or {}
    for key in (
        "real_name_normalized",
        "real_name",
        "display_name",
    ):
        v = (profile.get(key) or "").strip()
        if v:
            return v
    login = (member.get("name") or "").strip()
    return login or str(member.get("id") or "")


def upsert_slack_user_row(conn: sqlite3.Connection, member: dict[str, Any]) -> None:
    uid = member.get("id")
    if not uid:
        return
    profile = member.get("profile") or {}
    email = profile.get("email")
    if email is not None and str(email).strip() == "":
        email = None
    conn.execute(
        """
        INSERT INTO users (id, name, email, status)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name = excluded.name,
            email = excluded.email,
            status = excluded.status
        """,
        (uid, slack_user_display_name(member), email, slack_user_status(member)),
    )


def resolve_cached_user_display_name(
    conn: sqlite3.Connection,
    logger: logging.Logger,
    session: requests.Session,
    user_id: Any,
    cache: dict[str, str],
) -> str:
    """Prefer `users` table; else users.info + upsert; else UNKNOWN_USER_NAME."""
    if user_id is None or str(user_id).strip() == "":
        return UNKNOWN_USER_NAME
    uid = str(user_id).strip()
    if uid in cache:
        return cache[uid]
    row = conn.execute("SELECT name FROM users WHERE id = ?", (uid,)).fetchone()
    if row and row[0] is not None and str(row[0]).strip():
        name = str(row[0]).strip()
        cache[uid] = name
        return name
    data = slack_post(logger, session, "users.info", {"user": uid})
    if data.get("ok") and isinstance(data.get("user"), dict):
        u = data["user"]
        upsert_slack_user_row(conn, u)
        n = slack_user_display_name(u)
        if n and str(n).strip():
            name = str(n).strip()
            cache[uid] = name
            return name
        logger.warning("users.info returned empty name for user id %s", uid)
    else:
        logger.warning(
            "users.info failed for user id %s: %s",
            uid,
            data.get("error", data),
        )
    cache[uid] = UNKNOWN_USER_NAME
    return UNKNOWN_USER_NAME


def upsert_message(
    conn: sqlite3.Connection,
    channel_id: str,
    msg: dict[str, Any],
    channel_name: str | None,
    logger: logging.Logger,
    session: requests.Session,
    user_name_cache: dict[str, str],
) -> Literal["insert", "update"] | None:
    ts = msg.get("ts")
    if ts is None:
        return None
    user_name = resolve_cached_user_display_name(
        conn, logger, session, msg.get("user"), user_name_cache
    )
    human_time = slack_ts_to_human_time(str(ts))
    existed = (
        conn.execute(
            "SELECT 1 FROM messages WHERE channel_id = ? AND ts = ? LIMIT 1",
            (channel_id, ts),
        ).fetchone()
        is not None
    )
    conn.execute(
        """
        INSERT INTO messages (channel_id, user, ts, client_msg_id, text, channel_name, user_name, human_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(channel_id, ts) DO UPDATE SET
            user = excluded.user,
            client_msg_id = excluded.client_msg_id,
            text = excluded.text,
            channel_name = excluded.channel_name,
            user_name = excluded.user_name,
            human_time = excluded.human_time
        """,
        (
            channel_id,
            msg.get("user"),
            ts,
            msg.get("client_msg_id"),
            msg.get("text"),
            channel_name,
            user_name,
            human_time,
        ),
    )
    return "update" if existed else "insert"


def get_channel_latest_ts(conn: sqlite3.Connection, channel_id: str) -> str | None:
    row = conn.execute(
        "SELECT latest_ts FROM channel_state WHERE channel_id = ?",
        (channel_id,),
    ).fetchone()
    return row[0] if row else None


def get_channel_name_from_state(
    conn: sqlite3.Connection, channel_id: str
) -> str | None:
    row = conn.execute(
        "SELECT channel_name FROM channel_state WHERE channel_id = ?",
        (channel_id,),
    ).fetchone()
    if not row or row[0] is None or str(row[0]).strip() == "":
        return None
    return str(row[0])


def set_channel_latest_ts(
    conn: sqlite3.Connection,
    channel_id: str,
    latest_ts: str,
    channel_name: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO channel_state (channel_id, latest_ts, channel_name)
        VALUES (?, ?, ?)
        ON CONFLICT(channel_id) DO UPDATE SET
            latest_ts = excluded.latest_ts,
            channel_name = excluded.channel_name
        """,
        (channel_id, latest_ts, channel_name),
    )


def max_message_ts_for_channel(
    conn: sqlite3.Connection, channel_id: str
) -> str | None:
    row = conn.execute(
        """
        SELECT ts FROM messages
        WHERE channel_id = ?
        ORDER BY CAST(ts AS REAL) DESC
        LIMIT 1
        """,
        (channel_id,),
    ).fetchone()
    return row[0] if row else None


def compute_oldest_boundary(
    stored_latest: str | None, lookback_hours: int
) -> str | None:
    """If no stored_latest, return None (full history). Else min(stored, now-lookback)."""
    if stored_latest is None:
        return None
    latest_f = parse_slack_ts(stored_latest)
    if latest_f is None:
        return None
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).timestamp()
    boundary = min(latest_f, cutoff)
    return slack_ts_from_unix(boundary)


def slack_form_body(payload: dict[str, Any]) -> dict[str, str]:
    """Slack Web API accepts form-urlencoded; all argument values must be strings."""
    form: dict[str, str] = {}
    for key, val in payload.items():
        if val is None:
            continue
        if key == "cursor" and str(val).strip() == "":
            continue
        form[key] = str(val)
    return form


def slack_post(
    logger: logging.Logger,
    session: requests.Session,
    method: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """POST to Slack Web API with retries; returns parsed JSON dict."""
    url = f"{SLACK_API_BASE}/{method}"
    token = SLACK_BOT_TOKEN.strip()
    headers = {"Authorization": f"Bearer {token}"}
    attempt = 0
    backoff = float(BACKOFF_BASE_SECONDS)
    while True:
        attempt += 1
        try:
            # Use form body (not JSON); some methods reject JSON shapes with invalid_arguments.
            resp = session.post(
                url,
                headers=headers,
                data=slack_form_body(payload),
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt >= MAX_RETRIES:
                raise
            delay = min(backoff, float(BACKOFF_MAX_SECONDS))
            jitter = random.uniform(0, delay * 0.1)
            sleep_s = delay + jitter
            logger.warning(
                "Retry %s/%s for %s: network error %s; sleeping %.2fs",
                attempt,
                MAX_RETRIES,
                method,
                e,
                sleep_s,
            )
            time.sleep(sleep_s)
            backoff = min(backoff * 2, float(BACKOFF_MAX_SECONDS))
            continue

        if resp.status_code == 429:
            if attempt >= MAX_RETRIES:
                resp.raise_for_status()
            ra = resp.headers.get("Retry-After")
            try:
                delay = float(ra) if ra is not None else backoff
            except ValueError:
                delay = backoff
            delay = min(delay, float(BACKOFF_MAX_SECONDS))
            logger.warning(
                "Retry %s/%s for %s: HTTP 429; Retry-After=%s, sleeping %.2fs",
                attempt,
                MAX_RETRIES,
                method,
                ra,
                delay,
            )
            time.sleep(delay)
            backoff = min(backoff * 2, float(BACKOFF_MAX_SECONDS))
            continue

        if 500 <= resp.status_code < 600:
            if attempt >= MAX_RETRIES:
                resp.raise_for_status()
            delay = min(backoff, float(BACKOFF_MAX_SECONDS))
            jitter = random.uniform(0, delay * 0.1)
            sleep_s = delay + jitter
            logger.warning(
                "Retry %s/%s for %s: HTTP %s; sleeping %.2fs",
                attempt,
                MAX_RETRIES,
                method,
                resp.status_code,
                sleep_s,
            )
            time.sleep(sleep_s)
            backoff = min(backoff * 2, float(BACKOFF_MAX_SECONDS))
            continue

        if resp.status_code >= 400 and resp.status_code != 429:
            # Permanent client error — do not retry
            try:
                return resp.json()
            except json.JSONDecodeError:
                resp.raise_for_status()

        try:
            data = resp.json()
        except json.JSONDecodeError as e:
            if attempt >= MAX_RETRIES:
                raise
            delay = min(backoff, float(BACKOFF_MAX_SECONDS))
            logger.warning(
                "Retry %s/%s for %s: invalid JSON %s; sleeping %.2fs",
                attempt,
                MAX_RETRIES,
                method,
                e,
                delay,
            )
            time.sleep(delay)
            backoff = min(backoff * 2, float(BACKOFF_MAX_SECONDS))
            continue

        if not data.get("ok"):
            err = data.get("error", "")
            if err in ("ratelimited", "rate_limited", "service_unavailable"):
                if attempt >= MAX_RETRIES:
                    return data
                ra = resp.headers.get("Retry-After")
                try:
                    delay = float(ra) if ra is not None else float(
                        data.get("retry_after", backoff)
                    )
                except (TypeError, ValueError):
                    delay = backoff
                delay = min(delay, float(BACKOFF_MAX_SECONDS))
                logger.warning(
                    "Retry %s/%s for %s: Slack error=%s; sleeping %.2fs",
                    attempt,
                    MAX_RETRIES,
                    method,
                    err,
                    delay,
                )
                time.sleep(delay)
                backoff = min(backoff * 2, float(BACKOFF_MAX_SECONDS))
                continue
            return data

        return data


def fetch_history_pages(
    logger: logging.Logger,
    session: requests.Session,
    channel_id: str,
    oldest: str | None,
) -> list[dict[str, Any]]:
    """Paginate conversations.history; returns all messages (order as API returns)."""
    all_msgs: list[dict[str, Any]] = []
    cursor: str | None = None
    page = 0
    while True:
        page += 1
        body: dict[str, Any] = {
            "channel": channel_id,
            "limit": PAGE_LIMIT,
        }
        if oldest is not None:
            body["oldest"] = oldest
        if cursor:
            body["cursor"] = cursor

        data = slack_post(logger, session, "conversations.history", body)
        if not data.get("ok"):
            raise RuntimeError(
                f"conversations.history failed: {data.get('error', data)}"
            )

        messages = data.get("messages") or []
        logger.info(
            "Channel %s: history page %s, %s messages",
            channel_id,
            page,
            len(messages),
        )
        all_msgs.extend(messages)

        meta = data.get("response_metadata") or {}
        next_c = meta.get("next_cursor") or ""
        if not next_c:
            break
        cursor = next_c
    return all_msgs


def fetch_replies_pages(
    logger: logging.Logger,
    session: requests.Session,
    channel_id: str,
    thread_ts: str,
) -> list[dict[str, Any]]:
    all_msgs: list[dict[str, Any]] = []
    cursor: str | None = None
    page = 0
    ts_param = slack_ts_for_api(thread_ts)
    chan = str(channel_id).strip()
    if not ts_param:
        logger.warning("Channel %s: skip replies; empty thread_ts after normalize", chan)
        return all_msgs
    while True:
        page += 1
        # Only channel, ts, and optional cursor. Omit `limit` so Slack applies the correct
        # default for this workspace (some installs return invalid_arguments for limit).
        body: dict[str, Any] = {
            "channel": chan,
            "ts": ts_param,
        }
        if cursor:
            body["cursor"] = cursor

        data = slack_post(logger, session, "conversations.replies", body)
        if not data.get("ok"):
            logger.error(
                "conversations.replies failed channel=%s ts=%s response=%s",
                chan,
                ts_param,
                data,
            )
            raise RuntimeError(
                f"conversations.replies failed: {data.get('error', data)}"
            )

        messages = data.get("messages") or []
        logger.info(
            "Channel %s: replies page %s for thread_ts=%s, %s messages",
            chan,
            page,
            ts_param,
            len(messages),
        )
        all_msgs.extend(messages)

        meta = data.get("response_metadata") or {}
        next_c = meta.get("next_cursor") or ""
        if not next_c:
            break
        cursor = next_c
    return all_msgs


def fetch_channel_display_name(
    logger: logging.Logger,
    session: requests.Session,
    channel_id: str,
) -> str:
    """Human-readable channel name from Slack (needs channels:read or equivalent)."""
    chan = str(channel_id).strip()
    data = slack_post(logger, session, "conversations.info", {"channel": chan})
    if not data.get("ok"):
        raise RuntimeError(f"conversations.info failed: {data.get('error', data)}")
    ch = data.get("channel") or {}
    name = ch.get("name")
    if not name:
        raise RuntimeError("conversations.info response missing channel name")
    return str(name)


def resolve_message_channel_name(
    conn: sqlite3.Connection,
    channel_id: str,
    slack_display_name: str,
) -> str:
    """Use channel_state.channel_name when the row exists; otherwise the name from Slack."""
    chan = str(channel_id).strip()
    if get_channel_latest_ts(conn, chan) is not None:
        conn.execute(
            "UPDATE channel_state SET channel_name = ? WHERE channel_id = ?",
            (slack_display_name, chan),
        )
    stored = get_channel_name_from_state(conn, chan)
    return stored if stored is not None else slack_display_name


def process_channel(
    logger: logging.Logger,
    session: requests.Session,
    conn: sqlite3.Connection,
    channel_id: str,
) -> None:
    slack_name = fetch_channel_display_name(logger, session, channel_id)
    channel_name = resolve_message_channel_name(conn, channel_id, slack_name)
    logger.info("Channel %s: display name %r", channel_id, channel_name)

    state_latest = get_channel_latest_ts(conn, channel_id)
    if state_latest is None:
        oldest = None  # full history
        logger.info("Channel %s: no prior state; fetching full history", channel_id)
    else:
        oldest = compute_oldest_boundary(state_latest, ROLLING_LOOKBACK_HOURS)
        logger.info(
            "Channel %s: incremental fetch from oldest=%s (state latest=%s)",
            channel_id,
            oldest,
            state_latest,
        )

    history_messages = fetch_history_pages(logger, session, channel_id, oldest)

    inserts = 0
    updates = 0
    user_name_cache: dict[str, str] = {}

    for msg in history_messages:
        kind = upsert_message(
            conn, channel_id, msg, channel_name, logger, session, user_name_cache
        )
        if kind == "insert":
            inserts += 1
        elif kind == "update":
            updates += 1

        thread_ts = msg.get("thread_ts")
        ts = msg.get("ts")
        reply_count = msg.get("reply_count") or 0
        if thread_ts and (reply_count > 0 or thread_ts == ts):
            t_ins = 0
            t_upd = 0
            try:
                for reply in fetch_replies_pages(
                    logger, session, channel_id, thread_ts
                ):
                    rk = upsert_message(
                        conn,
                        channel_id,
                        reply,
                        channel_name,
                        logger,
                        session,
                        user_name_cache,
                    )
                    if rk == "insert":
                        t_ins += 1
                    elif rk == "update":
                        t_upd += 1
            except RuntimeError as e:
                # One bad thread must not fail the whole channel (Slack may reject some subtypes).
                logger.warning(
                    "Channel %s: skipped thread parent_ts=%s subtype=%s: %s",
                    channel_id,
                    thread_ts,
                    msg.get("subtype"),
                    e,
                )
            else:
                inserts += t_ins
                updates += t_upd
                if t_ins or t_upd:
                    logger.info(
                        "Channel %s: thread %s replies insert=%s update=%s",
                        channel_id,
                        thread_ts,
                        t_ins,
                        t_upd,
                    )

    conn.commit()
    logger.info(
        "Channel %s: message upserts insert=%s update=%s",
        channel_id,
        inserts,
        updates,
    )

    max_ts = max_message_ts_for_channel(conn, channel_id)
    if max_ts is not None:
        set_channel_latest_ts(conn, channel_id, max_ts, channel_name)
        conn.commit()
        logger.info("Channel %s: channel_state.latest_ts set to %s", channel_id, max_ts)
    else:
        # Avoid re-downloading full history on every run for empty channels.
        now_ts = slack_ts_from_unix(time.time())
        set_channel_latest_ts(conn, channel_id, now_ts, channel_name)
        conn.commit()
        logger.info(
            "Channel %s: no messages; channel_state.latest_ts set to now (%s)",
            channel_id,
            now_ts,
        )


def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Script start: fetching Slack messages into %s", DB_PATH)

    conn = open_db()
    session = requests.Session()

    ok_channels = 0
    fail_channels = 0

    for channel_id in CHANNEL_IDS:
        try:
            logger.info("Processing channel %s", channel_id)
            process_channel(logger, session, conn, channel_id)
            ok_channels += 1
            logger.info("Finished channel %s successfully", channel_id)
        except Exception as e:
            fail_channels += 1
            logger.exception("Error processing channel %s: %s", channel_id, e)

    conn.close()
    logger.info(
        "Done. Channels OK=%s failed=%s (see log file under %s)",
        ok_channels,
        fail_channels,
        LOG_DIR,
    )


if __name__ == "__main__":
    main()
