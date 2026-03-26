# =============================================================================
# CONFIG — OpenAI (edit API key and model)
# =============================================================================

OPENAI_API_KEY = ""
# Docs: https://platform.openai.com/docs/models — use gpt-5.4-nano or gpt-5-nano, etc.
OPENAI_MODEL = "gpt-5.4-nano"

# Optional short system line sent on every call (set to "" to skip)
OPENAI_SYSTEM_PROMPT = "You write clear, factual summaries of Slack activity. Be concise."

# =============================================================================
# CONFIG — Database (same SQLite file as message_downloader.py)
# =============================================================================

SQLITE_DB_FILENAME = "slack_messages.db"

# =============================================================================
# CONFIG — Time window and timezone for summary_date labels (America/New_York)
# =============================================================================

SUMMARY_TIMEZONE = "America/New_York"
LOOKBACK_HOURS = 24

# =============================================================================
# CONFIG — DAILY: one prompt per channel_id (Slack C… / G… id)
# Messages from the last LOOKBACK_HOURS are listed under your text.
# Only channels listed here get a daily (and then weekly/monthly) summary.
# =============================================================================

CHANNEL_PROMPTS_DAILY = {
    "C0AM3SP0BGE": """Summarize this channel for leadership. Focus on decisions, blockers, and owners.
Use bullet points. Mention dates when relevant.""",
    "C08D9RKJAMB": """Summarize support themes and unresolved customer issues.""",
}

# =============================================================================
# CONFIG — WEEKLY (runs on Sunday, America/New_York): roll up the last 7 daily rows
# Per-channel override; if a channel id is missing, DEFAULT_WEEKLY_PROMPT is used.
# =============================================================================

DEFAULT_WEEKLY_PROMPT = """Below are daily summaries for one Slack channel for the past seven calendar days (labeled by date).
Produce one concise weekly summary: themes, progress, risks, and what to watch next week.
Do not repeat boilerplate from each day unless it matters for the week."""

CHANNEL_PROMPTS_WEEKLY: dict[str, str] = {
    # "C123...": """Custom weekly instructions for this channel...""",
}

# =============================================================================
# CONFIG — MONTHLY (runs on the last calendar day of each month in SUMMARY_TIMEZONE)
# Rolls up all weekly summaries stored for that calendar month for each channel.
# =============================================================================

DEFAULT_MONTHLY_PROMPT = """Below are weekly summaries for one Slack channel for a single calendar month (labeled by week-ending date).
Produce one executive monthly summary: outcomes, trends, open risks, and recommended follow-ups."""

CHANNEL_PROMPTS_MONTHLY: dict[str, str] = {}

# =============================================================================
# End of CONFIG
# =============================================================================

import logging
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from openai import OpenAI

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / SQLITE_DB_FILENAME
TZ = ZoneInfo(SUMMARY_TIMEZONE)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def open_db() -> sqlite3.Connection:
    if not DB_PATH.is_file():
        logging.getLogger(__name__).warning("Database will be created at %s", DB_PATH)
    return sqlite3.connect(DB_PATH)


def ensure_channel_summaries_table(conn: sqlite3.Connection) -> None:
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


def resolve_channel_name(conn: sqlite3.Connection, channel_id: str) -> str:
    row = conn.execute(
        "SELECT channel_name FROM channel_state WHERE channel_id = ?",
        (channel_id,),
    ).fetchone()
    if row and row[0] and str(row[0]).strip():
        return str(row[0]).strip()
    row = conn.execute(
        """
        SELECT channel_name FROM messages
        WHERE channel_id = ? AND channel_name IS NOT NULL AND trim(channel_name) != ''
        LIMIT 1
        """,
        (channel_id,),
    ).fetchone()
    if row and row[0]:
        return str(row[0]).strip()
    return channel_id


def fetch_messages_window(
    conn: sqlite3.Connection, channel_id: str, cutoff_unix: float
) -> list[str]:
    rows = conn.execute(
        """
        SELECT human_time, user_name, text
        FROM messages
        WHERE channel_id = ?
          AND CAST(ts AS REAL) >= ?
        ORDER BY CAST(ts AS REAL) ASC
        """,
        (channel_id, cutoff_unix),
    ).fetchall()
    lines: list[str] = []
    for human_time, user_name, text in rows:
        who = user_name or "?"
        when = human_time or "?"
        body = (text or "").replace("\r\n", "\n").strip()
        lines.append(f"[{when}] {who}: {body}")
    return lines


def save_summary(
    conn: sqlite3.Connection,
    summary_date: str,
    channel_id: str,
    channel_name: str,
    summary_kind: str,
    response: str,
    model: str,
) -> None:
    created = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO channel_summaries (
            summary_date, channel_id, channel_name, summary_kind, response, model, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(summary_date, channel_id, summary_kind) DO UPDATE SET
            channel_name = excluded.channel_name,
            response = excluded.response,
            model = excluded.model,
            created_at = excluded.created_at
        """,
        (
            summary_date,
            channel_id,
            channel_name,
            summary_kind,
            response,
            model,
            created,
        ),
    )


def call_openai(client: OpenAI, user_content: str) -> str:
    messages = []
    sys_p = (OPENAI_SYSTEM_PROMPT or "").strip()
    if sys_p:
        messages.append({"role": "system", "content": sys_p})
    messages.append({"role": "user", "content": user_content})
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=messages,
    )
    choice = resp.choices[0].message
    text = (choice.content or "").strip()
    if not text:
        raise RuntimeError("OpenAI returned empty content")
    return text


def run_daily_for_channel(
    conn: sqlite3.Connection,
    client: OpenAI,
    channel_id: str,
    report_date: date,
    cutoff_unix: float,
) -> None:
    log = logging.getLogger(__name__)
    ch_name = resolve_channel_name(conn, channel_id)
    prompt = CHANNEL_PROMPTS_DAILY.get(channel_id)
    if not prompt:
        log.warning("No CHANNEL_PROMPTS_DAILY for %s — skipping", channel_id)
        return
    lines = fetch_messages_window(conn, channel_id, cutoff_unix)
    date_s = report_date.isoformat()
    if not lines:
        save_summary(
            conn,
            date_s,
            channel_id,
            ch_name,
            "daily",
            "No Slack messages in the configured lookback window.",
            "",
        )
        log.info("Daily %s %s: no messages, stored placeholder", channel_id, date_s)
        return
    body = "\n".join(lines)
    user_content = f"{prompt.strip()}\n\n--- Messages (chronological) ---\n{body}"
    text = call_openai(client, user_content)
    save_summary(conn, date_s, channel_id, ch_name, "daily", text, OPENAI_MODEL)
    log.info("Daily %s %s: stored (%s chars)", channel_id, date_s, len(text))


def run_weekly_for_channel(
    conn: sqlite3.Connection, client: OpenAI, channel_id: str, today: date
) -> None:
    log = logging.getLogger(__name__)
    if today.weekday() != 6:
        return
    ch_name = resolve_channel_name(conn, channel_id)
    start = today - timedelta(days=6)
    rows = conn.execute(
        """
        SELECT summary_date, response
        FROM channel_summaries
        WHERE channel_id = ?
          AND summary_kind = 'daily'
          AND summary_date >= ?
          AND summary_date <= ?
        ORDER BY summary_date ASC
        """,
        (channel_id, start.isoformat(), today.isoformat()),
    ).fetchall()
    if not rows:
        log.info("Weekly %s: no daily rows in [%s .. %s], skip", channel_id, start, today)
        return
    prompt = CHANNEL_PROMPTS_WEEKLY.get(channel_id) or DEFAULT_WEEKLY_PROMPT
    parts = [f"{d}:\n{r.strip()}" for d, r in rows]
    blob = "\n\n---\n\n".join(parts)
    user_content = f"{prompt.strip()}\n\n--- Daily summaries ---\n{blob}"
    text = call_openai(client, user_content)
    save_summary(conn, today.isoformat(), channel_id, ch_name, "weekly", text, OPENAI_MODEL)
    log.info("Weekly %s (week ending %s): stored", channel_id, today.isoformat())


def last_day_of_month(d: date) -> date:
    if d.month == 12:
        return date(d.year, 12, 31)
    nxt = date(d.year, d.month + 1, 1)
    return nxt - timedelta(days=1)


def run_monthly_for_channel(
    conn: sqlite3.Connection, client: OpenAI, channel_id: str, today: date
) -> None:
    log = logging.getLogger(__name__)
    if today != last_day_of_month(today):
        return
    ch_name = resolve_channel_name(conn, channel_id)
    first = date(today.year, today.month, 1)
    last = today.isoformat()
    first_s = first.isoformat()
    rows = conn.execute(
        """
        SELECT summary_date, response
        FROM channel_summaries
        WHERE channel_id = ?
          AND summary_kind = 'weekly'
          AND summary_date >= ?
          AND summary_date <= ?
        ORDER BY summary_date ASC
        """,
        (channel_id, first_s, last),
    ).fetchall()
    if not rows:
        log.info("Monthly %s: no weekly rows in %s-%02d, skip", channel_id, today.year, today.month)
        return
    prompt = CHANNEL_PROMPTS_MONTHLY.get(channel_id) or DEFAULT_MONTHLY_PROMPT
    parts = [f"Week ending {d}:\n{r.strip()}" for d, r in rows]
    blob = "\n\n---\n\n".join(parts)
    user_content = f"{prompt.strip()}\n\n--- Weekly summaries ---\n{blob}"
    text = call_openai(client, user_content)
    # One row per channel per month; summary_date = month end (Eastern calendar)
    save_summary(conn, last, channel_id, ch_name, "monthly", text, OPENAI_MODEL)
    log.info("Monthly %s (%s): stored", channel_id, last)


def main() -> None:
    setup_logging()
    log = logging.getLogger(__name__)

    key = OPENAI_API_KEY.strip()
    if not key or key == "replace_me":
        log.error("Set OPENAI_API_KEY in summarize_slack_gpt.py CONFIG.")
        sys.exit(1)

    if not CHANNEL_PROMPTS_DAILY:
        log.error("CHANNEL_PROMPTS_DAILY is empty — add at least one channel_id and prompt.")
        sys.exit(1)

    conn = open_db()
    try:
        ensure_channel_summaries_table(conn)
        client = OpenAI(api_key=key)

        now_utc = datetime.now(timezone.utc)
        now_local = now_utc.astimezone(TZ)
        today = now_local.date()
        cutoff_unix = now_utc.timestamp() - LOOKBACK_HOURS * 3600

        channel_ids = list(CHANNEL_PROMPTS_DAILY.keys())
        for cid in channel_ids:
            try:
                run_daily_for_channel(conn, client, cid, today, cutoff_unix)
                time.sleep(0.3)
            except Exception as e:
                log.exception("Daily failed for %s: %s", cid, e)

        conn.commit()

        for cid in channel_ids:
            try:
                run_weekly_for_channel(conn, client, cid, today)
                time.sleep(0.3)
            except Exception as e:
                log.exception("Weekly failed for %s: %s", cid, e)

        conn.commit()

        for cid in channel_ids:
            try:
                run_monthly_for_channel(conn, client, cid, today)
                time.sleep(0.3)
            except Exception as e:
                log.exception("Monthly failed for %s: %s", cid, e)

        conn.commit()
        log.info("Finished. Summaries in table channel_summaries on %s", DB_PATH)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
