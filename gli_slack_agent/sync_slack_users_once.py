# =============================================================================
# ONE-OFF: sync entire Slack workspace user list into SQLite `users` table.
# Run once:  python sync_slack_users_once.py
#
# Requires bot scopes: users:read  (and users:read.email if you want emails)
# =============================================================================

# Use your bot token here, or leave "replace_me" to reuse SLACK_BOT_TOKEN from message_downloader.py
SLACK_BOT_TOKEN = "replace_me"
SQLITE_DB_FILENAME = "slack_messages.db"
# users.list allows up to 200 per page
USER_PAGE_LIMIT = 200

# =============================================================================
# End of CONFIG
# =============================================================================

import logging
import sqlite3
import sys
from pathlib import Path
from typing import Any

import requests

# Reuse Slack HTTP + retry logic from message_downloader (same folder).
import message_downloader as md

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / SQLITE_DB_FILENAME


def ensure_users_table(conn: sqlite3.Connection) -> None:
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


def fetch_all_users(
    logger: logging.Logger, session: requests.Session
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    cursor: str | None = None
    page = 0
    while True:
        page += 1
        body: dict[str, Any] = {"limit": USER_PAGE_LIMIT}
        if cursor:
            body["cursor"] = cursor
        data = md.slack_post(logger, session, "users.list", body)
        if not data.get("ok"):
            raise RuntimeError(f"users.list failed: {data.get('error', data)}")
        members = data.get("members") or []
        logger.info("users.list page %s: %s members", page, len(members))
        out.extend(members)
        meta = data.get("response_metadata") or {}
        next_c = (meta.get("next_cursor") or "").strip()
        if not next_c:
            break
        cursor = next_c
    return out


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logger = logging.getLogger("sync_slack_users_once")

    token = SLACK_BOT_TOKEN.strip()
    if not token or token == "replace_me":
        token = md.SLACK_BOT_TOKEN.strip()
    if not token or token == "replace_me":
        logger.error(
            "Set SLACK_BOT_TOKEN in this file or in message_downloader.py CONFIG."
        )
        sys.exit(1)

    md.SLACK_BOT_TOKEN = token

    if not DB_PATH.is_file():
        logger.info("Creating database at %s", DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_users_table(conn)
        session = requests.Session()
        members = fetch_all_users(logger, session)
        for m in members:
            md.upsert_slack_user_row(conn, m)
        conn.commit()
        logger.info("Done. Upserted %s users into %s", len(members), DB_PATH)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
