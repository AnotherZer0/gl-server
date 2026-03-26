# =============================================================================
# SQL EXPLORER — edit the QUERY block below, then run: python explore_db.py
# =============================================================================
# Database file lives next to this script (same folder as message_downloader.py).

SQLITE_DB_FILENAME = "slack_messages.db"

# ---------------------------------------------------------------------------
# YOUR QUERY — change everything between the quotes; keep the triple-quotes.
# Example messages: SELECT human_time, user_name, text FROM messages ORDER BY CAST(ts AS REAL) DESC LIMIT 50;
# Messages Schema: channel_id, user, ts, client_msg_id, text, channel_name, user_name, human_time
# channel_summaries: summary_date, channel_id, channel_name, summary_kind (daily|weekly|monthly), response, model, created_at
# ---------------------------------------------------------------------------
QUERY = """
SELECT channel_name, user_name, human_time, text
FROM messages
where channel_id == 'C08D9RKJAMB'
ORDER BY ts DESC
LIMIT 100;
"""
# ---------------------------------------------------------------------------

# =============================================================================
# End of easy-edit section (you normally do not need to change below)
# =============================================================================

import sqlite3
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / SQLITE_DB_FILENAME


def main() -> None:
    if not DB_PATH.is_file():
        print(f"Database not found: {DB_PATH}", file=sys.stderr)
        sys.exit(1)

    sql = QUERY.strip()
    if not sql:
        print("QUERY is empty — edit the QUERY variable at the top of explore_db.py.", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(sql)

        # SELECT-style: print rows. Other statements (PRAGMA, etc.) still work.
        if cur.description:
            cols = [d[0] for d in cur.description]
            print(" | ".join(cols))
            print("-" * min(120, max(40, sum(len(c) for c in cols) + 3 * len(cols))))
            for row in cur.fetchall():
                print(" | ".join("" if v is None else str(v) for v in row))
        else:
            conn.commit()
            print(f"OK (rowcount={cur.rowcount})")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
