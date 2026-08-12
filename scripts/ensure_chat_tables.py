"""Create chat_* tables on Neon if missing. Safe to re-run."""
from __future__ import annotations

import os
import sys

try:
    import psycopg2
except ImportError:
    os.system(f"{sys.executable} -m pip install psycopg2-binary -q")
    import psycopg2

HOST = os.getenv("POSTGRES_HOST", "ep-noisy-sun-aizw55l4.c-4.us-east-1.aws.neon.tech")
PORT = int(os.getenv("POSTGRES_PORT", "5432"))
USER = os.getenv("POSTGRES_USER", "neondb_owner")
PASSWORD = os.getenv("POSTGRES_PASSWORD", "npg_dcg37brLCZUu")
DB = os.getenv("POSTGRES_DB", "zpc")
SSL = os.getenv("POSTGRES_SSLMODE", "require")

STMTS = [
    "CREATE SCHEMA IF NOT EXISTS public",
    """
CREATE TABLE IF NOT EXISTS public.chat_rooms (
    id              TEXT         PRIMARY KEY,
    room_type       SMALLINT     NOT NULL DEFAULT 0,
    name            TEXT,
    created_by      TEXT         NOT NULL CHECK (created_by <> ''),
    property_id     BIGINT,
    avatar_url      TEXT,
    is_archived     BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
)
""",
    """
CREATE TABLE IF NOT EXISTS public.chat_room_members (
    room_id         TEXT         NOT NULL REFERENCES public.chat_rooms(id) ON DELETE CASCADE,
    user_id         TEXT         NOT NULL CHECK (user_id <> ''),
    role            TEXT         NOT NULL DEFAULT 'member',
    joined_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    left_at         TIMESTAMPTZ,
    muted_until     TIMESTAMPTZ,
    is_pinned       BOOLEAN      NOT NULL DEFAULT FALSE,
    PRIMARY KEY (room_id, user_id)
)
""",
    """
CREATE TABLE IF NOT EXISTS public.chat_room_last_activity (
    room_id          TEXT         PRIMARY KEY REFERENCES public.chat_rooms(id) ON DELETE CASCADE,
    last_message_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_message_id  TEXT,
    last_sender_id   TEXT,
    preview_text     TEXT
)
""",
    """
CREATE TABLE IF NOT EXISTS public.chat_read_receipts (
    room_id          TEXT         NOT NULL REFERENCES public.chat_rooms(id) ON DELETE CASCADE,
    user_id          TEXT         NOT NULL CHECK (user_id <> ''),
    last_read_at     TIMESTAMPTZ  NOT NULL,
    last_read_msg_id TEXT,
    PRIMARY KEY (room_id, user_id)
)
""",
    "CREATE INDEX IF NOT EXISTS idx_chat_rooms_created_by ON public.chat_rooms(created_by)",
    "CREATE INDEX IF NOT EXISTS idx_chat_members_user ON public.chat_room_members(user_id) WHERE left_at IS NULL",
    "CREATE INDEX IF NOT EXISTS idx_last_activity_time ON public.chat_room_last_activity(last_message_at DESC)",
    """
CREATE OR REPLACE FUNCTION public.trg_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $trg$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$trg$
""",
    "DROP TRIGGER IF EXISTS set_chat_rooms_updated_at ON public.chat_rooms",
    """
CREATE TRIGGER set_chat_rooms_updated_at
    BEFORE UPDATE ON public.chat_rooms
    FOR EACH ROW EXECUTE PROCEDURE public.trg_set_updated_at()
""",
]


def main() -> None:
    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        dbname=DB,
        sslmode=SSL,
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SHOW search_path")
    print("search_path:", cur.fetchone()[0])
    cur.execute("SELECT current_schema(), current_database(), current_user")
    print("schema/db/user:", cur.fetchone())
    cur.execute(
        "SELECT schemaname, tablename FROM pg_tables "
        "WHERE tablename LIKE 'chat_%' ORDER BY 1, 2"
    )
    print("before:", cur.fetchall())

    for stmt in STMTS:
        cur.execute(stmt)

    cur.execute(
        "SELECT schemaname, tablename FROM pg_tables "
        "WHERE tablename LIKE 'chat_%' ORDER BY 1, 2"
    )
    print("after:", cur.fetchall())
    cur.close()
    conn.close()
    print("OK — chat tables ready")


if __name__ == "__main__":
    main()
