-- ═══════════════════════════════════════════════════════════════════════════════
-- ZPC Chat – PostgreSQL Schema (rooms + membership)
-- ═══════════════════════════════════════════════════════════════════════════════
-- These tables live in the SAME PostgreSQL database as the existing `users` table.
--
-- User IDs are stored as TEXT (the gRPC layer treats them as strings).
-- A CHECK constraint enforces non-empty strings instead of a FK to users(id),
-- because user_id values come from the auth token and may be UUIDs or integers
-- represented as strings.
--
-- Messages themselves are stored in Cassandra (see cassandra_chat.cql).
-- PostgreSQL handles the relational, low-cardinality metadata:
--   rooms, room_members, last_activity, read_receipts
-- ═══════════════════════════════════════════════════════════════════════════════


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: chat_rooms
--
-- One row per room (DM or group).
-- room_id matches the Cassandra partition key so both databases stay in sync
-- without an extra lookup (e.g. "dm:alice:bob" or "group:<uuid>").
--
-- Columns written today by Go code:
--   id, room_type, name, created_by, property_id, created_at
-- Columns defined for future use (not yet written by Go):
--   avatar_url   – group avatar or property thumbnail
--   is_archived  – soft-delete / hide from inbox
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS chat_rooms (
    id              TEXT         PRIMARY KEY,               -- "dm:alice:bob" | "group:<uuid>"
    room_type       SMALLINT     NOT NULL DEFAULT 0,        -- 0=DM  1=GROUP
    name            TEXT,                                   -- NULL for DMs, required for groups
    created_by      TEXT         NOT NULL
                        CHECK (created_by <> ''),           -- user ID string (from auth token)
    property_id     BIGINT,                                 -- optional FK to properties table
    avatar_url      TEXT,                                   -- future: group avatar URL
    is_archived     BOOLEAN      NOT NULL DEFAULT FALSE,    -- future: hide from inbox
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Useful for "rooms created by user X" queries
CREATE INDEX IF NOT EXISTS idx_chat_rooms_created_by ON chat_rooms(created_by);

-- NOTE: idx_chat_rooms_property is intentionally omitted.
-- property_id is written by Go code but only when explicitly set; an index
-- would be beneficial only after a SetPropertyID API is implemented.


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: chat_room_members
--
-- Many-to-many: which users are in which rooms.
--
-- Columns written today by Go code:
--   room_id, user_id  (role/muted_until/is_pinned use column defaults)
-- Columns defined for future use:
--   left_at     – set when a user leaves a group (currently members never leave)
--   muted_until – push notification mute expiry
--   is_pinned   – user has pinned this conversation
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS chat_room_members (
    room_id         TEXT         NOT NULL REFERENCES chat_rooms(id) ON DELETE CASCADE,
    user_id         TEXT         NOT NULL CHECK (user_id <> ''),
    role            TEXT         NOT NULL DEFAULT 'member', -- 'member' | 'admin'
    joined_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    left_at         TIMESTAMPTZ,                            -- NULL = still member
    muted_until     TIMESTAMPTZ,                            -- NULL = not muted
    is_pinned       BOOLEAN      NOT NULL DEFAULT FALSE,

    PRIMARY KEY (room_id, user_id)
);

-- Critical: used by GetUserRooms() and GetRoom() — "all active members of room X"
CREATE INDEX IF NOT EXISTS idx_chat_members_user ON chat_room_members(user_id)
    WHERE left_at IS NULL;


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: chat_room_last_activity
--
-- Maintained by UpdateLastActivity() on every new message.
-- Drives inbox sidebar ordering without hitting Cassandra for timestamps.
--
-- All columns are actively written by Go code.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS chat_room_last_activity (
    room_id         TEXT         PRIMARY KEY REFERENCES chat_rooms(id) ON DELETE CASCADE,
    last_message_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_message_id TEXT,                                  -- Cassandra message UUID
    last_sender_id  TEXT,                                  -- user ID string
    preview_text    TEXT                                   -- truncated for sidebar display
);

-- Critical: used for ORDER BY last_message_at DESC in inbox queries
CREATE INDEX IF NOT EXISTS idx_last_activity_time ON chat_room_last_activity(last_message_at DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: chat_read_receipts
--
-- Maintained by UpdateReadReceipt() on every read receipt event and on
-- GetMessages() calls (auto-mark-as-read).
-- Cassandra is the source of truth; this table enables fast SQL unread-count
-- queries (e.g. "how many unread rooms does agent X have?").
--
-- All columns are actively written by Go code.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS chat_read_receipts (
    room_id          TEXT         NOT NULL REFERENCES chat_rooms(id) ON DELETE CASCADE,
    user_id          TEXT         NOT NULL CHECK (user_id <> ''),
    last_read_at     TIMESTAMPTZ  NOT NULL,
    last_read_msg_id TEXT,

    PRIMARY KEY (room_id, user_id)
);


-- ─────────────────────────────────────────────────────────────────────────────
-- Trigger: auto-update updated_at on chat_rooms
-- Required because Go code does not explicitly set updated_at on UPDATE.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION trg_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS set_chat_rooms_updated_at ON chat_rooms;
CREATE TRIGGER set_chat_rooms_updated_at
    BEFORE UPDATE ON chat_rooms
    FOR EACH ROW EXECUTE FUNCTION trg_set_updated_at();


-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW: v_user_inbox
--
-- Used by client apps / reporting to build the conversation sidebar.
-- Not queried by the Go service itself (Go uses GetUserRooms + Cassandra).
-- Usage: SELECT * FROM v_user_inbox WHERE user_id = '42' ORDER BY last_message_at DESC LIMIT 20;
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_user_inbox AS
SELECT
    m.user_id,
    r.id              AS room_id,
    r.room_type,
    r.name            AS room_name,
    r.avatar_url,
    la.last_message_at,
    la.preview_text,
    la.last_sender_id,
    CASE
        WHEN rr.last_read_at IS NULL          THEN 1
        WHEN la.last_message_at > rr.last_read_at THEN 1
        ELSE 0
    END               AS has_unread,
    m.is_pinned,
    m.muted_until
FROM  chat_room_members       m
JOIN  chat_rooms              r  ON r.id = m.room_id
LEFT  JOIN chat_room_last_activity la ON la.room_id = r.id
LEFT  JOIN chat_read_receipts  rr ON rr.room_id = r.id AND rr.user_id = m.user_id
WHERE m.left_at IS NULL
  AND r.is_archived = FALSE;


-- ─────────────────────────────────────────────────────────────────────────────
-- VIEW: v_dm_rooms
--
-- Used by client apps to find an existing DM room given two user IDs.
-- Not queried by Go (Go uses GetOrCreateDM which computes the deterministic ID).
-- Usage: SELECT room_id FROM v_dm_rooms WHERE user_ids @> ARRAY['alice', 'bob'];
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_dm_rooms AS
SELECT
    r.id AS room_id,
    ARRAY_AGG(m.user_id ORDER BY m.user_id) AS user_ids
FROM  chat_rooms        r
JOIN  chat_room_members m ON m.room_id = r.id AND m.left_at IS NULL
WHERE r.room_type = 0
GROUP BY r.id;



-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: chat_rooms
--
-- One row per room (DM or group).
-- room_id matches the string used as the Cassandra partition key so both
-- databases can be joined by application code without an extra lookup.
--
-- Real-estate context: a room can optionally be linked to a property listing
-- (e.g. "DM about listing #42" or "Buyer group for 10 Maple St").
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS chat_rooms (
    id              TEXT         PRIMARY KEY,                    -- "dm:alice:bob" | "group:<uuid>"
    room_type       SMALLINT     NOT NULL DEFAULT 0,            -- 0=DM  1=GROUP
    name            TEXT,                                        -- NULL for DMs, required for groups
    created_by      BIGINT       NOT NULL
                        REFERENCES users(id) ON DELETE SET NULL,
    property_id     BIGINT,                                      -- optional FK to properties table
    avatar_url      TEXT,                                        -- group avatar / property thumbnail
    is_archived     BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chat_rooms_property ON chat_rooms(property_id)
    WHERE property_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_chat_rooms_created_by ON chat_rooms(created_by);


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: chat_room_members
--
-- Many-to-many: which users are in which rooms.
-- role: 'member' (default) | 'admin' (can add/remove members in groups)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS chat_room_members (
    room_id         TEXT         NOT NULL REFERENCES chat_rooms(id) ON DELETE CASCADE,
    user_id         BIGINT       NOT NULL REFERENCES users(id)      ON DELETE CASCADE,
    role            TEXT         NOT NULL DEFAULT 'member',         -- 'member' | 'admin'
    joined_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    left_at         TIMESTAMPTZ,                                    -- NULL = still member
    muted_until     TIMESTAMPTZ,                                    -- NULL = not muted
    is_pinned       BOOLEAN      NOT NULL DEFAULT FALSE,

    PRIMARY KEY (room_id, user_id)
);

-- Fast lookup: "give me all rooms for user X" (for building the sidebar)
CREATE INDEX IF NOT EXISTS idx_chat_members_user ON chat_room_members(user_id)
    WHERE left_at IS NULL;


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: chat_room_last_activity
--
-- Maintained by the application on every new message (or via trigger).
-- Drives sidebar ordering without hitting Cassandra for timestamps.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS chat_room_last_activity (
    room_id         TEXT         PRIMARY KEY REFERENCES chat_rooms(id) ON DELETE CASCADE,
    last_message_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_message_id TEXT,                                          -- Cassandra message UUID
    last_sender_id  BIGINT       REFERENCES users(id) ON DELETE SET NULL,
    preview_text    TEXT                                           -- truncated for sidebar
);

CREATE INDEX IF NOT EXISTS idx_last_activity_time ON chat_room_last_activity(last_message_at DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: chat_read_receipts  (mirror of Cassandra read_receipts in Postgres)
--
-- Optional: keep a PG copy so SQL reports ("unread counts per agent") are fast.
-- The Cassandra table is the source of truth; this is a reporting replica.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS chat_read_receipts (
    room_id         TEXT         NOT NULL REFERENCES chat_rooms(id) ON DELETE CASCADE,
    user_id         BIGINT       NOT NULL REFERENCES users(id)      ON DELETE CASCADE,
    last_read_at    TIMESTAMPTZ  NOT NULL,
    last_read_msg_id TEXT,

    PRIMARY KEY (room_id, user_id)
);


-- ─────────────────────────────────────────────────────────────────────────────
-- Trigger: auto-update updated_at on chat_rooms
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION trg_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS set_chat_rooms_updated_at ON chat_rooms;
CREATE TRIGGER set_chat_rooms_updated_at
    BEFORE UPDATE ON chat_rooms
    FOR EACH ROW EXECUTE FUNCTION trg_set_updated_at();


-- ─────────────────────────────────────────────────────────────────────────────
-- Helpful VIEWS
-- ─────────────────────────────────────────────────────────────────────────────

-- View: user inbox — all active rooms for a user with last activity
-- Usage: SELECT * FROM v_user_inbox WHERE user_id = 42 ORDER BY last_message_at DESC LIMIT 20;
CREATE OR REPLACE VIEW v_user_inbox AS
SELECT
    m.user_id,
    r.id              AS room_id,
    r.room_type,
    r.name            AS room_name,
    r.avatar_url,
    la.last_message_at,
    la.preview_text,
    la.last_sender_id,
    -- unread = messages after user's last read
    CASE
        WHEN rr.last_read_at IS NULL THEN 1   -- never opened = has unread
        WHEN la.last_message_at > rr.last_read_at THEN 1
        ELSE 0
    END               AS has_unread,
    m.is_pinned,
    m.muted_until
FROM  chat_room_members       m
JOIN  chat_rooms              r  ON r.id = m.room_id
LEFT  JOIN chat_room_last_activity la ON la.room_id = r.id
LEFT  JOIN chat_read_receipts  rr ON rr.room_id = r.id AND rr.user_id = m.user_id
WHERE m.left_at IS NULL
  AND r.is_archived = FALSE;


-- View: DM room lookup — given two users, find the room_id in one query
-- Usage: SELECT room_id FROM v_dm_rooms WHERE user_ids @> ARRAY[1::bigint, 2::bigint];
CREATE OR REPLACE VIEW v_dm_rooms AS
SELECT
    r.id AS room_id,
    ARRAY_AGG(m.user_id ORDER BY m.user_id) AS user_ids
FROM  chat_rooms        r
JOIN  chat_room_members m ON m.room_id = r.id AND m.left_at IS NULL
WHERE r.room_type = 0   -- DM only
GROUP BY r.id;
