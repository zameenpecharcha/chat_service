package repository

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// RoomMeta mirrors chat.RoomMeta but lives in the repository layer to avoid
// import cycles. The application maps between the two as needed.
type RoomMeta struct {
	RoomID     string
	RoomType   int    // 0=DM 1=GROUP
	Name       string // empty for DMs
	CreatedBy  string // user ID string (matches users.id cast to text)
	PropertyID *int64 // nullable FK to properties table
	MemberIDs  []string
	CreatedAt  time.Time
}

// RoomRepository persists room metadata and membership in PostgreSQL.
type RoomRepository struct {
	db *sql.DB
}

// NewRoomRepository opens a PostgreSQL connection.
// dsn example: "host=localhost port=5432 user=root password=secret dbname=zpc sslmode=disable"
func NewRoomRepository(dsn string) (*RoomRepository, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres open: %w", err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.PingContext(context.Background()); err != nil {
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	return &RoomRepository{db: db}, nil
}

// Close releases the database connection pool.
func (r *RoomRepository) Close() error { return r.db.Close() }

// Migrate creates the chat schema tables if they don't exist yet.
// Idempotent — safe to call on every startup.
// Tables are created in public to avoid search_path / Neon pooler surprises.
func (r *RoomRepository) Migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE SCHEMA IF NOT EXISTS public`,
		`CREATE TABLE IF NOT EXISTS public.chat_rooms (
			id              TEXT         PRIMARY KEY,
			room_type       SMALLINT     NOT NULL DEFAULT 0,
			name            TEXT,
			created_by      TEXT         NOT NULL CHECK (created_by <> ''),
			property_id     BIGINT,
			avatar_url      TEXT,
			is_archived     BOOLEAN      NOT NULL DEFAULT FALSE,
			created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
			updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS public.chat_room_members (
			room_id         TEXT         NOT NULL REFERENCES public.chat_rooms(id) ON DELETE CASCADE,
			user_id         TEXT         NOT NULL CHECK (user_id <> ''),
			role            TEXT         NOT NULL DEFAULT 'member',
			joined_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
			left_at         TIMESTAMPTZ,
			muted_until     TIMESTAMPTZ,
			is_pinned       BOOLEAN      NOT NULL DEFAULT FALSE,
			PRIMARY KEY (room_id, user_id)
		)`,
		`CREATE TABLE IF NOT EXISTS public.chat_room_last_activity (
			room_id          TEXT         PRIMARY KEY REFERENCES public.chat_rooms(id) ON DELETE CASCADE,
			last_message_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
			last_message_id  TEXT,
			last_sender_id   TEXT,
			preview_text     TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS public.chat_read_receipts (
			room_id          TEXT         NOT NULL REFERENCES public.chat_rooms(id) ON DELETE CASCADE,
			user_id          TEXT         NOT NULL CHECK (user_id <> ''),
			last_read_at     TIMESTAMPTZ  NOT NULL,
			last_read_msg_id TEXT,
			PRIMARY KEY (room_id, user_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_chat_rooms_created_by ON public.chat_rooms(created_by)`,
		`CREATE INDEX IF NOT EXISTS idx_chat_members_user ON public.chat_room_members(user_id) WHERE left_at IS NULL`,
		`CREATE INDEX IF NOT EXISTS idx_last_activity_time ON public.chat_room_last_activity(last_message_at DESC)`,
		`CREATE OR REPLACE FUNCTION public.trg_set_updated_at()
		RETURNS TRIGGER LANGUAGE plpgsql AS $trg$
		BEGIN
			NEW.updated_at = NOW();
			RETURN NEW;
		END;
		$trg$`,
		`DROP TRIGGER IF EXISTS set_chat_rooms_updated_at ON public.chat_rooms`,
		// EXECUTE PROCEDURE works on PG11–16; FUNCTION is PG14+ alias.
		`CREATE TRIGGER set_chat_rooms_updated_at
			BEFORE UPDATE ON public.chat_rooms
			FOR EACH ROW EXECUTE PROCEDURE public.trg_set_updated_at()`,
	}
	for _, stmt := range stmts {
		if _, err := r.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate: %w\nstmt: %s", err, stmt)
		}
	}
	return nil
}

// UpsertRoom inserts a room row (idempotent — ON CONFLICT DO NOTHING).
func (r *RoomRepository) UpsertRoom(ctx context.Context, m *RoomMeta) error {
	var propertyID sql.NullInt64
	if m.PropertyID != nil {
		propertyID = sql.NullInt64{Int64: *m.PropertyID, Valid: true}
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO chat_rooms (id, room_type, name, created_by, property_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $6)
		ON CONFLICT (id) DO NOTHING`,
		m.RoomID, m.RoomType, nullString(m.Name), m.CreatedBy, propertyID, m.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert room: %w", err)
	}
	// Upsert all members
	for _, uid := range m.MemberIDs {
		if _, err := r.db.ExecContext(ctx, `
			INSERT INTO chat_room_members (room_id, user_id)
			VALUES ($1, $2)
			ON CONFLICT (room_id, user_id) DO NOTHING`,
			m.RoomID, uid,
		); err != nil {
			return fmt.Errorf("upsert member %s: %w", uid, err)
		}
	}
	return nil
}

// GetRoom returns room metadata and current members.
func (r *RoomRepository) GetRoom(ctx context.Context, roomID string) (*RoomMeta, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, room_type, COALESCE(name,''), created_by, created_at
		FROM   chat_rooms
		WHERE  id = $1`, roomID)

	var m RoomMeta
	if err := row.Scan(&m.RoomID, &m.RoomType, &m.Name, &m.CreatedBy, &m.CreatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get room: %w", err)
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT user_id FROM chat_room_members
		WHERE  room_id = $1 AND left_at IS NULL`, roomID)
	if err != nil {
		return nil, fmt.Errorf("get members: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var uid string
		if err := rows.Scan(&uid); err != nil {
			return nil, err
		}
		m.MemberIDs = append(m.MemberIDs, uid)
	}
	return &m, rows.Err()
}

// GetOrCreateDM returns the existing DM room for the pair, or creates it.
// Room ID is deterministic: "dm:<sortedA>:<sortedB>".
func (r *RoomRepository) GetOrCreateDM(ctx context.Context, userA, userB, createdBy string) (*RoomMeta, error) {
	ids := []string{userA, userB}
	sort.Strings(ids)
	roomID := "dm:" + strings.Join(ids, ":")

	existing, err := r.GetRoom(ctx, roomID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return existing, nil
	}
	m := &RoomMeta{
		RoomID:    roomID,
		RoomType:  0,
		CreatedBy: createdBy,
		MemberIDs: ids,
		CreatedAt: time.Now().UTC(),
	}
	return m, r.UpsertRoom(ctx, m)
}

// CreateGroup creates a new group room with a UUID room ID.
func (r *RoomRepository) CreateGroup(ctx context.Context, roomID, name, createdBy string, memberIDs []string) (*RoomMeta, error) {
	m := &RoomMeta{
		RoomID:    roomID,
		RoomType:  1,
		Name:      name,
		CreatedBy: createdBy,
		MemberIDs: memberIDs,
		CreatedAt: time.Now().UTC(),
	}
	return m, r.UpsertRoom(ctx, m)
}

// UpdateLastActivity records the most-recent message metadata for a room.
func (r *RoomRepository) UpdateLastActivity(ctx context.Context,
	roomID, msgID, senderID, preview string, at time.Time) error {

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO chat_room_last_activity
		  (room_id, last_message_at, last_message_id, last_sender_id, preview_text)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (room_id) DO UPDATE SET
		  last_message_at = EXCLUDED.last_message_at,
		  last_message_id = EXCLUDED.last_message_id,
		  last_sender_id  = EXCLUDED.last_sender_id,
		  preview_text    = EXCLUDED.preview_text`,
		roomID, at, msgID, senderID, preview,
	)
	return err
}

// UpdateReadReceipt sets the last-read cursor for a user in a room.
func (r *RoomRepository) UpdateReadReceipt(ctx context.Context,
	roomID, userID, msgID string, at time.Time) error {

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO chat_read_receipts (room_id, user_id, last_read_at, last_read_msg_id)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (room_id, user_id) DO UPDATE SET
		  last_read_at     = EXCLUDED.last_read_at,
		  last_read_msg_id = EXCLUDED.last_read_msg_id`,
		roomID, userID, at, msgID,
	)
	return err
}

// GetUserRooms returns all active room IDs a user belongs to.
func (r *RoomRepository) GetUserRooms(ctx context.Context, userID string) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT room_id FROM chat_room_members
		WHERE  user_id = $1 AND left_at IS NULL`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var roomIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		roomIDs = append(roomIDs, id)
	}
	return roomIDs, rows.Err()
}

// UserRoom is a room combined with last-message preview and unread flag.
type UserRoom struct {
	RoomID        string
	RoomType      int    // 0=DM 1=GROUP
	Name          string // empty for DMs
	LastMessage   string
	LastMessageAt int64  // Unix ms (0 if no messages yet)
	HasUnread     bool
	MemberIDs     []string
}

// GetUserRoomsDetailed returns all active rooms for a user with last-message
// metadata and a per-room unread flag, sorted by most-recent activity first.
func (r *RoomRepository) GetUserRoomsDetailed(ctx context.Context, userID string) ([]UserRoom, error) {
	// Step 1: fetch all active rooms for this user including room metadata.
	roomRows, err := r.db.QueryContext(ctx, `
		SELECT cr.id, cr.room_type, COALESCE(cr.name, '')
		FROM   chat_rooms         cr
		JOIN   chat_room_members  m  ON m.room_id = cr.id
		WHERE  m.user_id = $1 AND m.left_at IS NULL`, userID)
	if err != nil {
		return nil, fmt.Errorf("GetUserRoomsDetailed rooms: %w", err)
	}
	defer roomRows.Close()

	type roomBase struct {
		id       string
		roomType int
		name     string
	}
	var bases []roomBase
	for roomRows.Next() {
		var b roomBase
		if err := roomRows.Scan(&b.id, &b.roomType, &b.name); err != nil {
			return nil, err
		}
		bases = append(bases, b)
	}
	if err := roomRows.Err(); err != nil {
		return nil, err
	}
	if len(bases) == 0 {
		return nil, nil
	}

	// Step 2: for each room, collect members, last activity and unread status.
	result := make([]UserRoom, 0, len(bases))
	for _, b := range bases {
		// Members
		mRows, err := r.db.QueryContext(ctx, `
			SELECT user_id FROM chat_room_members
			WHERE  room_id = $1 AND left_at IS NULL`, b.id)
		if err != nil {
			return nil, fmt.Errorf("GetUserRoomsDetailed members: %w", err)
		}
		var memberIDs []string
		for mRows.Next() {
			var uid string
			if err := mRows.Scan(&uid); err != nil {
				mRows.Close()
				return nil, err
			}
			memberIDs = append(memberIDs, uid)
		}
		mRows.Close()

		// Last activity
		var lastMsg string
		var lastAt int64
		laRow := r.db.QueryRowContext(ctx, `
			SELECT COALESCE(preview_text,''), COALESCE(EXTRACT(EPOCH FROM last_message_at)*1000,0)::BIGINT
			FROM   chat_room_last_activity
			WHERE  room_id = $1`, b.id)
		_ = laRow.Scan(&lastMsg, &lastAt)

		// Unread flag: last activity after this user's read cursor, and the
		// last message was NOT sent by this user (own sends must not badge).
		var hasUnread bool
		rrRow := r.db.QueryRowContext(ctx, `
			SELECT CASE
			  WHEN la.last_message_at IS NULL THEN false
			  WHEN la.last_sender_id IS NOT NULL AND la.last_sender_id = $2 THEN false
			  WHEN rr.last_read_at   IS NULL THEN true
			  WHEN la.last_message_at > rr.last_read_at THEN true
			  ELSE false
			END
			FROM  chat_room_last_activity la
			LEFT  JOIN chat_read_receipts rr
				  ON rr.room_id = la.room_id AND rr.user_id = $2
			WHERE la.room_id = $1`, b.id, userID)
		_ = rrRow.Scan(&hasUnread)

		result = append(result, UserRoom{
			RoomID:        b.id,
			RoomType:      b.roomType,
			Name:          b.name,
			LastMessage:   lastMsg,
			LastMessageAt: lastAt,
			HasUnread:     hasUnread,
			MemberIDs:     memberIDs,
		})
	}

	// Sort by last activity descending (most recent first).
	sort.Slice(result, func(i, j int) bool {
		return result[i].LastMessageAt > result[j].LastMessageAt
	})

	return result, nil
}

// ── helpers ──────────────────────────────────────────────────────────────────

func nullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

