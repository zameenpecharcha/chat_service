package chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// RoomKind mirrors the proto RoomType enum without creating an import cycle.
type RoomKind int32

const (
	RoomKindDM    RoomKind = 0
	RoomKindGroup RoomKind = 1
)

// RoomMeta holds persistent metadata about a chat room.
type RoomMeta struct {
	RoomID    string    `json:"room_id"`
	Name      string    `json:"name"` // empty for DMs
	Kind      RoomKind  `json:"kind"`
	CreatedBy string    `json:"created_by"`
	CreatedAt time.Time `json:"created_at"`
	MemberIDs []string  `json:"member_ids"`
}

// RoomStore persists room metadata in Redis when available, or in-process
// memory otherwise. Memory fallback is suitable for single-instance dev use.
type RoomStore struct {
	rdb *redis.Client
	mu  sync.RWMutex
	mem map[string]*RoomMeta
}

// NewRoomStore creates a RoomStore. Pass nil rdb to use the memory fallback.
func NewRoomStore(rdb *redis.Client) *RoomStore {
	return &RoomStore{rdb: rdb, mem: make(map[string]*RoomMeta)}
}

// CreateDM returns the existing DM room for the pair, or creates one.
// The room_id is deterministic: "dm:<lower_id>:<higher_id>".
func (s *RoomStore) CreateDM(ctx context.Context, userA, userB, createdBy string) (*RoomMeta, error) {
	if userA == "" || userB == "" {
		return nil, errors.New("both member IDs are required for a DM")
	}
	ids := []string{userA, userB}
	sort.Strings(ids)
	roomID := "dm:" + strings.Join(ids, ":")

	if existing, _ := s.Get(ctx, roomID); existing != nil {
		return existing, nil
	}
	meta := &RoomMeta{
		RoomID:    roomID,
		Name:      "",
		Kind:      RoomKindDM,
		CreatedBy: createdBy,
		CreatedAt: time.Now().UTC(),
		MemberIDs: ids,
	}
	return meta, s.save(ctx, meta)
}

// CreateGroup creates a new group room with a UUID-based room_id.
func (s *RoomStore) CreateGroup(ctx context.Context, name, createdBy string, memberIDs []string) (*RoomMeta, error) {
	if name == "" {
		return nil, errors.New("group name is required")
	}
	if len(memberIDs) < 2 {
		return nil, errors.New("a group requires at least 2 members")
	}
	meta := &RoomMeta{
		RoomID:    "group:" + uuid.NewString(),
		Name:      name,
		Kind:      RoomKindGroup,
		CreatedBy: createdBy,
		CreatedAt: time.Now().UTC(),
		MemberIDs: memberIDs,
	}
	return meta, s.save(ctx, meta)
}

// Get retrieves room metadata by room_id. Returns (nil, nil) when not found.
func (s *RoomStore) Get(ctx context.Context, roomID string) (*RoomMeta, error) {
	if s.rdb != nil {
		data, err := s.rdb.Get(ctx, s.key(roomID)).Bytes()
		if err == redis.Nil {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("redis get room: %w", err)
		}
		var meta RoomMeta
		if err := json.Unmarshal(data, &meta); err != nil {
			return nil, fmt.Errorf("unmarshal room: %w", err)
		}
		return &meta, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mem[roomID], nil
}

func (s *RoomStore) save(ctx context.Context, meta *RoomMeta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal room: %w", err)
	}
	if s.rdb != nil {
		return s.rdb.Set(ctx, s.key(meta.RoomID), data, 0).Err()
	}
	s.mu.Lock()
	s.mem[meta.RoomID] = meta
	s.mu.Unlock()
	return nil
}

func (s *RoomStore) key(roomID string) string {
	return "chat:room:" + roomID
}
