package chat

// RedisPresenceStore is a Redis-backed online/last-seen tracker.
//
// WHY REDIS FOR PRESENCE AT SCALE
// ────────────────────────────────
// The in-process presenceStore in grpc_server.go has two fatal problems at scale:
//
//   1. Not visible across pods — if user A is connected to pod-1 and user B checks
//      presence from pod-2, pod-2 sees A as offline because its local map is empty.
//
//   2. Lost on restart — when a pod crashes or rolls out during a deploy, all
//      presence data is gone. Users see everyone as offline momentarily.
//
// Redis solves both:
//   • Every pod writes the same key → all pods read the same truth.
//   • TTL-based heartbeat: an online user's key expires in 60 s; the server
//     refreshes it every 30 s. If the pod crashes without sending an offline event
//     the key expires naturally — eventual consistency within 60 s.
//
// KEY DESIGN
//   chat:presence:<userID>   →   "1"          (TTL: presenceTTL)
//   chat:lastseen:<userID>   →   "<unix-ms>"  (no TTL — permanent)
//
// SCALE NUMBERS
//   • 1M online users × ~50 bytes per key ≈ 50 MB RAM in Redis — trivial.
//   • 1M heartbeat writes every 30 s ≈ 33 K writes/sec — well within Redis limits.

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	pb "chat-service/app/pb"
)

const (
	presenceTTL       = 60 * time.Second
	presenceRefresh   = 30 * time.Second
	presenceKeyPrefix = "chat:presence:"
	lastSeenPrefix    = "chat:lastseen:"
)

// PresenceStore is the interface both the in-process and Redis implementations satisfy.
// grpc_server.go depends on this interface so the backend can be swapped via config.
type PresenceStore interface {
	SetOnline(ctx context.Context, userID string, online bool)
	Get(ctx context.Context, userIDs []string) []*pb.PresenceInfo
}

// ─────────────────────────────────────────────────────────────────────────────
// RedisPresenceStore
// ─────────────────────────────────────────────────────────────────────────────

type RedisPresenceStore struct {
	client *redis.Client
}

func NewRedisPresenceStore(client *redis.Client) *RedisPresenceStore {
	return &RedisPresenceStore{client: client}
}

func (r *RedisPresenceStore) SetOnline(ctx context.Context, userID string, online bool) {
	if online {
		// Set presence key with TTL; heartbeat goroutine will refresh it
		r.client.Set(ctx, presenceKeyPrefix+userID, "1", presenceTTL)
	} else {
		// Remove presence key immediately on clean disconnect
		r.client.Del(ctx, presenceKeyPrefix+userID)
		// Persist last-seen timestamp permanently
		r.client.Set(ctx, lastSeenPrefix+userID, strconv.FormatInt(time.Now().UnixMilli(), 10), 0)
	}
}

func (r *RedisPresenceStore) Get(ctx context.Context, userIDs []string) []*pb.PresenceInfo {
	result := make([]*pb.PresenceInfo, 0, len(userIDs))
	for _, uid := range userIDs {
		info := &pb.PresenceInfo{UserId: uid}

		// Check online (key exists iff TTL > 0)
		n, err := r.client.Exists(ctx, presenceKeyPrefix+uid).Result()
		info.IsOnline = err == nil && n > 0

		// Last seen (stored as unix-ms string)
		if !info.IsOnline {
			if ms, err := r.client.Get(ctx, lastSeenPrefix+uid).Result(); err == nil {
				info.LastSeenUnixMs, _ = strconv.ParseInt(ms, 10, 64)
			}
		}
		result = append(result, info)
	}
	return result
}

// StartHeartbeat keeps the presence key alive for userID while the stream is open.
// Call it in a goroutine; it stops when ctx is cancelled (i.e. when the gRPC stream closes).
func (r *RedisPresenceStore) StartHeartbeat(ctx context.Context, userID string) {
	ticker := time.NewTicker(presenceRefresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.client.Expire(ctx, presenceKeyPrefix+userID, presenceTTL)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// LocalPresenceStore  (single-process fallback — used when Redis is absent)
// ─────────────────────────────────────────────────────────────────────────────

type LocalPresenceStore struct {
	mu   sync.RWMutex
	data map[string]*pb.PresenceInfo
}

func NewLocalPresenceStore() *LocalPresenceStore {
	return &LocalPresenceStore{data: make(map[string]*pb.PresenceInfo)}
}

func (l *LocalPresenceStore) SetOnline(_ context.Context, userID string, online bool) {
	l.mu.Lock()
	info, ok := l.data[userID]
	if !ok {
		info = &pb.PresenceInfo{UserId: userID}
		l.data[userID] = info
	}
	info.IsOnline = online
	if !online {
		info.LastSeenUnixMs = time.Now().UnixMilli()
	}
	l.mu.Unlock()
}

func (l *LocalPresenceStore) Get(_ context.Context, userIDs []string) []*pb.PresenceInfo {
	l.mu.RLock()
	defer l.mu.RUnlock()
	result := make([]*pb.PresenceInfo, 0, len(userIDs))
	for _, uid := range userIDs {
		if info, ok := l.data[uid]; ok {
			result = append(result, info)
		} else {
			result = append(result, &pb.PresenceInfo{UserId: uid, IsOnline: false})
		}
	}
	return result
}
