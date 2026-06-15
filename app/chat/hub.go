package chat

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"chat-service/app/broker"
	pb "chat-service/app/pb"
)

// Client represents a single connected gRPC stream.
// Fields are exported so the server package can read them.
type Client struct {
	UserID string
	RoomID string
	Send   chan *pb.ServerMessage
}

// NewClient creates a Client with a buffered outbound channel.
func NewClient(userID, roomID string) *Client {
	return &Client{
		UserID: userID,
		RoomID: roomID,
		Send:   make(chan *pb.ServerMessage, 256),
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Hub — sharded for low lock contention at high concurrency
// ─────────────────────────────────────────────────────────────────────────────
// WHY SHARD?
// With a single global sync.RWMutex every Broadcast() call — even for different
// rooms — contends on the same lock. At 100K rooms and ~10K msg/s that becomes
// the bottleneck. Sharding into 64 buckets (by hash of roomID) reduces
// contention by 64×, making each lock approximately idle.

const numShards = 64

type roomShard struct {
	mu    sync.RWMutex
	rooms map[string]*room
}

type room struct {
	clients map[*Client]struct{}
	cancel  func()
}

// Hub manages all active rooms and their connected clients.
// It is safe for concurrent use.
type Hub struct {
	shards   [numShards]roomShard
	broker   broker.Broker
	connCnt  atomic.Int64 // current number of registered clients
	maxConns int64        // 0 = unlimited
}

func NewHub(b broker.Broker) *Hub {
	return NewHubWithLimit(b, 0)
}

func NewHubWithLimit(b broker.Broker, maxConns int64) *Hub {
	if b == nil {
		b = broker.Noop{}
	}
	h := &Hub{broker: b, maxConns: maxConns}
	for i := range h.shards {
		h.shards[i].rooms = make(map[string]*room)
	}
	return h
}

// ConnCount returns the current number of connected clients.
func (h *Hub) ConnCount() int64 { return h.connCnt.Load() }

func (h *Hub) shard(roomID string) *roomShard {
	// FNV-1a hash of roomID, masked to numShards
	var hash uint32 = 2166136261
	for i := 0; i < len(roomID); i++ {
		hash ^= uint32(roomID[i])
		hash *= 16777619
	}
	return &h.shards[hash%numShards]
}

func (h *Hub) Register(ctx context.Context, c *Client) {
	sh := h.shard(c.RoomID)
	sh.mu.Lock()
	r, ok := sh.rooms[c.RoomID]
	if !ok {
		r = &room{clients: make(map[*Client]struct{})}
		ch, cancel, _ := h.broker.Subscribe(ctx, c.RoomID)
		r.cancel = cancel
		go h.forwardFromBroker(c.RoomID, ch)
		sh.rooms[c.RoomID] = r
	}
	r.clients[c] = struct{}{}
	sh.mu.Unlock()
	h.connCnt.Add(1)
}

func (h *Hub) Unregister(c *Client) {
	sh := h.shard(c.RoomID)
	sh.mu.Lock()
	if r, ok := sh.rooms[c.RoomID]; ok {
		delete(r.clients, c)
		close(c.Send)
		if len(r.clients) == 0 {
			if r.cancel != nil {
				r.cancel()
			}
			delete(sh.rooms, c.RoomID)
		}
	}
	sh.mu.Unlock()
	h.connCnt.Add(-1)
}

// AtCapacity returns true when maxConns > 0 and the limit is reached.
// The gRPC server checks this before accepting new streams.
func (h *Hub) AtCapacity() bool {
	return h.maxConns > 0 && h.connCnt.Load() >= h.maxConns
}

// Broadcast delivers msg to every client in the room.
// When publish is true the message is also published to the broker so other
// service instances can forward it to their locally connected clients.
func (h *Hub) Broadcast(ctx context.Context, msg *pb.ServerMessage, publish bool) {
	sh := h.shard(msg.RoomId)
	sh.mu.RLock()
	if r, ok := sh.rooms[msg.RoomId]; ok {
		for c := range r.clients {
			select {
			case c.Send <- msg:
			default:
				// slow consumer: drop rather than block the broadcaster
			}
		}
	}
	sh.mu.RUnlock()

	if publish {
		data, _ := json.Marshal(msg)
		_ = h.broker.Publish(ctx, msg.RoomId, data)
	}
}

func (h *Hub) forwardFromBroker(roomID string, ch <-chan []byte) {
	for raw := range ch {
		var m pb.ServerMessage
		if err := json.Unmarshal(raw, &m); err == nil {
			if m.DeliveredAtUnixMs == 0 {
				m.DeliveredAtUnixMs = time.Now().UnixMilli()
			}
			h.Broadcast(context.Background(), &m, false)
		}
	}
}
