package chat

import (
    "context"
    "encoding/json"
    "sync"
    "time"

    "chat-service/internal/broker"
    pb "chat-service/internal/pb"
)

type Client struct {
    userID string
    roomID string
    send   chan *pb.ServerMessage
}

type room struct {
    clients map[*Client]struct{}
    cancel  func()
}

type Hub struct {
    mu      sync.RWMutex
    rooms   map[string]*room
    broker  broker.Broker
}

func NewHub(b broker.Broker) *Hub {
    if b == nil { b = broker.Noop{} }
    return &Hub{rooms: make(map[string]*room), broker: b}
}

func (h *Hub) Register(ctx context.Context, c *Client) {
    h.mu.Lock()
    r, ok := h.rooms[c.roomID]
    if !ok {
        r = &room{clients: make(map[*Client]struct{})}
        // subscribe to broker for cross-instance messages
        ch, cancel, _ := h.broker.Subscribe(ctx, c.roomID)
        r.cancel = cancel
        go h.forwardFromBroker(c.roomID, ch)
        h.rooms[c.roomID] = r
    }
    r.clients[c] = struct{}{}
    h.mu.Unlock()
}

func (h *Hub) Unregister(c *Client) {
    h.mu.Lock()
    if r, ok := h.rooms[c.roomID]; ok {
        delete(r.clients, c)
        close(c.send)
        if len(r.clients) == 0 {
            if r.cancel != nil { r.cancel() }
            delete(h.rooms, c.roomID)
        }
    }
    h.mu.Unlock()
}

func (h *Hub) Broadcast(ctx context.Context, msg *pb.ServerMessage, publish bool) {
    h.mu.RLock()
    r, ok := h.rooms[msg.RoomId]
    if ok {
        for c := range r.clients {
            select {
            case c.send <- msg:
            default:
            }
        }
    }
    h.mu.RUnlock()
    if publish {
        // fan-out across instances
        data, _ := json.Marshal(msg)
        _ = h.broker.Publish(ctx, msg.RoomId, data)
    }
}

func (h *Hub) forwardFromBroker(roomID string, ch <-chan []byte) {
    for b := range ch {
        var m pb.ServerMessage
        if err := json.Unmarshal(b, &m); err == nil {
            // mark as delivered now if not set
            if m.DeliveredAtUnixMs == 0 {
                m.DeliveredAtUnixMs = time.Now().UnixMilli()
            }
            h.Broadcast(context.Background(), &m, false)
        }
    }
}


