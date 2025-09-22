package broker

import (
    "context"
    "fmt"

    "github.com/redis/go-redis/v9"
)

type RedisBroker struct {
    client *redis.Client
}

func NewRedisBroker(addr, password string) *RedisBroker {
    if addr == "" {
        return nil
    }
    rdb := redis.NewClient(&redis.Options{Addr: addr, Password: password})
    return &RedisBroker{client: rdb}
}

func (r *RedisBroker) channel(roomID string) string { return fmt.Sprintf("room:%s", roomID) }

func (r *RedisBroker) Publish(ctx context.Context, roomID string, data []byte) error {
    if r == nil || r.client == nil { return nil }
    return r.client.Publish(ctx, r.channel(roomID), data).Err()
}

func (r *RedisBroker) Subscribe(ctx context.Context, roomID string) (<-chan []byte, func(), error) {
    ch := make(chan []byte, 128)
    if r == nil || r.client == nil {
        cancel := func() { close(ch) }
        return ch, cancel, nil
    }
    ps := r.client.Subscribe(ctx, r.channel(roomID))
    // start reader goroutine
    go func() {
        defer close(ch)
        for {
            msg, err := ps.ReceiveMessage(ctx)
            if err != nil { return }
            if msg != nil { ch <- []byte(msg.Payload) }
        }
    }()
    cancel := func() { _ = ps.Close() }
    return ch, cancel, nil
}


