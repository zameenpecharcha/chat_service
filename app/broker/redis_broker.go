package broker

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// NewRedisClient creates a Redis client.
// If url is non-empty (e.g. "rediss://default:pass@host:6379") it takes
// precedence and addr/password are ignored — this is the correct way to
// connect to Upstash Redis which requires TLS (rediss://).
// Returns nil when both url and addr are empty.
func NewRedisClient(url, addr, password string) *redis.Client {
	if url != "" {
		opt, err := redis.ParseURL(url)
		if err != nil {
			return nil
		}
		return redis.NewClient(opt)
	}
	if addr == "" {
		return nil
	}
	return redis.NewClient(&redis.Options{Addr: addr, Password: password})
}

type RedisBroker struct {
	client *redis.Client
}

// NewRedisBroker creates a RedisBroker with its own Redis client.
func NewRedisBroker(addr, password string) *RedisBroker {
	return NewRedisBrokerFromClient(NewRedisClient("", addr, password))
}

// NewRedisBrokerFromClient wraps an existing Redis client.
// Useful when the same *redis.Client is shared with other components.
func NewRedisBrokerFromClient(client *redis.Client) *RedisBroker {
	if client == nil {
		return nil
	}
	return &RedisBroker{client: client}
}

func (r *RedisBroker) channel(roomID string) string { return fmt.Sprintf("room:%s", roomID) }

func (r *RedisBroker) Publish(ctx context.Context, roomID string, data []byte) error {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.Publish(ctx, r.channel(roomID), data).Err()
}

func (r *RedisBroker) Subscribe(ctx context.Context, roomID string) (<-chan []byte, func(), error) {
	ch := make(chan []byte, 128)
	if r == nil || r.client == nil {
		cancel := func() { close(ch) }
		return ch, cancel, nil
	}
	ps := r.client.Subscribe(ctx, r.channel(roomID))
	go func() {
		defer close(ch)
		for {
			msg, err := ps.ReceiveMessage(ctx)
			if err != nil {
				return
			}
			if msg != nil {
				ch <- []byte(msg.Payload)
			}
		}
	}()
	cancel := func() { _ = ps.Close() }
	return ch, cancel, nil
}
