package broker

import (
    "context"
)

type Broker interface {
    Publish(ctx context.Context, roomID string, data []byte) error
    Subscribe(ctx context.Context, roomID string) (<-chan []byte, func(), error)
}

// Noop broker is used when no external pub/sub is configured
type Noop struct{}

func (Noop) Publish(ctx context.Context, roomID string, data []byte) error { return nil }
func (Noop) Subscribe(ctx context.Context, roomID string) (<-chan []byte, func(), error) {
    ch := make(chan []byte)
    cancel := func() { close(ch) }
    return ch, cancel, nil
}


