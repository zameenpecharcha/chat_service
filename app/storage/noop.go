package storage

import (
	"context"
	"errors"
	"time"
)

// Noop is a Storage implementation used when no object storage is configured.
// All operations return an error so callers can surface a meaningful message.
type Noop struct{}

func (Noop) PutPresignedURL(_ context.Context, _, _ string, _ int64) (string, time.Time, error) {
	return "", time.Time{}, errors.New("object storage not configured")
}

func (Noop) GetPresignedURL(_ context.Context, _ string) (string, time.Time, error) {
	return "", time.Time{}, errors.New("object storage not configured")
}
