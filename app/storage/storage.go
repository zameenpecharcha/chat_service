package storage

import (
	"context"
	"time"
)

// Storage abstracts object storage for media uploads and downloads.
// Clients upload directly to object storage via a presigned PUT URL;
// the resulting media_key is embedded in the chat message so recipients
// can fetch a presigned GET URL to download the file.
type Storage interface {
	// PutPresignedURL returns a short-lived HTTP PUT URL the client uses
	// to upload a file directly to object storage without credentials.
	PutPresignedURL(ctx context.Context, key, mimeType string, sizeBytes int64) (url string, expiresAt time.Time, err error)

	// GetPresignedURL returns a short-lived HTTP GET URL for downloading
	// an already-uploaded object.
	GetPresignedURL(ctx context.Context, key string) (url string, expiresAt time.Time, err error)
}
