package storage

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const presignTTL = 15 * time.Minute

// MinioStorage implements Storage using a MinIO (or S3-compatible) backend.
// Clients receive presigned PUT URLs to upload files directly; the service
// never proxies file bytes, keeping the gRPC transport lean.
type MinioStorage struct {
	client *minio.Client
	bucket string
}

// NewMinioStorage connects to a MinIO/S3-compatible endpoint.
// Returns (nil, nil) when endpoint is empty so callers can skip setup.
func NewMinioStorage(endpoint, accessKey, secretKey, bucket string, useSSL bool) (*MinioStorage, error) {
	if endpoint == "" {
		return nil, nil
	}
	c, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("minio connect: %w", err)
	}
	return &MinioStorage{client: c, bucket: bucket}, nil
}

// EnsureBucket creates the bucket if it does not already exist.
func (m *MinioStorage) EnsureBucket(ctx context.Context) error {
	exists, err := m.client.BucketExists(ctx, m.bucket)
	if err != nil {
		return fmt.Errorf("check bucket: %w", err)
	}
	if !exists {
		if err := m.client.MakeBucket(ctx, m.bucket, minio.MakeBucketOptions{}); err != nil {
			return fmt.Errorf("create bucket: %w", err)
		}
	}
	return nil
}

// PutPresignedURL returns a presigned HTTP PUT URL valid for presignTTL.
func (m *MinioStorage) PutPresignedURL(ctx context.Context, key, _ string, _ int64) (string, time.Time, error) {
	u, err := m.client.PresignedPutObject(ctx, m.bucket, key, presignTTL)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("presign put: %w", err)
	}
	return u.String(), time.Now().Add(presignTTL), nil
}

// GetPresignedURL returns a presigned HTTP GET URL valid for presignTTL.
func (m *MinioStorage) GetPresignedURL(ctx context.Context, key string) (string, time.Time, error) {
	u, err := m.client.PresignedGetObject(ctx, m.bucket, key, presignTTL, url.Values{})
	if err != nil {
		return "", time.Time{}, fmt.Errorf("presign get: %w", err)
	}
	return u.String(), time.Now().Add(presignTTL), nil
}
