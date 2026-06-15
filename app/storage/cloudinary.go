package storage

import (
	"context"
	"crypto/sha1"
	"fmt"
	"sort"
	"strings"
	"time"
)

// CloudinaryStorage implements the Storage interface using Cloudinary.
//
// Upload flow (same contract as S3/MinIO — no client code change needed):
//  1. Client calls RequestUpload gRPC → gets a signed Cloudinary upload URL.
//  2. Client POSTs the file to that URL (multipart form: field "file").
//  3. Cloudinary stores it under the given public_id (= media_key).
//  4. Recipients call GetDownloadUrl gRPC → get the CDN delivery URL.
//
// Swap to S3: change STORAGE_BACKEND=s3 in .env — no code change required.
type CloudinaryStorage struct {
	cloudName string
	apiKey    string
	apiSecret string
}

// NewCloudinaryStorage creates a CloudinaryStorage.
// Returns nil when cloudName is empty so callers can skip setup cleanly.
func NewCloudinaryStorage(cloudName, apiKey, apiSecret string) *CloudinaryStorage {
	if cloudName == "" || apiKey == "" || apiSecret == "" {
		return nil
	}
	return &CloudinaryStorage{
		cloudName: cloudName,
		apiKey:    apiKey,
		apiSecret: apiSecret,
	}
}

// PutPresignedURL returns a signed Cloudinary upload endpoint.
// The client POSTs the file as a multipart form field named "file" to this URL.
// All required auth params (api_key, timestamp, signature) are embedded as
// query parameters so the client needs no Cloudinary credentials.
func (c *CloudinaryStorage) PutPresignedURL(_ context.Context, key, _ string, _ int64) (string, time.Time, error) {
	ts := time.Now().Unix()
	expiresAt := time.Now().Add(15 * time.Minute)

	params := map[string]string{
		"public_id": key,
		"timestamp": fmt.Sprintf("%d", ts),
	}
	sig := c.sign(params)

	uploadURL := fmt.Sprintf(
		"https://api.cloudinary.com/v1_1/%s/auto/upload?api_key=%s&timestamp=%d&public_id=%s&signature=%s",
		c.cloudName, c.apiKey, ts, key, sig,
	)
	return uploadURL, expiresAt, nil
}

// GetPresignedURL returns the Cloudinary CDN delivery URL for an uploaded asset.
// Cloudinary CDN URLs don't expire by default — they're served from a global CDN.
// The media_key stored in the message IS the Cloudinary public_id.
func (c *CloudinaryStorage) GetPresignedURL(_ context.Context, key string) (string, time.Time, error) {
	// "auto/upload" resource type covers images, videos, raw files (PDF, etc.)
	url := fmt.Sprintf("https://res.cloudinary.com/%s/auto/upload/%s", c.cloudName, key)
	// Return a far-future expiry — CDN URLs don't expire unless you enable signed delivery
	expiresAt := time.Now().Add(24 * time.Hour * 365)
	return url, expiresAt, nil
}

// sign computes the Cloudinary API request signature.
// Algorithm: SHA-1( sorted_key=value&...&key=value + apiSecret )
func (c *CloudinaryStorage) sign(params map[string]string) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(params))
	for _, k := range keys {
		parts = append(parts, k+"="+params[k])
	}

	toSign := strings.Join(parts, "&") + c.apiSecret
	h := sha1.New()
	h.Write([]byte(toSign))
	return fmt.Sprintf("%x", h.Sum(nil))
}
