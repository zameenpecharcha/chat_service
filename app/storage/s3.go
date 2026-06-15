package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const s3PresignTTL = 15 * time.Minute

// S3Storage implements Storage using AWS S3 (or any S3-compatible endpoint
// such as MinIO, Cloudflare R2, Backblaze B2, DigitalOcean Spaces).
//
// Config options:
//   - AWS S3:   set Region, leave Endpoint empty, use IAM role or key/secret.
//   - MinIO:    set Endpoint (e.g. "http://localhost:9000"), Region="us-east-1",
//     ForcePathStyle=true, AccessKeyID / SecretAccessKey.
//   - Cloudflare R2: Endpoint = "https://<accountid>.r2.cloudflarestorage.com"
type S3Storage struct {
	client        *s3.Client
	presignClient *s3.PresignClient
	bucket        string
}

// S3Config holds all configuration for the S3/compatible storage backend.
type S3Config struct {
	Bucket          string
	Region          string // "us-east-1" for AWS; any value for MinIO
	Endpoint        string // empty → AWS; set for MinIO / R2 / etc.
	AccessKeyID     string // empty → use IAM instance role (AWS only)
	SecretAccessKey string
	ForcePathStyle  bool // must be true for MinIO
}

// NewS3Storage creates an S3Storage from the given config.
// Returns (nil, nil) when Bucket is empty so callers can skip setup.
func NewS3Storage(cfg S3Config) (*S3Storage, error) {
	if cfg.Bucket == "" {
		return nil, nil
	}

	var opts []func(*awsconfig.LoadOptions) error

	// Static credentials (MinIO / explicit keys); falls back to IAM when empty.
	if cfg.AccessKeyID != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		))
	}
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("s3 config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = cfg.ForcePathStyle
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)
	return &S3Storage{
		client:        client,
		presignClient: s3.NewPresignClient(client),
		bucket:        cfg.Bucket,
	}, nil
}

// ObjectKey generates a deterministic, URL-safe object key for a media file.
// Example: "rooms/dm:alice:bob/2026/06/10/<uuid>_photo.jpg"
func ObjectKey(roomID, fileName string) string {
	now := time.Now().UTC()
	return fmt.Sprintf("rooms/%s/%d/%02d/%02d/%s",
		roomID, now.Year(), now.Month(), now.Day(), fileName)
}

// PutPresignedURL returns a short-lived presigned HTTP PUT URL.
// The client uploads directly to S3 — the service never proxies bytes.
func (s *S3Storage) PutPresignedURL(ctx context.Context, key, mimeType string, _ int64) (string, time.Time, error) {
	req, err := s.presignClient.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		ContentType: aws.String(mimeType),
	}, s3.WithPresignExpires(s3PresignTTL))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("presign put: %w", err)
	}
	return req.URL, time.Now().Add(s3PresignTTL), nil
}

// GetPresignedURL returns a short-lived presigned HTTP GET URL.
func (s *S3Storage) GetPresignedURL(ctx context.Context, key string) (string, time.Time, error) {
	req, err := s.presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(s3PresignTTL))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("presign get: %w", err)
	}
	return req.URL, time.Now().Add(s3PresignTTL), nil
}
