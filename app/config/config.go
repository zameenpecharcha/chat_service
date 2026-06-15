package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

type Config struct {
	ServiceName string `yaml:"service_name"`
	Port        int    `yaml:"port"`
	LogLevel    string `yaml:"log_level"`
	// RedisURL is a full connection URL e.g. "rediss://default:pass@host:6379"
	// Used for Upstash (TLS required) or any Redis that provides a URL.
	// When set, RedisAddr and RedisPassword are ignored.
	RedisURL      string `yaml:"redis_url"`
	RedisAddr     string `yaml:"redis_addr"`
	RedisPassword string `yaml:"redis_password"`

	// ── Object storage (MinIO kept for local dev; S3 for production) ─────────
	// Set StorageBackend to "s3" or "minio" (default = "minio").
	StorageBackend string `yaml:"storage_backend"` // "minio" | "s3"
	MinioBucket    string `yaml:"minio_bucket"`
	// MinIO-specific
	MinioEndpoint  string `yaml:"minio_endpoint"`
	MinioAccessKey string `yaml:"minio_access_key"`
	MinioSecretKey string `yaml:"minio_secret_key"`
	MinioUseSSL    bool   `yaml:"minio_use_ssl"`
	// AWS S3-specific (also works for Cloudflare R2, DigitalOcean Spaces)
	S3Bucket          string `yaml:"s3_bucket"`
	S3Region          string `yaml:"s3_region"`
	S3Endpoint        string `yaml:"s3_endpoint"` // empty = real AWS
	S3AccessKeyID     string `yaml:"s3_access_key_id"`
	S3SecretAccessKey string `yaml:"s3_secret_access_key"`
	S3ForcePathStyle  bool   `yaml:"s3_force_path_style"` // true for MinIO via S3 driver
	// Cloudinary (free tier, ideal for MVP — swap to s3 later via STORAGE_BACKEND)
	CloudinaryCloudName string `yaml:"cloudinary_cloud_name"`
	CloudinaryAPIKey    string `yaml:"cloudinary_api_key"`
	CloudinaryAPISecret string `yaml:"cloudinary_api_secret"`

	// ── MongoDB (message history) ───────────────────────────────────────────
	// MONGO_URI accepts any valid connection string:
	//   local:  "mongodb://localhost:27017"
	//   Atlas:  "mongodb+srv://user:pass@cluster0.mongodb.net" (free tier)
	// Leave empty to run with in-memory message history (dev / test).
	MongoURI string `yaml:"mongo_uri"`
	MongoDB  string `yaml:"mongo_db"` // default: "zpc_chat"

	// ── PostgreSQL (rooms, members, activity) ────────────────────────────────
	PostgresDSN  string `yaml:"postgres_dsn"` // full DSN or individual parts below
	PostgresHost string `yaml:"postgres_host"`
	PostgresPort int    `yaml:"postgres_port"`
	PostgresUser string `yaml:"postgres_user"`
	PostgresPass string `yaml:"postgres_password"`
	PostgresDB   string `yaml:"postgres_db"`
	PostgresSSL  string `yaml:"postgres_sslmode"` // disable | require | verify-full

	// ── Kafka (replaces Redis pub/sub for 1M+ user scale) ────────────────────
	// KAFKA_BROKERS: comma-separated e.g. "kafka1:9092,kafka2:9092"
	// Leave empty to use Redis pub/sub (or Noop if Redis also absent).
	KafkaBrokersRaw string `yaml:"kafka_brokers"`  // raw comma-separated string
	KafkaTopic      string `yaml:"kafka_topic"`    // default: "chat-messages"
	KafkaGroupID    string `yaml:"kafka_group_id"` // default: "chat-service"

	// ── Scalability limits ────────────────────────────────────────────────────
	// MaxConnsPerPod caps how many simultaneous gRPC streams one pod accepts.
	// When hit the server returns codes.ResourceExhausted so the client retries
	// another pod. Tune based on your instance memory (default: 80000).
	MaxConnsPerPod int `yaml:"max_conns_per_pod"`

	// ── Auth (JWT RS256) ──────────────────────────────────────────────────────
	// Must point to the SAME public.pem used by user_service for token issuance.
	// Path is relative to the working directory (default: "config/public.pem").
	// Set JWT_DISABLED=true to skip verification in local dev (never in prod).
	JWTPublicKeyPath string `yaml:"jwt_public_key_path"`
	JWTAudience      string `yaml:"jwt_audience"` // default: "graphql-api"
	JWTIssuer        string `yaml:"jwt_issuer"`   // default: "ZPC"
	JWTDisabled      bool   `yaml:"jwt_disabled"` // dev-only escape hatch
}

func Default() Config {
	return Config{
		ServiceName:      "chat-service",
		Port:             50051,
		LogLevel:         "info",
		StorageBackend:   "minio",
		MinioBucket:      "chat-media",
		S3Bucket:         "chat-media",
		S3Region:         "us-east-1",
		MongoDB:          "zpc_chat",
		PostgresHost:     "localhost",
		PostgresPort:     5432,
		PostgresSSL:      "disable",
		KafkaTopic:       "chat-messages",
		KafkaGroupID:     "chat-service",
		MaxConnsPerPod:   80000,
		JWTPublicKeyPath: "config/public.pem",
		JWTAudience:      "graphql-api",
		JWTIssuer:        "ZPC",
	}
}

// KafkaBrokers returns the parsed slice from the comma-separated env/yaml value.
// Empty strings are filtered out so an unset KAFKA_BROKERS returns nil.
func (c Config) KafkaBrokers() []string {
	if c.KafkaBrokersRaw == "" {
		if v := os.Getenv("KAFKA_BROKERS"); v != "" {
			c.KafkaBrokersRaw = v
		}
	}
	var out []string
	for _, b := range splitComma(c.KafkaBrokersRaw) {
		if b != "" {
			out = append(out, b)
		}
	}
	return out
}

// PostgresDSNBuilt returns a connection string, preferring the explicit DSN
// field if set, otherwise building one from the individual host/port/user fields.
func (c Config) PostgresDSNBuilt() string {
	if c.PostgresDSN != "" {
		return c.PostgresDSN
	}
	if c.PostgresHost == "" {
		return ""
	}
	ssl := c.PostgresSSL
	if ssl == "" {
		ssl = "disable"
	}
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.PostgresHost, c.PostgresPort, c.PostgresUser, c.PostgresPass, c.PostgresDB, ssl)
}

func splitComma(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			out = append(out, strings.TrimSpace(s[start:i]))
			start = i + 1
		}
	}
	out = append(out, strings.TrimSpace(s[start:]))
	return out
}

func Load() (Config, error) {
	_ = godotenv.Load()
	cfg := Default()

	configPath := getenv("CONFIG_FILE", "config/config.yaml")
	if y, err := os.ReadFile(configPath); err == nil {
		_ = yaml.Unmarshal(y, &cfg)
	}

	if v := os.Getenv("SERVICE_NAME"); v != "" {
		cfg.ServiceName = v
	}
	if v := os.Getenv("PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			cfg.Port = p
		}
	}
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}

	// Redis
	if v := os.Getenv("REDIS_URL"); v != "" {
		cfg.RedisURL = v
	}
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		cfg.RedisAddr = v
	}
	if v := os.Getenv("REDIS_PASSWORD"); v != "" {
		cfg.RedisPassword = v
	}

	// Storage backend
	if v := os.Getenv("STORAGE_BACKEND"); v != "" {
		cfg.StorageBackend = v
	}

	// MinIO
	if v := os.Getenv("MINIO_ENDPOINT"); v != "" {
		cfg.MinioEndpoint = v
	}
	if v := os.Getenv("MINIO_ACCESS_KEY"); v != "" {
		cfg.MinioAccessKey = v
	}
	if v := os.Getenv("MINIO_SECRET_KEY"); v != "" {
		cfg.MinioSecretKey = v
	}
	if v := os.Getenv("MINIO_BUCKET"); v != "" {
		cfg.MinioBucket = v
	}
	if v := os.Getenv("MINIO_USE_SSL"); v == "true" {
		cfg.MinioUseSSL = true
	}

	// AWS S3
	if v := os.Getenv("S3_BUCKET"); v != "" {
		cfg.S3Bucket = v
	}
	if v := os.Getenv("S3_REGION"); v != "" {
		cfg.S3Region = v
	}
	if v := os.Getenv("S3_ENDPOINT"); v != "" {
		cfg.S3Endpoint = v
	}
	if v := os.Getenv("AWS_ACCESS_KEY_ID"); v != "" {
		cfg.S3AccessKeyID = v
	}
	if v := os.Getenv("AWS_SECRET_ACCESS_KEY"); v != "" {
		cfg.S3SecretAccessKey = v
	}
	if v := os.Getenv("S3_FORCE_PATH_STYLE"); v == "true" {
		cfg.S3ForcePathStyle = true
	}

	// Cloudinary
	if v := os.Getenv("CLOUDINARY_CLOUD_NAME"); v != "" {
		cfg.CloudinaryCloudName = v
	}
	if v := os.Getenv("CLOUDINARY_API_KEY"); v != "" {
		cfg.CloudinaryAPIKey = v
	}
	if v := os.Getenv("CLOUDINARY_API_SECRET"); v != "" {
		cfg.CloudinaryAPISecret = v
	}

	// MongoDB
	if v := os.Getenv("MONGO_URI"); v != "" {
		cfg.MongoURI = v
	}
	if v := os.Getenv("MONGO_DB"); v != "" {
		cfg.MongoDB = v
	}

	// PostgreSQL
	if v := os.Getenv("POSTGRES_DSN"); v != "" {
		cfg.PostgresDSN = v
	}
	if v := os.Getenv("POSTGRES_HOST"); v != "" {
		cfg.PostgresHost = v
	}
	if v := os.Getenv("POSTGRES_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			cfg.PostgresPort = p
		}
	}
	if v := os.Getenv("POSTGRES_USER"); v != "" {
		cfg.PostgresUser = v
	}
	if v := os.Getenv("POSTGRES_PASSWORD"); v != "" {
		cfg.PostgresPass = v
	}
	if v := os.Getenv("POSTGRES_DB"); v != "" {
		cfg.PostgresDB = v
	}
	if v := os.Getenv("POSTGRES_SSLMODE"); v != "" {
		cfg.PostgresSSL = v
	}

	// Kafka
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		cfg.KafkaBrokersRaw = v
	}
	if v := os.Getenv("KAFKA_TOPIC"); v != "" {
		cfg.KafkaTopic = v
	}
	if v := os.Getenv("KAFKA_GROUP_ID"); v != "" {
		cfg.KafkaGroupID = v
	}
	if v := os.Getenv("MAX_CONNS_PER_POD"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MaxConnsPerPod = n
		}
	}

	// JWT auth
	if v := os.Getenv("JWT_PUBLIC_KEY_PATH"); v != "" {
		cfg.JWTPublicKeyPath = v
	}
	if v := os.Getenv("JWT_AUDIENCE"); v != "" {
		cfg.JWTAudience = v
	}
	if v := os.Getenv("JWT_ISSUER"); v != "" {
		cfg.JWTIssuer = v
	}
	if os.Getenv("JWT_DISABLED") == "true" {
		cfg.JWTDisabled = true
	}

	return cfg, validate(cfg)
}

func validate(c Config) error {
	if c.Port <= 0 || c.Port > 65535 {
		return errors.New("invalid port")
	}
	return nil
}

// Addr formats a listen address from a port number.
func Addr(port int) string { return fmt.Sprintf(":%d", port) }

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// DurationEnv reads a time.Duration from an environment variable.
func DurationEnv(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
