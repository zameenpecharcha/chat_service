package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"chat-service/app/broker"
	"chat-service/app/chat"
	"chat-service/app/config"
	"chat-service/app/interceptors"
	"chat-service/app/logger"
	pb "chat-service/app/pb"
	"chat-service/app/repository"
	"chat-service/app/server"
	"chat-service/app/storage"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}
	log := logger.New(cfg.LogLevel)

	// ── Redis ─────────────────────────────────────────────────────────────────
	redisClient := broker.NewRedisClient(cfg.RedisURL, cfg.RedisAddr, cfg.RedisPassword)
	if redisClient != nil {
		addr := cfg.RedisAddr
		if cfg.RedisURL != "" {
			addr = cfg.RedisURL
		}
		log.Info().Str("addr", addr).Msg("redis connected")
	}

	var b broker.Broker = broker.Noop{}
	if rb := broker.NewRedisBrokerFromClient(redisClient); rb != nil {
		b = rb
	}

	// ── Object storage ────────────────────────────────────────────────────────
	// STORAGE_BACKEND controls which backend is active:
	//   "cloudinary" — free tier, ideal for MVP (images, video, PDF, raw files)
	//   "s3"         — AWS S3, Cloudflare R2, DigitalOcean Spaces, Backblaze B2
	//   "minio"      — local dev / self-hosted S3-compatible
	// Swap backends by changing STORAGE_BACKEND in .env — no code change needed.
	var st storage.Storage
	backend := strings.ToLower(cfg.StorageBackend)
	switch backend {
	case "cloudinary":
		cs := storage.NewCloudinaryStorage(
			cfg.CloudinaryCloudName, cfg.CloudinaryAPIKey, cfg.CloudinaryAPISecret)
		if cs != nil {
			st = cs
			log.Info().Str("cloud", cfg.CloudinaryCloudName).Msg("Cloudinary storage ready")
		}
	case "s3":
		if cfg.S3Bucket != "" {
			s3st, err := storage.NewS3Storage(storage.S3Config{
				Bucket:          cfg.S3Bucket,
				Region:          cfg.S3Region,
				Endpoint:        cfg.S3Endpoint,
				AccessKeyID:     cfg.S3AccessKeyID,
				SecretAccessKey: cfg.S3SecretAccessKey,
				ForcePathStyle:  cfg.S3ForcePathStyle,
			})
			if err != nil {
				log.Fatal().Err(err).Msg("failed to initialise S3 storage")
			}
			if s3st != nil {
				st = s3st
				log.Info().Str("bucket", cfg.S3Bucket).Str("region", cfg.S3Region).Msg("S3 storage ready")
			}
		}
	default:
		if cfg.MinioEndpoint != "" {
			ms, err := storage.NewMinioStorage(
				cfg.MinioEndpoint, cfg.MinioAccessKey,
				cfg.MinioSecretKey, cfg.MinioBucket, cfg.MinioUseSSL,
			)
			if err != nil {
				log.Fatal().Err(err).Msg("failed to initialise MinIO storage")
			}
			if ms != nil {
				if err := ms.EnsureBucket(context.Background()); err != nil {
					log.Warn().Err(err).Msg("could not ensure MinIO bucket — uploads may fail")
				}
				st = ms
				log.Info().Str("endpoint", cfg.MinioEndpoint).Str("bucket", cfg.MinioBucket).Msg("MinIO storage ready")
			}
		}
	}
	if st == nil {
		log.Warn().Msg("no object storage configured — media uploads disabled")
	}

	// ── Message history (MongoDB if available, otherwise in-memory fallback) ─
	var msgRepo repository.MessageStore
	var fallbackMsgStore repository.MessageStore
	if cfg.MongoURI != "" {
		mr, err := repository.NewMessageRepository(cfg.MongoURI, cfg.MongoDB)
		if err != nil {
			log.Warn().Err(err).Msg("MongoDB unavailable — falling back to in-memory history")
		} else {
			msgRepo = mr
			log.Info().Str("db", cfg.MongoDB).Msg("MongoDB ready")
		}
	}
	if msgRepo == nil {
		fallbackMsgStore = repository.NewInMemoryMessageStore()
		msgRepo = fallbackMsgStore
		log.Warn().Msg("message history using in-memory fallback")
	}

	// ── PostgreSQL (rooms, members, activity) ─────────────────────────────────
	var pgRepo *repository.RoomRepository
	if dsn := cfg.PostgresDSNBuilt(); dsn != "" {
		pr, err := repository.NewRoomRepository(dsn)
		if err != nil {
			log.Warn().Err(err).Msg("PostgreSQL unavailable — using in-memory room store")
		} else {
			if err := pr.Migrate(context.Background()); err != nil {
				log.Warn().Err(err).Msg("PostgreSQL migration failed")
			} else {
				pgRepo = pr
				log.Info().Msg("PostgreSQL room store ready")
			}
		}
	} else {
		log.Warn().Msg("POSTGRES_HOST not set — using in-memory room store")
	}

	// ── Kafka broker (replaces Redis pub/sub for 1M+ users) ──────────────────
	// When KAFKA_BROKERS is set, messages are written to a durable Kafka topic.
	// Each pod consumes from the same consumer group so every pod fans out
	// messages to its locally connected clients.
	// Without Kafka the service falls back to Redis pub/sub (or in-memory Noop).
	if kafkaBrokers := cfg.KafkaBrokers(); len(kafkaBrokers) > 0 {
		kb, err := broker.NewKafkaBroker(context.Background(), broker.KafkaConfig{
			Brokers: kafkaBrokers,
			Topic:   cfg.KafkaTopic,
			GroupID: cfg.KafkaGroupID,
		})
		if err != nil {
			log.Warn().Err(err).Msg("Kafka unavailable — falling back to Redis/Noop broker")
		} else {
			b = kb
			log.Info().Strs("brokers", kafkaBrokers).Str("topic", cfg.KafkaTopic).Msg("Kafka broker ready")
		}
	}

	// ── Presence store (Redis-backed for multi-pod; local for single-pod dev) ─
	// Redis presence uses a TTL heartbeat: key expires in 60 s, refreshed every
	// 30 s while the stream is alive. Works correctly with rolling deployments.
	var ps chat.PresenceStore
	if redisClient != nil {
		ps = chat.NewRedisPresenceStore(redisClient)
		log.Info().Msg("Redis presence store ready (multi-pod mode)")
	} else {
		ps = chat.NewLocalPresenceStore()
		log.Warn().Msg("no Redis — using in-process presence store (single-pod only)")
	}

	// ── In-memory / Redis room store (fallback) ───────────────────────────────
	rs := chat.NewRoomStore(redisClient)

	// ── JWT auth interceptor ──────────────────────────────────────────────────
	// Mirrors user_service/app/interceptors/auth_interceptor.py:
	//   same public.pem, same audience (graphql-api), same issuer (ZPC).
	// Set JWT_DISABLED=true only in local dev without an auth_service running.
	var grpcOpts []grpc.ServerOption
	if cfg.JWTDisabled {
		log.Warn().Msg("JWT auth DISABLED — set JWT_DISABLED=false before deploying to production")
	} else {
		auth, err := interceptors.NewAuthInterceptor(
			cfg.JWTPublicKeyPath, cfg.JWTAudience, cfg.JWTIssuer)
		if err != nil {
			log.Warn().Err(err).Str("path", cfg.JWTPublicKeyPath).
				Msg("JWT public key not loaded — auth DISABLED (place config/public.pem to enable)")
		} else {
			grpcOpts = append(grpcOpts,
				grpc.UnaryInterceptor(auth.UnaryServerInterceptor()),
				grpc.StreamInterceptor(auth.StreamServerInterceptor()),
			)
			log.Info().Str("key", cfg.JWTPublicKeyPath).Msg("JWT auth ready")
		}
	}

	// ── Wire and start the gRPC server ────────────────────────────────────────
	h := chat.NewHubWithLimit(b, int64(cfg.MaxConnsPerPod))
	s := grpc.NewServer(grpcOpts...)
	pb.RegisterChatServiceServer(s, server.NewChatServer(h, rs, pgRepo, msgRepo, st, ps))
	_ = fallbackMsgStore

	hs := health.NewServer()
	healthpb.RegisterHealthServer(s, hs)
	reflection.Register(s)

	addr := config.Addr(cfg.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to listen")
	}
	go func() {
		log.Info().Str("addr", addr).Msg("gRPC server starting")
		if err := s.Serve(lis); err != nil {
			log.Fatal().Err(err).Msg("server error")
		}
	}()

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	<-sigc

	log.Info().Msg("shutting down…")
	cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stopped := make(chan struct{})
	go func() { s.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-cctx.Done():
		s.Stop()
	}
	if msgRepo != nil {
		msgRepo.Close()
	}
	if pgRepo != nil {
		_ = pgRepo.Close()
	}
	log.Info().Msg("bye")
}
