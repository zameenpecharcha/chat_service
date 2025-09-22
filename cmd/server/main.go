package main

import (
    "context"
    "net"
    "os"
    "os/signal"
    "syscall"
    "time"

    "google.golang.org/grpc"
    "google.golang.org/grpc/health"
    healthpb "google.golang.org/grpc/health/grpc_health_v1"
    "google.golang.org/grpc/reflection"

    "chat-service/internal/broker"
    "chat-service/internal/chat"
    "chat-service/internal/config"
    "chat-service/internal/logger"
    "chat-service/internal/server"
    pb "chat-service/internal/pb"
)

func main() {
    cfg, err := config.Load()
    if err != nil { panic(err) }
    log := logger.New(cfg.LogLevel)

    var b broker.Broker = broker.Noop{}
    if cfg.RedisAddr != "" {
        if rb := broker.NewRedisBroker(cfg.RedisAddr, cfg.RedisPassword); rb != nil {
            b = rb
        }
    }

    h := chat.NewHub(b)
    s := grpc.NewServer()

    // Register Chat gRPC server
    pb.RegisterChatServiceServer(s, server.NewChatServer(h))

    // Health and reflection
    hs := health.NewServer()
    healthpb.RegisterHealthServer(s, hs)
    reflection.Register(s)

    addr := config.Addr(cfg.Port)
    lis, err := net.Listen("tcp", addr)
    if err != nil { log.Fatal().Err(err).Msg("failed to listen") }
    go func() {
        log.Info().Str("addr", addr).Msg("gRPC server starting")
        if err := s.Serve(lis); err != nil { log.Fatal().Err(err).Msg("server error") }
    }()

    // Graceful shutdown
    sigc := make(chan os.Signal, 1)
    signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
    <-sigc

    log.Info().Msg("shutting down...")
    cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    stopped := make(chan struct{})
    go func() { s.GracefulStop(); close(stopped) }()
    select {
    case <-stopped:
    case <-cctx.Done():
        s.Stop()
    }
    log.Info().Msg("bye")
}

// nothing below


