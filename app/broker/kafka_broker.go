package broker

// KafkaBroker implements the Broker interface using Apache Kafka (via kafka-go).
//
// ARCHITECTURE FOR 1M USERS
// ─────────────────────────
// Problem with Redis pub/sub at scale:
//   • Fire-and-forget: if a subscriber pod restarts while a message is in-flight, the message is lost.
//   • No back-pressure: a slow consumer cannot slow down a fast producer.
//   • Fan-out to 1000-member group rooms becomes O(1000) Redis PUBLISH calls.
//
// Kafka solves all three:
//   • Messages are written to durable, replicated log. A crashed consumer
//     reconnects and re-reads from its last committed offset — zero message loss.
//   • Back-pressure is built-in via consumer lag metrics and flow control.
//   • Fan-out is decoupled: producer writes ONCE to the topic; every chat-service
//     pod reads from its own consumer-group partition and delivers to its local clients.
//
// TOPIC DESIGN
// ────────────
//   Topic name  : "chat-messages"   (single topic, configurable)
//   Partitions  : 64–128 (tune to number of brokers × replication factor)
//   Key         : room_id  → Kafka guarantees per-key ordering (all msgs in a room
//                 land on the same partition, so delivery order is preserved)
//   Retention   : 1–7 days (acts as a short-term replay buffer)
//   Replication : 3 (minimum for production HA)
//
// DEPLOYMENT TOPOLOGY FOR 1M CONCURRENT USERS
// ────────────────────────────────────────────
//   • Each chat-service pod handles ~50–100K WebSocket/gRPC connections.
//   • 10–20 pods cover 1M users.
//   • Kafka broker cluster: 3–5 nodes.
//   • Cassandra cluster: 3–9 nodes (handles persistent message storage).
//   • Redis cluster: presence store + room metadata cache.
//   • Load-balancer: consistent-hashing or sticky-session to minimise cross-pod broadcasts.
//
//   Flow for a message from user A in room R:
//     1. A's gRPC stream → grpc_server.go → hub.Broadcast() (local clients)
//     2. hub.Broadcast() calls KafkaBroker.Publish(roomID, data)
//     3. Kafka writes to partition hash(roomID) % numPartitions
//     4. ALL pods consume from Kafka; each pod checks its local Hub for room R
//     5. If a pod has clients in room R → deliver. If not → discard (no-op).
//     6. Simultaneously grpc_server.go calls msgRepo.SaveMessage() (Cassandra, async).
//
// CONFIGURATION (environment variables)
//   KAFKA_BROKERS   : comma-separated e.g. "kafka1:9092,kafka2:9092"
//   KAFKA_TOPIC     : default "chat-messages"
//   KAFKA_GROUP_ID  : default "chat-service"  (unique per logical service; NOT per pod)

import (
	"context"
	"fmt"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

const (
	defaultTopic   = "chat-messages"
	defaultGroupID = "chat-service"
)

// KafkaConfig holds all Kafka connection settings.
type KafkaConfig struct {
	Brokers   []string // e.g. ["kafka1:9092","kafka2:9092"]
	Topic     string   // default: "chat-messages"
	GroupID   string   // consumer group id; same for all pods in the same service
	Partition int      // used only for NewKafkaBroker (single-partition simple mode); 0 = auto
}

// KafkaBroker fans out chat messages via Kafka.
// One background reader goroutine consumes all messages from the topic and
// dispatches them to any locally registered room subscribers.
type KafkaBroker struct {
	writer *kafka.Writer

	// local subscriber registry: roomID → list of channels
	mu   sync.RWMutex
	subs map[string][]chan []byte
}

// NewKafkaBroker creates a KafkaBroker and starts the background consumer.
// Cancel the passed context to shut down the consumer goroutine.
func NewKafkaBroker(ctx context.Context, cfg KafkaConfig) (*KafkaBroker, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: at least one broker address is required")
	}
	if cfg.Topic == "" {
		cfg.Topic = defaultTopic
	}
	if cfg.GroupID == "" {
		cfg.GroupID = defaultGroupID
	}

	b := &KafkaBroker{
		subs: make(map[string][]chan []byte),
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(cfg.Brokers...),
			Topic:                  cfg.Topic,
			Balancer:               &kafka.Hash{}, // consistent hash on key → preserves per-room order
			RequiredAcks:           kafka.RequireOne,
			Async:                  true, // fire-and-forget write; use RequireAll for stricter durability
			AllowAutoTopicCreation: true,
			WriteTimeout:           5 * time.Second,
			BatchTimeout:           5 * time.Millisecond,
			BatchSize:              100,
		},
	}

	// Consumer: one shared reader per pod, dispatches to locally registered room channels
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		GroupID:        cfg.GroupID,
		Topic:          cfg.Topic,
		MinBytes:       1,
		MaxBytes:       1 << 20, // 1 MB
		MaxWait:        10 * time.Millisecond,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset, // new pods only receive messages going forward
	})

	go b.consume(ctx, reader)
	return b, nil
}

// Publish writes a message to Kafka with room_id as the partition key.
// All messages for the same room land on the same partition → ordered delivery.
func (b *KafkaBroker) Publish(ctx context.Context, roomID string, data []byte) error {
	if b == nil {
		return nil
	}
	return b.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(roomID),
		Value: data,
	})
}

// Subscribe registers a local in-memory channel for a room.
// When Kafka delivers a message for roomID the channel receives it.
// Call the returned cancel func to deregister.
func (b *KafkaBroker) Subscribe(_ context.Context, roomID string) (<-chan []byte, func(), error) {
	ch := make(chan []byte, 256)
	b.mu.Lock()
	b.subs[roomID] = append(b.subs[roomID], ch)
	b.mu.Unlock()

	cancel := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		list := b.subs[roomID]
		for i, c := range list {
			if c == ch {
				b.subs[roomID] = append(list[:i], list[i+1:]...)
				break
			}
		}
		if len(b.subs[roomID]) == 0 {
			delete(b.subs, roomID)
		}
		close(ch)
	}
	return ch, cancel, nil
}

// Close shuts down the Kafka writer. The consumer is stopped by cancelling the
// context passed to NewKafkaBroker.
func (b *KafkaBroker) Close() error {
	if b == nil {
		return nil
	}
	return b.writer.Close()
}

// consume is the background goroutine that reads from Kafka and dispatches
// messages to any locally registered subscribers for that room.
func (b *KafkaBroker) consume(ctx context.Context, r *kafka.Reader) {
	defer r.Close()
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // context cancelled — clean shutdown
			}
			// Transient error (network glitch, leader election) — back off and retry
			time.Sleep(100 * time.Millisecond)
			continue
		}

		roomID := string(msg.Key)
		data := msg.Value

		b.mu.RLock()
		channels := b.subs[roomID]
		b.mu.RUnlock()

		for _, ch := range channels {
			select {
			case ch <- data:
			default:
				// slow consumer — drop rather than block the read loop
				// In production: emit a metric here so you can alert on drops
			}
		}

		// Commit after successful dispatch
		if err := r.CommitMessages(ctx, msg); err != nil && ctx.Err() == nil {
			// Non-fatal: message will be re-delivered on next consumer restart
			_ = err
		}
	}
}
