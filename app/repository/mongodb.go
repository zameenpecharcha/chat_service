package repository

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	pb "chat-service/app/pb"
)

// ─────────────────────────────────────────────────────────────────────────────
// Document types
// ─────────────────────────────────────────────────────────────────────────────

type messageDoc struct {
	RoomID           string         `bson:"room_id"`
	SentAt           time.Time      `bson:"sent_at"`
	MessageID        string         `bson:"message_id"`
	UserID           string         `bson:"user_id"`
	MessageType      int32          `bson:"message_type"`
	TextBody         string         `bson:"text_body,omitempty"`
	DeliveredAt      time.Time      `bson:"delivered_at,omitempty"`
	MediaKey         string         `bson:"media_key,omitempty"`
	MediaName        string         `bson:"media_name,omitempty"`
	MediaMimeType    string         `bson:"media_mime_type,omitempty"`
	MediaSizeBytes   int64          `bson:"media_size_bytes,omitempty"`
	ReplyToMessageID string         `bson:"reply_to_message_id,omitempty"`
	IsDeleted        bool           `bson:"is_deleted"`
	EditedAt         time.Time      `bson:"edited_at,omitempty"`
	ReactionSummary  map[string]int `bson:"reaction_summary,omitempty"`
}

type readReceiptDoc struct {
	RoomID        string    `bson:"room_id"`
	UserID        string    `bson:"user_id"`
	LastReadAt    time.Time `bson:"last_read_at"`
	LastReadMsgID string    `bson:"last_read_msg_id"`
}

type mediaUploadDoc struct {
	MediaKey   string    `bson:"_id"`
	UploaderID string    `bson:"uploader_id"`
	RoomID     string    `bson:"room_id"`
	FileName   string    `bson:"file_name"`
	MimeType   string    `bson:"mime_type"`
	SizeBytes  int64     `bson:"size_bytes"`
	UploadedAt time.Time `bson:"uploaded_at"`
	ExpiresAt  time.Time `bson:"expires_at,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// MessageRepository — MongoDB-backed
// ─────────────────────────────────────────────────────────────────────────────

// UserRoomSummary contains the minimal room data needed for the inbox sidebar.
type UserRoomSummary struct {
	RoomID        string
	LastMessage   string
	LastMessageAt time.Time
	MemberIDs     []string // extracted from dm:a:b room_id for DMs
}

// MessageStore is the persistence contract used by the chat server.
type MessageStore interface {
	SaveMessage(ctx context.Context, msg *pb.ServerMessage) error
	SoftDeleteMessage(ctx context.Context, roomID string, sentAtUnixMs int64, messageID string) error
	EditMessage(ctx context.Context, messageID, userID, newText string) (int64, error)
	SoftDeleteMessageByOwner(ctx context.Context, messageID, userID string) (bool, error)
	SaveMediaUpload(ctx context.Context, mediaKey, uploaderID, roomID, fileName, mimeType string, sizeBytes int64, expiresAt time.Time) error
	UpdateReadReceipt(ctx context.Context, roomID, userID, lastMsgID string, lastReadAt time.Time) error
	GetMessagesBefore(ctx context.Context, roomID string, beforeUnixMs int64, limit int) ([]*pb.ServerMessage, error)
	GetUserRooms(ctx context.Context, userID string) ([]UserRoomSummary, error)
	Close()
}

// MessageRepository persists and retrieves chat messages from MongoDB.
// Compatible with MongoDB Atlas M0 (free tier) and any self-hosted instance.
type MessageRepository struct {
	client   *mongo.Client
	messages *mongo.Collection
	receipts *mongo.Collection
	media    *mongo.Collection
}

// NewMessageRepository connects to MongoDB and returns a repository.
// uri is a standard MongoDB connection string, e.g.:
//
//	"mongodb://localhost:27017"                     (local)
//	"mongodb+srv://user:pass@cluster.mongodb.net"   (Atlas)
//
// dbName is the database to use (default: "zpc_chat").
func NewMessageRepository(uri, dbName string) (*MessageRepository, error) {
	if dbName == "" {
		dbName = "zpc_chat"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("mongodb connect: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("mongodb ping: %w", err)
	}

	db := client.Database(dbName)
	r := &MessageRepository{
		client:   client,
		messages: db.Collection("messages"),
		receipts: db.Collection("read_receipts"),
		media:    db.Collection("media_uploads"),
	}

	if err := r.ensureIndexes(ctx); err != nil {
		return nil, err
	}

	return r, nil
}

// ensureIndexes creates all required indexes if they don't already exist.
// Safe to call on every startup (idempotent).
func (r *MessageRepository) ensureIndexes(ctx context.Context) error {
	// messages: primary query pattern — room + time DESC
	_, err := r.messages.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "room_id", Value: 1}, {Key: "sent_at", Value: -1}},
			Options: options.Index().SetName("room_time"),
		},
		{
			Keys:    bson.D{{Key: "room_id", Value: 1}, {Key: "sent_at", Value: -1}, {Key: "is_deleted", Value: 1}},
			Options: options.Index().SetName("room_time_deleted"),
		},
		{
			// For soft-delete and reaction updates by message_id
			Keys:    bson.D{{Key: "message_id", Value: 1}},
			Options: options.Index().SetName("message_id").SetUnique(true),
		},
	})
	if err != nil {
		return fmt.Errorf("messages index: %w", err)
	}

	// read_receipts: unique (room_id, user_id) pair
	_, err = r.receipts.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "room_id", Value: 1}, {Key: "user_id", Value: 1}},
		Options: options.Index().SetName("room_user").SetUnique(true),
	})
	if err != nil {
		return fmt.Errorf("read_receipts index: %w", err)
	}

	// media_uploads: query by room and by uploader
	_, err = r.media.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "room_id", Value: 1}, {Key: "uploaded_at", Value: -1}},
			Options: options.Index().SetName("room_uploads"),
		},
		{
			Keys:    bson.D{{Key: "uploader_id", Value: 1}},
			Options: options.Index().SetName("uploader"),
		},
	})
	if err != nil {
		return fmt.Errorf("media_uploads index: %w", err)
	}

	return nil
}

// Close disconnects from MongoDB.
func (r *MessageRepository) Close() {
	if r.client != nil {
		_ = r.client.Disconnect(context.Background())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Write operations
// ─────────────────────────────────────────────────────────────────────────────

// SaveMessage persists a ServerMessage to the messages collection.
func (r *MessageRepository) SaveMessage(ctx context.Context, msg *pb.ServerMessage) error {
	doc := messageDoc{
		RoomID:           msg.GetRoomId(),
		SentAt:           time.UnixMilli(msg.GetSentAtUnixMs()).UTC(),
		MessageID:        msg.GetMessageId(),
		UserID:           msg.GetUserId(),
		MessageType:      int32(msg.GetType()),
		TextBody:         msg.GetText(),
		DeliveredAt:      time.UnixMilli(msg.GetDeliveredAtUnixMs()).UTC(),
		MediaKey:         msg.GetMediaKey(),
		MediaName:        msg.GetMediaName(),
		MediaMimeType:    msg.GetMediaMimeType(),
		MediaSizeBytes:   msg.GetMediaSizeBytes(),
		ReplyToMessageID: msg.GetReplyToMessageId(),
		IsDeleted:        msg.GetIsDeleted(),
	}

	_, err := r.messages.InsertOne(ctx, doc)
	return err
}

// SoftDeleteMessage marks a single message as deleted without removing the document.
// sentAtUnixMs is accepted for signature compatibility but not used (MongoDB
// looks up by message_id directly, unlike Cassandra which needed the partition key).
func (r *MessageRepository) SoftDeleteMessage(ctx context.Context, _ string, sentAtUnixMs int64, messageID string) error {
	_, err := r.messages.UpdateOne(ctx,
		bson.M{"message_id": messageID},
		bson.M{"$set": bson.M{"is_deleted": true}},
	)
	return err
}

// SoftDeleteMessageByOwner soft-deletes only if message_id belongs to userID.
func (r *MessageRepository) SoftDeleteMessageByOwner(ctx context.Context, messageID, userID string) (bool, error) {
	res, err := r.messages.UpdateOne(ctx,
		bson.M{"message_id": messageID, "user_id": userID},
		bson.M{"$set": bson.M{"is_deleted": true}},
	)
	if err != nil {
		return false, err
	}
	return res.ModifiedCount > 0 || res.MatchedCount > 0, nil
}

// EditMessage updates text for a message owned by userID. Returns edited_at unix ms.
func (r *MessageRepository) EditMessage(ctx context.Context, messageID, userID, newText string) (int64, error) {
	now := time.Now().UTC()
	res, err := r.messages.UpdateOne(ctx,
		bson.M{
			"message_id": messageID,
			"user_id":    userID,
			"is_deleted": bson.M{"$ne": true},
		},
		bson.M{"$set": bson.M{
			"text_body": newText,
			"edited_at": now,
		}},
	)
	if err != nil {
		return 0, err
	}
	if res.MatchedCount == 0 {
		return 0, fmt.Errorf("message not found or not owned by user")
	}
	return now.UnixMilli(), nil
}

// SaveMediaUpload records a media upload in the media_uploads collection.
func (r *MessageRepository) SaveMediaUpload(ctx context.Context,
	mediaKey, uploaderID, roomID, fileName, mimeType string,
	sizeBytes int64, expiresAt time.Time) error {

	doc := mediaUploadDoc{
		MediaKey:   mediaKey,
		UploaderID: uploaderID,
		RoomID:     roomID,
		FileName:   fileName,
		MimeType:   mimeType,
		SizeBytes:  sizeBytes,
		UploadedAt: time.Now().UTC(),
		ExpiresAt:  expiresAt,
	}
	_, err := r.media.InsertOne(ctx, doc)
	return err
}

// UpdateReadReceipt upserts a user's last-read cursor for a room.
func (r *MessageRepository) UpdateReadReceipt(ctx context.Context,
	roomID, userID, lastMsgID string, lastReadAt time.Time) error {

	_, err := r.receipts.UpdateOne(ctx,
		bson.M{"room_id": roomID, "user_id": userID},
		bson.M{"$set": bson.M{
			"last_read_at":     lastReadAt.UTC(),
			"last_read_msg_id": lastMsgID,
		}},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}

// ─────────────────────────────────────────────────────────────────────────────
// Read operations
// ─────────────────────────────────────────────────────────────────────────────

// GetMessages returns the latest `limit` messages in a room, newest first.
func (r *MessageRepository) GetMessages(ctx context.Context, roomID string, limit, _ int) ([]*pb.ServerMessage, error) {
	if limit <= 0 {
		limit = 50
	}
	// Include soft-deleted messages so clients can show Teams-style tombstones.
	return r.queryMessages(ctx,
		bson.M{"room_id": roomID},
		limit,
	)
}

// GetMessagesBefore returns up to `limit` messages sent strictly before
// beforeUnixMs, newest first. Pass 0 to fetch the latest messages.
func (r *MessageRepository) GetMessagesBefore(ctx context.Context, roomID string, beforeUnixMs int64, limit int) ([]*pb.ServerMessage, error) {
	if limit <= 0 {
		limit = 50
	}
	if beforeUnixMs == 0 {
		return r.GetMessages(ctx, roomID, limit, 0)
	}
	return r.queryMessages(ctx,
		bson.M{
			"room_id": roomID,
			"sent_at": bson.M{"$lt": time.UnixMilli(beforeUnixMs).UTC()},
		},
		limit,
	)
}

// queryMessages is the shared find helper used by GetMessages and GetMessagesBefore.
func (r *MessageRepository) queryMessages(ctx context.Context, filter bson.M, limit int) ([]*pb.ServerMessage, error) {
	opts := options.Find().
		SetSort(bson.D{{Key: "sent_at", Value: -1}}).
		SetLimit(int64(limit))

	cur, err := r.messages.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("mongodb find messages: %w", err)
	}
	defer cur.Close(ctx)

	var results []*pb.ServerMessage
	for cur.Next(ctx) {
		var doc messageDoc
		if err := cur.Decode(&doc); err != nil {
			continue
		}
		results = append(results, &pb.ServerMessage{
			RoomId:            doc.RoomID,
			UserId:            doc.UserID,
			MessageId:         doc.MessageID,
			Text:              doc.TextBody,
			Type:              pb.MessageType(doc.MessageType),
			SentAtUnixMs:      doc.SentAt.UnixMilli(),
			DeliveredAtUnixMs: doc.DeliveredAt.UnixMilli(),
			MediaKey:          doc.MediaKey,
			MediaName:         doc.MediaName,
			MediaMimeType:     doc.MediaMimeType,
			MediaSizeBytes:    doc.MediaSizeBytes,
			ReplyToMessageId:  doc.ReplyToMessageID,
			IsDeleted:         doc.IsDeleted,
			EditedAtUnixMs: func() int64 {
				if doc.EditedAt.IsZero() {
					return 0
				}
				return doc.EditedAt.UnixMilli()
			}(),
			EventType: pb.EventType_EVENT_TYPE_MESSAGE,
		})
	}
	return results, cur.Err()
}

// GetUserRooms finds all rooms a user has participated in by scanning MongoDB messages.
// For DMs the room_id is "dm:a:b" (sorted), so any room_id containing the userID
// as a DM segment belongs to this user. For group rooms the user must appear as sender.
// Returns one summary per room sorted by most-recent message first.
func (r *MessageRepository) GetUserRooms(ctx context.Context, userID string) ([]UserRoomSummary, error) {
	// Exact DM segment regex:
	// Supports both ":" and "-" formats (legacy data check)
	exactDmRegex := "^dm[:-]" + userID + "[:-]|^dm[:-][^:-]+[:-]" + userID + "$"

	pipeline := bson.A{
		// Step 1: match messages for this user (as sender or DM participant)
		bson.M{"$match": bson.M{
			"is_deleted": bson.M{"$ne": true},
			"$or": bson.A{
				bson.M{"user_id": userID},
				bson.M{"room_id": bson.M{"$regex": exactDmRegex}},
			},
		}},
		// Step 2: sort by sent_at desc so $first gives us the latest message
		bson.M{"$sort": bson.M{"sent_at": -1}},
		// Step 3: group by room_id, pick first (latest) message
		bson.M{"$group": bson.M{
			"_id":          "$room_id",
			"lastText":     bson.M{"$first": "$text_body"},
			"lastSentAt":   bson.M{"$first": "$sent_at"},
		}},
		// Step 4: sort groups by most recent
		bson.M{"$sort": bson.M{"lastSentAt": -1}},
		// Step 5: limit to 100 rooms
		bson.M{"$limit": 100},
	}

	cur, err := r.messages.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("GetUserRooms aggregate: %w", err)
	}
	defer cur.Close(ctx)

	var results []UserRoomSummary
	for cur.Next(ctx) {
		var row struct {
			RoomID    string    `bson:"_id"`
			LastText  string    `bson:"lastText"`
			LastSentAt time.Time `bson:"lastSentAt"`
		}
		if err := cur.Decode(&row); err != nil {
			continue
		}
		// Extract member IDs from DM room_id (supports both dm: and dm-)
		var memberIDs []string
		if len(row.RoomID) > 3 && (row.RoomID[:3] == "dm:" || row.RoomID[:3] == "dm-") {
			parts := splitRoomID(row.RoomID)
			if len(parts) == 2 {
				memberIDs = parts
			}
		}
		results = append(results, UserRoomSummary{
			RoomID:        row.RoomID,
			LastMessage:   row.LastText,
			LastMessageAt: row.LastSentAt,
			MemberIDs:     memberIDs,
		})
	}
	return results, cur.Err()
}

func splitRoomID(roomID string) []string {
	// "dm:a:b" or "dm-a-b" → ["a", "b"]
	if len(roomID) <= 3 {
		return nil
	}
	rest := roomID[3:] // strip "dm:" or "dm-"
	sep := -1
	for i, c := range rest {
		if c == ':' || c == '-' {
			sep = i
			break
		}
	}
	if sep < 0 {
		return nil
	}
	return []string{rest[:sep], rest[sep+1:]}
}

func containsStr(s, sub string) bool {
	return len(s) >= len(sub) && func() bool {
		for i := 0; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	}()
}

func hasSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}
