package repository

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	pb "chat-service/app/pb"
)

// ─────────────────────────────────────────────────────────────────────────────
// Document types matching prompt requirements
// ─────────────────────────────────────────────────────────────────────────────

type conversationDoc struct {
	ID            string    `bson:"_id"`
	Type          string    `bson:"type"` // "DIRECT" or "GROUP"
	Participants  []string  `bson:"participants"`
	GroupName     string    `bson:"group_name,omitempty"`
	GroupPhoto    string    `bson:"group_photo,omitempty"`
	Description   string    `bson:"description,omitempty"`
	MemberCount   int32     `bson:"member_count,omitempty"`
	LastMessage   string    `bson:"last_message,omitempty"`
	LastMessageID string    `bson:"last_message_id,omitempty"`
	LastMessageAt time.Time `bson:"last_message_at,omitempty"`
	CreatedBy     string    `bson:"created_by"`
	CreatedAt     time.Time `bson:"created_at"`
}

type groupMemberDoc struct {
	ID             string    `bson:"_id"`
	ConversationID string    `bson:"conversation_id"`
	UserID         string    `bson:"user_id"`
	Role           string    `bson:"role"`   // "OWNER", "ADMIN", "MEMBER"
	JoinedAt       time.Time `bson:"joined_at"`
	Status         string    `bson:"status"` // "ACTIVE", "INACTIVE"
}

type messageDoc struct {
	ID               string    `bson:"_id"`
	MessageID        string    `bson:"message_id,omitempty"`
	ConversationID   string    `bson:"conversation_id"`
	SenderID         string    `bson:"sender_id"`
	MessageType      string    `bson:"message_type"` // "TEXT", "IMAGE", "VIDEO", etc.
	Content          string    `bson:"content"`
	CreatedAt        time.Time `bson:"created_at"`
	DeliveredAt      time.Time `bson:"delivered_at,omitempty"`
	MediaKey         string    `bson:"media_key,omitempty"`
	MediaName        string    `bson:"media_name,omitempty"`
	MediaMimeType    string    `bson:"media_mime_type,omitempty"`
	MediaSizeBytes   int64     `bson:"media_size_bytes,omitempty"`
	ReplyToMessageID string    `bson:"reply_to_message_id,omitempty"`
	IsDeleted        bool      `bson:"is_deleted"`
	EditedAt         time.Time `bson:"edited_at,omitempty"`


	// Legacy BSON compatibility fields
	RoomID   string    `bson:"room_id,omitempty"`
	UserID   string    `bson:"user_id,omitempty"`
	TextBody string    `bson:"text_body,omitempty"`
	SentAt   time.Time `bson:"sent_at,omitempty"`
}

type messageReadDoc struct {
	ID             string   `bson:"_id"`
	ConversationID string   `bson:"conversation_id"`
	SenderID       string   `bson:"sender_id"`
	Content        string   `bson:"content"`
	ReadBy         []string `bson:"read_by"`
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

type UserRoomSummary struct {
	RoomID        string
	LastMessage   string
	LastMessageAt time.Time
	MemberIDs     []string
}

type MessageStore interface {
	SaveMessage(ctx context.Context, msg *pb.ServerMessage) error
	SoftDeleteMessage(ctx context.Context, roomID string, sentAtUnixMs int64, messageID string) error
	EditMessage(ctx context.Context, messageID, userID, newText string) (int64, error)
	SoftDeleteMessageByOwner(ctx context.Context, messageID, userID string) (bool, error)
	SaveMediaUpload(ctx context.Context, mediaKey, uploaderID, roomID, fileName, mimeType string, sizeBytes int64, expiresAt time.Time) error
	UpdateReadReceipt(ctx context.Context, roomID, userID, lastMsgID string, lastReadAt time.Time) error
	GetMessagesBefore(ctx context.Context, roomID string, beforeUnixMs int64, limit int) ([]*pb.ServerMessage, error)
	GetUserRooms(ctx context.Context, userID string) ([]UserRoomSummary, error)
	SearchMessages(ctx context.Context, convID, query string, limit int) ([]*pb.ServerMessage, error)
	CreateOrGetDirectConversation(ctx context.Context, userA, userB, createdBy string) (*pb.ConversationDetail, error)
	CreateGroupConversation(ctx context.Context, groupName, groupPhoto, description, createdBy string, memberIDs []string) (*pb.ConversationDetail, error)
	GetConversation(ctx context.Context, convID string) (*pb.ConversationDetail, []*pb.GroupMember, error)
	AddGroupMember(ctx context.Context, convID, userID, operatorID, role string) error
	RemoveGroupMember(ctx context.Context, convID, userID, operatorID string) error
	LeaveGroup(ctx context.Context, convID, userID string) error
	PromoteAdmin(ctx context.Context, convID, userID, operatorID string) error
	TransferOwnership(ctx context.Context, convID, currentOwnerID, newOwnerID string) error
	DeleteGroup(ctx context.Context, convID, ownerID string) error
	MarkMessageRead(ctx context.Context, convID, userID, msgID string) error
	GetUnreadCount(ctx context.Context, userID, convID string) (int32, error)
	Close()
}

type MessageRepository struct {
	client        *mongo.Client
	conversations *mongo.Collection
	groupMembers  *mongo.Collection
	messages      *mongo.Collection
	messageRead   *mongo.Collection
	receipts      *mongo.Collection
	media         *mongo.Collection
}

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
		client:        client,
		conversations: db.Collection("conversations"),
		groupMembers:  db.Collection("group_members"),
		messages:      db.Collection("messages"),
		messageRead:   db.Collection("message_read"),
		receipts:      db.Collection("read_receipts"),
		media:         db.Collection("media_uploads"),
	}

	if err := r.ensureIndexes(ctx); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *MessageRepository) ensureIndexes(ctx context.Context) error {
	// conversations indexes
	_, _ = r.conversations.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "participants", Value: 1}},
			Options: options.Index().SetName("conv_participants"),
		},
		{
			Keys:    bson.D{{Key: "type", Value: 1}},
			Options: options.Index().SetName("conv_type"),
		},
	})

	// group_members indexes
	_, _ = r.groupMembers.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "conversation_id", Value: 1}, {Key: "user_id", Value: 1}},
			Options: options.Index().SetName("gm_conv_user").SetUnique(true),
		},
		{
			Keys:    bson.D{{Key: "user_id", Value: 1}},
			Options: options.Index().SetName("gm_user"),
		},
	})

	// messages indexes
	_, _ = r.messages.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "conversation_id", Value: 1}, {Key: "created_at", Value: -1}},
			Options: options.Index().SetName("conv_time"),
		},
		{
			Keys:    bson.D{{Key: "room_id", Value: 1}, {Key: "sent_at", Value: -1}},
			Options: options.Index().SetName("room_time"),
		},
	})

	// message_read index
	_, _ = r.messageRead.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "conversation_id", Value: 1}},
		Options: options.Index().SetName("mr_conv"),
	})

	return nil
}

func (r *MessageRepository) Close() {
	if r.client != nil {
		_ = r.client.Disconnect(context.Background())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Conversations & Group Management Operations
// ─────────────────────────────────────────────────────────────────────────────

func (r *MessageRepository) CreateOrGetDirectConversation(ctx context.Context, userA, userB, createdBy string) (*pb.ConversationDetail, error) {
	if userA == "" || userB == "" {
		return nil, fmt.Errorf("userA and userB are required")
	}
	participants := []string{userA, userB}
	sort.Strings(participants)
	convID := fmt.Sprintf("dm:%s:%s", participants[0], participants[1])
	now := time.Now().UTC()
	doc := conversationDoc{
		ID:           convID,
		Type:         "DIRECT",
		Participants: participants,
		CreatedBy:    createdBy,
		CreatedAt:    now,
	}

	// Check if conversation exists for this pair (may still be legacy CONV_*).
	filter := bson.M{
		"type":         "DIRECT",
		"participants": participants,
	}
	var existing conversationDoc
	err := r.conversations.FindOne(ctx, filter).Decode(&existing)
	if err == nil {
		detail := docToConversationDetail(&existing)
		// Always return the canonical dm:* id so list/create/persist stay aligned with Postgres.
		detail.ConversationId = convID
		if existing.ID != convID {
			_, _ = r.conversations.UpdateOne(ctx,
				bson.M{"_id": convID},
				bson.M{"$setOnInsert": doc},
				options.UpdateOne().SetUpsert(true),
			)
		}
		return detail, nil
	}

	_, err = r.conversations.InsertOne(ctx, doc)
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		return nil, fmt.Errorf("create direct conversation: %w", err)
	}
	if err != nil {
		_ = r.conversations.FindOne(ctx, bson.M{"_id": convID}).Decode(&existing)
		return docToConversationDetail(&existing), nil
	}

	return docToConversationDetail(&doc), nil
}

func (r *MessageRepository) CreateGroupConversation(ctx context.Context, groupName, groupPhoto, description, createdBy string, memberIDs []string) (*pb.ConversationDetail, error) {
	if groupName == "" {
		return nil, fmt.Errorf("group_name is required")
	}
	if createdBy == "" {
		return nil, fmt.Errorf("created_by is required")
	}

	uniqueMembers := make(map[string]bool)
	uniqueMembers[createdBy] = true
	for _, m := range memberIDs {
		if m != "" {
			uniqueMembers[m] = true
		}
	}
	finalMembers := make([]string, 0, len(uniqueMembers))
	for m := range uniqueMembers {
		finalMembers = append(finalMembers, m)
	}

	convID := fmt.Sprintf("CONV_%d", time.Now().UnixNano())
	now := time.Now().UTC()

	conv := conversationDoc{
		ID:           convID,
		Type:         "GROUP",
		Participants: finalMembers,
		GroupName:    groupName,
		GroupPhoto:   groupPhoto,
		Description:  description,
		MemberCount:  int32(len(finalMembers)),
		CreatedBy:    createdBy,
		CreatedAt:    now,
	}

	_, err := r.conversations.InsertOne(ctx, conv)
	if err != nil {
		return nil, fmt.Errorf("insert group conversation: %w", err)
	}

	// Insert group_members documents
	var gmDocs []interface{}
	for _, m := range finalMembers {
		role := "MEMBER"
		if m == createdBy {
			role = "OWNER"
		}
		gmDocs = append(gmDocs, groupMemberDoc{
			ID:             fmt.Sprintf("GM_%s_%s", convID, m),
			ConversationID: convID,
			UserID:         m,
			Role:           role,
			JoinedAt:       now,
			Status:         "ACTIVE",
		})
	}
	if len(gmDocs) > 0 {
		_, err = r.groupMembers.InsertMany(ctx, gmDocs)
		if err != nil {
			return nil, fmt.Errorf("insert group members: %w", err)
		}
	}

	return docToConversationDetail(&conv), nil
}

func (r *MessageRepository) GetConversation(ctx context.Context, convID string) (*pb.ConversationDetail, []*pb.GroupMember, error) {
	var cDoc conversationDoc
	err := r.conversations.FindOne(ctx, bson.M{"_id": convID}).Decode(&cDoc)
	if err != nil {
		return nil, nil, fmt.Errorf("get conversation: %w", err)
	}

	var members []*pb.GroupMember
	if cDoc.Type == "GROUP" {
		cur, err := r.groupMembers.Find(ctx, bson.M{"conversation_id": convID, "status": "ACTIVE"})
		if err == nil {
			defer cur.Close(ctx)
			for cur.Next(ctx) {
				var gm groupMemberDoc
				if err := cur.Decode(&gm); err == nil {
					members = append(members, &pb.GroupMember{
						Id:             gm.ID,
						ConversationId: gm.ConversationID,
						UserId:         gm.UserID,
						Role:           gm.Role,
						JoinedAt:       gm.JoinedAt.UnixMilli(),
						Status:         gm.Status,
					})
				}
			}
		}
	}

	return docToConversationDetail(&cDoc), members, nil
}

func (r *MessageRepository) AddGroupMember(ctx context.Context, convID, userID, operatorID, role string) error {
	if role == "" {
		role = "MEMBER"
	}
	now := time.Now().UTC()
	gm := groupMemberDoc{
		ID:             fmt.Sprintf("GM_%s_%s", convID, userID),
		ConversationID: convID,
		UserID:         userID,
		Role:           role,
		JoinedAt:       now,
		Status:         "ACTIVE",
	}

	_, err := r.groupMembers.UpdateOne(ctx,
		bson.M{"conversation_id": convID, "user_id": userID},
		bson.M{"$set": bson.M{
			"role":      role,
			"joined_at": now,
			"status":    "ACTIVE",
		}},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return err
	}

	_ = gm
	_, _ = r.conversations.UpdateOne(ctx,
		bson.M{"_id": convID},
		bson.M{
			"$addToSet": bson.M{"participants": userID},
			"$inc":      bson.M{"member_count": 1},
		},
	)
	return nil
}

func (r *MessageRepository) RemoveGroupMember(ctx context.Context, convID, userID, operatorID string) error {
	_, err := r.groupMembers.UpdateOne(ctx,
		bson.M{"conversation_id": convID, "user_id": userID},
		bson.M{"$set": bson.M{"status": "INACTIVE"}},
	)
	if err != nil {
		return err
	}

	_, _ = r.conversations.UpdateOne(ctx,
		bson.M{"_id": convID},
		bson.M{
			"$pull": bson.M{"participants": userID},
			"$inc":  bson.M{"member_count": -1},
		},
	)
	return nil
}

func (r *MessageRepository) LeaveGroup(ctx context.Context, convID, userID string) error {
	return r.RemoveGroupMember(ctx, convID, userID, userID)
}

func (r *MessageRepository) PromoteAdmin(ctx context.Context, convID, userID, operatorID string) error {
	res, err := r.groupMembers.UpdateOne(ctx,
		bson.M{"conversation_id": convID, "user_id": userID, "status": "ACTIVE"},
		bson.M{"$set": bson.M{"role": "ADMIN"}},
	)
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("active member not found in group")
	}
	return nil
}

func (r *MessageRepository) TransferOwnership(ctx context.Context, convID, currentOwnerID, newOwnerID string) error {
	_, err := r.groupMembers.UpdateOne(ctx,
		bson.M{"conversation_id": convID, "user_id": newOwnerID, "status": "ACTIVE"},
		bson.M{"$set": bson.M{"role": "OWNER"}},
	)
	if err != nil {
		return err
	}
	if currentOwnerID != "" {
		_, _ = r.groupMembers.UpdateOne(ctx,
			bson.M{"conversation_id": convID, "user_id": currentOwnerID},
			bson.M{"$set": bson.M{"role": "ADMIN"}},
		)
	}
	return nil
}

func (r *MessageRepository) DeleteGroup(ctx context.Context, convID, ownerID string) error {
	_, err := r.conversations.DeleteOne(ctx, bson.M{"_id": convID})
	if err != nil {
		return err
	}
	_, _ = r.groupMembers.DeleteMany(ctx, bson.M{"conversation_id": convID})
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Messages & Read Receipts Operations
// ─────────────────────────────────────────────────────────────────────────────

func (r *MessageRepository) SaveMessage(ctx context.Context, msg *pb.ServerMessage) error {
	convID := msg.GetRoomId()
	senderID := msg.GetUserId()
	msgID := msg.GetMessageId()
	if msgID == "" {
		msgID = uuid.NewString()
	}
	createdAt := time.UnixMilli(msg.GetSentAtUnixMs()).UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	msgType := msg.GetType().String()

	doc := messageDoc{
		ID:               msgID,
		MessageID:        msgID,
		ConversationID:   convID,
		SenderID:         senderID,
		MessageType:      msgType,
		Content:          msg.GetText(),
		CreatedAt:        createdAt,
		DeliveredAt:      time.UnixMilli(msg.GetDeliveredAtUnixMs()).UTC(),
		MediaKey:         msg.GetMediaKey(),
		MediaName:        msg.GetMediaName(),
		MediaMimeType:    msg.GetMediaMimeType(),
		MediaSizeBytes:   msg.GetMediaSizeBytes(),
		ReplyToMessageID: msg.GetReplyToMessageId(),
		IsDeleted:        msg.GetIsDeleted(),
		// legacy fallback tags
		RoomID:   convID,
		UserID:   senderID,
		TextBody: msg.GetText(),
		SentAt:   createdAt,
	}

	_, err := r.messages.InsertOne(ctx, doc)
	if err != nil {
		return fmt.Errorf("insert message: %w", err)
	}

	// Update conversation last_message
	preview := msg.GetText()
	if preview == "" && msg.GetMediaName() != "" {
		preview = "📎 " + msg.GetMediaName()
	}
	_, _ = r.conversations.UpdateOne(ctx,
		bson.M{"_id": convID},
		bson.M{"$set": bson.M{
			"last_message":    preview,
			"last_message_id": msgID,
			"last_message_at": createdAt,
		}},
	)

	// Upsert message_read document
	readDoc := messageReadDoc{
		ID:             msgID,
		ConversationID: convID,
		SenderID:       senderID,
		Content:        preview,
		ReadBy:         []string{senderID},
	}
	_, _ = r.messageRead.UpdateOne(ctx,
		bson.M{"_id": msgID},
		bson.M{
			"$setOnInsert": readDoc,
			"$addToSet":    bson.M{"read_by": senderID},
		},
		options.UpdateOne().SetUpsert(true),
	)

	return nil
}

func (r *MessageRepository) MarkMessageRead(ctx context.Context, convID, userID, msgID string) error {
	now := time.Now().UTC()
	_ = r.UpdateReadReceipt(ctx, convID, userID, msgID, now)

	if msgID != "" {
		_, err := r.messageRead.UpdateOne(ctx,
			bson.M{"_id": msgID},
			bson.M{"$addToSet": bson.M{"read_by": userID}},
		)
		return err
	}

	// Mark all messages in conversation read by user
	_, err := r.messageRead.UpdateMany(ctx,
		bson.M{"conversation_id": convID},
		bson.M{"$addToSet": bson.M{"read_by": userID}},
	)
	return err
}

func (r *MessageRepository) GetUnreadCount(ctx context.Context, userID, convID string) (int32, error) {
	filter := bson.M{
		"sender_id": bson.M{"$ne": userID},
		"read_by":   bson.M{"$ne": userID},
	}
	if convID != "" {
		filter["conversation_id"] = convID
	}
	count, err := r.messageRead.CountDocuments(ctx, filter)
	if err != nil {
		return 0, err
	}
	return int32(count), nil
}

func (r *MessageRepository) SearchMessages(ctx context.Context, convID, query string, limit int) ([]*pb.ServerMessage, error) {
	if limit <= 0 {
		limit = 50
	}
	filter := bson.M{
		"is_deleted": bson.M{"$ne": true},
		"$and": bson.A{
			bson.M{"$or": bson.A{
				bson.M{"conversation_id": convID},
				bson.M{"room_id": convID},
			}},
			bson.M{"$or": bson.A{
				bson.M{"content": bson.M{"$regex": query, "$options": "i"}},
				bson.M{"text_body": bson.M{"$regex": query, "$options": "i"}},
			}},
		},
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "created_at", Value: -1}}).
		SetLimit(int64(limit))

	cur, err := r.messages.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("search messages: %w", err)
	}
	defer cur.Close(ctx)

	var results []*pb.ServerMessage
	for cur.Next(ctx) {
		var doc messageDoc
		if err := cur.Decode(&doc); err != nil {
			continue
		}
		results = append(results, docToServerMessage(&doc))
	}
	return results, cur.Err()
}

func (r *MessageRepository) SoftDeleteMessage(ctx context.Context, _ string, sentAtUnixMs int64, messageID string) error {
	_, err := r.messages.UpdateOne(ctx,
		bson.M{"$or": bson.A{bson.M{"_id": messageID}, bson.M{"message_id": messageID}}},
		bson.M{"$set": bson.M{"is_deleted": true}},
	)
	return err
}

func (r *MessageRepository) SoftDeleteMessageByOwner(ctx context.Context, messageID, userID string) (bool, error) {
	res, err := r.messages.UpdateOne(ctx,
		bson.M{
			"$and": bson.A{
				bson.M{"$or": bson.A{bson.M{"_id": messageID}, bson.M{"message_id": messageID}}},
				bson.M{"$or": bson.A{bson.M{"sender_id": userID}, bson.M{"user_id": userID}}},
			},
		},
		bson.M{"$set": bson.M{"is_deleted": true}},
	)
	if err != nil {
		return false, err
	}
	return res.ModifiedCount > 0 || res.MatchedCount > 0, nil
}

func (r *MessageRepository) EditMessage(ctx context.Context, messageID, userID, newText string) (int64, error) {
	now := time.Now().UTC()
	res, err := r.messages.UpdateOne(ctx,
		bson.M{
			"is_deleted": bson.M{"$ne": true},
			"$and": bson.A{
				bson.M{"$or": bson.A{bson.M{"_id": messageID}, bson.M{"message_id": messageID}}},
				bson.M{"$or": bson.A{bson.M{"sender_id": userID}, bson.M{"user_id": userID}}},
			},
		},
		bson.M{"$set": bson.M{
			"content":   newText,
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

func (r *MessageRepository) GetMessages(ctx context.Context, roomID string, limit, _ int) ([]*pb.ServerMessage, error) {
	if limit <= 0 {
		limit = 50
	}
	ids := roomIDAliases(roomID)
	return r.queryMessages(ctx,
		bson.M{"$or": bson.A{
			bson.M{"conversation_id": bson.M{"$in": ids}},
			bson.M{"room_id": bson.M{"$in": ids}},
		}},
		limit,
	)
}

func (r *MessageRepository) GetMessagesBefore(ctx context.Context, roomID string, beforeUnixMs int64, limit int) ([]*pb.ServerMessage, error) {
	if limit <= 0 {
		limit = 50
	}
	if beforeUnixMs == 0 {
		return r.GetMessages(ctx, roomID, limit, 0)
	}
	t := time.UnixMilli(beforeUnixMs).UTC()
	ids := roomIDAliases(roomID)
	return r.queryMessages(ctx,
		bson.M{
			"$and": bson.A{
				bson.M{"$or": bson.A{
					bson.M{"conversation_id": bson.M{"$in": ids}},
					bson.M{"room_id": bson.M{"$in": ids}},
				}},
				bson.M{"$or": bson.A{bson.M{"created_at": bson.M{"$lt": t}}, bson.M{"sent_at": bson.M{"$lt": t}}}},
			},
		},
		limit,
	)
}

func (r *MessageRepository) queryMessages(ctx context.Context, filter bson.M, limit int) ([]*pb.ServerMessage, error) {
	opts := options.Find().
		SetSort(bson.D{{Key: "created_at", Value: -1}, {Key: "sent_at", Value: -1}}).
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
		results = append(results, docToServerMessage(&doc))
	}
	return results, cur.Err()
}

func (r *MessageRepository) GetUserRooms(ctx context.Context, userID string) ([]UserRoomSummary, error) {
	// Query conversations collection first
	cur, err := r.conversations.Find(ctx, bson.M{
		"$or": bson.A{
			bson.M{"participants": userID},
			bson.M{"created_by": userID},
		},
	})
	if err == nil {
		defer cur.Close(ctx)
		var summaries []UserRoomSummary
		for cur.Next(ctx) {
			var doc conversationDoc
			if err := cur.Decode(&doc); err == nil {
				summaries = append(summaries, UserRoomSummary{
					RoomID:        doc.ID,
					LastMessage:   doc.LastMessage,
					LastMessageAt: doc.LastMessageAt,
					MemberIDs:     doc.Participants,
				})
			}
		}
		if len(summaries) > 0 {
			return summaries, nil
		}
	}

	// Fallback to messages aggregate
	exactDmRegex := "^dm[:-]" + userID + "[:-]|^dm[:-][^:-]+[:-]" + userID + "$"
	pipeline := bson.A{
		bson.M{"$match": bson.M{
			"is_deleted": bson.M{"$ne": true},
			"$or": bson.A{
				bson.M{"sender_id": userID},
				bson.M{"user_id": userID},
				bson.M{"conversation_id": bson.M{"$regex": exactDmRegex}},
				bson.M{"room_id": bson.M{"$regex": exactDmRegex}},
			},
		}},
		bson.M{"$sort": bson.M{"created_at": -1, "sent_at": -1}},
		bson.M{"$group": bson.M{
			"_id":        "$conversation_id",
			"lastText":   bson.M{"$first": "$content"},
			"lastSentAt": bson.M{"$first": "$created_at"},
		}},
		bson.M{"$sort": bson.M{"lastSentAt": -1}},
		bson.M{"$limit": 100},
	}

	acur, err := r.messages.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("GetUserRooms aggregate: %w", err)
	}
	defer acur.Close(ctx)

	var results []UserRoomSummary
	for acur.Next(ctx) {
		var row struct {
			RoomID     string    `bson:"_id"`
			LastText   string    `bson:"lastText"`
			LastSentAt time.Time `bson:"lastSentAt"`
		}
		if err := acur.Decode(&row); err != nil || row.RoomID == "" {
			continue
		}
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
	return results, acur.Err()
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func docToConversationDetail(d *conversationDoc) *pb.ConversationDetail {
	roomType := pb.RoomType_ROOM_TYPE_DM
	if d.Type == "GROUP" {
		roomType = pb.RoomType_ROOM_TYPE_GROUP
	}
	return &pb.ConversationDetail{
		ConversationId: d.ID,
		Type:           roomType,
		Participants:   d.Participants,
		GroupName:      d.GroupName,
		GroupPhoto:     d.GroupPhoto,
		Description:    d.Description,
		MemberCount:    d.MemberCount,
		LastMessage:    d.LastMessage,
		LastMessageId:  d.LastMessageID,
		LastMessageAt:  d.LastMessageAt.UnixMilli(),
		CreatedBy:      d.CreatedBy,
		CreatedAt:      d.CreatedAt.UnixMilli(),
	}
}

func docToServerMessage(doc *messageDoc) *pb.ServerMessage {
	cID := doc.ConversationID
	if cID == "" {
		cID = doc.RoomID
	}
	sID := doc.SenderID
	if sID == "" {
		sID = doc.UserID
	}
	text := doc.Content
	if text == "" {
		text = doc.TextBody
	}
	createdAt := doc.CreatedAt
	if createdAt.IsZero() {
		createdAt = doc.SentAt
	}

	msgType := pb.MessageType_MESSAGE_TYPE_TEXT
	if t, ok := pb.MessageType_value["MESSAGE_TYPE_"+strings.ToUpper(doc.MessageType)]; ok {
		msgType = pb.MessageType(t)
	}

	return &pb.ServerMessage{
		RoomId:            cID,
		UserId:            sID,
		MessageId:         doc.ID,
		Text:              text,
		Type:              msgType,
		SentAtUnixMs:      createdAt.UnixMilli(),
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
	}
}

func splitRoomID(roomID string) []string {
	if len(roomID) <= 3 {
		return nil
	}
	rest := roomID[3:]
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

// roomIDAliases returns the canonical room id plus legacy CONV_* / unsorted forms
// so history written under the old scheme is still readable.
func roomIDAliases(roomID string) []string {
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return nil
	}
	seen := map[string]struct{}{roomID: {}}
	out := []string{roomID}
	add := func(id string) {
		id = strings.TrimSpace(id)
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}

	canonicalDM := func(a, b string) {
		if a == "" || b == "" {
			return
		}
		ids := []string{a, b}
		sort.Strings(ids)
		add(fmt.Sprintf("dm:%s:%s", ids[0], ids[1]))
		add(fmt.Sprintf("CONV_%s_%s", ids[0], ids[1]))
		add(fmt.Sprintf("CONV_%s_%s", a, b))
		add(fmt.Sprintf("CONV_%s_%s", b, a))
	}

	if strings.HasPrefix(roomID, "dm:") {
		parts := strings.Split(roomID, ":")
		if len(parts) >= 3 {
			canonicalDM(parts[1], strings.Join(parts[2:], ":"))
		}
	} else if strings.HasPrefix(roomID, "CONV_") {
		rest := strings.TrimPrefix(roomID, "CONV_")
		// UUIDs are 36 chars; legacy id is CONV_<uuid>_<uuid>
		if len(rest) >= 73 && rest[36] == '_' {
			canonicalDM(rest[:36], rest[37:])
		}
	}
	return out
}
