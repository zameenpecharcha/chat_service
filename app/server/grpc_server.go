package server

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"chat-service/app/chat"
	pb "chat-service/app/pb"
	"chat-service/app/repository"
	"chat-service/app/storage"
)

// ChatServer implements pb.ChatServiceServer.
// Presence backend: pass chat.NewRedisPresenceStore(redisClient) for multi-pod;
// pass nil to fall back to local in-memory store (dev / single-pod).
type ChatServer struct {
	pb.UnimplementedChatServiceServer
	hub      *chat.Hub
	rooms    *chat.RoomStore
	pgRooms  *repository.RoomRepository
	msgRepo  repository.MessageStore
	storage  storage.Storage
	presence chat.PresenceStore
}

func NewChatServer(
	h *chat.Hub,
	rs *chat.RoomStore,
	pg *repository.RoomRepository,
	msg repository.MessageStore,
	st storage.Storage,
	ps chat.PresenceStore,
) *ChatServer {
	if ps == nil {
		ps = chat.NewLocalPresenceStore()
	}
	return &ChatServer{
		hub: h, rooms: rs, pgRooms: pg, msgRepo: msg, storage: st, presence: ps,
	}
}

// ── Chat ──────────────────────────────────────────────────────────────────────

func (s *ChatServer) Chat(stream pb.ChatService_ChatServer) error {
	// Reject new connections when this pod is at capacity.
	// The client (or api_gateway) should retry a different pod.
	if s.hub.AtCapacity() {
		return status.Errorf(codes.ResourceExhausted,
			"server at capacity (%d connections): try another pod", s.hub.ConnCount())
	}

	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetUserId() == "" || first.GetRoomId() == "" {
		return status.Error(codes.InvalidArgument, "user_id and room_id are required in the first message")
	}

	userID := first.GetUserId()
	// Bug-1/2/3 fix: normalize the roomId from the first frame so the hub
	// registers and broadcasts under the canonical "dm:low:high" key.
	roomID := normalizeRoomID(first.GetRoomId())
	log.Printf("[chat-service] stream connected user=%s room=%s (raw=%s)", userID, roomID, first.GetRoomId())

	client := chat.NewClient(userID, roomID)
	ctx := stream.Context()
	s.hub.Register(ctx, client)

	// Start Redis heartbeat to keep the presence key alive across pod restarts.
	// If presence is a RedisPresenceStore this refreshes the TTL every 30 s;
	// if it's the local fallback it's a no-op goroutine that just reads ctx.Done().
	if rps, ok := s.presence.(*chat.RedisPresenceStore); ok {
		go rps.StartHeartbeat(ctx, userID)
	}

	defer func() {
		s.hub.Unregister(client)
		// Mark offline and broadcast presence event to room
		s.presence.SetOnline(context.Background(), userID, false)
		s.hub.Broadcast(ctx, &pb.ServerMessage{
			RoomId:         roomID,
			UserId:         userID,
			EventType:      pb.EventType_EVENT_TYPE_PRESENCE,
			IsOnline:       false,
			LastSeenUnixMs: time.Now().UnixMilli(),
		}, true)
	}()

	// Mark online and broadcast presence
	s.presence.SetOnline(ctx, userID, true)
	s.hub.Broadcast(ctx, &pb.ServerMessage{
		RoomId:    roomID,
		UserId:    userID,
		EventType: pb.EventType_EVENT_TYPE_PRESENCE,
		IsOnline:  true,
	}, true)

	// Sender goroutine: hub → gRPC stream
	done := make(chan struct{})
	go func() {
		defer close(done)
		for m := range client.Send {
			// Attach fresh presigned URL for media messages
			if m.MediaKey != "" && s.storage != nil && m.EventType == pb.EventType_EVENT_TYPE_MESSAGE {
				if u, _, e := s.storage.GetPresignedURL(ctx, m.MediaKey); e == nil {
					m.MediaUrl = u
				}
			}
			if err := stream.Send(m); err != nil {
				return
			}
		}
	}()

	// Process first frame if it carries content
	s.processIncoming(ctx, first)

	// Receive loop
	var recvErr error
	for {
		in, err := stream.Recv()
		if err != nil {
			recvErr = err
			break
		}
		s.processIncoming(ctx, in)
	}

	<-done
	return recvErr
}

// processIncoming routes a ClientMessage to the correct handler based on EventType.
func (s *ChatServer) processIncoming(ctx context.Context, in *pb.ClientMessage) {
	// Bug-2/3 fix: always normalize the roomId before routing or persisting so
	// all messages land under the canonical DM key regardless of what the client sent.
	roomID := normalizeRoomID(in.GetRoomId())

	switch in.GetEventType() {

	case pb.EventType_EVENT_TYPE_TYPING_START, pb.EventType_EVENT_TYPE_TYPING_STOP:
		// Typing indicator: broadcast control event, never persist
		s.hub.Broadcast(ctx, &pb.ServerMessage{
			RoomId:    roomID,
			UserId:    in.GetUserId(),
			EventType: in.GetEventType(),
		}, false)

	case pb.EventType_EVENT_TYPE_READ_RECEIPT:
		log.Printf("[chat-service] read receipt room=%s user=%s message=%s", roomID, in.GetUserId(), in.GetMessageId())
		// Read receipt: update Cassandra + Postgres, broadcast ✓✓ to room
		if in.GetMessageId() != "" {
			now := time.Now()
			go func() {
				bg := context.Background()
				if s.msgRepo != nil {
					_ = s.msgRepo.UpdateReadReceipt(bg, roomID, in.GetUserId(), in.GetMessageId(), now)
				}
				if s.pgRooms != nil {
					_ = s.pgRooms.UpdateReadReceipt(bg, roomID, in.GetUserId(), in.GetMessageId(), now)
				}
			}()
			s.hub.Broadcast(ctx, &pb.ServerMessage{
				RoomId:    roomID,
				UserId:    in.GetUserId(),
				MessageId: in.GetMessageId(),
				EventType: pb.EventType_EVENT_TYPE_READ_RECEIPT,
				Status:    pb.MessageStatus_MESSAGE_STATUS_READ,
			}, false)
		}

	case pb.EventType_EVENT_TYPE_REACTION:
		// Emoji reaction: broadcast to room (persist to Cassandra in future iteration)
		if in.GetMessageId() != "" && in.GetReactionEmoji() != "" {
			s.hub.Broadcast(ctx, &pb.ServerMessage{
				RoomId:        roomID,
				UserId:        in.GetUserId(),
				MessageId:     in.GetMessageId(),
				EventType:     pb.EventType_EVENT_TYPE_REACTION,
				ReactionEmoji: in.GetReactionEmoji(),
			}, true)
		}

	case pb.EventType_EVENT_TYPE_DELETE:
		// Soft delete: mark in Cassandra, broadcast tombstone
		if in.GetMessageId() != "" {
			if s.msgRepo != nil {
				go func() {
					_ = s.msgRepo.SoftDeleteMessage(context.Background(),
						roomID, in.GetSentAtUnixMs(), in.GetMessageId())
				}()
			}
			s.hub.Broadcast(ctx, &pb.ServerMessage{
				RoomId:    roomID,
				UserId:    in.GetUserId(),
				MessageId: in.GetMessageId(),
				EventType: pb.EventType_EVENT_TYPE_DELETE,
				IsDeleted: true,
			}, true)
		}

	default:
		log.Printf("[chat-service] incoming message room=%s user=%s event=%v text=%q media=%s", roomID, in.GetUserId(), in.GetEventType(), in.GetText(), in.GetMediaKey())
		// EVENT_TYPE_MESSAGE (0) or any unknown value — treat as a chat message
		if in.GetText() == "" && in.GetMediaKey() == "" {
			return // empty frame (e.g. the register-only first message)
		}
		sm := toServerMsg(in)
		sm.Status = pb.MessageStatus_MESSAGE_STATUS_SENT
		s.hub.Broadcast(ctx, sm, true)
		s.persistMessage(ctx, sm)
	}
}

// persistMessage saves to Cassandra + updates Postgres last-activity (async).
func (s *ChatServer) persistMessage(ctx context.Context, msg *pb.ServerMessage) {
	if s.msgRepo == nil && s.pgRooms == nil {
		return
	}
	go func() {
		bg := context.Background()
		if s.msgRepo != nil {
			_ = s.msgRepo.SaveMessage(bg, msg)
		}
		if s.pgRooms != nil {
			preview := msg.GetText()
			if preview == "" && msg.GetMediaName() != "" {
				preview = "📎 " + msg.GetMediaName()
			}
			if len(preview) > 120 {
				preview = preview[:120]
			}
			// Bug-3 fix: log the error so FK failures are visible instead of silent.
			if err := s.pgRooms.UpdateLastActivity(bg,
				msg.GetRoomId(), msg.GetMessageId(), msg.GetUserId(),
				preview, time.UnixMilli(msg.GetSentAtUnixMs()).UTC()); err != nil {
				log.Printf("[chat-service] UpdateLastActivity room=%s err=%v", msg.GetRoomId(), err)
			}
		}
	}()
}

// ── GetMessages ───────────────────────────────────────────────────────────────

func (s *ChatServer) GetMessages(ctx context.Context, req *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	roomID := req.GetRoomId()
	log.Printf("[chat-service] get messages room=%s user=%s limit=%d before=%d", roomID, req.GetUserId(), req.GetLimit(), req.GetBeforeUnixMs())
	if roomID == "" {
		return nil, status.Error(codes.InvalidArgument, "room_id is required")
	}
	if s.msgRepo == nil {
		return &pb.GetMessagesResponse{}, nil // graceful: no history configured
	}

	limit := int(req.GetLimit())
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	msgs, err := s.msgRepo.GetMessagesBefore(ctx, roomID, req.GetBeforeUnixMs(), limit+1)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get messages: %v", err)
	}

	hasMore := len(msgs) > limit
	if hasMore {
		msgs = msgs[:limit]
	}

	// Auto-update read receipt for the requester (mark conversation as read)
	if req.GetUserId() != "" && len(msgs) > 0 {
		newest := msgs[0]
		go func() {
			bg := context.Background()
			now := time.Now()
			if s.msgRepo != nil {
				_ = s.msgRepo.UpdateReadReceipt(bg, roomID, req.GetUserId(), newest.GetMessageId(), now)
			}
			if s.pgRooms != nil {
				_ = s.pgRooms.UpdateReadReceipt(bg, roomID, req.GetUserId(), newest.GetMessageId(), now)
			}
		}()
	}

	return &pb.GetMessagesResponse{Messages: msgs, HasMore: hasMore}, nil
}

// ── GetPresence ───────────────────────────────────────────────────────────────

func (s *ChatServer) GetPresence(ctx context.Context, req *pb.GetPresenceRequest) (*pb.GetPresenceResponse, error) {
	if len(req.GetUserIds()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "user_ids is required")
	}
	return &pb.GetPresenceResponse{Users: s.presence.Get(ctx, req.GetUserIds())}, nil
}

// ── CreateRoom ────────────────────────────────────────────────────────────────

func (s *ChatServer) CreateRoom(ctx context.Context, req *pb.CreateRoomRequest) (*pb.CreateRoomResponse, error) {
	log.Printf("[chat-service] create room type=%v createdBy=%s members=%v name=%q", req.GetType(), req.GetCreatedBy(), req.GetMemberIds(), req.GetName())
	if req.GetCreatedBy() == "" {
		return nil, status.Error(codes.InvalidArgument, "created_by is required")
	}

	switch req.GetType() {
	case pb.RoomType_ROOM_TYPE_DM:
		if len(req.GetMemberIds()) != 2 {
			return nil, status.Error(codes.InvalidArgument, "DM requires exactly 2 member_ids")
		}
		if s.pgRooms != nil {
			meta, err := s.pgRooms.GetOrCreateDM(ctx,
				req.GetMemberIds()[0], req.GetMemberIds()[1], req.GetCreatedBy())
			if err != nil {
				return nil, status.Errorf(codes.Internal, "create DM (pg): %v", err)
			}
			return &pb.CreateRoomResponse{RoomId: meta.RoomID, Name: meta.Name}, nil
		}
		meta, err := s.rooms.CreateDM(ctx,
			req.GetMemberIds()[0], req.GetMemberIds()[1], req.GetCreatedBy())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "create DM: %v", err)
		}
		return &pb.CreateRoomResponse{RoomId: meta.RoomID, Name: meta.Name}, nil

	case pb.RoomType_ROOM_TYPE_GROUP:
		if req.GetName() == "" {
			return nil, status.Error(codes.InvalidArgument, "name is required for a group room")
		}
		if s.pgRooms != nil {
			roomID := "group:" + uuid.NewString()
			meta, err := s.pgRooms.CreateGroup(ctx,
				roomID, req.GetName(), req.GetCreatedBy(), req.GetMemberIds())
			if err != nil {
				return nil, status.Errorf(codes.Internal, "create group (pg): %v", err)
			}
			return &pb.CreateRoomResponse{RoomId: meta.RoomID, Name: meta.Name}, nil
		}
		meta, err := s.rooms.CreateGroup(ctx,
			req.GetName(), req.GetCreatedBy(), req.GetMemberIds())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "create group: %v", err)
		}
		return &pb.CreateRoomResponse{RoomId: meta.RoomID, Name: meta.Name}, nil

	default:
		return nil, status.Error(codes.InvalidArgument, "unknown room type")
	}
}

// ── RequestUpload ─────────────────────────────────────────────────────────────

func (s *ChatServer) RequestUpload(ctx context.Context, req *pb.UploadRequest) (*pb.UploadResponse, error) {
	if s.storage == nil {
		return nil, status.Error(codes.Unimplemented, "object storage is not configured on this server")
	}
	if req.GetUserId() == "" || req.GetRoomId() == "" || req.GetFileName() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id, room_id, and file_name are required")
	}
	const maxFileBytes = 100 * 1024 * 1024 // 100 MB hard cap
	if req.GetFileSizeBytes() > maxFileBytes {
		return nil, status.Errorf(codes.InvalidArgument, "file too large: max %d MB", maxFileBytes/1024/1024)
	}

	key := fmt.Sprintf("rooms/%s/%s/%d/%s",
		req.GetRoomId(), req.GetUserId(), time.Now().UnixMilli(), req.GetFileName())
	key = strings.ReplaceAll(key, ":", "_")

	uploadURL, expiresAt, err := s.storage.PutPresignedURL(ctx, key, req.GetMimeType(), req.GetFileSizeBytes())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "generate upload URL: %v", err)
	}

	if s.msgRepo != nil {
		go func() {
			_ = s.msgRepo.SaveMediaUpload(context.Background(),
				key, req.GetUserId(), req.GetRoomId(),
				req.GetFileName(), req.GetMimeType(), req.GetFileSizeBytes(), expiresAt)
		}()
	}

	return &pb.UploadResponse{
		MediaKey:        key,
		UploadUrl:       uploadURL,
		ExpiresAtUnixMs: expiresAt.UnixMilli(),
	}, nil
}

// ── GetDownloadUrl ────────────────────────────────────────────────────────────

func (s *ChatServer) GetDownloadUrl(ctx context.Context, req *pb.GetDownloadUrlRequest) (*pb.GetDownloadUrlResponse, error) {
	if s.storage == nil {
		return nil, status.Error(codes.Unimplemented, "object storage is not configured on this server")
	}
	if req.GetMediaKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "media_key is required")
	}
	u, expiresAt, err := s.storage.GetPresignedURL(ctx, req.GetMediaKey())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "generate download URL: %v", err)
	}
	return &pb.GetDownloadUrlResponse{
		Url:             u,
		ExpiresAtUnixMs: expiresAt.UnixMilli(),
	}, nil
}

// ── GetUserRooms ──────────────────────────────────────────────────────────────

func (s *ChatServer) GetUserRooms(ctx context.Context, req *pb.GetUserRoomsRequest) (*pb.GetUserRoomsResponse, error) {
	userID := req.GetUserId()
	if userID == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}

	// Try Postgres first (has richer metadata: unread flag, group name, etc.)
	if s.pgRooms != nil {
		rooms, err := s.pgRooms.GetUserRoomsDetailed(ctx, userID)
		if err != nil {
			log.Printf("[chat-service] GetUserRooms pg error user=%s: %v", userID, err)
		} else if len(rooms) > 0 {
			pbRooms := make([]*pb.UserRoom, 0, len(rooms))
			for _, r := range rooms {
				rt := pb.RoomType_ROOM_TYPE_DM
				if r.RoomType == 1 {
					rt = pb.RoomType_ROOM_TYPE_GROUP
				}
				pbRooms = append(pbRooms, &pb.UserRoom{
					RoomId:        r.RoomID,
					RoomType:      rt,
					Name:          r.Name,
					LastMessage:   r.LastMessage,
					LastMessageAt: r.LastMessageAt,
					HasUnread:     r.HasUnread,
					MemberIds:     r.MemberIDs,
				})
			}
			return &pb.GetUserRoomsResponse{Rooms: pbRooms}, nil
		}
	}

	// Fallback: query MongoDB messages directly for rooms this user participated in.
	// This covers the case where pgRooms is nil or Postgres tables are empty.
	if s.msgRepo == nil {
		return &pb.GetUserRoomsResponse{}, nil
	}

	summaries, err := s.msgRepo.GetUserRooms(ctx, userID)
	if err != nil {
		log.Printf("[chat-service] GetUserRooms mongo error user=%s: %v", userID, err)
		return &pb.GetUserRoomsResponse{}, nil
	}

	pbRooms := make([]*pb.UserRoom, 0, len(summaries))
	for _, r := range summaries {
		rt := pb.RoomType_ROOM_TYPE_DM
		if len(r.RoomID) > 6 && r.RoomID[:6] == "group:" {
			rt = pb.RoomType_ROOM_TYPE_GROUP
		}
		pbRooms = append(pbRooms, &pb.UserRoom{
			RoomId:        r.RoomID,
			RoomType:      rt,
			Name:          "",
			LastMessage:   r.LastMessage,
			LastMessageAt: r.LastMessageAt.UnixMilli(),
			HasUnread:     false,
			MemberIds:     r.MemberIDs,
		})
	}
	log.Printf("[chat-service] GetUserRooms mongo fallback user=%s rooms=%d", userID, len(pbRooms))
	return &pb.GetUserRoomsResponse{Rooms: pbRooms}, nil
}

// ── helpers ───────────────────────────────────────────────────────────────────


func normalizeRoomID(roomID string) string {
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return roomID
	}
	if strings.HasPrefix(roomID, "dm-") {
		parts := strings.Split(roomID, "-")
		if len(parts) >= 3 && parts[1] != "" && parts[2] != "" {
			userA := parts[1]
			userB := strings.Join(parts[2:], "-")
			if userA != "" && userB != "" {
				ids := []string{userA, userB}
				sort.Strings(ids)
				return fmt.Sprintf("dm:%s:%s", ids[0], ids[1])
			}
		}
	}
	if strings.HasPrefix(roomID, "dm:") {
		parts := strings.Split(roomID, ":")
		if len(parts) >= 3 && parts[1] != "" && parts[2] != "" {
			ids := []string{parts[1], parts[2]}
			if ids[0] != "" && ids[1] != "" {
				sort.Strings(ids)
				return fmt.Sprintf("dm:%s:%s", ids[0], ids[1])
			}
		}
	}
	return roomID
}

func toServerMsg(m *pb.ClientMessage) *pb.ServerMessage {
	// Bug-2 fix: normalize the roomId before building the ServerMessage so that
	// both the hub broadcast and the MongoDB save use the canonical key.
	roomID := normalizeRoomID(m.GetRoomId())
	return &pb.ServerMessage{
		RoomId:            roomID,
		UserId:            m.GetUserId(),
		MessageId:         m.GetMessageId(),
		Text:              m.GetText(),
		SentAtUnixMs:      m.GetSentAtUnixMs(),
		DeliveredAtUnixMs: time.Now().UnixMilli(),
		Type:              m.GetType(),
		MediaKey:          m.GetMediaKey(),
		MediaName:         m.GetMediaName(),
		MediaSizeBytes:    m.GetMediaSizeBytes(),
		MediaMimeType:     m.GetMediaMimeType(),
		EventType:         pb.EventType_EVENT_TYPE_MESSAGE,
		ReplyToMessageId:  m.GetReplyToMessageId(),
	}
}
