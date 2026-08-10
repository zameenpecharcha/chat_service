package integration_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	pb "chat-service/app/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const serverAddr = "127.0.0.1:50051"

func dial(t *testing.T) pb.ChatServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewChatServiceClient(conn)
}

// ── CreateRoom ────────────────────────────────────────────────────────────────

func TestCreateDMRoom(t *testing.T) {
	client := dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.CreateRoom(ctx, &pb.CreateRoomRequest{
		CreatedBy: "alice",
		Type:      pb.RoomType_ROOM_TYPE_DM,
		MemberIds: []string{"alice", "bob"},
	})
	if err != nil {
		t.Fatalf("CreateRoom DM: %v", err)
	}
	if resp.RoomId == "" {
		t.Fatal("expected non-empty room_id")
	}
	t.Logf("DM room_id: %s", resp.RoomId)

	// Calling again must return the same room (idempotent)
	resp2, err := client.CreateRoom(ctx, &pb.CreateRoomRequest{
		CreatedBy: "alice",
		Type:      pb.RoomType_ROOM_TYPE_DM,
		MemberIds: []string{"bob", "alice"}, // reversed order
	})
	if err != nil {
		t.Fatalf("CreateRoom DM idempotent: %v", err)
	}
	if resp.RoomId != resp2.RoomId {
		t.Errorf("expected same room_id for same pair, got %q vs %q", resp.RoomId, resp2.RoomId)
	}
	t.Logf("DM idempotent OK: %s == %s", resp.RoomId, resp2.RoomId)
}

func TestCreateGroupRoom(t *testing.T) {
	client := dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.CreateRoom(ctx, &pb.CreateRoomRequest{
		CreatedBy: "alice",
		Name:      "Team Chat",
		Type:      pb.RoomType_ROOM_TYPE_GROUP,
		MemberIds: []string{"alice", "bob", "charlie"},
	})
	if err != nil {
		t.Fatalf("CreateRoom group: %v", err)
	}
	if resp.RoomId == "" {
		t.Fatal("expected non-empty room_id")
	}
	if resp.Name != "Team Chat" {
		t.Errorf("expected name %q, got %q", "Team Chat", resp.Name)
	}
	t.Logf("Group room_id: %s  name: %s", resp.RoomId, resp.Name)
}

func TestCreateRoom_Validation(t *testing.T) {
	client := dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// DM with wrong member count
	_, err := client.CreateRoom(ctx, &pb.CreateRoomRequest{
		CreatedBy: "alice",
		Type:      pb.RoomType_ROOM_TYPE_DM,
		MemberIds: []string{"alice"},
	})
	if err == nil {
		t.Error("expected error for DM with 1 member, got nil")
	}
	t.Logf("DM validation error (expected): %v", err)

	// Group without name
	_, err = client.CreateRoom(ctx, &pb.CreateRoomRequest{
		CreatedBy: "alice",
		Type:      pb.RoomType_ROOM_TYPE_GROUP,
		MemberIds: []string{"alice", "bob"},
	})
	if err == nil {
		t.Error("expected error for group without name, got nil")
	}
	t.Logf("Group validation error (expected): %v", err)
}

// ── RequestUpload ─────────────────────────────────────────────────────────────

func TestRequestUpload(t *testing.T) {
	client := dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.RequestUpload(ctx, &pb.UploadRequest{
		UserId:        "alice",
		RoomId:        "dm:alice:bob",
		FileName:      "photo.jpg",
		MimeType:      "image/jpeg",
		FileSizeBytes: 1024,
	})
	if err != nil {
		t.Logf("RequestUpload error (if storage unconfigured): %v", err)
		return
	}
	if resp.UploadUrl == "" || resp.MediaKey == "" {
		t.Errorf("expected upload_url and media_key, got url=%q key=%q", resp.UploadUrl, resp.MediaKey)
	}
	t.Logf("RequestUpload OK: key=%s url=%s", resp.MediaKey, resp.UploadUrl)
}

// ── Chat stream (DM) ──────────────────────────────────────────────────────────

func TestChat_DM(t *testing.T) {
	client := dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create the room first
	roomResp, err := client.CreateRoom(ctx, &pb.CreateRoomRequest{
		CreatedBy: "alice",
		Type:      pb.RoomType_ROOM_TYPE_DM,
		MemberIds: []string{"alice", "bob"},
	})
	if err != nil {
		t.Fatalf("CreateRoom: %v", err)
	}
	roomID := roomResp.RoomId

	// Open Alice's stream
	aliceStream, err := client.Chat(ctx)
	if err != nil {
		t.Fatalf("Alice chat stream: %v", err)
	}
	// Open Bob's stream
	bobStream, err := client.Chat(ctx)
	if err != nil {
		t.Fatalf("Bob chat stream: %v", err)
	}

	// Register both clients (first message = join; no text so no broadcast yet)
	if err := aliceStream.Send(&pb.ClientMessage{RoomId: roomID, UserId: "alice", MessageId: "alice-join"}); err != nil {
		t.Fatalf("Alice join: %v", err)
	}
	if err := bobStream.Send(&pb.ClientMessage{RoomId: roomID, UserId: "bob", MessageId: "bob-join"}); err != nil {
		t.Fatalf("Bob join: %v", err)
	}
	// Give the server time to process both registrations before sending messages.
	time.Sleep(100 * time.Millisecond)

	send := func(stream pb.ChatService_ChatClient, userID, text string) {
		t.Helper()
		msgID := fmt.Sprintf("%s-%d", userID, time.Now().UnixNano())
		if err := stream.Send(&pb.ClientMessage{
			RoomId:       roomID,
			UserId:       userID,
			MessageId:    msgID,
			Text:         text,
			SentAtUnixMs: time.Now().UnixMilli(),
		}); err != nil {
			t.Errorf("send from %s: %v", userID, err)
		}
	}

	recvMsg := func(stream pb.ChatService_ChatClient, label string) *pb.ServerMessage {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			ch := make(chan *pb.ServerMessage, 1)
			go func() {
				m, err := stream.Recv()
				if err != nil && err != io.EOF {
					t.Logf("%s recv err: %v", label, err)
				}
				ch <- m
			}()
			select {
			case m := <-ch:
				if m != nil && m.EventType == pb.EventType_EVENT_TYPE_PRESENCE {
					continue // skip presence events
				}
				return m
			case <-deadline:
				t.Errorf("%s timed out waiting for message", label)
				return nil
			}
		}
	}

	// Alice sends; both are registered → both receive the broadcast
	send(aliceStream, "alice", "Hello Bob!")

	// Alice receives own message
	m := recvMsg(aliceStream, "alice")
	if m == nil {
		t.Fatal("alice got nil message")
	}
	if m.Text != "Hello Bob!" {
		t.Errorf("alice expected %q got %q", "Hello Bob!", m.Text)
	}
	t.Logf("Alice received own message: %q", m.Text)

	// Bob also receives Alice's message (drain it before his own echo)
	mBobFirst := recvMsg(bobStream, "bob receives alice msg")
	if mBobFirst == nil {
		t.Fatal("bob got nil on alice's message")
	}
	if mBobFirst.Text != "Hello Bob!" {
		t.Errorf("bob expected alice's %q got %q", "Hello Bob!", mBobFirst.Text)
	}
	t.Logf("Bob received Alice's message: %q", mBobFirst.Text)

	// Bob sends a reply
	send(bobStream, "bob", "Hey Alice!")

	// Alice receives Bob's reply
	m2 := recvMsg(aliceStream, "alice from bob")
	if m2 == nil {
		t.Fatal("alice got nil on bob's message")
	}
	if m2.Text != "Hey Alice!" {
		t.Errorf("alice expected %q got %q", "Hey Alice!", m2.Text)
	}
	t.Logf("Alice received Bob's message: %q", m2.Text)

	// Bob also receives his own broadcast
	m3 := recvMsg(bobStream, "bob own echo")
	if m3 == nil {
		t.Fatal("bob got nil on own message")
	}
	if m3.Text != "Hey Alice!" {
		t.Errorf("bob expected %q got %q", "Hey Alice!", m3.Text)
	}
	t.Logf("Bob received own echo: %q", m3.Text)

	_ = aliceStream.CloseSend()
	_ = bobStream.CloseSend()
}


// ── Chat stream (Group) ───────────────────────────────────────────────────────

func TestChat_Group(t *testing.T) {
	client := dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	roomResp, err := client.CreateRoom(ctx, &pb.CreateRoomRequest{
		CreatedBy: "alice",
		Name:      "Test Group",
		Type:      pb.RoomType_ROOM_TYPE_GROUP,
		MemberIds: []string{"alice", "bob", "charlie"},
	})
	if err != nil {
		t.Fatalf("CreateRoom group: %v", err)
	}
	roomID := roomResp.RoomId

	openStream := func(userID string) pb.ChatService_ChatClient {
		s, err := client.Chat(ctx)
		if err != nil {
			t.Fatalf("%s stream open: %v", userID, err)
		}
		// register / join
		if err := s.Send(&pb.ClientMessage{RoomId: roomID, UserId: userID, MessageId: userID + "-join"}); err != nil {
			t.Fatalf("%s join: %v", userID, err)
		}
		return s
	}

	alice := openStream("alice")
	bob := openStream("bob")
	charlie := openStream("charlie")
	// Wait for all three joins to be processed server-side before broadcasting.
	time.Sleep(100 * time.Millisecond)

	// Alice sends a group message
	if err := alice.Send(&pb.ClientMessage{
		RoomId:       roomID,
		UserId:       "alice",
		MessageId:    "grp-msg-1",
		Text:         "Hello group!",
		SentAtUnixMs: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatalf("alice send: %v", err)
	}

	// All three should receive it — start receivers concurrently so one slow
	// receiver doesn't block the others from draining their buffers.
	type result struct {
		label string
		msg   *pb.ServerMessage
	}
	results := make(chan result, 3)
	for _, tc := range []struct {
		label  string
		stream pb.ChatService_ChatClient
	}{
		{"alice", alice},
		{"bob", bob},
		{"charlie", charlie},
	} {
		go func(label string, s pb.ChatService_ChatClient) {
			for {
				m, err := s.Recv()
				if err != nil {
					results <- result{label, nil}
					return
				}
				if m != nil && m.EventType == pb.EventType_EVENT_TYPE_PRESENCE {
					continue
				}
				results <- result{label, m}
				return
			}
		}(tc.label, tc.stream)
	}

	deadline := time.After(5 * time.Second)
	for i := 0; i < 3; i++ {
		select {
		case r := <-results:
			if r.msg == nil || r.msg.Text != "Hello group!" {
				t.Errorf("%s: expected 'Hello group!' got %v", r.label, r.msg)
			} else {
				t.Logf("%s received: %q from %s", r.label, r.msg.Text, r.msg.UserId)
			}
		case <-deadline:
			t.Error("timed out waiting for group message delivery")
		}
	}

	_ = alice.CloseSend()
	_ = bob.CloseSend()
	_ = charlie.CloseSend()
}

// ── Media message in chat ─────────────────────────────────────────────────────

func TestChat_MediaMessage(t *testing.T) {
	client := dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	roomResp, err := client.CreateRoom(ctx, &pb.CreateRoomRequest{
		CreatedBy: "alice",
		Type:      pb.RoomType_ROOM_TYPE_DM,
		MemberIds: []string{"alice", "bob"},
	})
	if err != nil {
		t.Fatalf("CreateRoom: %v", err)
	}
	roomID := roomResp.RoomId

	alice, _ := client.Chat(ctx)
	bob, _ := client.Chat(ctx)
	// register both, then wait for server to process joins before sending media
	_ = alice.Send(&pb.ClientMessage{RoomId: roomID, UserId: "alice", MessageId: "a-join"})
	_ = bob.Send(&pb.ClientMessage{RoomId: roomID, UserId: "bob", MessageId: "b-join"})
	time.Sleep(100 * time.Millisecond)

	// Alice shares an image (media_key already uploaded out-of-band in real flow)
	_ = alice.Send(&pb.ClientMessage{
		RoomId:         roomID,
		UserId:         "alice",
		MessageId:      "img-1",
		Type:           pb.MessageType_MESSAGE_TYPE_IMAGE,
		MediaKey:       "rooms/" + roomID + "/alice/1234/photo.jpg",
		MediaName:      "photo.jpg",
		MediaMimeType:  "image/jpeg",
		MediaSizeBytes: 204800,
		SentAtUnixMs:   time.Now().UnixMilli(),
	})

	// Alice receives own broadcast
	ch := make(chan *pb.ServerMessage, 1)
	go func() {
		for {
			m, err := alice.Recv()
			if err != nil {
				ch <- nil
				return
			}
			if m != nil && (m.EventType == pb.EventType_EVENT_TYPE_PRESENCE || m.Type == pb.MessageType_MESSAGE_TYPE_TEXT) {
				continue
			}
			ch <- m
			return
		}
	}()
	select {
	case m := <-ch:
		if m == nil {
			t.Fatal("alice got nil on media message")
		}
		if m.Type != pb.MessageType_MESSAGE_TYPE_IMAGE {
			t.Errorf("expected IMAGE type, got %v", m.Type)
		}
		if m.MediaKey == "" {
			t.Error("media_key should be set")
		}
		t.Logf("Alice received media message: type=%v key=%s name=%s", m.Type, m.MediaKey, m.MediaName)
	case <-time.After(5 * time.Second):
		t.Error("timed out waiting for media message")
	}

	_ = alice.CloseSend()
	_ = bob.CloseSend()
}

// ── Extended gRPC APIs tests ──────────────────────────────────────────────────

func TestCreateGroupAndMemberManagement(t *testing.T) {
	client := dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. CreateGroup
	grpResp, err := client.CreateGroup(ctx, &pb.CreateGroupRequest{
		CreatedBy:   "USR1001",
		GroupName:   "Hyderabad Investors",
		GroupPhoto:  "MEDIA123",
		Description: "Investment Discussion",
		MemberIds:   []string{"USR1001", "USR2001"},
	})
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	convID := grpResp.Conversation.ConversationId
	if convID == "" {
		t.Fatal("expected non-empty conversation_id")
	}

	// 2. GetConversation
	cResp, err := client.GetConversation(ctx, &pb.GetConversationRequest{
		ConversationId: convID,
		UserId:         "USR1001",
	})
	if err != nil {
		t.Fatalf("GetConversation: %v", err)
	}
	if cResp.Conversation.GroupName != "Hyderabad Investors" {
		t.Errorf("expected group_name 'Hyderabad Investors', got %q", cResp.Conversation.GroupName)
	}

	// 3. AddGroupMember
	_, err = client.AddGroupMember(ctx, &pb.AddGroupMemberRequest{
		ConversationId: convID,
		UserId:         "USR3001",
		OperatorId:     "USR1001",
		Role:           "MEMBER",
	})
	if err != nil {
		t.Fatalf("AddGroupMember: %v", err)
	}

	// 4. PromoteAdmin
	_, err = client.PromoteAdmin(ctx, &pb.PromoteAdminRequest{
		ConversationId: convID,
		UserId:         "USR2001",
		OperatorId:     "USR1001",
	})
	if err != nil {
		t.Fatalf("PromoteAdmin: %v", err)
	}

	// 5. TransferOwnership
	_, err = client.TransferOwnership(ctx, &pb.TransferOwnershipRequest{
		ConversationId:  convID,
		CurrentOwnerId: "USR1001",
		NewOwnerId:     "USR2001",
	})
	if err != nil {
		t.Fatalf("TransferOwnership: %v", err)
	}

	// 6. LeaveGroup
	_, err = client.LeaveGroup(ctx, &pb.LeaveGroupRequest{
		ConversationId: convID,
		UserId:         "USR3001",
	})
	if err != nil {
		t.Fatalf("LeaveGroup: %v", err)
	}

	// 7. DeleteGroup
	_, err = client.DeleteGroup(ctx, &pb.DeleteGroupRequest{
		ConversationId: convID,
		OwnerId:        "USR2001",
	})
	if err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}
}

func TestSendMessage_Search_Edit_Delete_Read(t *testing.T) {
	client := dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	convID := "CONV_TEST_API"

	// 1. SendMessage
	sendResp, err := client.SendMessage(ctx, &pb.SendMessageRequest{
		ConversationId: convID,
		SenderId:       "U1001",
		Content:        "Hi Rohit",
		MessageType:    pb.MessageType_MESSAGE_TYPE_TEXT,
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	msgID := sendResp.Message.MessageId
	if msgID == "" {
		t.Fatal("expected non-empty message_id")
	}
	time.Sleep(100 * time.Millisecond)

	// 2. EditMessage
	editResp, err := client.EditMessage(ctx, &pb.EditMessageRequest{
		MessageId:      msgID,
		UserId:         "U1001",
		ConversationId: convID,
		NewContent:     "Hi Rohit - Edited",
	})
	if err != nil {
		t.Fatalf("EditMessage: %v", err)
	}
	if editResp.Message.Text != "Hi Rohit - Edited" {
		t.Errorf("expected updated text, got %q", editResp.Message.Text)
	}

	// 3. MarkMessageRead
	_, err = client.MarkMessageRead(ctx, &pb.MarkMessageReadRequest{
		ConversationId: convID,
		UserId:         "U2001",
		MessageId:      msgID,
	})
	if err != nil {
		t.Fatalf("MarkMessageRead: %v", err)
	}

	// 4. GetUnreadCount
	unResp, err := client.GetUnreadCount(ctx, &pb.GetUnreadCountRequest{
		UserId:         "U2001",
		ConversationId: convID,
	})
	if err != nil {
		t.Fatalf("GetUnreadCount: %v", err)
	}
	t.Logf("Unread count for U2001: %d", unResp.TotalUnreadCount)

	// 5. DeleteMessage
	delResp, err := client.DeleteMessage(ctx, &pb.DeleteMessageRequest{
		MessageId:      msgID,
		UserId:         "U1001",
		ConversationId: convID,
	})
	if err != nil {
		t.Fatalf("DeleteMessage: %v", err)
	}
	if !delResp.Success {
		t.Error("expected delete success true")
	}
}

