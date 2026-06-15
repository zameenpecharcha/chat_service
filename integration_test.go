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

func TestRequestUpload_NoStorage(t *testing.T) {
	client := dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Server has no MinIO configured; expect Unimplemented
	_, err := client.RequestUpload(ctx, &pb.UploadRequest{
		UserId:   "alice",
		RoomId:   "dm:alice:bob",
		FileName: "photo.jpg",
		MimeType: "image/jpeg",
	})
	if err == nil {
		t.Error("expected error when storage not configured, got nil")
	}
	t.Logf("RequestUpload no-storage error (expected): %v", err)
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

	recv := func(stream pb.ChatService_ChatClient, label string) *pb.ServerMessage {
		t.Helper()
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
			return m
		case <-time.After(5 * time.Second):
			t.Errorf("%s timed out waiting for message", label)
			return nil
		}
	}

	// Alice sends; both are registered → both receive the broadcast
	send(aliceStream, "alice", "Hello Bob!")

	// Alice receives own message
	m := recv(aliceStream, "alice")
	if m == nil {
		t.Fatal("alice got nil message")
	}
	if m.Text != "Hello Bob!" {
		t.Errorf("alice expected %q got %q", "Hello Bob!", m.Text)
	}
	if m.DeliveredAtUnixMs == 0 {
		t.Error("delivered_at_unix_ms should be set")
	}
	t.Logf("Alice received own message: %q  delivered_at=%d", m.Text, m.DeliveredAtUnixMs)

	// Bob also receives Alice's message (drain it before his own echo)
	mBobFirst := recv(bobStream, "bob receives alice msg")
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
	m2 := recv(aliceStream, "alice from bob")
	if m2 == nil {
		t.Fatal("alice got nil on bob's message")
	}
	if m2.Text != "Hey Alice!" {
		t.Errorf("alice expected %q got %q", "Hey Alice!", m2.Text)
	}
	t.Logf("Alice received Bob's message: %q", m2.Text)

	// Bob also receives his own broadcast
	m3 := recv(bobStream, "bob own echo")
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
			m, _ := s.Recv()
			results <- result{label, m}
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
	go func() { m, _ := alice.Recv(); ch <- m }()
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
