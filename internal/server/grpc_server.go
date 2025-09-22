package server

import (
    "context"
    "time"

    "chat-service/internal/chat"
    pb "chat-service/internal/pb"
)

type ChatServer struct {
    pb.UnimplementedChatServiceServer
    hub *chat.Hub
}

func NewChatServer(h *chat.Hub) *ChatServer { return &ChatServer{hub: h} }

func (s *ChatServer) Chat(stream pb.ChatService_ChatServer) error {
    // First message registers the client (must include user_id and room_id)
    first, err := stream.Recv()
    if err != nil { return err }
    client := &chat.Client{userID: first.GetUserId(), roomID: first.GetRoomId(), send: make(chan *pb.ServerMessage, 256)}
    ctx := stream.Context()
    s.hub.Register(ctx, client)
    defer s.hub.Unregister(client)

    // Sender goroutine to push messages to client
    done := make(chan struct{})
    go func() {
        defer close(done)
        for m := range client.send {
            if err := stream.Send(m); err != nil { return }
        }
    }()

    // Count the first message as a chat message if it has text
    if t := first.GetText(); t != "" {
        s.hub.Broadcast(ctx, &pb.ServerMessage{
            RoomId: first.GetRoomId(),
            UserId: first.GetUserId(),
            MessageId: first.GetMessageId(),
            Text: t,
            SentAtUnixMs: first.GetSentAtUnixMs(),
            DeliveredAtUnixMs: time.Now().UnixMilli(),
        }, true)
    }

    // Receive loop
    for {
        in, err := stream.Recv()
        if err != nil { return err }
        s.hub.Broadcast(ctx, &pb.ServerMessage{
            RoomId: in.GetRoomId(),
            UserId: in.GetUserId(),
            MessageId: in.GetMessageId(),
            Text: in.GetText(),
            SentAtUnixMs: in.GetSentAtUnixMs(),
            DeliveredAtUnixMs: time.Now().UnixMilli(),
        }, true)
    }

    // Wait sender goroutine if ever needed
    <-done
}


