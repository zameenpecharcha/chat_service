# ZPC Chat — Database Design

## Why Cassandra for messages, PostgreSQL for metadata

| Concern | Cassandra | PostgreSQL |
|---|---|---|
| Message history | ✅ Primary store | ❌ Would grow unbounded |
| Room metadata | ❌ No JOINs | ✅ Relational, FK to users |
| Read receipts | ✅ Source of truth | Optional reporting copy |
| User inbox view | ✅ Denormalized fast read | ✅ v_user_inbox view |
| Property links | ❌ No FK | ✅ `property_id` FK |
| Aggregations/reports | ❌ Weak | ✅ SQL aggregate queries |

---

## Table Relationships

```
PostgreSQL                          Cassandra
══════════════════════════          ════════════════════════════════════════

users                               messages_by_room
  id (PK)  ◄───────────────────┐     (room_id, bucket) ← partition key
  email                         │     sent_at           ← clustering (DESC)
  first_name                    │     message_id        ← tie-breaker
  ...                           │     user_id  ──────────────────────────► users.id
                                 │     message_type           (app-level join)
                                 │     text_body
chat_rooms                       │     media_key
  id (PK) ◄────────────────────┼──   deleted_at
  room_type (DM=0, GROUP=1)     │
  name                          │   user_conversations
  created_by ──────────────────►│     user_id           ← partition key
  property_id ─────┐            │     last_message_at   ← clustering (DESC)
  is_archived      │            │     room_id
  created_at       │            │     room_type / name / preview
                   │            │     unread_count (counter)
properties         │            │
  id (PK) ◄────────┘            │   read_receipts
  title                         │     (room_id, user_id) ← PK
  address                        │     last_read_at
  ...                            │     last_read_msg_id
                                 │
chat_room_members                │   media_uploads
  room_id ────────────────────► │     media_key (PK)
  user_id ─────────────────────►┘     uploader_id
  role (member | admin)               room_id
  joined_at                           file_name / mime_type / size
  left_at (NULL = active)             uploaded_at
  muted_until
  is_pinned                       message_reactions
                                      (room_id, message_id) ← partition
chat_room_last_activity               user_id             ← clustering
  room_id (PK) ──────────────────    emoji
  last_message_at                     reacted_at
  preview_text
  last_sender_id

chat_read_receipts (PG reporting copy)
  room_id + user_id (PK)
  last_read_at
```

---

## Cassandra Time-Bucket Pattern (Prevents Hot Partitions)

```
room_id = "dm:alice:bob"

bucket "2026-06-10"  │ sent_at DESC │ message_id
─────────────────────┼──────────────┼───────────────────────
dm:alice:bob         │ 23:59:58.123 │ uuid-A    "See you tomorrow"
dm:alice:bob         │ 23:45:01.000 │ uuid-B    "Great, sent the docs 📎"
dm:alice:bob         │ 22:10:44.500 │ uuid-C    "Can we schedule a tour?"
...
```

To load the last 50 messages, the app reads from today's bucket.  
If fewer than 50, it reads yesterday's bucket next, and so on.

---

## Application Write Flow (new message)

```
1. Browser WS → api_gateway → chat_service (gRPC)
2. chat_service broadcasts to all connected clients in room (in-memory Hub)
3. chat_service writes to Cassandra:
     INSERT INTO messages_by_room (room_id, bucket, sent_at, message_id, ...)
4. chat_service UPSERTs user_conversations for each room member:
     UPDATE user_conversations SET last_message_at=?, preview_text=? WHERE user_id=?
5. api_gateway (or chat_service) updates PostgreSQL:
     UPDATE chat_room_last_activity SET last_message_at=?, preview_text=? WHERE room_id=?
```

---

## Real-Estate Specific Extensions

Since ZPC is a real-estate social media app, a room can be tied to a property:

```sql
-- "Property inquiry" room — auto-created when a buyer taps "Chat with Agent"
INSERT INTO chat_rooms (id, room_type, name, created_by, property_id)
VALUES ('dm:buyer42:agent7', 0, NULL, 42, 1001);
```

This lets you build:
- **"All inquiries for my listing"** — `SELECT * FROM chat_rooms WHERE property_id = 1001`
- **Property chat widget** on listing pages
- **Agent dashboard** — unread counts per property

---

## Unread Count Calculation

```
Cassandra (fast path, per-user):
  SELECT unread_count FROM user_conversations WHERE user_id = ? AND room_id = ?

PostgreSQL (report path, per-agent):
  SELECT r.name, COUNT(*) AS rooms_with_unread
  FROM v_user_inbox i JOIN chat_rooms r ON r.id = i.room_id
  WHERE i.user_id = ? AND i.has_unread = 1
  GROUP BY r.name;
```

---

## Summary of Files

| File | Purpose |
|---|---|
| `schema/cassandra_chat.cql` | All Cassandra tables (messages, inbox, receipts, reactions, media) |
| `schema/postgres_rooms.sql` | PostgreSQL tables (rooms, members, activity, views) |
