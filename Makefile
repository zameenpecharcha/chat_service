.PHONY: proto run tidy build docker fmt vet

PROTOC?=protoc

proto:
	mkdir -p internal/pb
	$(PROTOC) \
		--proto_path=proto \
		--go_out=internal/pb --go_opt=paths=source_relative \
		--go-grpc_out=internal/pb --go-grpc_opt=paths=source_relative \
		proto/chat.proto

fmt:
	gofmt -w .

vet:
	go vet ./...

tidy:
	go mod tidy

run:
	go run ./cmd/server

build:
	go build -o bin/chat-service ./cmd/server

docker:
	docker build -t chat-service:latest .


