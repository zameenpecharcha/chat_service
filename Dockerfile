# syntax=docker/dockerfile:1

FROM golang:1.21-alpine AS build
WORKDIR /src
COPY . .
RUN apk add --no-cache bash make protoc protobuf-dev git && \
    go env -w CGO_ENABLED=0 && \
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && \
    make proto && \
    go build -o /out/chat-service ./cmd/server

FROM gcr.io/distroless/base-debian12:nonroot
WORKDIR /app
COPY --from=build /out/chat-service /app/chat-service
COPY .env.example /app/.env
ENV PORT=50060
EXPOSE 50060
USER nonroot
ENTRYPOINT ["/app/chat-service"]


