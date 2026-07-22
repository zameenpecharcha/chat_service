# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS build

WORKDIR /src

# Copy only dependency files first
COPY go.mod go.sum ./

# Download dependencies (cached)
RUN go mod download

# Copy application source
COPY . .

# Build
RUN CGO_ENABLED=0 go build -o /out/chat-service ./cmd/server

FROM gcr.io/distroless/base-debian12:nonroot

WORKDIR /app

COPY --from=build /out/chat-service .
COPY --from=build /src/config ./config

ENV PORT=50051

EXPOSE 50051

ENTRYPOINT ["/app/chat-service"]