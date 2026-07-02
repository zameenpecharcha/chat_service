# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS build
WORKDIR /src
COPY . .
RUN go env -w CGO_ENABLED=0 && \
    go build -o /out/chat-service ./cmd/server

FROM gcr.io/distroless/base-debian12:nonroot
WORKDIR /app
COPY --from=build /out/chat-service /app/chat-service
COPY --from=build /src/config /app/config
ENV PORT=50051
EXPOSE 50051
USER nonroot
ENTRYPOINT ["/app/chat-service"]


