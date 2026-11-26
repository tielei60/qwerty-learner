FROM golang:1.22-alpine AS builder
WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /sync-server ./cmd/sync-server

FROM alpine:3.20
WORKDIR /app
RUN apk add --no-cache ca-certificates

COPY --from=builder /sync-server /usr/local/bin/sync-server

ENV SYNC_DATA_DIR=/data
ENV SYNC_NATS_HOST=127.0.0.1
ENV SYNC_WS_HOST=127.0.0.1
ENV SYNC_WS_PORT=9322

VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/sync-server"]
