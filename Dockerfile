# ==============================================================================
# Beam — High-Throughput Concurrent Byte-Range Streaming Engine
# Multi-stage lightweight Dockerfile (<20MB total image footprint)
# ==============================================================================
FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY . .

# Compile static standalone Linux executable with symbols stripped
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o beam-web ./cmd/web

# Final runtime image
FROM alpine:3.20

WORKDIR /app
RUN apk --no-cache add ca-certificates tzdata

# Copy binary and static web assets
COPY --from=builder /app/beam-web /app/beam-web
COPY --from=builder /app/web/static /app/web/static

# Expose web dashboard port
EXPOSE 8080

ENTRYPOINT ["/app/beam-web"]
CMD ["-port", "8080"]
