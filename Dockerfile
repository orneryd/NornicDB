# NornicDB Docker Image
# Neo4j-compatible graph database with GPU-accelerated vector search

# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /build

# Install build dependencies
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary with embedded UI
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o nornicdb ./cmd/nornicdb

# Runtime stage
FROM alpine:3.19

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Copy binary from builder
COPY --from=builder /build/nornicdb /app/nornicdb

# Copy entrypoint script
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

# Create data directory
RUN mkdir -p /data

# Expose ports
# 7474 - HTTP API & UI
# 7687 - Bolt protocol
EXPOSE 7474 7687

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD wget --spider -q http://localhost:7474/health || exit 1

# Default environment variables
ENV NORNICDB_DATA_DIR=/data \
    NORNICDB_HTTP_PORT=7474 \
    NORNICDB_BOLT_PORT=7687 \
    NORNICDB_EMBEDDING_URL=http://llama-server:8080 \
    NORNICDB_EMBEDDING_MODEL=mxbai-embed-large \
    NORNICDB_EMBEDDING_DIM=1024 \
    NORNICDB_NO_AUTH=true

# Entry point script handles environment variable expansion
ENTRYPOINT ["/app/docker-entrypoint.sh"]

# Additional arguments can be passed via CMD
CMD []
