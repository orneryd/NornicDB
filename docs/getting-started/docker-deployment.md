# Docker Deployment Guide

**Deploy NornicDB in production using Docker with best practices.**

## 📋 Prerequisites

- Docker installed (20.10+)
- Docker Compose (optional, for multi-container setups)
- 4GB+ RAM recommended
- Apple Silicon Mac (for Metal GPU acceleration) or x86_64 Linux

## Quick Start

### Pull and Run

```bash
# Apple Silicon (recommended for most users)
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-arm64-metal-bge:latest

# NVIDIA GPU (Linux)
docker run -d \
  --name nornicdb \
  --gpus all \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-amd64-cuda-bge:latest

# CPU only (Windows/Linux)
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-amd64-cpu:latest
```

Access NornicDB at:

- **HTTP API:** http://localhost:7474
- **Bolt Protocol:** bolt://localhost:7687

## 🔧 Production Deployment

### With Persistent Storage

```bash
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  -v nornicdb-logs:/logs \
  --restart unless-stopped \
  timothyswt/nornicdb-arm64-metal-bge:latest
```

### With Environment Variables

```bash
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  -e NORNICDB_LOG_LEVEL=info \
  -e NORNICDB_LOW_MEMORY=true \
  -e NORNICDB_EMBEDDING_PROVIDER=ollama \
  -e NORNICDB_EMBEDDING_MODEL=mxbai-embed-large \
  --restart unless-stopped \
  timothyswt/nornicdb-arm64-metal-bge:latest
```

For the full supported environment-variable surface, use the canonical [Environment Variables Reference](../operations/environment-variables.md).

## 🐳 Docker Compose

### Basic Setup

Create `docker-compose.yml`:

```yaml
version: "3.8"

services:
  nornicdb:
    image: timothyswt/nornicdb-arm64-metal-bge:latest
    container_name: nornicdb
    ports:
      - "7474:7474" # HTTP API
      - "7687:7687" # Bolt protocol
    volumes:
      - nornicdb-data:/data
      - nornicdb-logs:/logs
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:7474/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s

volumes:
  nornicdb-data:
  nornicdb-logs:
```

Start with:

```bash
docker-compose up -d
```

### With Ollama for Embeddings

```yaml
version: "3.8"

services:
  ollama:
    image: ollama/ollama:latest
    container_name: ollama
    ports:
      - "11434:11434"
    volumes:
      - ollama-data:/root/.ollama
    restart: unless-stopped

  nornicdb:
    image: timothyswt/nornicdb-arm64-metal-bge:latest
    container_name: nornicdb
    depends_on:
      - ollama
    ports:
      - "7474:7474"
      - "7687:7687"
      - "6334:6334" # Qdrant gRPC (optional)
    volumes:
      - nornicdb-data:/data
      - nornicdb-logs:/logs
    environment:
      - NORNICDB_EMBEDDING_PROVIDER=ollama
      - NORNICDB_EMBEDDING_API_URL=http://ollama:11434
      - NORNICDB_EMBEDDING_MODEL=mxbai-embed-large
      - NORNICDB_EMBEDDING_ENABLED=true
      - NORNICDB_QDRANT_GRPC_ENABLED=true
      - NORNICDB_QDRANT_GRPC_LISTEN_ADDR=:6334
    restart: unless-stopped

volumes:
  ollama-data:
  nornicdb-data:
  nornicdb-logs:
```

## ⚙️ Configuration

Use the canonical operational docs for supported settings instead of a shortened deployment-only table:

- [Environment Variables Reference](../operations/environment-variables.md)
- [Configuration Guide](../operations/configuration.md)
- [Docker Operations Guide](../operations/docker.md)

### Volume Mounts

| Path      | Purpose                        |
| --------- | ------------------------------ |
| `/data`   | Database storage (persistent)  |
| `/logs`   | Application logs               |
| `/config` | Configuration files (optional) |

## 🔒 Security Best Practices

### 1. Use Strong Passwords

```bash
# Generate secure password
openssl rand -base64 32

# Use in docker-compose
export NORNICDB_PASSWORD=$(openssl rand -base64 32)
```

### 2. Enable TLS/HTTPS

Terminate TLS at a reverse proxy or ingress in front of the container, then pass HTTP traffic to NornicDB on a private network. Authenticated non-loopback HTTP requires an explicit trusted-proxy IP/CIDR, and wildcard CORS is rejected. Use the tested configuration and Nginx example in [Reverse Proxy and TLS Termination](../operations/reverse-proxy.md).

HTTP proxy trust does not apply to Bolt or Qdrant gRPC. Disable those listeners when unused, or secure them independently as described in the reverse-proxy guide.
Authenticated public gRPC requires `NORNICDB_QDRANT_GRPC_TLS_ENABLED=true`, a
mounted server certificate and key, and the matching TLS path variables.
Optional mTLS also requires a client CA and `request_verify` or
`require_verify` client-auth mode.

### 3. Restrict Network Access

```yaml
services:
  nornicdb:
    # ... other config
    networks:
      - backend
    # Don't expose ports publicly
    # Use reverse proxy instead

networks:
  backend:
    internal: true
```

## 📊 Monitoring

### Health Checks

```bash
# Check container health
docker ps

# View logs
docker logs nornicdb

# Follow logs
docker logs -f nornicdb

# Check resource usage
docker stats nornicdb
```

### Prometheus Metrics

For metrics exposure, scrape the supported endpoints described in [API Reference](../api-reference/openapi.md) and the monitoring guidance in [Operations](../operations/README.md). Keep deployment docs focused on container topology and leave runtime observability settings to the canonical operations pages.

## 🔄 Backup & Restore

### Backup

```bash
# Stop container
docker stop nornicdb

# Backup data volume
docker run --rm \
  -v nornicdb-data:/data \
  -v $(pwd):/backup \
  alpine tar czf /backup/nornicdb-backup-$(date +%Y%m%d).tar.gz /data

# Restart container
docker start nornicdb
```

### Restore

```bash
# Stop container
docker stop nornicdb

# Restore data
docker run --rm \
  -v nornicdb-data:/data \
  -v $(pwd):/backup \
  alpine sh -c "cd / && tar xzf /backup/nornicdb-backup-20251201.tar.gz"

# Restart container
docker start nornicdb
```

## 🐛 Troubleshooting

### Container Won't Start

```bash
# Check logs
docker logs nornicdb

# Check if ports are available
lsof -i :7474
lsof -i :7687

# Remove and recreate
docker rm -f nornicdb
docker run ...
```

### GPU Not Working

```bash
# Check GPU availability
docker run --rm timothyswt/nornicdb-arm64-metal-bge:latest \
  /bin/sh -c "ls -la /dev/dri || echo 'No GPU devices found'"

# Enable GPU access (Linux)
docker run --gpus all ...

# Enable Metal (macOS - automatic)
```

### Out of Memory

NornicDB's default high-performance mode uses ~1GB RAM. Combined with embedding models, this can exceed Docker's default 2GB limit.

**Option 1: Enable Low Memory Mode (recommended)**

```yaml
services:
  nornicdb:
    environment:
      - NORNICDB_LOW_MEMORY=true # Reduces RAM to ~50MB
      - GOGC=100
```

**Option 2: Increase Memory Limit**

```bash
# Increase memory limit
docker update --memory 8g nornicdb

# Or in docker-compose
services:
  nornicdb:
    mem_limit: 4g
```

See **[Low Memory Mode Guide](../operations/low-memory-mode.md)** for details.

## ⏭️ Next Steps

- **[Operations Guide](../operations/README.md)** - Monitoring, backup, scaling
- **[Scaling Guide](../operations/scaling.md)** - Reverse proxies, capacity planning, and topology
- **[Configuration Guide](../operations/configuration.md)** - Supported runtime and YAML settings
- **[Performance Tuning](../performance/http-optimization-options.md)** - Optimize for your workload
- **[Security Guide](../compliance/README.md)** - GDPR, HIPAA, SOC2 compliance

---

**Need help?** → **[Troubleshooting Guide](../operations/troubleshooting.md)**  
**Production ready?** → **[Operations Guide](../operations/README.md)**
