# Docker Deployment Guide

**Deploy NornicDB in production using Docker with best practices.**

## 📋 Prerequisites

- Docker installed (20.10+)
- Docker Compose (optional, for multi-container setups)
- 4GB+ RAM recommended
- Apple Silicon Mac (for Metal GPU acceleration) or x86_64 Linux

## 🚀 Quick Start

### Pull and Run

```bash
# Pull the latest image
docker pull timothyswt/nornicdb-arm64-metal:latest

# Run with default settings
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  timothyswt/nornicdb-arm64-metal:latest
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
  timothyswt/nornicdb-arm64-metal:latest
```

### With Environment Variables

```bash
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  -e NORNICDB_AUTH_ENABLED=true \
  -e NORNICDB_AUTH_USERNAME=admin \
  -e NORNICDB_AUTH_PASSWORD=secure_password \
  -e NORNICDB_GPU_ENABLED=true \
  -e NORNICDB_EMBEDDING_PROVIDER=ollama \
  -e NORNICDB_EMBEDDING_MODEL=mxbai-embed-large \
  --restart unless-stopped \
  timothyswt/nornicdb-arm64-metal:latest
```

## 🐳 Docker Compose

### Basic Setup

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  nornicdb:
    image: timothyswt/nornicdb-arm64-metal:latest
    container_name: nornicdb
    ports:
      - "7474:7474"  # HTTP API
      - "7687:7687"  # Bolt protocol
    volumes:
      - nornicdb-data:/data
      - nornicdb-logs:/logs
    environment:
      - NORNICDB_AUTH_ENABLED=true
      - NORNICDB_AUTH_USERNAME=admin
      - NORNICDB_AUTH_PASSWORD=${NORNICDB_PASSWORD}
      - NORNICDB_GPU_ENABLED=true
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:7474/"]
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
export NORNICDB_PASSWORD=your_secure_password
docker-compose up -d
```

### With Ollama for Embeddings

```yaml
version: '3.8'

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
    image: timothyswt/nornicdb-arm64-metal:latest
    container_name: nornicdb
    depends_on:
      - ollama
    ports:
      - "7474:7474"
      - "7687:7687"
      - "6334:6334"  # Qdrant gRPC (optional)
    volumes:
      - nornicdb-data:/data
      - nornicdb-logs:/logs
    environment:
      - NORNICDB_AUTH_ENABLED=true
      - NORNICDB_AUTH_USERNAME=admin
      - NORNICDB_AUTH_PASSWORD=${NORNICDB_PASSWORD}
      - NORNICDB_GPU_ENABLED=true
      - NORNICDB_EMBEDDING_PROVIDER=ollama
      - NORNICDB_EMBEDDING_URL=http://ollama:11434
      - NORNICDB_EMBEDDING_MODEL=mxbai-embed-large
      # Qdrant gRPC endpoint (optional, disabled by default)
      - NORNICDB_QDRANT_GRPC_ENABLED=true
      - NORNICDB_QDRANT_GRPC_LISTEN_ADDR=:6334
    restart: unless-stopped

volumes:
  ollama-data:
  nornicdb-data:
  nornicdb-logs:
```

## ⚙️ Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NORNICDB_AUTH_ENABLED` | `false` | Enable authentication |
| `NORNICDB_AUTH_USERNAME` | `admin` | Admin username |
| `NORNICDB_AUTH_PASSWORD` | - | Admin password (required if auth enabled) |
| `NORNICDB_GPU_ENABLED` | `true` | Enable GPU acceleration |
| `NORNICDB_LOW_MEMORY` | `false` | Enable low memory mode (~50MB vs ~1GB RAM) |
| `NORNICDB_EMBEDDING_PROVIDER` | `ollama` | Embedding provider (ollama/openai/local) |
| `NORNICDB_EMBEDDING_URL` | `http://localhost:11434` | Ollama URL |
| `NORNICDB_EMBEDDING_MODEL` | `mxbai-embed-large` | Embedding model |
| `NORNICDB_OPENAI_API_KEY` | - | OpenAI API key (if using OpenAI) |
| `NORNICDB_QDRANT_GRPC_ENABLED` | `false` | Enable Qdrant-compatible gRPC endpoint |
| `NORNICDB_LOG_LEVEL` | `info` | Log level (debug/info/warn/error) |
| `NORNICDB_MAX_MEMORY` | `4GB` | Maximum memory usage |
| `NORNICDB_CACHE_SIZE` | `1000` | Query cache size |

### Volume Mounts

| Path | Purpose |
|------|---------|
| `/data` | Database storage (persistent) |
| `/logs` | Application logs |
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

```yaml
services:
  nornicdb:
    # ... other config
    volumes:
      - ./certs:/certs:ro
    environment:
      - NORNICDB_TLS_ENABLED=true
      - NORNICDB_TLS_CERT=/certs/server.crt
      - NORNICDB_TLS_KEY=/certs/server.key
```

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

### 4. Use Docker Secrets

```yaml
services:
  nornicdb:
    # ... other config
    secrets:
      - nornicdb_password
    environment:
      - NORNICDB_AUTH_PASSWORD_FILE=/run/secrets/nornicdb_password

secrets:
  nornicdb_password:
    file: ./secrets/password.txt
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

```yaml
services:
  nornicdb:
    # ... other config
    environment:
      - NORNICDB_METRICS_ENABLED=true
      - NORNICDB_METRICS_PORT=9090
    ports:
      - "9090:9090"  # Metrics endpoint
```

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

## 🚀 Scaling

### Horizontal Scaling (Read Replicas)

```yaml
version: '3.8'

services:
  nornicdb-primary:
    image: timothyswt/nornicdb-arm64-metal:latest
    # ... primary config
    environment:
      - NORNICDB_ROLE=primary

  nornicdb-replica-1:
    image: timothyswt/nornicdb-arm64-metal:latest
    # ... replica config
    environment:
      - NORNICDB_ROLE=replica
      - NORNICDB_PRIMARY_URL=http://nornicdb-primary:7474

  nornicdb-replica-2:
    image: timothyswt/nornicdb-arm64-metal:latest
    # ... replica config
    environment:
      - NORNICDB_ROLE=replica
      - NORNICDB_PRIMARY_URL=http://nornicdb-primary:7474

  load-balancer:
    image: nginx:alpine
    ports:
      - "7474:80"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
    depends_on:
      - nornicdb-primary
      - nornicdb-replica-1
      - nornicdb-replica-2
```

### Resource Limits

```yaml
services:
  nornicdb:
    # ... other config
    deploy:
      resources:
        limits:
          cpus: '4'
          memory: 8G
        reservations:
          cpus: '2'
          memory: 4G
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
docker run --rm timothyswt/nornicdb-arm64-metal:latest \
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
      - NORNICDB_LOW_MEMORY=true  # Reduces RAM to ~50MB
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

### Data Corruption

```bash
# Stop container
docker stop nornicdb

# Restore from backup
# (see Backup & Restore section)

# Or start fresh
docker volume rm nornicdb-data
docker start nornicdb
```

## 📚 Advanced Configuration

### Custom Configuration File

Create `nornicdb.yaml`:

```yaml
server:
  port: 7474
  bolt_port: 7687
  
auth:
  enabled: true
  jwt_secret: "your-secret-key"
  
gpu:
  enabled: true
  backend: metal
  
embedding:
  provider: ollama
  url: http://ollama:11434
  model: mxbai-embed-large
  cache_size: 10000
  
storage:
  path: /data
  max_size: 100GB
  
performance:
  query_cache_size: 1000
  max_connections: 100
```

Mount in Docker:

```yaml
services:
  nornicdb:
    # ... other config
    volumes:
      - ./nornicdb.yaml:/config/nornicdb.yaml:ro
    environment:
      - NORNICDB_CONFIG=/config/nornicdb.yaml
```

## ⏭️ Next Steps

- **[Operations Guide](../operations/)** - Monitoring, backup, scaling
- **[Performance Tuning](../performance/optimization-guide.md)** - Optimize for your workload
- **[Security Guide](../compliance/)** - GDPR, HIPAA, SOC2 compliance

---

**Need help?** → **[Troubleshooting Guide](../operations/troubleshooting.md)**  
**Production ready?** → **[Operations Guide](../operations/)**
