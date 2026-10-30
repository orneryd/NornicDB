# Docker Operations Guide

**Operational guidance for running NornicDB in Docker production environments.**

## 📋 Available Images

### Production Images

| Architecture | GPU Support           | Image                                 | Use Case               |
| ------------ | --------------------- | ------------------------------------- | ---------------------- |
| ARM64        | Metal (Apple Silicon) | `timothyswt/nornicdb-arm64-metal-bge` | Production on Mac      |
| AMD64        | CUDA (NVIDIA)         | `timothyswt/nornicdb-amd64-cuda-bge`  | Production on Linux    |
| AMD64        | CPU only              | `timothyswt/nornicdb-amd64-cpu`       | Production without GPU |

### Development Images

| Architecture | Features         | Image                                          | Use Case                      |
| ------------ | ---------------- | ---------------------------------------------- | ----------------------------- |
| ARM64        | Metal + Heimdall | `timothyswt/nornicdb-arm64-metal-bge-heimdall` | Development with AI assistant |
| AMD64        | CUDA + Heimdall  | `timothyswt/nornicdb-amd64-cuda-bge-heimdall`  | Development with AI assistant |

## Quick Start

```bash
# Pull and run (Apple Silicon)
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-arm64-metal-bge:latest

# Pull and run (NVIDIA GPU)
docker run -d \
  --name nornicdb \
  --gpus all \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-amd64-cuda-bge:latest
```

## 🐳 Docker Compose

### Basic Setup

```yaml
version: "3.8"
services:
  nornicdb:
    image: timothyswt/nornicdb-arm64-metal-bge:latest
    container_name: nornicdb
    ports:
      - "7474:7474"
      - "7687:7687"
    volumes:
      - nornicdb-data:/data
      - nornicdb-logs:/logs
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:7474/health"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  nornicdb-data:
  nornicdb-logs:
```

### With Ollama (Embeddings)

```yaml
version: "3.8"
services:
  nornicdb:
    image: timothyswt/nornicdb-arm64-metal-bge:latest
    container_name: nornicdb
    depends_on:
      - ollama
    ports:
      - "7474:7474"
      - "7687:7687"
    volumes:
      - nornicdb-data:/data
      - nornicdb-logs:/logs
    environment:
      - NORNICDB_EMBEDDING_PROVIDER=ollama
      - NORNICDB_EMBEDDING_API_URL=http://ollama:11434
      - NORNICDB_EMBEDDING_MODEL=mxbai-embed-large
      - NORNICDB_EMBEDDING_ENABLED=true
    restart: unless-stopped

  ollama:
    image: ollama/ollama:latest
    container_name: ollama
    ports:
      - "11434:11434"
    volumes:
      - ollama-data:/root/.ollama
    restart: unless-stopped

volumes:
  nornicdb-data:
  nornicdb-logs:
  ollama-data:
```

### Production Configuration

```yaml
version: "3.8"
services:
  nornicdb:
    image: timothyswt/nornicdb-arm64-metal-bge:latest
    container_name: nornicdb
    ports:
      - "7474:7474"
      - "7687:7687"
    volumes:
      - nornicdb-data:/data
      - nornicdb-logs:/logs
    environment:
      - NORNICDB_LOG_LEVEL=info
    deploy:
      resources:
        limits:
          memory: 4G
          cpus: "2"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:7474/health"]
      interval: 30s
      timeout: 10s
      retries: 3
    restart: unless-stopped
    logging:
      driver: "json-file"
      options:
        max-size: "100m"
        max-file: "3"

volumes:
  nornicdb-data:
  nornicdb-logs:
```

## ⚙️ Configuration

Use the canonical runtime references instead of maintaining a partial Docker-only variable table here:

- [Environment Variables Reference](./environment-variables.md) for supported environment variables and defaults.
- [Configuration Guide](./configuration.md) for YAML and operational configuration.
- [Reverse Proxy and TLS Termination](./reverse-proxy.md) before exposing an authenticated container through Nginx or another ingress.

Common Docker-specific overrides that are verified in the runtime docs:

- `NORNICDB_LOW_MEMORY=true` to reduce memory usage.
- `NORNICDB_EMBEDDING_ENABLED=true` with `NORNICDB_EMBEDDING_PROVIDER`, `NORNICDB_EMBEDDING_API_KEY`, `NORNICDB_EMBEDDING_API_URL`, and `NORNICDB_EMBEDDING_MODEL` to enable semantic search. Voyage omits `NORNICDB_EMBEDDING_API_URL` unless you need a custom endpoint.
- `NORNICDB_QDRANT_GRPC_ENABLED=true` and `NORNICDB_QDRANT_GRPC_LISTEN_ADDR=:6334` when exposing the Qdrant-compatible gRPC interface. Authenticated public exposure also requires native TLS certificate/key settings; mount those files read-only.

### Volume Mounts

| Path      | Purpose                        |
| --------- | ------------------------------ |
| `/data`   | Database storage (persistent)  |
| `/logs`   | Application logs               |
| `/config` | Configuration files (optional) |

## 🔧 Building Custom Images

### From Source

```bash
# Clone repository
git clone https://github.com/timothyswt/nornicdb.git
cd nornicdb

# Build for Apple Silicon
make build-arm64-metal-bge

# Build for NVIDIA GPU
make build-amd64-cuda-bge

# Build CPU only
make build-amd64-cpu
```

### Custom Dockerfile

```dockerfile
FROM timothyswt/nornicdb-arm64-metal-bge:latest

# Add custom configuration
COPY nornicdb.yaml /config/nornicdb.yaml

# Set environment variables
ENV NORNICDB_CONFIG=/config/nornicdb.yaml

# Add custom plugins or tools
RUN apt-get update && apt-get install -y \
    curl \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Expose additional ports if needed
EXPOSE 8080
```

Build with:

```bash
docker build -t my-nornicdb:latest .
```

## 🖥️ GPU Acceleration

### Apple Silicon (Metal)

GPU acceleration is automatic on Apple Silicon Macs:

```bash
# Verify GPU is available
docker run --rm timothyswt/nornicdb-arm64-metal-bge:latest \
  /bin/sh -c "system_profiler SPDisplaysDataType | grep Metal"
```

### NVIDIA (CUDA)

Enable GPU access with `--gpus all`:

```bash
# Verify NVIDIA Docker setup
docker run --rm --gpus all nvidia/cuda:11.8-base-ubuntu20.04 nvidia-smi

# Run with GPU support
docker run -d \
  --name nornicdb \
  --gpus all \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-amd64-cuda-bge:latest
```

### Vulkan (Cross-platform)

For Vulkan GPU acceleration:

```bash
# Run with Vulkan support
docker run -d \
  --name nornicdb \
  --device /dev/dri \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-amd64-vulkan-bge:latest
```

## 📊 Health Checks

### Default Health Check

```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:7474/health"]
  interval: 30s
  timeout: 10s
  retries: 3
  start_period: 40s
```

### Custom Health Check

```yaml
healthcheck:
  test:
    [
      "CMD",
      "sh",
      "-c",
      "curl -f http://localhost:7474/health && nc -z localhost 7687",
    ]
  interval: 15s
  timeout: 5s
  retries: 5
  start_period: 60s
```

### Manual Health Check

```bash
# Check container status
docker ps

# Check health endpoint
curl -f http://localhost:7474/health

# Check Bolt protocol
nc -z localhost 7687
```

## 📝 Logs Management

### Viewing Logs

```bash
# View all logs
docker logs nornicdb

# Follow logs
docker logs -f nornicdb

# View last 100 lines
docker logs --tail 100 nornicdb

# View logs from last hour
docker logs --since 1h nornicdb

# View logs with timestamps
docker logs -t nornicdb
```

### Log Rotation

```yaml
services:
  nornicdb:
    # ... other config
    logging:
      driver: "json-file"
      options:
        max-size: "100m"
        max-file: "5"
```

### External Log Aggregation

```yaml
services:
  nornicdb:
    # ... other config
    logging:
      driver: "fluentd"
      options:
        fluentd-address: localhost:24224
        fluentd-async-connect: "true"
        tag: nornicdb
```

## 🔍 Troubleshooting

### Common Issues

#### Container Won't Start

```bash
# Check logs
docker logs nornicdb

# Check resource usage
docker stats nornicdb

# Check port conflicts
lsof -i :7474
lsof -i :7687

# Recreate container
docker rm -f nornicdb
docker run ...
```

#### GPU Not Working

```bash
# Check GPU devices
docker run --rm --gpus all nvidia/cuda:11.8-base-ubuntu20.04 nvidia-smi

# Check Metal support (macOS)
docker run --rm timothyswt/nornicdb-arm64-metal-bge:latest \
  /bin/sh -c "system_profiler SPDisplaysDataType | grep Metal"

# Enable GPU access
docker run --gpus all ...
```

#### Memory Issues

```bash
# Check memory usage
docker stats nornicdb --no-stream

# Enable low memory mode
docker exec -it nornicdb sh -c 'export NORNICDB_LOW_MEMORY=true && kill -HUP 1'

# Increase memory limit
docker update --memory 8g nornicdb
```

#### Performance Issues

```bash
# Check resource usage
docker stats nornicdb

# Check query performance with a Neo4j-compatible HTTP request
curl -X POST http://localhost:7474/db/nornic/tx/commit \
  -H "Content-Type: application/json" \
  -d '{"statements":[{"statement":"EXPLAIN MATCH (n) RETURN count(n)"}]}'
```

### Debug Mode

```bash
# Run with debug logging
docker run -d \
  --name nornicdb-debug \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  -e NORNICDB_LOG_LEVEL=debug \
  timothyswt/nornicdb-arm64-metal-bge:latest

# Access shell
docker exec -it nornicdb-debug sh

# Monitor processes
docker exec -it nornicdb-debug ps aux

# Check configuration
docker exec -it nornicdb-debug env | grep NORNICDB
```

## 📈 Performance Optimization

### Resource Limits

```yaml
services:
  nornicdb:
    # ... other config
    deploy:
      resources:
        limits:
          cpus: "4"
          memory: 8G
        reservations:
          cpus: "2"
          memory: 4G
```

### Performance Tuning

Prefer the canonical tuning guides over ad hoc Docker-only variables:

- [Performance Guide](../performance/README.md) for workload tuning.
- [Scaling Guide](./scaling.md) for capacity planning, reverse proxies, and multi-instance deployment.
- [Environment Variables Reference](./environment-variables.md) for supported cache and runtime knobs.

### Storage Optimization

```yaml
services:
  nornicdb:
    # ... other config
    volumes:
      - type: tmpfs
        target: /tmp
        tmpfs:
          size: 1G
      - nornicdb-data:/data
    environment:
      - NORNICDB_TMP_DIR=/tmp
```

## 🔄 Updates and Maintenance

### Updating Images

```bash
# Pull latest image
docker pull timothyswt/nornicdb-arm64-metal-bge:latest

# Stop and recreate
docker stop nornicdb
docker rm nornicdb
docker run -d ... timothyswt/nornicdb-arm64-metal-bge:latest
```

### Rolling Updates

```bash
# With docker-compose
docker-compose pull
docker-compose up -d --no-deps nornicdb
```

### Scheduled Maintenance

```bash
#!/bin/bash
# maintenance.sh

# Backup data
docker run --rm \
  -v nornicdb-data:/data \
  -v /backups:/backup \
  alpine tar czf /backup/nornicdb-$(date +%Y%m%d).tar.gz -C /data .

# Update image
docker pull timothyswt/nornicdb-arm64-metal-bge:latest

# Restart with new image
docker-compose up -d

# Health check
sleep 30
curl -f http://localhost:7474/health || echo "Health check failed"
```

## 📚 Related Documentation

- **[Docker Deployment Guide](../getting-started/docker-deployment.md)** - Getting started with Docker
- **[Operations Guide](./README.md)** - Production operations
- **[Low Memory Mode](./low-memory-mode.md)** - Memory optimization
- **[Troubleshooting Guide](./troubleshooting.md)** - Common issues and solutions

---

**Need help?** → **[Troubleshooting Guide](./troubleshooting.md)**  
**Production deployment?** → **[Docker Deployment Guide](../getting-started/docker-deployment.md)**
