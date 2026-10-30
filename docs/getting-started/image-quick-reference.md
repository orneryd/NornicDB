# Docker Image Quick Reference

**Quick reference for NornicDB Docker images and common commands.**

## 📦 Production Images

| Platform | Architecture | GPU Support | Image | Command |
|----------|-------------|-------------|-------|---------|
| Apple Silicon | ARM64 | Metal | `timothyswt/nornicdb-arm64-metal-bge` | `docker run -d -p 7474:7474 -p 7687:7687 -v nornicdb-data:/data timothyswt/nornicdb-arm64-metal-bge:latest` |
| NVIDIA GPU | AMD64 | CUDA | `timothyswt/nornicdb-amd64-cuda-bge` | `docker run -d --gpus all -p 7474:7474 -p 7687:7687 -v nornicdb-data:/data timothyswt/nornicdb-amd64-cuda-bge:latest` |
| CPU Only | AMD64 | None | `timothyswt/nornicdb-amd64-cpu` | `docker run -d -p 7474:7474 -p 7687:7687 -v nornicdb-data:/data timothyswt/nornicdb-amd64-cpu:latest` |

## 🛠️ Development Images

| Platform | Architecture | Features | Image | Command |
|----------|-------------|----------|-------|---------|
| Apple Silicon | ARM64 | Metal + Heimdall AI | `timothyswt/nornicdb-arm64-metal-bge-heimdall` | `docker run -d -p 7474:7474 -p 7687:7687 -v nornicdb-data:/data timothyswt/nornicdb-arm64-metal-bge-heimdall:latest` |
| NVIDIA GPU | AMD64 | CUDA + Heimdall AI | `timothyswt/nornicdb-amd64-cuda-bge-heimdall` | `docker run -d --gpus all -p 7474:7474 -p 7687:7687 -v nornicdb-data:/data timothyswt/nornicdb-amd64-cuda-bge-heimdall:latest` |

## Quick Start Commands

### Basic Setup

```bash
# Apple Silicon (Mac)
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

# CPU Only (Windows/Linux)
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-amd64-cpu:latest
```

### With Authentication

Authentication and access-control settings are documented centrally in the [Environment Variables Reference](../operations/environment-variables.md). Use this page for image selection and container commands, and use the operations docs for the supported auth configuration surface.

### With Semantic Search

```bash
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  -e NORNICDB_EMBEDDING_ENABLED=true \
  -e NORNICDB_EMBEDDING_PROVIDER=ollama \
  -e NORNICDB_EMBEDDING_MODEL=mxbai-embed-large \
  timothyswt/nornicdb-arm64-metal-bge:latest
```

### Low Memory Mode

```bash
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  -e NORNICDB_LOW_MEMORY=true \
  timothyswt/nornicdb-arm64-metal-bge:latest
```

## 🔧 Common Operations

### Container Management

```bash
# View logs
docker logs nornicdb

# Follow logs
docker logs -f nornicdb

# Stop container
docker stop nornicdb

# Start container
docker start nornicdb

# Remove container
docker rm nornicdb

# Access shell
docker exec -it nornicdb sh
```

### Data Management

```bash
# Backup data
docker run --rm \
  -v nornicdb-data:/data \
  -v $(pwd):/backup \
  alpine tar czf /backup/nornicdb-backup-$(date +%Y%m%d).tar.gz -C /data .

# Restore data
docker run --rm \
  -v nornicdb-data:/data \
  -v $(pwd):/backup \
  alpine tar xzf /backup/nornicdb-backup-20251201.tar.gz -C /data

# List volumes
docker volume ls | grep nornicdb
```

### Health Checks

```bash
# Check container status
docker ps

# Test HTTP API
curl -f http://localhost:7474/health

# Test Bolt protocol
nc -z localhost 7687

# Test query
curl -X POST http://localhost:7474/db/nornic/tx/commit \
  -H "Content-Type: application/json" \
  -d '{"statements":[{"statement":"RETURN 1 as test"}]}'
```

## 🏗️ Building Images

### Prerequisites

- Docker 20.10+
- Make tool
- Go 1.21+ (for building from source)

### Build Commands

```bash
# Clone repository
git clone https://github.com/timothyswt/nornicdb.git
cd nornicdb

# Build Apple Silicon image
make build-arm64-metal-bge

# Build NVIDIA GPU image
make build-amd64-cuda-bge

# Build CPU only image
make build-amd64-cpu

# Build with Heimdall AI
make build-arm64-metal-bge-heimdall
```

### Custom Build

```bash
# Build without cache
DOCKER_NO_CACHE=1 make build-arm64-metal-bge
```

## 🌐 Access Points

| Service | Port | Protocol | URL |
|---------|------|----------|-----|
| HTTP API | 7474 | HTTP | http://localhost:7474 |
| Bolt Protocol | 7687 | Bolt | bolt://localhost:7687 |
| Health Check | 7474 | HTTP | http://localhost:7474/health |
| Metrics (optional) | 9090 | HTTP | http://localhost:9090/metrics |

## 🔒 Environment Variables

Use the canonical [Environment Variables Reference](../operations/environment-variables.md) for supported settings and defaults. This quick reference intentionally stays focused on image choice, startup commands, and operational shortcuts.

## 📚 Documentation Links

- **[Docker Deployment Guide](./docker-deployment.md)** - Complete deployment guide
- **[Operations Guide](../operations/README.md)** - Production operations
- **[Docker Operations Guide](../operations/docker.md)** - Docker-specific operations
- **[Troubleshooting Guide](../operations/troubleshooting.md)** - Common issues

---

**Need help?** → **[Troubleshooting Guide](../operations/troubleshooting.md)**  
**Production deployment?** → **[Docker Deployment Guide](./docker-deployment.md)**
