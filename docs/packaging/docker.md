# Docker Deployment Plan

## Overview

Docker is the **recommended deployment method** for NornicDB in production environments. It provides:

- Consistent environment across platforms
- Easy updates and rollbacks
- Isolation from host system
- Simple orchestration with Docker Compose or Kubernetes

## Available Images

| Image                                 | Architecture | GPU   | Size   | Use Case                    |
| ------------------------------------- | ------------ | ----- | ------ | --------------------------- |
| `timothyswt/nornicdb-arm64-metal`     | arm64        | Metal | ~150MB | Apple Silicon, ARM servers  |
| `timothyswt/nornicdb-arm64-metal-bge` | arm64        | Metal | ~500MB | ARM with embedded BGE model |
| `timothyswt/nornicdb-amd64-cuda`      | amd64        | CUDA  | ~200MB | NVIDIA GPU servers          |
| `timothyswt/nornicdb-amd64-cuda-bge`  | amd64        | CUDA  | ~550MB | NVIDIA with embedded BGE    |
| `timothyswt/nornicdb-amd64-cpu`       | amd64        | None  | ~100MB | CPU-only, smallest          |

## Quick Start

### Basic Usage

```bash
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-arm64-metal:latest
```

### With External Embeddings (Ollama)

```bash
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  -e NORNICDB_EMBEDDING_PROVIDER=ollama \
  -e NORNICDB_EMBEDDING_ENDPOINT=http://host.docker.internal:11434 \
  -e NORNICDB_EMBEDDING_MODEL=nomic-embed-text \
  timothyswt/nornicdb-arm64-metal:latest
```

### With Embedded BGE Model

```bash
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-arm64-metal-bge:latest
```

## Docker Compose

### Basic Setup (`docker-compose.yml`)

```yaml
version: "3.8"

services:
  nornicdb:
    image: timothyswt/nornicdb-arm64-metal:latest
    container_name: nornicdb
    ports:
      - "7474:7474" # HTTP/UI
      - "7687:7687" # Bolt protocol
    volumes:
      - nornicdb-data:/data
      - nornicdb-logs:/var/log/nornicdb
    environment:
      - NORNICDB_LOG_LEVEL=info
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:7474/status"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 10s

volumes:
  nornicdb-data:
  nornicdb-logs:
```

### Full Stack with Ollama (`docker-compose.full.yml`)

```yaml
version: "3.8"

services:
  nornicdb:
    image: timothyswt/nornicdb-arm64-metal:latest
    container_name: nornicdb
    ports:
      - "7474:7474"
      - "7687:7687"
    volumes:
      - nornicdb-data:/data
    environment:
      - NORNICDB_LOG_LEVEL=info
      - NORNICDB_EMBEDDING_PROVIDER=ollama
      - NORNICDB_EMBEDDING_ENDPOINT=http://ollama:11434
      - NORNICDB_EMBEDDING_MODEL=nomic-embed-text
    depends_on:
      - ollama
    restart: unless-stopped

  ollama:
    image: ollama/ollama:latest
    container_name: ollama
    ports:
      - "11434:11434"
    volumes:
      - ollama-models:/root/.ollama
    # For GPU support on Linux:
    # deploy:
    #   resources:
    #     reservations:
    #       devices:
    #         - driver: nvidia
    #           count: 1
    #           capabilities: [gpu]
    restart: unless-stopped

  # Optional: Web UI for Ollama
  open-webui:
    image: ghcr.io/open-webui/open-webui:main
    container_name: open-webui
    ports:
      - "3000:8080"
    environment:
      - OLLAMA_BASE_URL=http://ollama:11434
    volumes:
      - open-webui-data:/app/backend/data
    depends_on:
      - ollama
    restart: unless-stopped

volumes:
  nornicdb-data:
  ollama-models:
  open-webui-data:
```

### With Mimir Server (`docker-compose.mimir.yml`)

```yaml
version: "3.8"

services:
  nornicdb:
    image: timothyswt/nornicdb-arm64-metal:latest
    container_name: nornicdb
    ports:
      - "7474:7474"
      - "7687:7687"
    volumes:
      - nornicdb-data:/data
    environment:
      - NORNICDB_LOG_LEVEL=info
      - NORNICDB_EMBEDDING_PROVIDER=ollama
      - NORNICDB_EMBEDDING_ENDPOINT=http://ollama:11434
    restart: unless-stopped

  mimir-server:
    image: timothyswt/mimir-server:latest
    container_name: mimir-server
    ports:
      - "3100:3100"
    environment:
      - NEO4J_URI=bolt://nornicdb:7687
      - NEO4J_USER=admin
      - NEO4J_PASSWORD=admin
      - EMBEDDING_PROVIDER=ollama
      - OLLAMA_URL=http://ollama:11434
    depends_on:
      - nornicdb
      - ollama
    restart: unless-stopped

  ollama:
    image: ollama/ollama:latest
    container_name: ollama
    ports:
      - "11434:11434"
    volumes:
      - ollama-models:/root/.ollama
    restart: unless-stopped

volumes:
  nornicdb-data:
  ollama-models:
```

## Environment Variables

| Variable                      | Default | Description                               |
| ----------------------------- | ------- | ----------------------------------------- |
| `NORNICDB_HTTP_PORT`          | `7474`  | HTTP server port                          |
| `NORNICDB_BOLT_PORT`          | `7687`  | Bolt protocol port                        |
| `NORNICDB_DATA_DIR`           | `/data` | Data directory                            |
| `NORNICDB_LOG_LEVEL`          | `info`  | Log level (debug, info, warn, error)      |
| `NORNICDB_AUTH_ENABLED`       | `false` | Enable authentication                     |
| `NORNICDB_AUTH_USER`          | `admin` | Default username                          |
| `NORNICDB_AUTH_PASSWORD`      | `admin` | Default password                          |
| `NORNICDB_EMBEDDING_PROVIDER` | `none`  | Embedding provider (none, ollama, openai) |
| `NORNICDB_EMBEDDING_ENDPOINT` | -       | Embedding API endpoint                    |
| `NORNICDB_EMBEDDING_MODEL`    | -       | Embedding model name                      |
| `NORNICDB_EMBEDDING_API_KEY`  | -       | API key for embeddings                    |
| `NORNICDB_MCP_ENABLED`        | `true`  | Enable MCP server                         |
| `NORNICDB_MCP_PORT`           | `3100`  | MCP server port                           |

## GPU Support

### NVIDIA CUDA (Linux)

```yaml
services:
  nornicdb:
    image: timothyswt/nornicdb-amd64-cuda:latest
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]
```

### Apple Metal (macOS)

Metal acceleration is automatic on Apple Silicon. No special configuration needed.

```bash
# Verify Metal is available
docker run --rm timothyswt/nornicdb-arm64-metal:latest nornicdb --version
```

## Volume Management

### Backup

```bash
# Stop container
docker stop nornicdb

# Backup volume
docker run --rm \
  -v nornicdb-data:/data \
  -v $(pwd):/backup \
  alpine tar czf /backup/nornicdb-backup-$(date +%Y%m%d).tar.gz /data

# Restart
docker start nornicdb
```

### Restore

```bash
docker stop nornicdb

docker run --rm \
  -v nornicdb-data:/data \
  -v $(pwd):/backup \
  alpine sh -c "rm -rf /data/* && tar xzf /backup/nornicdb-backup-YYYYMMDD.tar.gz -C /"

docker start nornicdb
```

### Migration

```bash
# Export from old container
docker exec nornicdb nornicdb export --format cypher > backup.cypher

# Import to new container
docker exec -i new-nornicdb nornicdb import < backup.cypher
```

## Health Checks

```bash
# HTTP health
curl http://localhost:7474/status

# Bolt connection test
docker exec nornicdb nornicdb health

# Container health
docker inspect --format='{{.State.Health.Status}}' nornicdb
```

## Logging

```bash
# View logs
docker logs nornicdb

# Follow logs
docker logs -f nornicdb

# With timestamps
docker logs -t nornicdb

# Last N lines
docker logs --tail 100 nornicdb
```

## Security

### Run as Non-Root

```yaml
services:
  nornicdb:
    image: timothyswt/nornicdb-arm64-metal:latest
    user: "1000:1000"
    volumes:
      - nornicdb-data:/data
```

### Read-Only Root Filesystem

```yaml
services:
  nornicdb:
    image: timothyswt/nornicdb-arm64-metal:latest
    read_only: true
    tmpfs:
      - /tmp
    volumes:
      - nornicdb-data:/data
```

### Network Isolation

```yaml
services:
  nornicdb:
    image: timothyswt/nornicdb-arm64-metal:latest
    networks:
      - backend
    # Don't expose ports publicly
    # Access only from other containers

networks:
  backend:
    internal: true
```

## Kubernetes

### Basic Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nornicdb
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nornicdb
  template:
    metadata:
      labels:
        app: nornicdb
    spec:
      containers:
        - name: nornicdb
          image: timothyswt/nornicdb-arm64-metal:latest
          ports:
            - containerPort: 7474
            - containerPort: 7687
          volumeMounts:
            - name: data
              mountPath: /data
          resources:
            requests:
              memory: "256Mi"
              cpu: "100m"
            limits:
              memory: "1Gi"
              cpu: "1000m"
          livenessProbe:
            httpGet:
              path: /status
              port: 7474
            initialDelaySeconds: 10
            periodSeconds: 30
          readinessProbe:
            httpGet:
              path: /status
              port: 7474
            initialDelaySeconds: 5
            periodSeconds: 10
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: nornicdb-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: nornicdb
spec:
  selector:
    app: nornicdb
  ports:
    - name: http
      port: 7474
    - name: bolt
      port: 7687
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: nornicdb-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
```

## Building Custom Images

### From Source

```bash
cd nornicdb

# Build for your architecture
make build-arm64-metal    # Apple Silicon
make build-amd64-cuda     # NVIDIA GPU

# Build all variants
make build-all
```

### Custom Dockerfile

```dockerfile
FROM timothyswt/nornicdb-arm64-metal:latest

# Add custom configuration
COPY config.yaml /etc/nornicdb/config.yaml

# Add custom models
COPY models/ /models/

ENV NORNICDB_CONFIG=/etc/nornicdb/config.yaml
```

## Implementation Checklist

- [x] Create arm64-metal image
- [x] Create amd64-cuda image
- [x] Create CPU-only image
- [x] Create BGE-embedded variants
- [x] Push to Docker Hub
- [x] Document docker-compose examples
- [ ] Add Kubernetes manifests
- [ ] Add Helm chart
- [ ] Set up multi-arch manifest
- [ ] Add GitHub Container Registry
- [ ] Document GPU passthrough
- [ ] Create slim/alpine variants
