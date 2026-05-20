# Quick Start Guide

**Get NornicDB running in minutes with Docker.**

## Option 1: Docker (Recommended)

### Apple Silicon (Mac)

```bash
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-arm64-metal-bge:latest
```

### NVIDIA GPU (Linux)

```bash
docker run -d \
  --name nornicdb \
  --gpus all \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-amd64-cuda-bge:latest
```

### CPU Only (Windows/Linux)

```bash
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-amd64-cpu:latest
```

## ✅ Verify Installation

```bash
# Check container is running
docker ps

# Test HTTP API
curl http://localhost:7474/health

# Test with a Neo4j-compatible Cypher HTTP request
curl -X POST http://localhost:7474/db/nornic/tx/commit \
  -H "Content-Type: application/json" \
  -d '{"statements":[{"statement":"CREATE (n:Person {name: \"Alice\"}) RETURN n"}]}'
```

## 🔗 Access NornicDB

- **HTTP API:** http://localhost:7474
- **Bolt Protocol:** bolt://localhost:7687
- **Health Check:** http://localhost:7474/health

## 🧪 Try Your First Query

NornicDB's Cypher-over-HTTP flow follows the Neo4j transaction endpoint shape.

```bash
# Create a node
curl -X POST http://localhost:7474/db/nornic/tx/commit \
  -H "Content-Type: application/json" \
  -d '{"statements":[{"statement":"CREATE (n:Person {name: \"Alice\", age: 30}) RETURN n"}]}'

# Query nodes
curl -X POST http://localhost:7474/db/nornic/tx/commit \
  -H "Content-Type: application/json" \
  -d '{"statements":[{"statement":"MATCH (n:Person) RETURN n.name, n.age"}]}'

# Create a relationship in one request
curl -X POST http://localhost:7474/db/nornic/tx/commit \
  -H "Content-Type: application/json" \
  -d '{"statements":[{"statement":"MERGE (a:Person {name: \"Alice\"}) MERGE (b:Person {name: \"Bob\"}) MERGE (a)-[:KNOWS]->(b) RETURN a, b"}]}'
```

## 🧠 Enable Semantic Search (Optional)

For embedding-powered search:

```bash
# Stop current container
docker stop nornicdb
docker rm nornicdb

# Start with Ollama for embeddings
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  -e NORNICDB_EMBEDDING_ENABLED=true \
  -e NORNICDB_EMBEDDING_PROVIDER=ollama \
  -e NORNICDB_EMBEDDING_MODEL=mxbai-embed-large \
  timothyswt/nornicdb-arm64-metal-bge:latest

# Or use docker-compose with Ollama
cat > docker-compose.yml << EOF
version: '3.8'
services:
  ollama:
    image: ollama/ollama:latest
    ports:
      - "11434:11434"
    volumes:
      - ollama-data:/root/.ollama

  nornicdb:
    image: timothyswt/nornicdb-arm64-metal-bge:latest
    depends_on:
      - ollama
    ports:
      - "7474:7474"
      - "7687:7687"
    volumes:
      - nornicdb-data:/data
    environment:
      - NORNICDB_EMBEDDING_ENABLED=true
      - NORNICDB_EMBEDDING_PROVIDER=ollama
      - NORNICDB_EMBEDDING_API_URL=http://ollama:11434
      - NORNICDB_EMBEDDING_MODEL=mxbai-embed-large

volumes:
  ollama-data:
  nornicdb-data:
EOF

docker-compose up -d
```

## 🔧 Common CLI Commands

```bash
# View logs
docker logs nornicdb

# Follow logs
docker logs -f nornicdb

# Access container shell
docker exec -it nornicdb sh

# Stop container
docker stop nornicdb

# Start container
docker start nornicdb

# Remove container
docker rm nornicdb

# Backup data
docker run --rm \
  -v nornicdb-data:/data \
  -v $(pwd):/backup \
  alpine tar czf /backup/nornicdb-backup-$(date +%Y%m%d).tar.gz -C /data .
```

## 🧪 Testing

```bash
# Run basic functionality tests
curl -X POST http://localhost:7474/db/nornic/tx/commit \
  -H "Content-Type: application/json" \
  -d '{"statements":[{"statement":"RETURN 1 as test"}]}'

# Test Bolt connection (requires nc command)
nc -z localhost 7687 && echo "Bolt port open" || echo "Bolt port closed"

# Performance test
time curl -X POST http://localhost:7474/db/nornic/tx/commit \
  -H "Content-Type: application/json" \
  -d '{"statements":[{"statement":"MATCH (n) RETURN count(n)"}]}'
```

## 🛑 Stopping NornicDB

```bash
# Graceful stop
docker stop nornicdb

# Remove container (keeps data)
docker rm nornicdb

# Remove data volume (deletes all data)
docker volume rm nornicdb-data
```

## 🔧 Troubleshooting

### Container Won't Start

```bash
# Check logs
docker logs nornicdb

# Check if ports are available
lsof -i :7474
lsof -i :7687

# Check Docker resources
docker system df
```

### Connection Issues

```bash
# Verify container is running
docker ps

# Check network connectivity
docker network ls
docker network inspect bridge

# Test from inside container
docker exec -it nornicdb curl http://localhost:7474/health
```

### GPU Not Working

```bash
# Check GPU availability (NVIDIA)
docker run --rm --gpus all nvidia/cuda:11.8-base-ubuntu20.04 nvidia-smi

# Check Metal support (macOS)
docker run --rm timothyswt/nornicdb-arm64-metal-bge:latest \
  /bin/sh -c "system_profiler SPDisplaysDataType | grep Metal"
```

### Out of Memory

NornicDB uses ~1GB RAM by default. Enable low memory mode:

```bash
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  -e NORNICDB_LOW_MEMORY=true \
  timothyswt/nornicdb-arm64-metal-bge:latest
```

## ⏭️ Next Steps

- **[Docker Deployment Guide](./docker-deployment.md)** - Production deployment with Docker
- **[Operations Guide](../operations/README.md)** - Production operations and monitoring
- **[Cypher Query Language](../user-guides/cypher-queries.md)** - Learn the query language
- **[API Documentation](../api-reference/README.md)** - REST and compatibility reference
- **[Performance Tuning](../performance/README.md)** - Optimize your setup

> Search defaults: BM25 fulltext and vector ANN indexes are both **enabled** and built **at startup** for every database — today's behaviour. Multi-tenant or memory-constrained deployments can defer index builds with `--search-bm25-warming=lazy` / `--search-vector-warming=lazy` or disable indexes entirely per-database. See [Per-Database Search Index Flags](../operations/configuration.md#per-database-search-index-control).

---

**Need help?** → **[Troubleshooting Guide](../operations/troubleshooting.md)**  
**Production ready?** → **[Docker Deployment Guide](./docker-deployment.md)**
