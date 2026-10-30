# Deployment Guide

**Deploy NornicDB to production environments.**

## Deployment Options

| Method         | Best For            | Complexity |
| -------------- | ------------------- | ---------- |
| Docker         | Most deployments    | Low        |
| Docker Compose | Multi-service       | Low        |
| Kubernetes     | Enterprise/Cloud    | Medium     |
| Bare Metal     | Maximum performance | High       |

## Docker Deployment

### Quick Start

```bash
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  -e NORNICDB_ADDRESS=0.0.0.0 \
  timothyswt/nornicdb-arm64-metal:latest
```

### Production Configuration

```bash
docker run -d \
  --name nornicdb \
  --restart unless-stopped \
  -p 7474:7474 \
  -p 7687:7687 \
  -v /opt/nornicdb/data:/data \
  -v /opt/nornicdb/config:/config \
  -e NORNICDB_ADDRESS=0.0.0.0 \
  -e NORNICDB_ENCRYPTION_PASSWORD="${ENCRYPTION_PASSWORD}" \
  -e NORNICDB_AUTH_JWT_SECRET="${JWT_SECRET}" \
  --memory=4g \
  --cpus=2 \
  timothyswt/nornicdb-arm64-metal:latest
```

### Docker Compose

```yaml
# docker-compose.yml
version: "3.8"
services:
  nornicdb:
    image: timothyswt/nornicdb-arm64-metal:latest
    ports:
      - "7474:7474"
      - "7687:7687"
    volumes:
      - nornicdb-data:/data
      - ./config:/config
    environment:
      NORNICDB_ADDRESS: "0.0.0.0"
      NORNICDB_ENCRYPTION_PASSWORD: "${ENCRYPTION_PASSWORD}"
      NORNICDB_AUTH_JWT_SECRET: "${JWT_SECRET}"
    restart: unless-stopped
    deploy:
      resources:
        limits:
          memory: 4G
          cpus: "2"

volumes:
  nornicdb-data:
```

## Kubernetes Deployment

### Deployment Manifest

```yaml
# nornicdb-deployment.yaml
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
          env:
            - name: NORNICDB_ADDRESS
              value: "0.0.0.0"
            - name: NORNICDB_AUTH_JWT_SECRET
              valueFrom:
                secretKeyRef:
                  name: nornicdb-secrets
                  key: jwt-secret
          volumeMounts:
            - name: data
              mountPath: /data
          resources:
            requests:
              memory: "2Gi"
              cpu: "1"
            limits:
              memory: "4Gi"
              cpu: "2"
          livenessProbe:
            httpGet:
              path: /health
              port: 7474
            initialDelaySeconds: 30
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /health
              port: 7474
            initialDelaySeconds: 5
            periodSeconds: 5
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
      targetPort: 7474
    - name: bolt
      port: 7687
      targetPort: 7687
```

### Secrets

```yaml
# nornicdb-secrets.yaml
apiVersion: v1
kind: Secret
metadata:
  name: nornicdb-secrets
type: Opaque
stringData:
  jwt-secret: "your-super-secret-jwt-key-min-32-chars"
  encryption-password: "your-encryption-password-32-chars"
```

## Bare Metal Installation

### Prerequisites

- Go 1.21+ (for building from source)
- 2GB+ RAM recommended
- SSD storage recommended

### Build from Source

```bash
git clone https://github.com/orneryd/nornicdb.git
cd nornicdb
make build
```

### Install Binary

```bash
# Copy binary
sudo cp bin/nornicdb /usr/local/bin/

# Create data directory
sudo mkdir -p /var/lib/nornicdb
sudo chown nornicdb:nornicdb /var/lib/nornicdb

# Create config directory
sudo mkdir -p /etc/nornicdb
```

### Systemd Service

```ini
# /etc/systemd/system/nornicdb.service
[Unit]
Description=NornicDB Graph Database
After=network.target

[Service]
Type=simple
User=nornicdb
Group=nornicdb
ExecStart=/usr/local/bin/nornicdb serve \
  --data-dir=/var/lib/nornicdb \
  --address=127.0.0.1
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
# Enable and start
sudo systemctl enable nornicdb
sudo systemctl start nornicdb
```

## Configuration

### Environment Variables

| Variable                       | Description                              | Default           |
| ------------------------------ | ---------------------------------------- | ----------------- |
| `NORNICDB_ADDRESS`             | Bind address                             | `127.0.0.1`       |
| `NORNICDB_HTTP_PORT`           | HTTP port                                | `7474`            |
| `NORNICDB_BOLT_PORT`           | Bolt port                                | `7687`            |
| `NORNICDB_DATA_DIR`            | Data directory                           | `./data`          |
| `NORNICDB_AUTH`                | `none` to disable; `user/pass` to enable | `none` (auth off) |
| `NORNICDB_AUTH_JWT_SECRET`     | JWT signing key                          | Required          |
| `NORNICDB_ENCRYPTION_PASSWORD` | Encryption key                           | Optional          |

### CLI Flags

```bash
nornicdb serve \
  --address=0.0.0.0 \
  --http-port=7474 \
  --bolt-port=7687 \
  --data-dir=/data \
  --embedding-url=http://ollama:11434 \
  --embedding-model=mxbai-embed-large
```

## Security Hardening

### Network Security

```yaml
# Bind to localhost only (default)
NORNICDB_ADDRESS: "127.0.0.1"

# For Docker/K8s, bind to all interfaces
NORNICDB_ADDRESS: "0.0.0.0"
```

### Enable TLS

For native HTTPS on the HTTP API:

```yaml
server:
  https:
    enabled: true
    port: 7473
    cert_file: /etc/nornicdb/server.crt
    key_file: /etc/nornicdb/server.key
```

For TLS termination in Nginx or another ingress, follow [Reverse Proxy and TLS Termination](reverse-proxy.md). Authenticated cleartext HTTP on a non-loopback backend requires `http_trusted_proxies` and a non-wildcard CORS policy.

### Enable Encryption

```bash
export NORNICDB_ENCRYPTION_PASSWORD="your-32-char-secure-password"
```

## Health Checks

### HTTP Health Check

```bash
curl http://localhost:7474/health
# {"status": "healthy"}
```

### Detailed Status

```bash
curl http://localhost:7474/status \
  -H "Authorization: Bearer $TOKEN"
```

## See Also

- **[Docker](docker.md)** - Docker-specific configuration
- **[Monitoring](monitoring.md)** - Metrics and alerting
- **[Scaling](scaling.md)** - Horizontal scaling
- **[Backup & Restore](backup-restore.md)** - Data protection
