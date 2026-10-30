# Raspberry Pi Deployment Plan

## Overview

NornicDB is optimized for Raspberry Pi deployments, making it ideal for:
- **Local AI assistants** with persistent memory
- **Home automation** knowledge graphs
- **Edge computing** with Ollama + NornicDB
- **Privacy-focused** personal data storage
- **IoT gateways** with device relationship tracking

## Supported Hardware

| Model | Architecture | Binary | RAM | Notes |
|-------|--------------|--------|-----|-------|
| Pi 5 | arm64 | `nornicdb-rpi64` | 4-8GB | Best performance |
| Pi 4 | arm64 | `nornicdb-rpi64` | 2-8GB | Recommended |
| Pi 3B+ (64-bit OS) | arm64 | `nornicdb-rpi64` | 1GB | Limited |
| Pi 3B+ (32-bit OS) | armv7 | `nornicdb-rpi32` | 1GB | Legacy |
| Pi Zero 2 W | arm64 | `nornicdb-rpi64` | 512MB | Minimal workloads |
| Pi Zero/Zero W | armv6 | `nornicdb-rpi-zero` | 512MB | Very limited |

## Quick Install

### One-Line Install (Pi 4/5 with 64-bit OS)
```bash
curl -sSL https://get.nornicdb.io/pi | bash
```

### Manual Install
```bash
# Download binary
curl -Lo /usr/local/bin/nornicdb \
  https://github.com/timothyswt/nornicdb/releases/latest/download/nornicdb-rpi64
chmod +x /usr/local/bin/nornicdb

# Install as service
sudo nornicdb install

# Start
sudo systemctl start nornicdb
sudo systemctl enable nornicdb

# Verify
curl http://localhost:7474/status
```

## Installation Script (`get.nornicdb.io/pi`)

```bash
#!/bin/bash
set -e

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║           NornicDB Raspberry Pi Installer                    ║"
echo "╚══════════════════════════════════════════════════════════════╝"

# Detect architecture
ARCH=$(uname -m)
case $ARCH in
    aarch64)
        BINARY="nornicdb-rpi64"
        ;;
    armv7l)
        BINARY="nornicdb-rpi32"
        ;;
    armv6l)
        BINARY="nornicdb-rpi-zero"
        ;;
    *)
        echo "Unsupported architecture: $ARCH"
        exit 1
        ;;
esac

echo "Detected: $ARCH → $BINARY"

# Check available memory
MEM_MB=$(free -m | awk '/^Mem:/{print $2}')
if [ "$MEM_MB" -lt 512 ]; then
    echo "⚠️  Warning: Less than 512MB RAM detected"
    echo "   NornicDB may have limited functionality"
fi

# Download
VERSION=${NORNICDB_VERSION:-"latest"}
URL="https://github.com/timothyswt/nornicdb/releases/${VERSION}/download/${BINARY}"

echo "Downloading NornicDB..."
curl -Lo /tmp/nornicdb "$URL"
chmod +x /tmp/nornicdb

# Install
echo "Installing to /usr/local/bin/nornicdb..."
sudo mv /tmp/nornicdb /usr/local/bin/nornicdb

# Create user and directories
echo "Setting up service user and directories..."
sudo useradd -r -s /bin/false nornicdb 2>/dev/null || true
sudo mkdir -p /var/lib/nornicdb
sudo mkdir -p /var/log/nornicdb
sudo chown -R nornicdb:nornicdb /var/lib/nornicdb
sudo chown -R nornicdb:nornicdb /var/log/nornicdb

# Install systemd service
echo "Installing systemd service..."
sudo tee /etc/systemd/system/nornicdb.service > /dev/null << 'EOF'
[Unit]
Description=NornicDB Graph Database
After=network.target

[Service]
Type=simple
User=nornicdb
Group=nornicdb
ExecStart=/usr/local/bin/nornicdb serve --data-dir /var/lib/nornicdb
Restart=on-failure
RestartSec=5

# Pi-optimized memory limits
MemoryMax=75%
MemoryHigh=60%

# File limits
LimitNOFILE=65536

Environment=NORNICDB_LOG_LEVEL=info
Environment=GOMAXPROCS=4

[Install]
WantedBy=multi-user.target
EOF

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable nornicdb
sudo systemctl start nornicdb

# Wait for startup
sleep 3

# Verify
if curl -s http://localhost:7474/status | grep -q "ok"; then
    echo ""
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║  ✓ NornicDB installed successfully!                         ║"
    echo "╠══════════════════════════════════════════════════════════════╣"
    echo "║  Web UI:    http://$(hostname -I | awk '{print $1}'):7474    ║"
    echo "║  Bolt:      bolt://$(hostname -I | awk '{print $1}'):7687    ║"
    echo "║                                                              ║"
    echo "║  Commands:                                                   ║"
    echo "║    sudo systemctl status nornicdb                            ║"
    echo "║    sudo journalctl -u nornicdb -f                            ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
else
    echo "⚠️  NornicDB installed but may not be running"
    echo "   Check: sudo systemctl status nornicdb"
fi
```

## Performance Optimization

### Memory Configuration

For Raspberry Pi with limited RAM, tune the configuration:

```yaml
# /etc/nornicdb/config.yaml
storage:
  # Use smaller cache for low-memory systems
  cache_size_mb: 128  # Default: 512
  
  # Flush more frequently to reduce memory pressure
  flush_interval: 30s  # Default: 60s
  
embeddings:
  # Reduce embedding batch size
  batch_size: 5  # Default: 32
  
  # Use smaller embedding model if running locally
  # model: nomic-embed-text-v1.5  # 137MB
  model: all-minilm-l6-v2  # 23MB (if using local embedder)
```

### Swap Configuration

For Pi with 1GB RAM or less, ensure swap is configured:

```bash
# Check current swap
free -h

# Increase swap if needed (Pi OS)
sudo dphys-swapfile swapoff
sudo sed -i 's/CONF_SWAPSIZE=.*/CONF_SWAPSIZE=2048/' /etc/dphys-swapfile
sudo dphys-swapfile setup
sudo dphys-swapfile swapon
```

### CPU Governor

Set to performance mode for consistent throughput:

```bash
# Temporary
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# Permanent (add to /etc/rc.local)
echo 'echo performance | tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor' \
  | sudo tee -a /etc/rc.local
```

## Running with Ollama (Local AI Stack)

The killer use case: **Local AI with persistent memory**

### Install Ollama
```bash
curl -fsSL https://ollama.ai/install.sh | sh

# Pull a small model suitable for Pi
ollama pull tinyllama  # 637MB, good for Pi 4
# or
ollama pull phi       # 1.6GB, better but needs 4GB+ RAM
```

### Configure NornicDB for Ollama
```yaml
# /etc/nornicdb/config.yaml
embeddings:
  provider: ollama
  endpoint: http://localhost:11434
  model: nomic-embed-text
  
mcp:
  enabled: true
```

### Example: AI Assistant with Memory

```python
import ollama
import requests

NORNICDB = "http://localhost:7474"

def remember(content, tags=None):
    """Store a memory in NornicDB"""
    requests.post(f"{NORNICDB}/nornicdb/store", json={
        "content": content,
        "type": "memory",
        "tags": tags or []
    })

def recall(query, limit=5):
    """Search memories semantically"""
    resp = requests.post(f"{NORNICDB}/nornicdb/search", json={
        "query": query,
        "limit": limit
    })
    return resp.json()

def chat_with_memory(user_input):
    # Search for relevant memories
    memories = recall(user_input, limit=3)
    
    # Build context from memories
    context = "\n".join([m["content"] for m in memories])
    
    # Generate response with Ollama
    response = ollama.chat(model='tinyllama', messages=[
        {"role": "system", "content": f"Relevant context:\n{context}"},
        {"role": "user", "content": user_input}
    ])
    
    # Store the interaction
    remember(f"User asked: {user_input}\nAssistant said: {response['message']['content']}")
    
    return response['message']['content']
```

## Headless Operation

For Pi Zero or server-only deployments, use headless mode:

```bash
# Build headless binary (smaller, no UI dependencies)
make cross-rpi-headless

# Or download headless release
curl -Lo /usr/local/bin/nornicdb \
  https://github.com/timothyswt/nornicdb/releases/latest/download/nornicdb-rpi64-headless
```

## Monitoring

### Check Resource Usage
```bash
# Memory
free -h

# CPU
top -p $(pgrep nornicdb)

# Disk
df -h /var/lib/nornicdb

# Service status
systemctl status nornicdb
```

### Prometheus Metrics (Optional)
```bash
# Enable metrics endpoint
sudo tee -a /etc/nornicdb/config.yaml << EOF
metrics:
  enabled: true
  port: 9090
EOF

sudo systemctl restart nornicdb

# Scrape metrics
curl http://localhost:9090/metrics
```

## Backup & Restore

### Backup
```bash
# Stop service
sudo systemctl stop nornicdb

# Backup data directory
sudo tar -czf nornicdb-backup-$(date +%Y%m%d).tar.gz /var/lib/nornicdb

# Restart
sudo systemctl start nornicdb
```

### Restore
```bash
sudo systemctl stop nornicdb
sudo rm -rf /var/lib/nornicdb/*
sudo tar -xzf nornicdb-backup-YYYYMMDD.tar.gz -C /
sudo chown -R nornicdb:nornicdb /var/lib/nornicdb
sudo systemctl start nornicdb
```

## Troubleshooting

### Service Won't Start
```bash
# Check logs
sudo journalctl -u nornicdb -n 50

# Check permissions
ls -la /var/lib/nornicdb

# Run manually to see errors
sudo -u nornicdb /usr/local/bin/nornicdb serve --data-dir /var/lib/nornicdb
```

### Out of Memory
```bash
# Check memory usage
free -h

# Reduce cache size
echo 'cache_size_mb: 64' | sudo tee -a /etc/nornicdb/config.yaml
sudo systemctl restart nornicdb
```

### Slow Performance
```bash
# Check if throttling
vcgencmd get_throttled

# Check temperature
vcgencmd measure_temp

# Add heatsink/fan if over 80°C
```

## Implementation Checklist

- [ ] Create optimized arm64 build
- [ ] Create armv7 build  
- [ ] Create armv6 build (Pi Zero)
- [ ] Create headless variants
- [ ] Write one-line installer script
- [ ] Test on Pi 5 (4GB, 8GB)
- [ ] Test on Pi 4 (2GB, 4GB, 8GB)
- [ ] Test on Pi 3B+
- [ ] Test on Pi Zero 2 W
- [ ] Test on Pi Zero W (armv6)
- [ ] Create Pi-specific systemd unit with memory limits
- [ ] Document Ollama integration
- [ ] Create Pi disk image with NornicDB pre-installed (stretch goal)
- [ ] Add to GitHub Actions for ARM builds
