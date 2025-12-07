# NornicDB macOS Installation Guide

Complete guide for installing NornicDB as a native macOS service with menu bar app.

## Quick Install (Recommended)

For most users, this one-liner does everything:

```bash
# Clone repository
git clone https://github.com/orneryd/nornicdb.git
cd nornicdb

# Build and install everything
make macos-all
```

This will:
1. ✅ Build NornicDB binary for your Mac (Apple Silicon or Intel)
2. ✅ Build the menu bar app
3. ✅ Install as a LaunchAgent service
4. ✅ Create configuration in `~/Library/Application Support/NornicDB`
5. ✅ Launch the menu bar app
6. ✅ Start the server

## Step-by-Step Installation

### Prerequisites

- **macOS 11.0+** (Big Sur or later)
- **Xcode Command Line Tools**: `xcode-select --install`
- **Go 1.21+**: `brew install go` (if not already installed)

### 1. Build NornicDB

```bash
# Build the database binary
make build

# Verify it works
./bin/nornicdb --version
```

### 2. Build Menu Bar App (Optional but Recommended)

```bash
# Build the Swift menu bar application
make macos-menubar

# Output: macos/build/NornicDB.app
```

### 3. Install as Service

```bash
# Run installation script
./macos/scripts/install.sh
```

The installer will:
- Copy binary to `/usr/local/bin/nornicdb`
- Create data directory at `/usr/local/var/nornicdb`
- Set up log directory at `/usr/local/var/log/nornicdb`
- Install config to `~/Library/Application Support/NornicDB/config.yaml`
- Install LaunchAgent to `~/Library/LaunchAgents/`
- Install menu bar app to `~/Applications/NornicDB.app`
- Start the service
- Launch the menu bar app

## Post-Installation

### Verify Installation

```bash
# Check if service is running
launchctl list | grep nornicdb

# Expected output:
# PID    Status  Label
# 12345  0       com.nornicdb.server

# Check server health
curl http://localhost:7687/health

# View logs
tail -f /usr/local/var/log/nornicdb/stdout.log
```

### Access NornicDB

Once installed, you can access NornicDB through:

**1. Web UI**
```
http://localhost:7687
```

**2. Bolt Protocol** (Neo4j-compatible drivers)
```
bolt://localhost:7687
```

**3. Menu Bar App**
- Look for the database icon in your menu bar (top right)
- Click for quick actions and status

## Using the Menu Bar App

### Quick Actions

Click the menu bar icon to:

- **Open Web UI** - Launch browser to NornicDB UI
- **Start/Stop/Restart** - Control server
- **Settings** - Configure features (see below)
- **Show Logs** - Open log directory
- **About/Updates** - Version info and updates

### Configuring Features

1. Click menu bar icon
2. Select **Settings...** (or press `⌘,`)
3. Toggle features on/off:
   - **Embeddings** - Vector search capabilities
   - **K-Means** - Automatic clustering
   - **Auto-TLP** - Temporal link prediction
   - **Heimdall** - AI monitoring
4. Click **Save & Restart**
5. Server restarts automatically with new config

## Configuration

### Config File Location

```
~/Library/Application Support/NornicDB/config.yaml
```

### Edit Configuration

**Option 1: Use Settings GUI (Recommended)**
- Click menu bar icon → **Settings...**
- Toggle features with switches
- Click **Save & Restart**

**Option 2: Edit YAML Directly**
- Click menu bar icon → **Open Config File**
- Edit in your preferred text editor
- Save file
- Click menu bar icon → **Restart Server**

### Important Settings

```yaml
# Server
server:
  port: 7687              # Bolt/HTTP port
  host: "0.0.0.0"        # Listen on all interfaces

# Features (toggle via menu bar app)
embedding:
  enabled: true          # Vector embeddings
  
kmeans:
  enabled: true          # Clustering
  
auto_tlp:
  enabled: true          # Temporal predictions
  
heimdall:
  enabled: true          # AI monitoring
```

## Service Management

### Using Menu Bar App (Easiest)

Just click the icon and select Start/Stop/Restart!

### Using launchctl Commands

```bash
# Start service
launchctl start com.nornicdb.server

# Stop service
launchctl stop com.nornicdb.server

# Restart service (reload config)
launchctl kickstart -k gui/$(id -u)/com.nornicdb.server

# Check status
launchctl list com.nornicdb.server

# View detailed info
launchctl print gui/$(id -u)/com.nornicdb.server
```

### Disable Auto-Start

```bash
# Unload service (won't start on login)
launchctl unload ~/Library/LaunchAgents/com.nornicdb.server.plist

# Re-enable
launchctl load ~/Library/LaunchAgents/com.nornicdb.server.plist
```

## Troubleshooting

### Server Won't Start

**1. Check logs first**
```bash
tail -n 50 /usr/local/var/log/nornicdb/stderr.log
```

**2. Common issues:**

**Port already in use:**
```bash
# Find what's using port 7687
lsof -i :7687

# Kill the process
kill -9 <PID>
```

**Permission denied:**
```bash
# Fix ownership
sudo chown -R $(whoami):staff /usr/local/var/nornicdb
sudo chown -R $(whoami):staff /usr/local/var/log/nornicdb
```

**Binary not found:**
```bash
# Verify binary exists
ls -la /usr/local/bin/nornicdb

# Reinstall if missing
make build
sudo cp bin/nornicdb /usr/local/bin/
```

### Menu Bar App Not Showing

**1. Verify it's running:**
```bash
ps aux | grep -i nornicdb | grep -v grep
```

**2. Relaunch:**
```bash
open ~/Applications/NornicDB.app
```

**3. Check for crashes:**
```bash
log show --predicate 'process == "NornicDB"' --last 1h --info
```

### Configuration Issues

**Reset to defaults:**
```bash
# Backup current config
cp ~/Library/Application\ Support/NornicDB/config.yaml ~/config.backup

# Copy default config
cp nornicdb.example.yaml ~/Library/Application\ Support/NornicDB/config.yaml

# Restart server
launchctl kickstart -k gui/$(id -u)/com.nornicdb.server
```

## Upgrading

### Upgrade NornicDB

```bash
# Pull latest code
git pull origin main

# Rebuild and reinstall
make macos-all
```

Your data and configuration will be preserved.

### Upgrade Menu Bar App Only

```bash
# Rebuild app
make macos-menubar

# Copy to Applications
cp -R macos/build/NornicDB.app ~/Applications/

# Relaunch
killall NornicDB
open ~/Applications/NornicDB.app
```

## Uninstallation

### Complete Removal

```bash
# Run uninstall script
./macos/scripts/uninstall.sh
```

You'll be asked:
- Remove data directory? (Default: No - preserves your graphs)
- Remove configuration? (Default: No - preserves settings)

### Manual Removal

If the script doesn't work:

```bash
# Stop service
launchctl stop com.nornicdb.server
launchctl unload ~/Library/LaunchAgents/com.nornicdb.server.plist

# Remove files
sudo rm /usr/local/bin/nornicdb
rm ~/Library/LaunchAgents/com.nornicdb.server.plist
rm -rf ~/Applications/NornicDB.app

# Optional: Remove data and config
sudo rm -rf /usr/local/var/nornicdb
sudo rm -rf /usr/local/var/log/nornicdb
rm -rf ~/Library/Application\ Support/NornicDB
```

## Advanced Topics

### Running Multiple Instances

To run multiple NornicDB instances:

1. Copy the LaunchAgent plist with different label
2. Change port in config file
3. Load new LaunchAgent

```bash
# Copy plist
cp ~/Library/LaunchAgents/com.nornicdb.server.plist \
   ~/Library/LaunchAgents/com.nornicdb.server2.plist

# Edit new plist to change:
# - Label to com.nornicdb.server2
# - Config path to different location
# - Port in that config

# Load new service
launchctl load ~/Library/LaunchAgents/com.nornicdb.server2.plist
```

### Custom Installation Path

If you want to install elsewhere:

1. Edit `macos/scripts/install.sh`
2. Change paths at top of file
3. Run modified installer

### Development Setup

For development, you might want to run without the service:

```bash
# Build
make build

# Run directly (foreground)
./bin/nornicdb --config ./nornicdb.example.yaml

# Or with custom config
./bin/nornicdb --config /path/to/custom/config.yaml
```

## Getting Help

- **Documentation**: See [macos/README.md](README.md)
- **Features**: See [macos/FEATURES.md](FEATURES.md)
- **Issues**: [GitHub Issues](https://github.com/orneryd/nornicdb/issues)
- **Discussions**: [GitHub Discussions](https://github.com/orneryd/nornicdb/discussions)

## Quick Reference

```bash
# Installation
make macos-all                    # Build and install everything

# Service Control
launchctl start com.nornicdb.server
launchctl stop com.nornicdb.server
launchctl kickstart -k gui/$(id -u)/com.nornicdb.server

# Logs
tail -f /usr/local/var/log/nornicdb/stdout.log
tail -f /usr/local/var/log/nornicdb/stderr.log

# Configuration
open ~/Library/Application\ Support/NornicDB/config.yaml

# Uninstallation
./macos/scripts/uninstall.sh
```

## What's Installed?

| Component | Location | Purpose |
|-----------|----------|---------|
| Binary | `/usr/local/bin/nornicdb` | Database server |
| Config | `~/Library/Application Support/NornicDB/config.yaml` | Settings |
| Data | `/usr/local/var/nornicdb/data` | Graph database |
| Logs | `/usr/local/var/log/nornicdb/` | Server logs |
| Service | `~/Library/LaunchAgents/com.nornicdb.server.plist` | Auto-start |
| Menu App | `~/Applications/NornicDB.app` | GUI control |

---

**Enjoy your native macOS NornicDB experience! 🚀**
