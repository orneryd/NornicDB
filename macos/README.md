# NornicDB macOS Integration

Native macOS installation with service management and menu bar app.

## Features

- **🚀 System Service**: Runs as a LaunchAgent, starts automatically on login
- **📊 Menu Bar App**: Real-time health monitoring with quick controls
- **⚙️ Easy Management**: Simple install/uninstall scripts
- **🔄 Auto-Recovery**: Automatically restarts if it crashes
- **📝 Centralized Logs**: All logs in `/usr/local/var/log/nornicdb`

## Quick Start

### Installation

```bash
# Build NornicDB for your Mac
make build

# Install as a service with menu bar app
make macos-install

# Or build everything and install in one command
make macos-all
```

### Usage

The menu bar app provides:
- **Status Indicator**: Green (running), Red (stopped), Gray (unknown)
- **Quick Actions**:
  - Open Web UI
  - Start/Stop/Restart server
  - **Settings** - Toggle major features with GUI
  - Open configuration file
  - View logs
- **Keyboard Shortcuts**:
  - `⌘O` - Open Web UI
  - `⌘S` - Start/Stop
  - `⌘R` - Restart
  - `⌘,` - Open Settings
  - `⌘L` - Show Logs
  - `⌘Q` - Quit menu bar app (server keeps running)

#### Feature Toggles

The Settings window (`⌘,`) allows you to easily enable/disable major features:

- **Embeddings** - Vector embeddings for semantic search
- **K-Means Clustering** - Automatic node clustering and organization
- **Auto-TLP** - Automatic Topological Link prediction
- **Heimdall** - AI guardian with cognitive monitoring

Changes are saved to your config file and the server automatically restarts to apply them.

### Uninstallation

```bash
make macos-uninstall
# or
./macos/scripts/uninstall.sh
```

## Architecture

### Components

1. **NornicDB Binary** (`/usr/local/bin/nornicdb`)
   - The main database server
   - Built with Metal/GPU acceleration on Apple Silicon

2. **LaunchAgent** (`~/Library/LaunchAgents/com.nornicdb.server.plist`)
   - Manages the server lifecycle
   - Starts automatically on login
   - Restarts on crashes

3. **Menu Bar App** (`~/Applications/NornicDB.app`)
   - SwiftUI-based menu bar utility
   - Monitors server health every 10 seconds
   - Provides quick access to common actions

4. **Configuration** (`~/Library/Application Support/NornicDB/config.yaml`)
   - User-specific settings
   - Based on `nornicdb.example.yaml`

5. **Data Directory** (`/usr/local/var/nornicdb/data`)
   - Graph database storage
   - Persists across restarts

6. **Logs** (`/usr/local/var/nornicdb/`)
   - `stdout.log` - Standard output
   - `stderr.log` - Error logs

### File Locations

```
/usr/local/
├── bin/
│   └── nornicdb                    # Binary
└── var/
    ├── nornicdb/
    │   └── data/                   # Database
    └── log/
        └── nornicdb/
            ├── stdout.log
            └── stderr.log

~/Library/
├── Application Support/
│   └── NornicDB/
│       └── config.yaml              # Configuration
└── LaunchAgents/
    └── com.nornicdb.server.plist    # Service definition

~/Applications/
└── NornicDB.app                     # Menu bar app
```

## Manual Service Control

### Using launchctl

```bash
# Start service
launchctl start com.nornicdb.server

# Stop service
launchctl stop com.nornicdb.server

# Check status
launchctl list | grep nornicdb

# View service info
launchctl list com.nornicdb.server

# Reload configuration
launchctl unload ~/Library/LaunchAgents/com.nornicdb.server.plist
launchctl load ~/Library/LaunchAgents/com.nornicdb.server.plist
```

### Direct Control

```bash
# Run in foreground (for debugging)
/usr/local/bin/nornicdb --config ~/Library/Application\ Support/NornicDB/config.yaml

# Check health
curl http://localhost:7687/health

# View logs
tail -f /usr/local/var/log/nornicdb/stdout.log
tail -f /usr/local/var/log/nornicdb/stderr.log
```

## Building from Source

### Requirements

- macOS 11.0+ (Big Sur or later)
- Xcode 14.0+ (for menu bar app)
- Go 1.21+ (for NornicDB binary)

### Build Menu Bar App

```bash
# Build the Swift menu bar app
make macos-menubar

# Output: macos/build/NornicDB.app
```

### Build Complete Package

```bash
# Build everything
make macos-all

# This will:
# 1. Build the NornicDB binary
# 2. Build the menu bar app  
# 3. Run the installation script
```

## Troubleshooting

### Server Won't Start

```bash
# Check service status
launchctl list | grep nornicdb

# View error logs
cat /usr/local/var/log/nornicdb/stderr.log

# Try manual start
/usr/local/bin/nornicdb --config ~/Library/Application\ Support/NornicDB/config.yaml
```

### Menu Bar App Not Showing

```bash
# Verify it's running
ps aux | grep NornicDB

# Relaunch it
open ~/Applications/NornicDB.app

# Check for crashes
log show --predicate 'process == "NornicDB"' --last 1h
```

### Port Already in Use

```bash
# Find what's using port 7687
lsof -i :7687

# Kill the process if needed
kill -9 <PID>
```

### Reset Everything

```bash
# Complete uninstall (removes data)
./macos/scripts/uninstall.sh
# Answer 'y' to remove data and config

# Clean reinstall
make build
make macos-install
```

## Development

### Menu Bar App Development

The menu bar app is written in Swift using SwiftUI and AppKit:

```swift
// Source: macos/MenuBarApp/NornicDBMenuBar.swift
// Features:
// - NSStatusItem for menu bar presence
// - URLSession for health checks
// - Process for launchctl integration
// - SF Symbols for icons
```

To modify:

1. Edit `macos/MenuBarApp/NornicDBMenuBar.swift`
2. Rebuild: `make macos-menubar`
3. Test: `open macos/build/NornicDB.app`

### LaunchAgent Configuration

To modify service behavior, edit `macos/LaunchAgents/com.nornicdb.server.plist`:

```xml
<!-- Key settings -->
<key>RunAtLoad</key>
<true/>  <!-- Start on login -->

<key>KeepAlive</key>
<dict>
    <key>Crashed</key>
    <true/>  <!-- Restart on crash -->
</dict>

<key>ThrottleInterval</key>
<integer>30</integer>  <!-- Wait 30s before restart -->
```

After changes:
```bash
launchctl unload ~/Library/LaunchAgents/com.nornicdb.server.plist
launchctl load ~/Library/LaunchAgents/com.nornicdb.server.plist
```

## Distribution

### Creating a .pkg Installer

```bash
# Build and package
make macos-package

# Output: dist/NornicDB-arm64.pkg
```

This creates a signed macOS package that users can double-click to install.

### Homebrew Formula

Coming soon! We plan to publish to Homebrew for even easier installation:

```bash
brew install nornicdb
brew services start nornicdb
```

## Security & Privacy

- **File Access**: NornicDB only accesses its designated directories
- **Network**: Listens on localhost:7687 by default
- **LaunchAgent**: Runs as your user (not system-wide)
- **Menu Bar App**: Requests minimal permissions (only network for health checks)

## Support

- **Issues**: [GitHub Issues](https://github.com/orneryd/nornicdb/issues)
- **Docs**: [Full Documentation](https://github.com/orneryd/nornicdb/docs)
- **Discussions**: [GitHub Issues](https://github.com/orneryd/NornicDB/issues)
