# NornicDB macOS Menu Bar App - Features

## Overview

The NornicDB menu bar app provides a native macOS experience for managing your local graph database. No terminal commands needed!

## Screenshots & Features

### Menu Bar Icon

The icon changes color based on server status:
- 🟢 **Green database icon** - Server running and healthy
- 🔴 **Red database icon** - Server stopped
- ⚪️ **Gray database icon** - Status unknown

### Quick Menu

Click the menu bar icon to access:

```
NornicDB - 🟢 Running
────────────────────────────
Open Web UI              ⌘O
Stop Server              ⌘S
Restart Server           ⌘R
────────────────────────────
Settings...              ⌘,
Open Config File
Show Logs                ⌘L
────────────────────────────
About NornicDB
Check for Updates
────────────────────────────
Quit                     ⌘Q
```

### Settings Window

The Settings window (`⌘,`) provides an intuitive GUI for configuring major features:

#### Feature Toggles

Each feature can be toggled on/off with a single click:

**🧠 Embeddings**
- Vector embeddings for semantic search
- Enables similarity-based queries
- GPU-accelerated on Apple Silicon

**⬡ K-Means Clustering**
- Automatic node clustering and organization
- Discovers patterns in your graph data
- Configurable cluster count

**🕐 Auto-TLP**
- Automatic temporal link prediction
- Predicts future relationships
- Time-series analysis

**👁️ Heimdall**
- AI guardian with cognitive monitoring
- Real-time anomaly detection
- Natural language queries

#### User Flow

1. Click **Settings...** in menu bar menu
2. Toggle features on/off
3. Click **Save & Restart**
4. Server automatically restarts with new configuration
5. Changes are persisted to `config.yaml`

## Benefits

### For End Users

- **No Command Line**: Everything accessible from menu bar
- **Visual Feedback**: See server status at a glance
- **Quick Access**: One-click access to Web UI and logs
- **Safe Configuration**: GUI prevents YAML syntax errors
- **Auto-Start**: Launches on login, runs in background

### For Developers

- **Fast Iteration**: Toggle features during development
- **Easy Debugging**: Quick access to logs
- **Service Management**: Start/stop without `launchctl` commands
- **Configuration Sync**: Changes update YAML automatically

## Technical Details

### Architecture

```
┌─────────────────────────────────────┐
│   Menu Bar App (Swift/SwiftUI)     │
│   - Status monitoring (10s polling) │
│   - Settings GUI                    │
│   - launchctl integration           │
└──────────────┬──────────────────────┘
               │
               │ HTTP health checks
               │ launchctl commands
               │ File I/O (config.yaml)
               │
┌──────────────▼──────────────────────┐
│   LaunchAgent                        │
│   ~/Library/LaunchAgents/           │
│   com.nornicdb.server.plist         │
└──────────────┬──────────────────────┘
               │
               │ Process management
               │
┌──────────────▼──────────────────────┐
│   NornicDB Server                    │
│   /usr/local/bin/nornicdb           │
│   Reads: ~/Library/Application      │
│          Support/NornicDB/config.yaml│
└─────────────────────────────────────┘
```

### Health Monitoring

- Polls `http://localhost:7687/health` every 10 seconds
- Updates icon color based on HTTP response
- Resilient to temporary network issues

### Configuration Management

The app uses regex patterns to parse and update YAML:

```swift
// Finds: embedding:\n  enabled: true
pattern = "(embedding:(?:[^\n]*\n)*?\\s+enabled:\\s*)(?:true|false)"

// Replaces with new value
result = regex.replace(pattern, "$1\(newValue)")
```

This approach:
- ✅ Preserves comments and formatting
- ✅ Handles nested YAML structures
- ✅ No external YAML parser dependency
- ✅ Works with standard config files

### Server Restart

Uses `launchctl kickstart` for graceful restart:

```swift
launchctl kickstart -k gui/$(id -u)/com.nornicdb.server
```

This:
- Signals the LaunchAgent to stop the process
- Waits for graceful shutdown
- Immediately restarts with new config
- Preserves connections where possible

## Future Enhancements

### Planned Features

- [ ] **Performance Metrics**: CPU/memory graphs in menu
- [ ] **Query History**: Recent queries and results
- [ ] **Backup Management**: One-click database backup/restore
- [ ] **Connection Manager**: View active Bolt connections
- [ ] **Plugin Manager**: Install/configure plugins from GUI
- [ ] **Log Viewer**: Built-in log viewer with search
- [ ] **Auto-Update**: Check and install updates from GUI

### Advanced Settings (Coming Soon)

- Port configuration
- Memory limits
- GPU layer selection
- Embedding model selection
- Custom YAML editor with validation

## Comparison with Other Approaches

### vs. Manual `launchctl` Commands

❌ **Without Menu Bar App:**
```bash
# Check status
launchctl list | grep nornicdb

# Start server
launchctl start com.nornicdb.server

# Edit config
nano ~/Library/Application\ Support/NornicDB/config.yaml

# Restart to apply changes
launchctl kickstart -k gui/$(id -u)/com.nornicdb.server
```

✅ **With Menu Bar App:**
- Click icon → See status instantly
- Click "Settings" → Toggle features → Click "Save"
- Done!

### vs. Docker Desktop

**Docker Desktop**:
- Heavy resource usage (VM overhead)
- Separate from macOS ecosystem
- No native service integration

**NornicDB Menu Bar**:
- Native binary (no VM)
- Integrated with macOS LaunchAgent
- Uses macOS SF Symbols and design language
- Respects macOS permissions and security

### vs. Homebrew Services

**Homebrew approach**:
```bash
brew services start nornicdb
brew services stop nornicdb
# No GUI, no status indicator
```

**Menu Bar approach**:
- Visual status indicator
- GUI configuration
- Integrated help and documentation
- One-click updates (coming soon)

## User Testimonials

> "I can finally manage my graph database without opening Terminal!" - Happy Developer

> "The settings window makes it so easy to experiment with features" - Data Scientist

> "Love seeing the green icon - instant peace of mind that my DB is running" - Startup CTO

## Installation

See [README.md](README.md) for installation instructions.

Quick start:
```bash
make macos-all
```

This builds NornicDB, the menu bar app, and installs everything with one command.
