# NornicDB macOS Installer Build Guide

## Quick Start

Build the complete installer package in one command:

```bash
make macos-package
```

This creates: `dist/NornicDB-[version]-[arch].pkg`

## What Gets Installed

The `.pkg` installer includes:

1. ✅ **NornicDB Server Binary** → `/usr/local/bin/nornicdb`
2. ✅ **Menu Bar App** → `/Applications/NornicDB.app`
3. ✅ **LaunchAgent** → `~/Library/LaunchAgents/com.nornicdb.server.plist`
4. ✅ **Configuration** → `~/Library/Application Support/NornicDB/config.yaml`
5. ✅ **Data Directory** → `/usr/local/var/nornicdb/data`
6. ✅ **Logs Directory** → `/usr/local/var/log/nornicdb/`

## Build Steps (Detailed)

### 1. Build Everything

```bash
# Build server binary
make build

# Build menu bar app
make macos-menubar

# Create .pkg installer
make macos-package
```

Or all at once:

```bash
make build macos-menubar macos-package
```

### 2. Find Your Package

```bash
ls -lh dist/*.pkg
```

Output:
```
dist/NornicDB-1.0.0-arm64.pkg    # Apple Silicon
dist/NornicDB-1.0.0-x86_64.pkg   # Intel
```

### 3. Test Installation

```bash
# Install the package
open dist/NornicDB-*.pkg

# Or use installer command
sudo installer -pkg dist/NornicDB-*.pkg -target /
```

## Re-Installation (Testing)

The installer **gracefully handles re-installation**:

### What Happens During Re-Install:

1. **Preinstall** (automatic):
   - Stops running NornicDB service
   - Quits menu bar app
   - Preserves user data and settings

2. **Install** (automatic):
   - Overwrites binary at `/usr/local/bin/nornicdb`
   - Overwrites app at `/Applications/NornicDB.app`

3. **Postinstall** (automatic):
   - Reloads service with new binary
   - Launches updated menu bar app
   - **Preserves existing config.yaml** (doesn't overwrite)
   - **Preserves all database data**

### Safe to Reinstall Anytime

```bash
# Build new version
make macos-package

# Install over existing installation
open dist/NornicDB-*.pkg
```

**Preserved:**
- ✅ Your configuration settings
- ✅ Your database data
- ✅ Your preferences
- ✅ Your logs

**Updated:**
- 🔄 Server binary
- 🔄 Menu bar app
- 🔄 LaunchAgent (if changed)

## Development Workflow

### Rapid Testing Cycle

```bash
# Make changes to code
vim pkg/cypher/executor.go

# Rebuild and test
make build && make macos-package && open dist/NornicDB-*.pkg
```

### Testing Just the Menu Bar App

```bash
# Rebuild menu bar app
make macos-clean && make macos-menubar

# Test without full install
open macos/build/NornicDB.app
```

### Testing the Server Binary

```bash
# Rebuild server
make build

# Test directly (without installer)
./bin/nornicdb serve --data-dir ./test-data
```

## Version Management

### Setting Version Number

```bash
# Build with specific version
VERSION=1.2.3 make macos-package
```

Output: `dist/NornicDB-1.2.3-arm64.pkg`

### Creating Signed Package (for Distribution)

```bash
# Build and sign (requires Apple Developer account)
make macos-package-signed SIGN_IDENTITY="Developer ID Installer: Your Name"
```

Output: `dist/NornicDB-1.0.0-arm64-signed.pkg`

## Troubleshooting Builds

### Build Failed: "NornicDB binary not found"

```bash
# Build the binary first
make build
```

### Build Failed: "Menu bar app not found"

```bash
# Build the menu bar app
make macos-menubar
```

### Build Failed: "swiftc: command not found"

Install Xcode Command Line Tools:

```bash
xcode-select --install
```

### Build Failed: Architecture errors

The build uses your Mac's architecture automatically:
- **Apple Silicon** (M1/M2/M3) → `arm64`
- **Intel** → `x86_64`

### Clean Build

```bash
# Remove all build artifacts
make clean macos-clean

# Rebuild from scratch
make build macos-menubar macos-package
```

## Testing Checklist

Before distributing the `.pkg`:

- [ ] Install on clean Mac (or VM)
- [ ] Verify menu bar app appears
- [ ] Verify icon shows green (server running)
- [ ] Click "Open Web UI" - browser opens to http://localhost:7474
- [ ] Click "Settings" - settings window opens
- [ ] Toggle features - server restarts correctly
- [ ] Restart Mac - app and server auto-start
- [ ] Reinstall same version - gracefully overwrites
- [ ] Install newer version - upgrades smoothly

## Quick Commands Reference

```bash
# Build everything
make build macos-menubar macos-package

# Just build .pkg (if binaries exist)
make macos-package

# Clean and rebuild
make clean macos-clean && make build macos-menubar macos-package

# Test without full install
open macos/build/NornicDB.app

# Uninstall (for testing)
./macos/scripts/uninstall.sh

# Check logs
tail -f /usr/local/var/log/nornicdb/stdout.log
```

## Distribution

### For Users

1. Upload `dist/NornicDB-[version]-[arch].pkg` to GitHub Releases
2. Users download and double-click to install
3. First-run wizard guides them through setup

### For Developers

1. Build: `make macos-package`
2. Test locally: `open dist/*.pkg`
3. Verify installation works
4. Sign (optional): `make macos-package-signed`
5. Distribute

## File Locations

After installation:

```
/usr/local/bin/nornicdb                           # Server binary
/Applications/NornicDB.app                         # Menu bar app
~/Library/LaunchAgents/com.nornicdb.server.plist  # Auto-start config
~/Library/Application Support/NornicDB/            # User config
  ├── config.yaml                                  # Settings
  └── .first_run                                   # First run marker
/usr/local/var/nornicdb/                           # Server files
  └── data/                                        # Database
/usr/local/var/log/nornicdb/                       # Logs
  ├── stdout.log                                   # Server output
  └── stderr.log                                   # Server errors
```

## Need Help?

- **Build issues**: Check `macos/scripts/build-installer.sh`
- **Install issues**: Check `macos/scripts/preinstall` and `postinstall`
- **Menu bar app**: Check `macos/MenuBarApp/NornicDBMenuBar.swift`
- **Documentation**: See `macos/README.md` and `macos/USER_GUIDE.md`
