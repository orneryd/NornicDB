# NornicDB Packaging & Distribution

This directory contains deployment plans for packaging NornicDB as an installable service across all supported platforms.

## Supported Platforms

| Platform | Architecture | Package Type | Status |
|----------|-------------|--------------|--------|
| [macOS](./macos.md) | arm64 (Apple Silicon) | Homebrew, .pkg | 📋 Planned |
| [macOS](./macos.md) | amd64 (Intel) | Homebrew, .pkg | 📋 Planned |
| [Windows](./windows.md) | amd64 | MSI + Service | 📋 Planned |
| [Linux](./linux.md) | amd64 | deb, rpm, systemd | 📋 Planned |
| [Linux](./linux.md) | arm64 | deb, rpm, systemd | 📋 Planned |
| [Raspberry Pi](./raspberry-pi.md) | arm64/arm | deb, systemd | 📋 Planned |
| [Docker](./docker.md) | amd64, arm64 | Container | ✅ Available |

## Quick Cross-Compilation

Build binaries for all platforms from macOS:

```bash
cd nornicdb
make cross-all
```

Output:
```
bin/nornicdb-linux-amd64    # Linux x86_64
bin/nornicdb-linux-arm64    # Linux ARM64
bin/nornicdb-rpi64          # Raspberry Pi 4/5
bin/nornicdb-rpi32          # Raspberry Pi 2/3
bin/nornicdb-rpi-zero       # Raspberry Pi Zero
bin/nornicdb.exe            # Windows
```

## Distribution Strategy

### Developer-Focused
- **Homebrew** (macOS) - `brew install nornicdb`
- **Chocolatey** (Windows) - `choco install nornicdb`
- **Docker Hub** - `docker pull timothyswt/nornicdb`

### Enterprise/End-User
- **macOS .pkg** - Double-click installer with LaunchDaemon
- **Windows MSI** - Standard installer with Windows Service
- **Linux .deb/.rpm** - Native package managers

### Edge/IoT
- **Raspberry Pi** - Optimized ARM builds with systemd
- **NVIDIA Jetson** - ARM64 builds (same as linux-arm64)

## Directory Structure

```
packaging/
├── README.md                 # This file
├── macos.md                  # macOS deployment plan
├── windows.md                # Windows deployment plan
├── linux.md                  # Linux deployment plan
├── raspberry-pi.md           # Raspberry Pi deployment plan
└── docker.md                 # Docker deployment plan
```

## Implementation Priority

1. **Docker** ✅ - Already available
2. **Homebrew** - Highest impact for developer adoption
3. **Windows MSI** - Required for Windows market
4. **Linux systemd** - Server deployments
5. **Raspberry Pi** - Edge/IoT market
6. **macOS .pkg** - Enterprise macOS users
