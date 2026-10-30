# macOS Deployment Plan

## Overview

NornicDB on macOS supports two distribution methods:
1. **Homebrew** - Developer-focused, command-line installation
2. **.pkg Installer** - Enterprise/end-user, GUI installation

Both methods install NornicDB as a background service (LaunchDaemon) that starts automatically on boot.

## Target Architectures

| Architecture | Hardware | Binary |
|--------------|----------|--------|
| arm64 | Apple Silicon (M1/M2/M3/M4) | `nornicdb-darwin-arm64` |
| amd64 | Intel Macs | `nornicdb-darwin-amd64` |

## Method 1: Homebrew (Recommended)

### User Experience
```bash
# Install
brew tap timothyswt/nornicdb
brew install nornicdb

# Start as background service
brew services start nornicdb

# Check status
brew services list

# Stop
brew services stop nornicdb

# Uninstall
brew uninstall nornicdb
```

### Implementation

#### 1. Create Homebrew Tap Repository
```
github.com/timothyswt/homebrew-nornicdb/
├── Formula/
│   └── nornicdb.rb
└── README.md
```

#### 2. Formula Definition (`nornicdb.rb`)
```ruby
class Nornicdb < Formula
  desc "Lightweight graph database with vector search and AI memory"
  homepage "https://github.com/timothyswt/nornicdb"
  version "1.0.0"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/timothyswt/nornicdb/releases/download/v1.0.0/nornicdb-darwin-arm64.tar.gz"
      sha256 "PLACEHOLDER_SHA256_ARM64"
    end
    on_intel do
      url "https://github.com/timothyswt/nornicdb/releases/download/v1.0.0/nornicdb-darwin-amd64.tar.gz"
      sha256 "PLACEHOLDER_SHA256_AMD64"
    end
  end

  def install
    bin.install "nornicdb"
    
    # Create data directory
    (var/"nornicdb").mkpath
    (var/"log/nornicdb").mkpath
  end

  def post_install
    # Set correct permissions
    (var/"nornicdb").chmod 0755
  end

  service do
    run [opt_bin/"nornicdb", "serve", "--data-dir", var/"nornicdb"]
    keep_alive true
    working_dir var/"nornicdb"
    log_path var/"log/nornicdb/nornicdb.log"
    error_log_path var/"log/nornicdb/nornicdb-error.log"
    environment_variables NORNICDB_LOG_LEVEL: "info"
  end

  test do
    # Start server in background
    fork do
      exec bin/"nornicdb", "serve", "--port", "17688"
    end
    sleep 2
    
    # Test health endpoint
    output = shell_output("curl -s http://localhost:17688/status")
    assert_match "ok", output
  end
end
```

#### 3. Release Automation (GitHub Actions)
```yaml
# .github/workflows/homebrew-release.yml
name: Update Homebrew Formula
on:
  release:
    types: [published]

jobs:
  update-formula:
    runs-on: macos-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Build macOS binaries
        run: |
          GOOS=darwin GOARCH=arm64 go build -o nornicdb-darwin-arm64 ./cmd/nornicdb
          GOOS=darwin GOARCH=amd64 go build -o nornicdb-darwin-amd64 ./cmd/nornicdb
          
      - name: Create tarballs
        run: |
          tar -czf nornicdb-darwin-arm64.tar.gz nornicdb-darwin-arm64
          tar -czf nornicdb-darwin-amd64.tar.gz nornicdb-darwin-amd64
          
      - name: Upload to release
        uses: softprops/action-gh-release@v1
        with:
          files: |
            nornicdb-darwin-arm64.tar.gz
            nornicdb-darwin-amd64.tar.gz
            
      - name: Update Homebrew formula
        run: |
          # Calculate SHA256
          SHA_ARM64=$(shasum -a 256 nornicdb-darwin-arm64.tar.gz | cut -d' ' -f1)
          SHA_AMD64=$(shasum -a 256 nornicdb-darwin-amd64.tar.gz | cut -d' ' -f1)
          
          # Update formula in tap repo
          # ... (use GitHub API to update formula)
```

---

## Method 2: .pkg Installer

### User Experience
1. Download `NornicDB-1.0.0.pkg` from releases
2. Double-click to install
3. Follow installation wizard
4. Service starts automatically

### Implementation

#### 1. Directory Structure
```
packaging/macos/
├── scripts/
│   ├── preinstall           # Stop existing service
│   ├── postinstall          # Install service, create dirs
│   └── preremove            # Cleanup on uninstall
├── resources/
│   ├── welcome.html         # Installer welcome screen
│   ├── license.html         # License agreement
│   ├── conclusion.html      # Post-install message
│   └── background.png       # Installer background
├── distribution.xml         # Installer configuration
├── com.nornicdb.server.plist # LaunchDaemon definition
└── build-pkg.sh             # Build script
```

#### 2. LaunchDaemon (`com.nornicdb.server.plist`)
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" 
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.nornicdb.server</string>
    
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/nornicdb</string>
        <string>serve</string>
        <string>--data-dir</string>
        <string>/var/lib/nornicdb</string>
    </array>
    
    <key>RunAtLoad</key>
    <true/>
    
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    
    <key>WorkingDirectory</key>
    <string>/var/lib/nornicdb</string>
    
    <key>StandardOutPath</key>
    <string>/var/log/nornicdb/nornicdb.log</string>
    
    <key>StandardErrorPath</key>
    <string>/var/log/nornicdb/nornicdb-error.log</string>
    
    <key>EnvironmentVariables</key>
    <dict>
        <key>NORNICDB_LOG_LEVEL</key>
        <string>info</string>
    </dict>
    
    <!-- Resource limits -->
    <key>SoftResourceLimits</key>
    <dict>
        <key>NumberOfFiles</key>
        <integer>65536</integer>
    </dict>
    
    <key>HardResourceLimits</key>
    <dict>
        <key>NumberOfFiles</key>
        <integer>65536</integer>
    </dict>
</dict>
</plist>
```

#### 3. Post-Install Script (`scripts/postinstall`)
```bash
#!/bin/bash
set -e

# Create directories
mkdir -p /var/lib/nornicdb
mkdir -p /var/log/nornicdb

# Set permissions
chown -R root:wheel /var/lib/nornicdb
chown -R root:wheel /var/log/nornicdb
chmod 755 /var/lib/nornicdb
chmod 755 /var/log/nornicdb

# Install LaunchDaemon
cp /usr/local/share/nornicdb/com.nornicdb.server.plist \
   /Library/LaunchDaemons/

# Load and start service
launchctl load /Library/LaunchDaemons/com.nornicdb.server.plist

echo "NornicDB installed and started successfully!"
echo "Access at: http://localhost:7474"

exit 0
```

#### 4. Pre-Install Script (`scripts/preinstall`)
```bash
#!/bin/bash

# Stop existing service if running
if launchctl list | grep -q com.nornicdb.server; then
    launchctl unload /Library/LaunchDaemons/com.nornicdb.server.plist 2>/dev/null || true
fi

exit 0
```

#### 5. Build Script (`build-pkg.sh`)
```bash
#!/bin/bash
set -e

VERSION=${1:-"1.0.0"}
ARCH=$(uname -m)

echo "Building NornicDB.pkg for macOS ($ARCH) v$VERSION"

# Build binary
if [ "$ARCH" = "arm64" ]; then
    go build -o build/nornicdb ./cmd/nornicdb
else
    GOARCH=amd64 go build -o build/nornicdb ./cmd/nornicdb
fi

# Create package structure
mkdir -p build/pkg/usr/local/bin
mkdir -p build/pkg/usr/local/share/nornicdb
cp build/nornicdb build/pkg/usr/local/bin/
cp packaging/macos/com.nornicdb.server.plist build/pkg/usr/local/share/nornicdb/

# Build component package
pkgbuild \
    --root build/pkg \
    --identifier com.nornicdb.pkg \
    --version $VERSION \
    --scripts packaging/macos/scripts \
    build/NornicDB-component.pkg

# Build product archive (with GUI)
productbuild \
    --distribution packaging/macos/distribution.xml \
    --resources packaging/macos/resources \
    --package-path build \
    dist/NornicDB-$VERSION.pkg

# Sign (requires Developer ID)
# productsign --sign "Developer ID Installer: Your Name" \
#     dist/NornicDB-$VERSION.pkg \
#     dist/NornicDB-$VERSION-signed.pkg

echo "✓ Built: dist/NornicDB-$VERSION.pkg"
```

---

## Service Management

After installation, users can manage the service:

```bash
# Using launchctl (native)
sudo launchctl load /Library/LaunchDaemons/com.nornicdb.server.plist
sudo launchctl unload /Library/LaunchDaemons/com.nornicdb.server.plist
sudo launchctl list | grep nornicdb

# Using brew services (if installed via Homebrew)
brew services start nornicdb
brew services stop nornicdb
brew services restart nornicdb
```

## Data Locations

| Type | Path |
|------|------|
| Binary | `/usr/local/bin/nornicdb` |
| Data | `/var/lib/nornicdb/` |
| Logs | `/var/log/nornicdb/` |
| Config | `/etc/nornicdb/config.yaml` (optional) |
| LaunchDaemon | `/Library/LaunchDaemons/com.nornicdb.server.plist` |

## Code Signing & Notarization

For distribution outside the App Store, binaries must be:
1. **Signed** with a Developer ID certificate
2. **Notarized** by Apple

```bash
# Sign binary
codesign --sign "Developer ID Application: Your Name" \
    --options runtime \
    --timestamp \
    build/nornicdb

# Sign package
productsign --sign "Developer ID Installer: Your Name" \
    dist/NornicDB.pkg \
    dist/NornicDB-signed.pkg

# Notarize
xcrun notarytool submit dist/NornicDB-signed.pkg \
    --apple-id "your@email.com" \
    --team-id "XXXXXXXXXX" \
    --password "@keychain:AC_PASSWORD" \
    --wait

# Staple
xcrun stapler staple dist/NornicDB-signed.pkg
```

## Implementation Checklist

- [ ] Create Homebrew tap repository
- [ ] Write Homebrew formula
- [ ] Create .pkg installer scripts
- [ ] Design installer UI resources
- [ ] Set up code signing
- [ ] Set up notarization
- [ ] Add to GitHub Actions release workflow
- [ ] Test on Intel Mac
- [ ] Test on Apple Silicon Mac
- [ ] Document uninstall procedure
