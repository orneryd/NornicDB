#!/bin/bash

# NornicDB Installer Builder
# Creates a distributable .pkg that users can double-click to install
#
# Usage:
#   ./build-installer.sh              # Build lite package (no plugins)
#   ./build-installer.sh --full       # Build full package (with plugins)
#   ./build-installer.sh --both       # Build both packages

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="$PROJECT_ROOT/dist/installer"
VERSION=${VERSION:-"1.0.0"}

# Parse arguments
BUILD_FULL=false
BUILD_LITE=true
BUILD_BOTH=false

for arg in "$@"; do
    case $arg in
        --full)
            BUILD_FULL=true
            BUILD_LITE=false
            ;;
        --lite)
            BUILD_LITE=true
            BUILD_FULL=false
            ;;
        --both)
            BUILD_BOTH=true
            BUILD_LITE=true
            BUILD_FULL=true
            ;;
    esac
done

echo "🔨 Building NornicDB Installer v$VERSION"
echo ""

# Check we're on macOS
if [[ "$OSTYPE" != "darwin"* ]]; then
    echo "❌ Error: This script must run on macOS"
    exit 1
fi

# Check for required binaries
if [ ! -f "$PROJECT_ROOT/bin/nornicdb" ]; then
    echo "❌ Error: NornicDB binary not found"
    echo "   Run: make build"
    exit 1
fi

if [ ! -d "$PROJECT_ROOT/macos/build/NornicDB.app" ]; then
    echo "❌ Error: Menu bar app not found"
    echo "   Run: make macos-menubar"
    exit 1
fi

# Function to build a package variant
build_package() {
    local VARIANT=$1  # "lite" or "full"
    local INCLUDE_PLUGINS=$2  # true or false
    
    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "  Building $VARIANT package (plugins: $INCLUDE_PLUGINS)"
    echo "════════════════════════════════════════════════════════════════"
    
    # Clean and create build directory
    echo "📁 Preparing build directory..."
    rm -rf "$BUILD_DIR"
    mkdir -p "$BUILD_DIR"/{payload,scripts,resources,root/usr/local/bin,root/Applications}
    
    # Copy files to package root
    echo "📦 Copying files..."
    cp "$PROJECT_ROOT/bin/nornicdb" "$BUILD_DIR/root/usr/local/bin/"
    chmod +x "$BUILD_DIR/root/usr/local/bin/nornicdb"
    
    cp -R "$PROJECT_ROOT/macos/build/NornicDB.app" "$BUILD_DIR/root/Applications/"
    
    # Copy plugins if this is the full build
    if [ "$INCLUDE_PLUGINS" = "true" ]; then
        echo "📦 Including plugins..."
        
        # Create plugin directories
        mkdir -p "$BUILD_DIR/root/usr/local/share/nornicdb/plugins/apoc"
        mkdir -p "$BUILD_DIR/root/usr/local/share/nornicdb/plugins/heimdall"
        
        # Copy APOC plugins if they exist
        if [ -d "$PROJECT_ROOT/apoc/built-plugins" ] && ls "$PROJECT_ROOT/apoc/built-plugins"/*.so 1> /dev/null 2>&1; then
            cp "$PROJECT_ROOT/apoc/built-plugins"/*.so "$BUILD_DIR/root/usr/local/share/nornicdb/plugins/apoc/"
            echo "  ✓ APOC plugins copied"
        else
            echo "  ⚠ No APOC plugins found (run 'make plugins' first)"
        fi
        
        # Copy Heimdall plugins if they exist
        if [ -d "$PROJECT_ROOT/plugins/heimdall/built-plugins" ] && ls "$PROJECT_ROOT/plugins/heimdall/built-plugins"/*.so 1> /dev/null 2>&1; then
            cp "$PROJECT_ROOT/plugins/heimdall/built-plugins"/*.so "$BUILD_DIR/root/usr/local/share/nornicdb/plugins/heimdall/"
            echo "  ✓ Heimdall plugins copied"
        else
            echo "  ⚠ No Heimdall plugins found (run 'make plugins' first)"
        fi
    fi
    
    # Copy resources - use macOS-specific default config
    if [ -f "$PROJECT_ROOT/macos/default-config.yaml" ]; then
        cp "$PROJECT_ROOT/macos/default-config.yaml" "$BUILD_DIR/resources/default-config.yaml"
        echo "  ✓ Using macos/default-config.yaml"
    elif [ -f "$PROJECT_ROOT/nornicdb.example.yaml" ]; then
        cp "$PROJECT_ROOT/nornicdb.example.yaml" "$BUILD_DIR/resources/default-config.yaml"
        echo "  ✓ Using nornicdb.example.yaml as fallback"
    fi
    
    # Copy scripts
    cp "$PROJECT_ROOT/macos/scripts/preinstall" "$BUILD_DIR/scripts/"
    
    # Use different postinstall for full vs lite
    if [ "$INCLUDE_PLUGINS" = "true" ]; then
        # Create postinstall that sets up plugin paths
        cat > "$BUILD_DIR/scripts/postinstall" << 'POSTINSTALL_FULL'
#!/bin/bash

# NornicDB Post-Installation Script (Full Edition with Plugins)

set -e

# Get the actual user (not root, since installer runs as root)
ACTUAL_USER="${USER}"
if [ "$ACTUAL_USER" = "root" ]; then
    ACTUAL_USER=$(ls -l /dev/console | awk '{print $3}')
fi

USER_HOME=$(eval echo ~$ACTUAL_USER)
LOG_FILE="/tmp/nornicdb-install.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
    echo "$1"
}

log "Starting NornicDB post-installation (Full Edition) for user: $ACTUAL_USER..."

# Create config directory - use ~/.nornicdb to match server and menu bar app
CONFIG_DIR="$USER_HOME/.nornicdb"
sudo -u $ACTUAL_USER mkdir -p "$CONFIG_DIR"
sudo -u $ACTUAL_USER mkdir -p "$USER_HOME/Library/LaunchAgents"

# Create data, models, and log directories
mkdir -p /usr/local/var/nornicdb/data
mkdir -p /usr/local/var/nornicdb/models
mkdir -p /usr/local/var/log/nornicdb

# Set ownership
chown -R $ACTUAL_USER:staff /usr/local/var/nornicdb
chown -R $ACTUAL_USER:staff /usr/local/var/log/nornicdb

log "Created directories"

# Copy default config if none exists
if [ ! -f "$CONFIG_DIR/config.yaml" ]; then
    sudo -u $ACTUAL_USER cat > "$CONFIG_DIR/config.yaml" << 'CONFIGEOF'
# NornicDB Configuration (Full Edition)
# Edit via Settings app (⌘,) or manually

server:
  port: 7687
  host: "localhost"

storage:
  path: "/usr/local/var/nornicdb/data"

embedding:
  enabled: true
  provider: "local"

kmeans:
  enabled: true

heimdall:
  enabled: true
CONFIGEOF
    log "Created default configuration"
fi

# Create environment file with plugin paths
sudo -u $ACTUAL_USER cat > "$CONFIG_DIR/environment" << 'EOF'
# NornicDB Environment Configuration (Full Edition)
# This file is sourced by the NornicDB service

# Plugin directories (Full Edition includes pre-built plugins)
export NORNICDB_PLUGINS_DIR=/usr/local/share/nornicdb/plugins/apoc
export NORNICDB_HEIMDALL_PLUGINS_DIR=/usr/local/share/nornicdb/plugins/heimdall
export NORNICDB_HEIMDALL_ENABLED=true

# Embedding configuration
export NORNICDB_EMBEDDING_PROVIDER=local
export NORNICDB_KMEANS_CLUSTERING_ENABLED=true
EOF

log "Created environment file with plugin paths"

# Create/update launchd service with plugin environment
PLIST_PATH="$USER_HOME/Library/LaunchAgents/com.nornicdb.server.plist"

sudo -u $ACTUAL_USER cat > "$PLIST_PATH" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.nornicdb.server</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/nornicdb</string>
        <string>serve</string>
        <string>--data-dir</string>
        <string>/usr/local/var/nornicdb/data</string>
        <string>--bolt-port</string>
        <string>7687</string>
        <string>--http-port</string>
        <string>7474</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
        <key>HOME</key>
        <string>$USER_HOME</string>
        <key>NORNICDB_PLUGINS_DIR</key>
        <string>/usr/local/share/nornicdb/plugins/apoc</string>
        <key>NORNICDB_HEIMDALL_PLUGINS_DIR</key>
        <string>/usr/local/share/nornicdb/plugins/heimdall</string>
        <key>NORNICDB_HEIMDALL_ENABLED</key>
        <string>true</string>
        <key>NORNICDB_EMBEDDING_PROVIDER</key>
        <string>local</string>
        <key>NORNICDB_KMEANS_CLUSTERING_ENABLED</key>
        <string>true</string>
    </dict>
    <key>WorkingDirectory</key>
    <string>/usr/local/var/nornicdb</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
        <key>Crashed</key>
        <true/>
    </dict>
    <key>ThrottleInterval</key>
    <integer>30</integer>
    <key>StandardOutPath</key>
    <string>/usr/local/var/log/nornicdb/stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/usr/local/var/log/nornicdb/stderr.log</string>
    <key>ProcessType</key>
    <string>Interactive</string>
</dict>
</plist>
EOF

log "Created launchd service (Full Edition with plugins)"

# Load the service
sudo -u $ACTUAL_USER launchctl load "$PLIST_PATH" 2>/dev/null || true
log "Service loaded"

# Install menu bar app LaunchAgent for auto-start
sudo -u $ACTUAL_USER cat > "$USER_HOME/Library/LaunchAgents/com.nornicdb.menubar.plist" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.nornicdb.menubar</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/open</string>
        <string>-a</string>
        <string>/Applications/NornicDB.app</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <false/>
    <key>ProcessType</key>
    <string>Interactive</string>
</dict>
</plist>
EOF

sudo -u $ACTUAL_USER launchctl load "$USER_HOME/Library/LaunchAgents/com.nornicdb.menubar.plist" 2>/dev/null || true

# Create welcome file to trigger first-run wizard
sudo -u $ACTUAL_USER touch "$CONFIG_DIR/.first_run"
log "Created first-run marker"

# Launch menu bar app immediately
sudo -u $ACTUAL_USER open -a "/Applications/NornicDB.app" 2>/dev/null || true
log "Launched menu bar app"

echo ""
echo "✅ NornicDB installation complete! (Full Edition with APOC + Heimdall plugins)"
echo ""
echo "The menu bar app should now be running."
echo "Look for the database icon in your menu bar (top right)."
echo ""
echo "First-time setup wizard will guide you through configuration."
echo ""

exit 0
POSTINSTALL_FULL
    else
        # Use standard postinstall for lite version
        cp "$PROJECT_ROOT/macos/scripts/postinstall" "$BUILD_DIR/scripts/"
    fi
    
    chmod +x "$BUILD_DIR/scripts"/*
    
    # Create README for the package
    if [ "$INCLUDE_PLUGINS" = "true" ]; then
        cat > "$BUILD_DIR/resources/README.txt" << 'EOF'
NornicDB for macOS (Full Edition)

This installer includes:
• NornicDB Graph Database Server
• Menu Bar Application
• APOC Plugins (Extended Cypher Functions)
• Heimdall Plugins (AI/LLM Integration)

This installer will:
1. Install NornicDB binary to /usr/local/bin
2. Install menu bar app to Applications
3. Install plugins to /usr/local/share/nornicdb/plugins
4. Create configuration directory with plugin paths
5. Set up auto-start service with Heimdall enabled
6. Launch the application

After installation:
- Look for the database icon in your menu bar
- Click the icon for quick actions
- Press ⌘, for Settings
- Plugins are automatically configured

For help: https://github.com/orneryd/nornicdb

Thank you for using NornicDB!
EOF
    else
        cat > "$BUILD_DIR/resources/README.txt" << 'EOF'
NornicDB for macOS (Lite Edition)

This installer includes:
• NornicDB Graph Database Server
• Menu Bar Application

This installer will:
1. Install NornicDB binary to /usr/local/bin
2. Install menu bar app to Applications
3. Create configuration directory
4. Set up auto-start service
5. Launch the application

After installation:
- Look for the database icon in your menu bar
- Click the icon for quick actions
- Press ⌘, for Settings

For plugins (APOC, Heimdall), download the Full Edition.

For help: https://github.com/orneryd/nornicdb

Thank you for using NornicDB!
EOF
    fi
    
    # Get architecture
    ARCH=$(uname -m)
    
    if [ "$INCLUDE_PLUGINS" = "true" ]; then
        PKG_NAME="NornicDB-${VERSION}-${ARCH}-full.pkg"
        PKG_ID="com.nornicdb.pkg.full"
        PKG_TITLE="NornicDB (Full Edition)"
    else
        PKG_NAME="NornicDB-${VERSION}-${ARCH}-lite.pkg"
        PKG_ID="com.nornicdb.pkg.lite"
        PKG_TITLE="NornicDB (Lite Edition)"
    fi
    
    echo "📝 Building package: $PKG_NAME"
    
    # Build component package
    pkgbuild \
        --root "$BUILD_DIR/root" \
        --scripts "$BUILD_DIR/scripts" \
        --identifier "$PKG_ID" \
        --version "$VERSION" \
        --install-location "/" \
        "$BUILD_DIR/component.pkg"
    
    # Create distribution XML
    cat > "$BUILD_DIR/distribution.xml" << EOF
<?xml version="1.0" encoding="utf-8"?>
<installer-gui-script minSpecVersion="1">
    <title>$PKG_TITLE</title>
    <welcome file="README.txt"/>
    <pkg-ref id="$PKG_ID"/>
    <options customize="never" require-scripts="false" hostArchitectures="$ARCH"/>
    <volume-check>
        <allowed-os-versions>
            <os-version min="12.0"/>
        </allowed-os-versions>
    </volume-check>
    <choices-outline>
        <line choice="default">
            <line choice="$PKG_ID"/>
        </line>
    </choices-outline>
    <choice id="default"/>
    <choice id="$PKG_ID" visible="false">
        <pkg-ref id="$PKG_ID"/>
    </choice>
    <pkg-ref id="$PKG_ID" version="$VERSION" onConclusion="none">component.pkg</pkg-ref>
</installer-gui-script>
EOF
    
    # Build product (distribution) package
    productbuild \
        --distribution "$BUILD_DIR/distribution.xml" \
        --resources "$BUILD_DIR/resources" \
        --package-path "$BUILD_DIR" \
        "$PROJECT_ROOT/dist/$PKG_NAME"
    
    echo ""
    echo "✅ Package built: dist/$PKG_NAME"
    echo "   Size: $(du -h "$PROJECT_ROOT/dist/$PKG_NAME" | cut -f1)"
    
    # Optionally create DMG for distribution
    if command -v hdiutil &> /dev/null; then
        echo "💿 Creating DMG..."
        DMG_DIR="$BUILD_DIR/dmg"
        mkdir -p "$DMG_DIR"
        
        cp "$PROJECT_ROOT/dist/$PKG_NAME" "$DMG_DIR/"
        cp "$BUILD_DIR/resources/README.txt" "$DMG_DIR/"
        
        # Create Applications symlink for drag-and-drop DMGs (if we were doing that)
        # ln -s /Applications "$DMG_DIR/Applications"
        
        if [ "$INCLUDE_PLUGINS" = "true" ]; then
            DMG_NAME="NornicDB-${VERSION}-${ARCH}-full.dmg"
        else
            DMG_NAME="NornicDB-${VERSION}-${ARCH}-lite.dmg"
        fi
        
        hdiutil create \
            -volname "NornicDB $VERSION ($VARIANT)" \
            -srcfolder "$DMG_DIR" \
            -ov \
            -format UDZO \
            "$PROJECT_ROOT/dist/$DMG_NAME"
        
        echo "✅ DMG created: dist/$DMG_NAME"
        echo "   Size: $(du -h "$PROJECT_ROOT/dist/$DMG_NAME" | cut -f1)"
    fi
}

# Build requested packages
if [ "$BUILD_LITE" = "true" ]; then
    build_package "lite" "false"
fi

if [ "$BUILD_FULL" = "true" ]; then
    build_package "full" "true"
fi

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "🎉 Build complete!"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "Distribution files in: dist/"
ls -lh "$PROJECT_ROOT/dist/" | grep -E '\.pkg$|\.dmg$' || echo "  (none)"
echo ""
echo "🚀 To install:"
echo "   Double-click the .pkg file"
echo ""
echo "📤 To distribute:"
echo "   Upload to GitHub releases"
echo "   Users can download and double-click to install"
echo ""
if [ "$BUILD_BOTH" = "true" ]; then
    echo "📦 Package variants:"
    echo "   • Lite: Core database + menu bar app"
    echo "   • Full: Core + APOC plugins + Heimdall plugins"
    echo ""
fi
