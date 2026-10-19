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

cleanup_build_dir() {
    local target="$1"

    if [ ! -e "$target" ]; then
        return 0
    fi

    if rm -rf "$target" 2>/dev/null; then
        return 0
    fi

    echo "❌ Failed to remove $target; an earlier relocatable package may have installed root-owned files there."
    echo "   Remove the old staging directory as an administrator before rebuilding."
    return 1
}

mkdir -p "$PROJECT_ROOT/dist"
BUILD_DIR=$(mktemp -d "$PROJECT_ROOT/dist/installer.XXXXXX")
trap 'cleanup_build_dir "$BUILD_DIR"' EXIT

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
    cleanup_build_dir "$BUILD_DIR"
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
    
    # One upgrade/configuration owner for both editions.
    cp "$PROJECT_ROOT/macos/scripts/postinstall" "$BUILD_DIR/scripts/"
    mkdir -p "$BUILD_DIR/root/usr/local/share/nornicdb"
    cp "$BUILD_DIR/resources/default-config.yaml" "$BUILD_DIR/root/usr/local/share/nornicdb/default-config.yaml"

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
    pkgbuild --analyze --root "$BUILD_DIR/root" "$BUILD_DIR/components.plist"
    plutil -replace '0.BundleIsRelocatable' -bool NO "$BUILD_DIR/components.plist"
    pkgbuild \
        --root "$BUILD_DIR/root" \
        --component-plist "$BUILD_DIR/components.plist" \
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
        "$BUILD_DIR/$PKG_NAME"
    
    echo ""
    echo "✅ Package built: dist/$PKG_NAME"
    echo "   Size: $(du -h "$BUILD_DIR/$PKG_NAME" | cut -f1)"
    
    # Optionally create DMG for distribution
    if command -v hdiutil &> /dev/null; then
        echo "💿 Creating DMG..."
        DMG_DIR="$BUILD_DIR/dmg"
        mkdir -p "$DMG_DIR"
        
        cp "$BUILD_DIR/$PKG_NAME" "$DMG_DIR/"
        cp "$BUILD_DIR/resources/README.txt" "$DMG_DIR/"
        
        # Create Applications symlink for drag-and-drop DMGs (if we were doing that)
        # ln -s /Applications "$DMG_DIR/Applications"
        
        if [ "$INCLUDE_PLUGINS" = "true" ]; then
            DMG_NAME="NornicDB-${VERSION}-${ARCH}-full.dmg"
        else
            DMG_NAME="NornicDB-${VERSION}-${ARCH}-lite.dmg"
        fi

        # hdiutil's automatic estimate can leave too little room for the copied package.
        DMG_PAYLOAD_KIB=$(du -sk "$DMG_DIR" | cut -f1)
        DMG_SIZE_MIB=$(( (DMG_PAYLOAD_KIB * 3 / 2 + 64 * 1024 + 1023) / 1024 ))
        
        hdiutil create \
            -volname "NornicDB $VERSION ($VARIANT)" \
            -srcfolder "$DMG_DIR" \
            -size "${DMG_SIZE_MIB}m" \
            -ov \
            -format UDZO \
            "$BUILD_DIR/$DMG_NAME"
        
        echo "✅ DMG created: dist/$DMG_NAME"
        echo "   Size: $(du -h "$BUILD_DIR/$DMG_NAME" | cut -f1)"
        mv -f "$BUILD_DIR/$DMG_NAME" "$PROJECT_ROOT/dist/$DMG_NAME"
    fi
    mv -f "$BUILD_DIR/$PKG_NAME" "$PROJECT_ROOT/dist/$PKG_NAME"
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
