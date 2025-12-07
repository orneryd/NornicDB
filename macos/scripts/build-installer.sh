#!/bin/bash

# NornicDB Installer Builder
# Creates a distributable .pkg that users can double-click to install

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="$PROJECT_ROOT/dist/installer"
VERSION=${VERSION:-"1.0.0"}

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

# Clean and create build directory
echo "📁 Preparing build directory..."
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"/{payload,scripts,resources,root/usr/local/bin,root/Applications}

# Copy files to package root
echo "📦 Copying files..."
cp "$PROJECT_ROOT/bin/nornicdb" "$BUILD_DIR/root/usr/local/bin/"
chmod +x "$BUILD_DIR/root/usr/local/bin/nornicdb"

cp -R "$PROJECT_ROOT/macos/build/NornicDB.app" "$BUILD_DIR/root/Applications/"

# Copy resources
if [ -f "$PROJECT_ROOT/nornicdb.example.yaml" ]; then
    cp "$PROJECT_ROOT/nornicdb.example.yaml" "$BUILD_DIR/resources/"
fi

# Copy scripts
cp "$PROJECT_ROOT/macos/scripts/preinstall" "$BUILD_DIR/scripts/"
cp "$PROJECT_ROOT/macos/scripts/postinstall" "$BUILD_DIR/scripts/"
chmod +x "$BUILD_DIR/scripts"/*

# Create README for the package
cat > "$BUILD_DIR/resources/README.txt" << 'EOF'
NornicDB for macOS

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

For help: https://github.com/orneryd/nornicdb

Thank you for using NornicDB!
EOF

# Get architecture
ARCH=$(uname -m)
PKG_NAME="NornicDB-${VERSION}-${ARCH}.pkg"

echo "📝 Building package: $PKG_NAME"

# Build component package
pkgbuild \
    --root "$BUILD_DIR/root" \
    --scripts "$BUILD_DIR/scripts" \
    --identifier "com.nornicdb.pkg" \
    --version "$VERSION" \
    --install-location "/" \
    "$BUILD_DIR/component.pkg"

# Create distribution XML
cat > "$BUILD_DIR/distribution.xml" << EOF
<?xml version="1.0" encoding="utf-8"?>
<installer-gui-script minSpecVersion="1">
    <title>NornicDB</title>
    <welcome file="README.txt"/>
    <pkg-ref id="com.nornicdb.pkg"/>
    <options customize="never" require-scripts="false" hostArchitectures="$ARCH"/>
    <volume-check>
        <allowed-os-versions>
            <os-version min="12.0"/>
        </allowed-os-versions>
    </volume-check>
    <choices-outline>
        <line choice="default">
            <line choice="com.nornicdb.pkg"/>
        </line>
    </choices-outline>
    <choice id="default"/>
    <choice id="com.nornicdb.pkg" visible="false">
        <pkg-ref id="com.nornicdb.pkg"/>
    </choice>
    <pkg-ref id="com.nornicdb.pkg" version="$VERSION" onConclusion="none">component.pkg</pkg-ref>
</installer-gui-script>
EOF

# Build product (distribution) package
productbuild \
    --distribution "$BUILD_DIR/distribution.xml" \
    --resources "$BUILD_DIR/resources" \
    --package-path "$BUILD_DIR" \
    "$PROJECT_ROOT/dist/$PKG_NAME"

echo ""
echo "✅ Package built successfully!"
echo ""
echo "📦 Output: dist/$PKG_NAME"
echo "   Size: $(du -h "$PROJECT_ROOT/dist/$PKG_NAME" | cut -f1)"
echo ""
echo "🚀 To install:"
echo "   Double-click dist/$PKG_NAME"
echo ""
echo "📤 To distribute:"
echo "   Upload dist/$PKG_NAME to releases"
echo "   Users can download and double-click to install"
echo ""

# Optionally create DMG for distribution
if command -v hdiutil &> /dev/null; then
    echo "💿 Creating DMG..."
    DMG_DIR="$BUILD_DIR/dmg"
    mkdir -p "$DMG_DIR"
    
    cp "$PROJECT_ROOT/dist/$PKG_NAME" "$DMG_DIR/"
    cp "$BUILD_DIR/resources/README.txt" "$DMG_DIR/"
    
    # Create Applications symlink for drag-and-drop DMGs (if we were doing that)
    # ln -s /Applications "$DMG_DIR/Applications"
    
    DMG_NAME="NornicDB-${VERSION}-${ARCH}.dmg"
    hdiutil create \
        -volname "NornicDB $VERSION" \
        -srcfolder "$DMG_DIR" \
        -ov \
        -format UDZO \
        "$PROJECT_ROOT/dist/$DMG_NAME"
    
    echo "✅ DMG created: dist/$DMG_NAME"
    echo "   Size: $(du -h "$PROJECT_ROOT/dist/$DMG_NAME" | cut -f1)"
    echo ""
fi

echo "🎉 Build complete!"
echo ""
echo "Distribution files in: dist/"
ls -lh "$PROJECT_ROOT/dist/" | grep -E '\.pkg$|\.dmg$'
echo ""
