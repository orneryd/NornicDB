#!/bin/bash
# Generate macOS icons from SVG logo

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SVG_PATH="$PROJECT_ROOT/docs/assets/logos/nornicdb-logo.svg"
ICONSET_DIR="$SCRIPT_DIR/NornicDB.iconset"

if [ ! -f "$SVG_PATH" ]; then
    echo "❌ Error: SVG logo not found at $SVG_PATH"
    exit 1
fi

echo "🎨 Generating macOS icons from SVG..."

# Create iconset directory
mkdir -p "$ICONSET_DIR"

# Generate PNG icons at different sizes
# macOS requires: 16, 32, 64, 128, 256, 512, 1024
sizes=(16 32 64 128 256 512 1024)

for size in "${sizes[@]}"; do
    echo "  Creating ${size}x${size}..."
    
    # Use qlmanage to convert SVG to PNG (built into macOS)
    # Or use sips if available
    if command -v rsvg-convert &> /dev/null; then
        # Use rsvg-convert if available (brew install librsvg)
        rsvg-convert -w $size -h $size "$SVG_PATH" -o "$ICONSET_DIR/icon_${size}x${size}.png"
    elif command -v convert &> /dev/null; then
        # Use ImageMagick if available (brew install imagemagick)
        convert -background none -resize ${size}x${size} "$SVG_PATH" "$ICONSET_DIR/icon_${size}x${size}.png"
    else
        echo "⚠️  Warning: No SVG converter found. Install librsvg or imagemagick:"
        echo "   brew install librsvg"
        echo "   or"
        echo "   brew install imagemagick"
        echo ""
        echo "Using fallback: SF Symbol database icon"
        exit 1
    fi
    
    # Also create @2x versions for Retina displays
    if [ $size -le 512 ]; then
        double=$((size * 2))
        if command -v rsvg-convert &> /dev/null; then
            rsvg-convert -w $double -h $double "$SVG_PATH" -o "$ICONSET_DIR/icon_${size}x${size}@2x.png"
        elif command -v convert &> /dev/null; then
            convert -background none -resize ${double}x${double} "$SVG_PATH" "$ICONSET_DIR/icon_${size}x${size}@2x.png"
        fi
    fi
done

# Convert iconset to .icns file
echo "📦 Creating .icns file..."
iconutil -c icns "$ICONSET_DIR" -o "$SCRIPT_DIR/NornicDB.icns"

echo "✅ Icon created: $SCRIPT_DIR/NornicDB.icns"
echo ""
echo "To use this icon:"
echo "  1. Copy to app bundle: cp macos/Assets/NornicDB.icns macos/build/NornicDB.app/Contents/Resources/"
echo "  2. Update Info.plist with: <key>CFBundleIconFile</key><string>NornicDB</string>"
