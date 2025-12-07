#!/bin/bash

# NornicDB macOS Installation Script
# Installs NornicDB as a service with menu bar app

set -e

echo "🗄️  Installing NornicDB for macOS..."
echo ""

# Check if running on macOS
if [[ "$OSTYPE" != "darwin"* ]]; then
    echo "❌ Error: This script is for macOS only"
    exit 1
fi

# Check for required permissions
if [[ $EUID -eq 0 ]]; then
   echo "❌ Error: Do not run this script as root/sudo"
   echo "   The script will ask for sudo when needed"
   exit 1
fi

# Detect architecture
ARCH=$(uname -m)
if [[ "$ARCH" == "arm64" ]]; then
    echo "✓ Detected: Apple Silicon (ARM64)"
elif [[ "$ARCH" == "x86_64" ]]; then
    echo "✓ Detected: Intel (x86_64)"
else
    echo "❌ Error: Unsupported architecture: $ARCH"
    exit 1
fi

# Create necessary directories
echo ""
echo "📁 Creating directories..."
sudo mkdir -p /usr/local/bin
sudo mkdir -p /usr/local/var/nornicdb/data
sudo mkdir -p /usr/local/var/log/nornicdb
mkdir -p ~/Library/Application\ Support/NornicDB
mkdir -p ~/Library/LaunchAgents
mkdir -p ~/Applications

# Copy binary
echo "📦 Installing NornicDB binary..."
if [[ -f "./bin/nornicdb" ]]; then
    sudo cp ./bin/nornicdb /usr/local/bin/nornicdb
    sudo chmod +x /usr/local/bin/nornicdb
    echo "✓ Binary installed to /usr/local/bin/nornicdb"
else
    echo "❌ Error: Binary not found at ./bin/nornicdb"
    echo "   Please run 'make build' first"
    exit 1
fi

# Copy default config if it doesn't exist
if [[ ! -f ~/Library/Application\ Support/NornicDB/config.yaml ]]; then
    echo "⚙️  Installing default configuration..."
    if [[ -f "./nornicdb.example.yaml" ]]; then
        cp ./nornicdb.example.yaml ~/Library/Application\ Support/NornicDB/config.yaml
        echo "✓ Config installed to ~/Library/Application Support/NornicDB/config.yaml"
    else
        echo "⚠️  Warning: Example config not found, using defaults"
    fi
else
    echo "⚙️  Existing config found, keeping it"
fi

# Install LaunchAgent
echo "🚀 Installing service (LaunchAgent)..."
# Expand tilde in plist before copying
sed "s|~|$HOME|g" ./macos/LaunchAgents/com.nornicdb.server.plist > /tmp/com.nornicdb.server.plist
cp /tmp/com.nornicdb.server.plist ~/Library/LaunchAgents/com.nornicdb.server.plist
rm /tmp/com.nornicdb.server.plist
echo "✓ LaunchAgent installed"

# Load LaunchAgent
echo "▶️  Starting NornicDB service..."
launchctl unload ~/Library/LaunchAgents/com.nornicdb.server.plist 2>/dev/null || true
launchctl load ~/Library/LaunchAgents/com.nornicdb.server.plist
echo "✓ Service started"

# Install menu bar app if built
if [[ -f "./macos/build/NornicDB.app/Contents/MacOS/NornicDB" ]]; then
    echo "📲 Installing menu bar app..."
    cp -R ./macos/build/NornicDB.app ~/Applications/NornicDB.app
    echo "✓ Menu bar app installed to ~/Applications/NornicDB.app"
    echo ""
    echo "🎯 Launching menu bar app..."
    open ~/Applications/NornicDB.app
else
    echo "⚠️  Menu bar app not found (run 'make macos-menubar' to build it)"
fi

# Set ownership
sudo chown -R $(whoami):staff /usr/local/var/nornicdb
sudo chown -R $(whoami):staff /usr/local/var/log/nornicdb

echo ""
echo "✅ Installation complete!"
echo ""
echo "📍 Installation locations:"
echo "   Binary:     /usr/local/bin/nornicdb"
echo "   Config:     ~/Library/Application Support/NornicDB/config.yaml"
echo "   Data:       /usr/local/var/nornicdb/data"
echo "   Logs:       /usr/local/var/log/nornicdb"
echo "   Service:    ~/Library/LaunchAgents/com.nornicdb.server.plist"
echo "   Menu bar:   ~/Applications/NornicDB.app"
echo ""
echo "🔧 Useful commands:"
echo "   Start:      launchctl start com.nornicdb.server"
echo "   Stop:       launchctl stop com.nornicdb.server"
echo "   Restart:    launchctl restart com.nornicdb.server"
echo "   Status:     launchctl list | grep nornicdb"
echo "   Logs:       tail -f /usr/local/var/log/nornicdb/*.log"
echo "   Uninstall:  ./macos/scripts/uninstall.sh"
echo ""
echo "🌐 Access NornicDB:"
echo "   Web UI:     http://localhost:7687"
echo "   Bolt:       bolt://localhost:7687"
echo ""
