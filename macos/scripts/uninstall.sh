#!/bin/bash

# NornicDB macOS Uninstallation Script
# Removes NornicDB service and files

set -e

echo "🗑️  Uninstalling NornicDB from macOS..."
echo ""

# Check if running on macOS
if [[ "$OSTYPE" != "darwin"* ]]; then
    echo "❌ Error: This script is for macOS only"
    exit 1
fi

# Stop and unload service
if [[ -f ~/Library/LaunchAgents/com.nornicdb.server.plist ]]; then
    echo "🛑 Stopping NornicDB service..."
    launchctl stop com.nornicdb.server 2>/dev/null || true
    launchctl unload ~/Library/LaunchAgents/com.nornicdb.server.plist 2>/dev/null || true
    echo "✓ Service stopped"
fi

# Quit menu bar app
if pgrep -x "NornicDB" > /dev/null; then
    echo "🛑 Quitting menu bar app..."
    killall NornicDB 2>/dev/null || true
fi

# Ask about data removal
echo ""
read -p "❓ Remove data directory (/usr/local/var/nornicdb)? [y/N]: " -n 1 -r
echo
REMOVE_DATA=false
if [[ $REPLY =~ ^[Yy]$ ]]; then
    REMOVE_DATA=true
fi

# Ask about config removal
read -p "❓ Remove configuration (~/Library/Application Support/NornicDB)? [y/N]: " -n 1 -r
echo
REMOVE_CONFIG=false
if [[ $REPLY =~ ^[Yy]$ ]]; then
    REMOVE_CONFIG=true
fi

echo ""
echo "🗑️  Removing files..."

# Remove binary
if [[ -f /usr/local/bin/nornicdb ]]; then
    sudo rm -f /usr/local/bin/nornicdb
    echo "✓ Removed binary"
fi

# Remove LaunchAgent
if [[ -f ~/Library/LaunchAgents/com.nornicdb.server.plist ]]; then
    rm -f ~/Library/LaunchAgents/com.nornicdb.server.plist
    echo "✓ Removed LaunchAgent"
fi

# Remove menu bar app
if [[ -d ~/Applications/NornicDB.app ]]; then
    rm -rf ~/Applications/NornicDB.app
    echo "✓ Removed menu bar app"
fi

# Remove logs
if [[ -d /usr/local/var/log/nornicdb ]]; then
    sudo rm -rf /usr/local/var/log/nornicdb
    echo "✓ Removed logs"
fi

# Remove data if requested
if [[ "$REMOVE_DATA" = true ]]; then
    if [[ -d /usr/local/var/nornicdb ]]; then
        sudo rm -rf /usr/local/var/nornicdb
        echo "✓ Removed data directory"
    fi
else
    echo "⚠️  Kept data directory: /usr/local/var/nornicdb"
fi

# Remove config if requested
if [[ "$REMOVE_CONFIG" = true ]]; then
    if [[ -d ~/Library/Application\ Support/NornicDB ]]; then
        rm -rf ~/Library/Application\ Support/NornicDB
        echo "✓ Removed configuration"
    fi
else
    echo "⚠️  Kept configuration: ~/Library/Application Support/NornicDB"
fi

echo ""
echo "✅ Uninstallation complete!"
echo ""
if [[ "$REMOVE_DATA" = false ]]; then
    echo "💾 Your data has been preserved at: /usr/local/var/nornicdb"
    echo "   To remove it manually: sudo rm -rf /usr/local/var/nornicdb"
    echo ""
fi
if [[ "$REMOVE_CONFIG" = false ]]; then
    echo "⚙️  Your configuration has been preserved"
    echo "   To remove it manually: rm -rf ~/Library/Application\\ Support/NornicDB"
    echo ""
fi
