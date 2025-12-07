#!/bin/bash
# Test script to verify NornicDB config save/load

CONFIG_PATH=~/Library/Application\ Support/NornicDB/config.yaml

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  NornicDB Configuration Test"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

if [ ! -f "$CONFIG_PATH" ]; then
    echo "❌ Config file not found at: $CONFIG_PATH"
    exit 1
fi

echo "📄 Config file: $CONFIG_PATH"
echo ""

echo "🔧 Feature Settings:"
echo "-------------------"
grep -A 1 "embedding:" "$CONFIG_PATH" | grep "enabled:" | head -1
grep -A 1 "kmeans:" "$CONFIG_PATH" | grep "enabled:" | head -1
grep -A 1 "auto_tlp:" "$CONFIG_PATH" | grep "enabled:" | head -1
grep -A 1 "heimdall:" "$CONFIG_PATH" | grep "enabled:" | head -1
echo ""

echo "🤖 Model Settings:"
echo "------------------"
echo "Embedding model:"
grep -A 5 "embedding:" "$CONFIG_PATH" | grep "model:" | head -1
echo ""
echo "Heimdall model:"
grep -A 5 "heimdall:" "$CONFIG_PATH" | grep "model:" | head -1
echo ""

echo "🖥️  Server Settings:"
echo "-------------------"
grep -A 2 "server:" "$CONFIG_PATH" | grep -E "port:|host:"
echo ""

echo "📊 Service Status:"
echo "------------------"
if launchctl list | grep -q "com.nornicdb.server"; then
    echo "✅ Server service: Running"
    PID=$(launchctl list | grep com.nornicdb.server | awk '{print $1}')
    echo "   PID: $PID"
else
    echo "❌ Server service: Not running"
fi

if pgrep -x "NornicDB" > /dev/null; then
    echo "✅ Menu bar app: Running"
    PID=$(pgrep -x "NornicDB")
    echo "   PID: $PID"
else
    echo "❌ Menu bar app: Not running"
fi
echo ""

echo "🌐 Server Health:"
echo "-----------------"
if curl -s http://localhost:7474/health > /dev/null 2>&1; then
    echo "✅ Server responding on port 7474"
    curl -s http://localhost:7474/health
else
    echo "❌ Server not responding on port 7474"
fi
echo ""

echo "📁 Models Directory:"
echo "-------------------"
MODELS_DIR="/usr/local/var/nornicdb/models"
if [ -d "$MODELS_DIR" ]; then
    MODEL_COUNT=$(ls -1 "$MODELS_DIR"/*.gguf 2>/dev/null | wc -l | tr -d ' ')
    echo "✅ Models directory exists"
    echo "   Found $MODEL_COUNT .gguf files:"
    ls -1 "$MODELS_DIR"/*.gguf 2>/dev/null | xargs -n1 basename || echo "   (none)"
else
    echo "❌ Models directory not found: $MODELS_DIR"
fi
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
