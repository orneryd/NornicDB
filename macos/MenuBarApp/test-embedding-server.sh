#!/bin/bash
#
# Test script for NornicDB Apple ML Embedding Server
#

set -e

cd "$(dirname "$0")"

echo "🔨 Compiling embedding server test..."
swiftc -o /tmp/embedding-test \
    -framework Foundation \
    -framework NaturalLanguage \
    -framework Network \
    AppleMLEmbedder.swift \
    EmbeddingServer.swift \
    EmbeddingServerTest.swift \
    2>&1

if [ $? -eq 0 ]; then
    echo "✅ Compilation successful"
    echo ""
    echo "🚀 Running test..."
    echo ""
    /tmp/embedding-test
else
    echo "❌ Compilation failed"
    exit 1
fi
