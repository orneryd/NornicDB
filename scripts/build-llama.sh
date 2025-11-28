#!/bin/bash
# Build llama.cpp static libraries for NornicDB local embeddings
#
# Usage:
#   ./scripts/build-llama.sh [version]
#
# Examples:
#   ./scripts/build-llama.sh          # Uses default version (b4535)
#   ./scripts/build-llama.sh b4600    # Specific version
#
# Output:
#   lib/llama/libllama_{os}_{arch}.a
#   lib/llama/llama.h
#   lib/llama/ggml.h
#
# GPU Support:
#   - macOS/ARM64: Metal (automatic)
#   - Linux/AMD64: CUDA (if nvcc available)
#   - All platforms: CPU SIMD (AVX2/NEON)

set -euo pipefail

VERSION="${1:-b4535}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
OUTDIR="$PROJECT_ROOT/lib/llama"
TMPDIR="/tmp/llama-cpp-build-$$"

echo "🔧 Building llama.cpp $VERSION for NornicDB"
echo "   Output: $OUTDIR"

# Cleanup on exit
cleanup() {
    rm -rf "$TMPDIR"
}
trap cleanup EXIT

# Create output directory
mkdir -p "$OUTDIR"

# Clone llama.cpp
echo "📥 Cloning llama.cpp $VERSION..."
git clone --depth 1 --branch "$VERSION" https://github.com/ggerganov/llama.cpp.git "$TMPDIR"
cd "$TMPDIR"

# Detect platform
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
[[ "$ARCH" == "x86_64" ]] && ARCH="amd64"
[[ "$ARCH" == "aarch64" ]] && ARCH="arm64"

echo "   Platform: ${OS}/${ARCH}"

# Base CMake args for static library
CMAKE_ARGS="-DLLAMA_STATIC=ON -DBUILD_SHARED_LIBS=OFF -DLLAMA_BUILD_TESTS=OFF -DLLAMA_BUILD_EXAMPLES=OFF -DLLAMA_BUILD_SERVER=OFF"

# GPU-specific configuration
GPU_SUFFIX=""
if [[ "$OS" == "darwin" && "$ARCH" == "arm64" ]]; then
    echo "   GPU: Metal (Apple Silicon)"
    echo "   Features: Flash Attention, Embedded Metal Shaders"
    # Use GGML_ prefixed options (newer llama.cpp)
    CMAKE_ARGS="$CMAKE_ARGS -DGGML_METAL=ON"
    CMAKE_ARGS="$CMAKE_ARGS -DGGML_METAL_EMBED_LIBRARY=ON"  # Embed Metal shaders in binary
elif [[ "$OS" == "linux" && "$ARCH" == "amd64" ]] && command -v nvcc &> /dev/null; then
    echo "   GPU: CUDA detected"
    echo "   Features: Flash Attention for all quants"
    CMAKE_ARGS="$CMAKE_ARGS -DGGML_CUDA=ON"
    CMAKE_ARGS="$CMAKE_ARGS -DGGML_CUDA_FA_ALL_QUANTS=ON"  # Flash attention for all quants
    GPU_SUFFIX="_cuda"
else
    echo "   GPU: None (CPU only with SIMD)"
fi

# Build
echo "🏗️  Building..."
cmake -B build $CMAKE_ARGS
cmake --build build --config Release -j$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

# Find and combine all static libraries (llama.cpp now splits into multiple .a files)
LIB_NAME="libllama_${OS}_${ARCH}${GPU_SUFFIX}.a"
echo "📦 Creating combined library: $OUTDIR/$LIB_NAME"

# Find all static libraries
LIBS=$(find build -name "*.a" -type f 2>/dev/null | grep -E "(libllama|libggml|libcommon)" | sort -u)
if [[ -z "$LIBS" ]]; then
    echo "❌ Error: No static libraries found in build directory"
    exit 1
fi
echo "   Found libraries:"
for lib in $LIBS; do
    echo "      - $lib"
done

# Create combined library using libtool (macOS) or ar (Linux)
if [[ "$OS" == "darwin" ]]; then
    libtool -static -o "$OUTDIR/$LIB_NAME" $LIBS
else
    # On Linux, use ar to combine
    mkdir -p /tmp/ar_combine_$$
    cd /tmp/ar_combine_$$
    for lib in $LIBS; do
        ar x "$lib"
    done
    ar rcs "$OUTDIR/$LIB_NAME" *.o
    cd - > /dev/null
    rm -rf /tmp/ar_combine_$$
fi

# Copy all required headers
echo "📄 Copying headers..."

# llama.h
if [[ -f include/llama.h ]]; then
    cp include/llama.h "$OUTDIR/"
elif [[ -f src/llama.h ]]; then
    cp src/llama.h "$OUTDIR/"
fi

# ggml headers (all from ggml/include)
if [[ -d ggml/include ]]; then
    cp ggml/include/*.h "$OUTDIR/" 2>/dev/null || true
elif [[ -d include ]]; then
    cp include/ggml*.h "$OUTDIR/" 2>/dev/null || true
fi

# Create a version file
echo "$VERSION" > "$OUTDIR/VERSION"

echo ""
echo "✅ Build complete!"
echo "   Library: $OUTDIR/$LIB_NAME"
echo "   Headers: $OUTDIR/llama.h, $OUTDIR/ggml.h"
echo ""
echo "📝 Next steps:"
echo "   1. Set NORNICDB_EMBEDDING_PROVIDER=local"
echo "   2. Place your .gguf model in /data/models/"
echo "   3. Start NornicDB with --embedding-model=<model-name>"
