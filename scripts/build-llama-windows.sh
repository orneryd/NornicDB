#!/bin/bash
# Build llama.cpp for Windows with dynamic backend loading
# This creates a base library (MinGW-compiled) that loads GPU backends at runtime

set -e

echo "Building llama.cpp for Windows with dynamic backend loading..."

# Ensure we have llama.cpp vendored
if [ ! -d "vendor/llama.cpp" ]; then
    echo "Error: vendor/llama.cpp not found"
    echo "Run: git submodule update --init --recursive"
    exit 1
fi

# Check for MinGW cross-compiler
if ! command -v x86_64-w64-mingw32-gcc &> /dev/null; then
    echo "Error: MinGW cross-compiler not found"
    echo "Install: brew install mingw-w64 (macOS) or apt install mingw-w64 (Linux)"
    exit 1
fi

# Create build directory
mkdir -p build-windows
cd build-windows

# Configure CMake for Windows cross-compilation with dynamic backends
cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_SYSTEM_NAME=Windows \
    -DCMAKE_C_COMPILER=x86_64-w64-mingw32-gcc \
    -DCMAKE_CXX_COMPILER=x86_64-w64-mingw32-g++ \
    -DCMAKE_RC_COMPILER=x86_64-w64-mingw32-windres \
    -DGGML_BACKEND_DL=ON \
    -DGGML_BACKEND_SHARED=OFF \
    -DGGML_CUDA=OFF \
    -DGGML_METAL=OFF \
    -DGGML_VULKAN=OFF \
    -DGGML_NATIVE=OFF \
    -DGGML_OPENMP=ON \
    -DBUILD_SHARED_LIBS=OFF \
    -DLLAMA_BUILD_EXAMPLES=OFF \
    -DLLAMA_BUILD_TESTS=OFF \
    -DLLAMA_BUILD_SERVER=OFF \
    -S vendor/llama.cpp

# Build
cmake --build . --config Release -j $(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

# Create output directory
mkdir -p ../lib/llama

# Copy library
if [ -f "src/libllama.a" ]; then
    cp src/libllama.a ../lib/llama/libllama_windows_amd64.a
    echo "✓ Built: lib/llama/libllama_windows_amd64.a"
elif [ -f "libllama.a" ]; then
    cp libllama.a ../lib/llama/libllama_windows_amd64.a
    echo "✓ Built: lib/llama/libllama_windows_amd64.a"
else
    echo "Error: Could not find libllama.a"
    exit 1
fi

cd ..

echo ""
echo "✓ Windows base library built successfully!"
echo ""
echo "Next steps:"
echo "1. Build CUDA backend DLL with: ./scripts/build-cuda-backend-windows.bat"
echo "2. Test on Windows machine with CUDA installed"
echo ""
echo "Note: The base library is MinGW-compiled and will dynamically load"
echo "      the CUDA backend DLL (MSVC-compiled) at runtime on Windows."
