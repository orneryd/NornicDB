#!/bin/bash
# Compile GLSL shaders to SPIR-V for NornicDB Vulkan compute
# Requires Vulkan SDK with glslc compiler

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SHADER_DIR="$SCRIPT_DIR"

# Check for glslc
if ! command -v glslc &> /dev/null; then
    echo "Error: glslc not found. Please install the Vulkan SDK."
    echo "  - Windows: https://vulkan.lunarg.com/sdk/home"
    echo "  - Linux: apt install vulkan-sdk or dnf install vulkan-tools"
    echo "  - macOS: brew install vulkan-sdk"
    exit 1
fi

echo "Compiling GLSL compute shaders to SPIR-V..."

# Compile each shader
for shader in "$SHADER_DIR"/*.comp; do
    if [ -f "$shader" ]; then
        name=$(basename "$shader" .comp)
        echo "  Compiling $name.comp -> $name.spv"
        glslc -fshader-stage=compute -O "$shader" -o "$SHADER_DIR/$name.spv"
    fi
done

echo "Done! SPIR-V files generated."
ls -la "$SHADER_DIR"/*.spv 2>/dev/null || echo "No .spv files found"
