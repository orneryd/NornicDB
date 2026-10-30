# Building NornicDB with Vulkan Support

This document explains how to build NornicDB with Vulkan GPU acceleration enabled.

## Prerequisites

### All Platforms

1. **Vulkan SDK**: Download and install from https://vulkan.lunarg.com/
2. **GPU with Vulkan 1.1+ support**: NVIDIA, AMD, Intel, or Apple (via MoltenVK)

### Platform-Specific Setup

#### macOS (with MoltenVK)

```bash
# Install Vulkan SDK via Homebrew
brew install vulkan-sdk

# Or download from LunarG and set environment variables
export VULKAN_SDK=/path/to/vulkan-sdk
export CGO_CFLAGS="-I$VULKAN_SDK/include"
export CGO_LDFLAGS="-L$VULKAN_SDK/lib -lvulkan"
```

#### Linux

```bash
# Ubuntu/Debian
sudo apt install vulkan-tools libvulkan-dev vulkan-validationlayers

# Fedora
sudo dnf install vulkan-tools vulkan-loader-devel vulkan-validation-layers

# Set environment variables
export VULKAN_SDK=/path/to/vulkan-sdk  # If using SDK
export CGO_CFLAGS="-I$VULKAN_SDK/include"
export CGO_LDFLAGS="-L$VULKAN_SDK/lib -lvulkan"
```

#### Windows

1. Install Vulkan SDK from LunarG
2. Set environment variable:
   ```cmd
   set VULKAN_SDK=C:\VulkanSDK\1.x.x.x
   ```
3. The SDK installer typically sets this automatically

## Building

### Build with Vulkan Support (CGO)

```bash
# Set environment variables (if not already set)
export CGO_CFLAGS="-I$VULKAN_SDK/include"
export CGO_LDFLAGS="-L$VULKAN_SDK/lib -lvulkan"

# Build with cgovulkan tag
go build -tags cgovulkan ./cmd/nornicdb
```

### Build without Vulkan (PureGo - Default)

```bash
# No special flags needed - uses purego for dynamic library loading
go build ./cmd/nornicdb
```

## Compiling Shaders

The SPIR-V shaders are pre-compiled and embedded in the code. To recompile:

```bash
cd pkg/gpu/vulkan/shaders

# Linux/macOS
./compile.sh

# Windows
compile.bat
```

This requires `glslc` from the Vulkan SDK.

## Verifying Build

### Check if Vulkan is Available

```bash
# Run tests (will skip if Vulkan not available)
go test -tags cgovulkan ./pkg/gpu/vulkan -v
```

### Test SPIR-V Shader Structure

```bash
# Test that SPIR-V shader file is valid
go test -tags cgovulkan ./pkg/gpu/vulkan -run TestSPIRV -v
```

## Troubleshooting

### Error: 'vulkan/vulkan.h' file not found

**Solution**: Set `CGO_CFLAGS` to point to Vulkan SDK include directory:
```bash
export CGO_CFLAGS="-I$VULKAN_SDK/include"
```

### Error: undefined reference to 'vulkan functions'

**Solution**: Set `CGO_LDFLAGS` to link against Vulkan library:
```bash
export CGO_LDFLAGS="-L$VULKAN_SDK/lib -lvulkan"
```

### Error: No suitable GPU found

**Solution**: 
- Ensure GPU drivers are installed (NVIDIA/AMD/Intel)
- On macOS, ensure MoltenVK is installed
- Verify with `vulkaninfo` command (from Vulkan SDK)

### Build succeeds but tests skip

**Solution**: This is normal if Vulkan runtime is not available. The code will fall back to CPU implementation.

## Architecture

The Vulkan implementation has two modes:

1. **CGO Mode** (`cgovulkan` tag): Direct CGO bindings to Vulkan
   - Requires Vulkan SDK headers at build time
   - Faster compilation, static linking

2. **PureGo Mode** (default): Dynamic library loading via purego
   - No build-time dependencies
   - Automatically finds Vulkan library at runtime
   - Works on systems without Vulkan SDK installed

Both modes use the same SPIR-V shader bytecode embedded in the code.

