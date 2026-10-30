# Windows CUDA Setup with yzma

NornicDB uses [yzma](https://github.com/hybridgroup/yzma) for GPU-accelerated inference on Windows. This means:

✅ **No CGO required** — Just download prebuilt DLLs  
✅ **Easy CUDA support** — One command to download CUDA libraries  
✅ **Easy updates** — Re-run setup when llama.cpp updates  
✅ **No ABI conflicts** — yzma handles Windows DLL loading  

## Quick Setup (5 minutes)

### Step 1: Download CUDA Libraries

Run the setup script (or manually use yzma):

```powershell
# Option 1: Run setup script
.\scripts\setup-windows-cuda.bat

# Option 2: Manual setup
go install github.com/hybridgroup/yzma/cmd/yzma@latest
yzma install --lib .\lib\llama --processor cuda
```

### Step 2: Set Environment Variable

```powershell
# Temporary (for current PowerShell session)
$env:NORNICDB_LIB = ".\lib\llama"

# Or permanent (system-wide, Admin required)
[Environment]::SetEnvironmentVariable('NORNICDB_LIB', '.\lib\llama', 'User')
```

### Step 3: Build and Run

```powershell
# Build NornicDB
go build -o nornicdb.exe ./cmd/nornicdb

# Verify GPU detection
.\nornicdb.exe --check-gpu

# Run with GPU acceleration
.\nornicdb.exe
```

Expected output:

```
GPU Detection:
  GPU: NVIDIA GeForce RTX 4090 (24 GB)
  Backend: CUDA 12.3
  Status: ✓ Ready
```

---

## Prerequisites

- **Windows 10/11** (64-bit)
- **NVIDIA GPU** with CUDA Compute Capability 6.0 or higher
- **CUDA Toolkit 12.x** ([Download](https://developer.nvidia.com/cuda-downloads)) — drivers/runtime only
- **Go 1.21+** (for building from source)
- **PowerShell 5.0+** or Git Bash (for setup script)

## Setup CUDA Libraries

NornicDB uses [yzma](https://github.com/hybridgroup/yzma) for GPU acceleration. This provides a modern, no-CGO interface to llama.cpp with prebuilt CUDA support.

### One-Command Setup

Run the setup script from your NornicDB installation directory:

**PowerShell:**
```powershell
.\scripts\setup-windows-cuda.bat
```

**Git Bash / WSL:**
```bash
bash scripts/setup-windows-cuda.sh
```

This script will:
1. Download and install yzma (one-time only)
2. Download CUDA-optimized llama.cpp libraries to `./lib/llama/`
3. Show environment variable configuration
4. Verify GPU detection

### Manual Setup (Advanced)

If you prefer to set up manually:

```powershell
# 1. Install yzma
go get github.com/hybridgroup/yzma@latest

# 2. Download CUDA libraries
yzma install --lib ./lib/llama --processor cuda

# 3. Set environment variable (for cross-machine portability)
[Environment]::SetEnvironmentVariable("NORNICDB_LIB", "$(Get-Location)\lib\llama", "User")

# 4. Restart PowerShell for environment variable to take effect
```

---

## Verification

### Check GPU Utilization

While NornicDB is running, open another terminal:

```powershell
# Watch GPU usage in real-time
nvidia-smi -l 1
```

You should see:
- GPU utilization increasing during queries
- Memory usage showing loaded models
- Temperature rising during inference

### Benchmark Performance

```powershell
# Generate 1000 embeddings
.\nornicdb.exe benchmark --embeddings --count 1000

# Expected output:
# Embeddings: 1000/1000
# Time: 2.3s
# Rate: 434 embeddings/sec
# GPU Utilization: 95%
```

Compare with CPU-only mode:
```powershell
# Temporarily disable GPU
$env:NORNICDB_NO_CUDA = "1"

# Run benchmark again
.\nornicdb.exe benchmark --embeddings --count 1000

# Expected output:
# Embeddings: 1000/1000
# Time: 45.8s
# Rate: 21 embeddings/sec
# GPU Utilization: 0%

# Re-enable GPU
$env:NORNICDB_NO_CUDA = ""
```

**Expected Speedup**: 10-50x faster with GPU vs CPU (depends on model and hardware)

---

## Troubleshooting

### GPU Not Detected

**Symptom**: NornicDB reports "GPU backend not found, using CPU"

**Solutions**:

1. **Verify yzma libraries are present**:
   ```powershell
   dir lib\llama\*.dll
   
   # Should show:
   # ggml-cuda.dll (or ggml-metal.dll on ARM)
   # libllama.dll
   # libggml.dll
   ```

2. **Check NORNICDB_LIB environment variable**:
   ```powershell
   echo $env:NORNICDB_LIB
   
   # Should show path to lib/llama directory, or empty (uses default ./lib/llama)
   ```

3. **Verify GPU is CUDA-capable**:
   ```powershell
   nvidia-smi
   
   # Should list your GPU
   ```

4. **Check CUDA Toolkit version**:
   ```powershell
   nvcc --version
   
   # Should show CUDA 12.x
   ```

5. **Update GPU Drivers**:
   - Download latest drivers from [NVIDIA](https://www.nvidia.com/download/index.aspx)
   - Restart computer after driver update

### DLL Load Error

**Symptom**: "The code execution cannot proceed because libllama.dll was not found"

**Solution**: Ensure yzma libraries are properly installed:
```powershell
# Re-run setup script
.\scripts\setup-windows-cuda.bat

# Or manually:
yzma install --lib ./lib/llama --processor cuda
```

### Out of Memory Error

**Symptom**: "CUDA out of memory"

**Solutions**:

1. **Use a smaller model**:
   - Try quantized models (Q4_K_M, Q5_K_M instead of F16)
   - Use fewer GPU layers in config

2. **Close other GPU applications**:
   - Chrome/Edge (hardware acceleration)
   - Games
   - Other ML applications

3. **Check available VRAM**:
   ```powershell
   nvidia-smi --query-gpu=memory.free,memory.total --format=csv
   ```

### Slow Performance

**Symptom**: GPU is detected but performance is slower than expected

**Solutions**:

1. **Verify GPU is actually being used**:
   ```powershell
   nvidia-smi -l 1
   # GPU utilization should be >80% during queries
   ```

2. **Check Power Mode**:
   - NVIDIA Control Panel → Manage 3D Settings → Power Management Mode → "Prefer Maximum Performance"

3. **Check thermal throttling**:
   ```powershell
   nvidia-smi --query-gpu=temperature.gpu,power.draw --format=csv
   # Temperature should be <85°C
   ```

---

## Configuration

### Custom Library Path

By default, NornicDB looks for libraries in `./lib/llama/`. To use a custom location:

```powershell
# Windows (Persistent)
[Environment]::SetEnvironmentVariable("NORNICDB_LIB", "D:\nvidia-libs", "User")

# Windows (Current session only)
$env:NORNICDB_LIB = "D:\nvidia-libs"

# Linux/macOS
export NORNICDB_LIB="/opt/nvidia-libs"
```

### GPU Layer Configuration

Control how many model layers run on GPU:

```yaml
# nornicdb.yaml
embed:
  model: bge-m3-q8_0.gguf
  gpu_layers: -1  # -1 = all layers (default)
                  #  0 = CPU only
                  # 20 = 20 layers on GPU, rest on CPU
```

---

## Building from Source (Developers)

If you want to build NornicDB from source with CUDA support:

```powershell
# Ensure CUDA libraries are installed
.\scripts\setup-windows-cuda.bat

# Build with CUDA support
go build -o nornicdb.exe ./cmd/nornicdb

# Or build without CGO (uses yzma automatically)
go build -tags nolocalllm -o nornicdb.exe ./cmd/nornicdb
```

The build system automatically detects available CUDA libraries via yzma.

---

## More Information

- **yzma Project**: [github.com/hybridgroup/yzma](https://github.com/hybridgroup/yzma)
- **llama.cpp Project**: [github.com/ggerganov/llama.cpp](https://github.com/ggerganov/llama.cpp)
- **NVIDIA CUDA Documentation**: [docs.nvidia.com/cuda](https://docs.nvidia.com/cuda)
- **NornicDB Architecture**: See [Architecture Guide](../architecture/README.md)
