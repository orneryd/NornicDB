# Windows CUDA + Go CGO Bridge Proposal

**Problem Statement:** CUDA on Windows requires MSVC (nvcc needs cl.exe), but Go's CGO on Windows uses MinGW. These toolchains have incompatible C++ ABIs, preventing direct linking of MSVC-built CUDA libraries with MinGW-linked Go binaries.

**Goal:** Enable GPU-accelerated local embeddings on Windows while maintaining the existing Docker/Linux CUDA builds.

---

## The Core Problem

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        TOOLCHAIN INCOMPATIBILITY                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   CUDA Build Chain (NVIDIA)          Go CGO Build Chain (Go)               │
│   ─────────────────────────          ──────────────────────────            │
│   nvcc.exe (CUDA compiler)           gcc.exe (MinGW)                       │
│       │                                  │                                 │
│       ▼                                  ▼                                 │
│   cl.exe (MSVC C++ compiler)         g++.exe (MinGW C++)                   │
│       │                                  │                                 │
│       ▼                                  ▼                                 │
│   link.exe (MSVC linker)             ld.exe (GNU linker)                   │
│       │                                  │                                 │
│       ▼                                  ▼                                 │
│   .lib files (COFF format)           .a files (ar archive)                 │
│   MSVC C++ runtime (msvcp*.dll)      libstdc++ (MinGW)                     │
│   MSVC name mangling                 GCC name mangling                     │
│   SEH exception handling             SJLJ/DWARF exceptions                 │
│                                                                             │
│                    ╳ INCOMPATIBLE ╳                                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Specific failures:**
- `??_R4type_info@@` - MSVC RTTI symbols not found
- `__CxxFrameHandler4` - MSVC exception handling not available
- `__std_terminate` - MSVC runtime function missing
- Different name mangling schemes (MSVC vs Itanium/GCC)

---

## Solution Options

### Option 1: C DLL Bridge (Recommended)

Create a pure C interface DLL that wraps llama.cpp's C++ code. Since C has a stable ABI across compilers, both MSVC and MinGW can interoperate through it.

```
┌────────────────────────────────────────────────────────────────────────────┐
│                         C DLL BRIDGE ARCHITECTURE                          │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐        │
│  │   Go Binary     │    │  llama_bridge   │    │   llama.cpp     │        │
│  │   (MinGW)       │───▶│     .dll        │───▶│   + CUDA        │        │
│  │                 │    │   (C API)       │    │   (MSVC)        │        │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘        │
│         │                      │                      │                    │
│    CGO calls             Pure C interface       CUDA kernels              │
│    LoadLibrary()         No C++ in API          cuBLAS, etc.              │
│    GetProcAddress()      Stable ABI                                        │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

**Implementation:**

```c
// llama_bridge.h - Pure C interface (no C++ types)
#ifndef LLAMA_BRIDGE_H
#define LLAMA_BRIDGE_H

#ifdef _WIN32
    #ifdef LLAMA_BRIDGE_BUILD
        #define LLAMA_BRIDGE_API __declspec(dllexport)
    #else
        #define LLAMA_BRIDGE_API __declspec(dllimport)
    #endif
#else
    #define LLAMA_BRIDGE_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

// Opaque handles (hide C++ implementation)
typedef void* llama_bridge_model;
typedef void* llama_bridge_context;

// Initialization
LLAMA_BRIDGE_API int llama_bridge_init(void);
LLAMA_BRIDGE_API void llama_bridge_cleanup(void);

// Model loading
LLAMA_BRIDGE_API llama_bridge_model llama_bridge_load_model(
    const char* path,
    int n_gpu_layers
);
LLAMA_BRIDGE_API void llama_bridge_free_model(llama_bridge_model model);

// Context creation
LLAMA_BRIDGE_API llama_bridge_context llama_bridge_create_context(
    llama_bridge_model model,
    int n_ctx,
    int n_batch,
    int n_threads
);
LLAMA_BRIDGE_API void llama_bridge_free_context(llama_bridge_context ctx);

// Embedding generation
LLAMA_BRIDGE_API int llama_bridge_embed(
    llama_bridge_context ctx,
    const char* text,
    float* output,        // Pre-allocated buffer
    int output_size,      // Buffer size
    int* actual_size      // Actual embedding dimension
);

// Error handling
LLAMA_BRIDGE_API const char* llama_bridge_get_error(void);
LLAMA_BRIDGE_API int llama_bridge_get_embedding_dim(llama_bridge_model model);

#ifdef __cplusplus
}
#endif

#endif // LLAMA_BRIDGE_H
```

```cpp
// llama_bridge.cpp - MSVC/CUDA implementation
#define LLAMA_BRIDGE_BUILD
#include "llama_bridge.h"
#include "llama.h"
#include <string>
#include <mutex>

static std::string g_last_error;
static std::mutex g_error_mutex;

static void set_error(const std::string& err) {
    std::lock_guard<std::mutex> lock(g_error_mutex);
    g_last_error = err;
}

extern "C" {

LLAMA_BRIDGE_API int llama_bridge_init(void) {
    try {
        llama_backend_init();
        return 0;
    } catch (const std::exception& e) {
        set_error(e.what());
        return -1;
    }
}

LLAMA_BRIDGE_API llama_bridge_model llama_bridge_load_model(
    const char* path,
    int n_gpu_layers
) {
    try {
        llama_model_params params = llama_model_default_params();
        params.n_gpu_layers = n_gpu_layers < 0 ? 999 : n_gpu_layers;
        params.use_mmap = true;
        
        llama_model* model = llama_model_load_from_file(path, params);
        if (!model) {
            set_error("Failed to load model");
            return nullptr;
        }
        return static_cast<llama_bridge_model>(model);
    } catch (const std::exception& e) {
        set_error(e.what());
        return nullptr;
    }
}

LLAMA_BRIDGE_API int llama_bridge_embed(
    llama_bridge_context ctx,
    const char* text,
    float* output,
    int output_size,
    int* actual_size
) {
    try {
        auto* context = static_cast<llama_context*>(ctx);
        // ... tokenize, process, extract embeddings ...
        // Copy to output buffer, set actual_size
        return 0;
    } catch (const std::exception& e) {
        set_error(e.what());
        return -1;
    }
}

// ... other implementations ...

} // extern "C"
```

**Go side - Dynamic loading:**

```go
//go:build windows && cuda

package localllm

import (
    "fmt"
    "syscall"
    "unsafe"
)

var (
    llamaDLL           *syscall.DLL
    procInit           *syscall.Proc
    procLoadModel      *syscall.Proc
    procCreateContext  *syscall.Proc
    procEmbed          *syscall.Proc
    procFreeModel      *syscall.Proc
    procFreeContext    *syscall.Proc
    procGetError       *syscall.Proc
    procGetEmbedDim    *syscall.Proc
)

func init() {
    var err error
    llamaDLL, err = syscall.LoadDLL("llama_bridge.dll")
    if err != nil {
        // Fall back to CPU-only
        return
    }
    
    procInit, _ = llamaDLL.FindProc("llama_bridge_init")
    procLoadModel, _ = llamaDLL.FindProc("llama_bridge_load_model")
    procCreateContext, _ = llamaDLL.FindProc("llama_bridge_create_context")
    procEmbed, _ = llamaDLL.FindProc("llama_bridge_embed")
    procFreeModel, _ = llamaDLL.FindProc("llama_bridge_free_model")
    procFreeContext, _ = llamaDLL.FindProc("llama_bridge_free_context")
    procGetError, _ = llamaDLL.FindProc("llama_bridge_get_error")
    procGetEmbedDim, _ = llamaDLL.FindProc("llama_bridge_get_embedding_dim")
    
    procInit.Call()
}

type Model struct {
    handle  uintptr
    context uintptr
    embedDim int
}

func LoadModel(path string, nGPULayers int) (*Model, error) {
    if llamaDLL == nil {
        return nil, fmt.Errorf("CUDA DLL not loaded, use CPU build")
    }
    
    pathPtr, _ := syscall.BytePtrFromString(path)
    handle, _, _ := procLoadModel.Call(
        uintptr(unsafe.Pointer(pathPtr)),
        uintptr(nGPULayers),
    )
    
    if handle == 0 {
        return nil, fmt.Errorf("failed to load model: %s", getLastError())
    }
    
    dim, _, _ := procGetEmbedDim.Call(handle)
    
    return &Model{
        handle:   handle,
        embedDim: int(dim),
    }, nil
}

func (m *Model) Embed(text string) ([]float32, error) {
    textPtr, _ := syscall.BytePtrFromString(text)
    output := make([]float32, m.embedDim)
    var actualSize int32
    
    ret, _, _ := procEmbed.Call(
        m.context,
        uintptr(unsafe.Pointer(textPtr)),
        uintptr(unsafe.Pointer(&output[0])),
        uintptr(m.embedDim),
        uintptr(unsafe.Pointer(&actualSize)),
    )
    
    if ret != 0 {
        return nil, fmt.Errorf("embed failed: %s", getLastError())
    }
    
    return output[:actualSize], nil
}

func getLastError() string {
    ptr, _, _ := procGetError.Call()
    if ptr == 0 {
        return "unknown error"
    }
    return syscall.UTF8ToString((*[1024]byte)(unsafe.Pointer(ptr))[:])
}
```

**Build process:**

```powershell
# scripts/build-llama-bridge.ps1

# 1. Build llama.cpp with CUDA using MSVC
cmake -B build -G "Ninja" `
    -DCMAKE_BUILD_TYPE=Release `
    -DGGML_CUDA=ON `
    -DBUILD_SHARED_LIBS=OFF

cmake --build build --config Release

# 2. Build the bridge DLL
cl.exe /LD /EHsc /O2 /DLLAMA_BRIDGE_BUILD `
    /I"build" /I"include" `
    llama_bridge.cpp `
    build/libllama.lib `
    cudart.lib cublas.lib `
    /Fe:llama_bridge.dll

# 3. Copy DLL to lib directory
copy llama_bridge.dll lib\llama\
```

**Pros:**
- Clean separation of concerns
- C ABI is stable and universal
- DLL can be pre-built and distributed
- No CGO changes needed for Go build
- Easy to debug (separate processes conceptually)

**Cons:**
- Requires DLL distribution with binary
- Slight overhead from dynamic loading
- Extra build step for the DLL

---

### Option 2: Named Pipe / gRPC Local Service

Run llama.cpp as a separate process, communicate via IPC.

```
┌────────────────────────────────────────────────────────────────────────────┐
│                      LOCAL SERVICE ARCHITECTURE                            │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌─────────────────┐         IPC          ┌─────────────────┐             │
│  │   NornicDB      │◀───────────────────▶│  llama-server   │             │
│  │   (Go/MinGW)    │    Named Pipe        │  (MSVC/CUDA)    │             │
│  │                 │    or localhost      │                 │             │
│  └─────────────────┘                      └─────────────────┘             │
│         │                                        │                         │
│    Go HTTP/gRPC                           CUDA kernels                     │
│    client                                 GPU acceleration                 │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

**Implementation:**

llama.cpp already has a server mode. We could:
1. Use the existing `llama-server` binary
2. Build a minimal embedding-only server
3. Communicate via HTTP or named pipes

```go
// pkg/localllm/llama_windows_service.go
//go:build windows && cuda

package localllm

import (
    "bytes"
    "encoding/json"
    "fmt"
    "net/http"
    "os"
    "os/exec"
    "time"
)

type LlamaService struct {
    cmd      *exec.Cmd
    endpoint string
    client   *http.Client
}

func StartService(modelPath string, port int) (*LlamaService, error) {
    endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
    
    // Start llama-server.exe (MSVC-built with CUDA)
    cmd := exec.Command("llama-server.exe",
        "-m", modelPath,
        "--port", fmt.Sprintf("%d", port),
        "--embedding",
        "-ngl", "999",  // All layers on GPU
    )
    cmd.Stdout = os.Stdout
    cmd.Stderr = os.Stderr
    
    if err := cmd.Start(); err != nil {
        return nil, err
    }
    
    svc := &LlamaService{
        cmd:      cmd,
        endpoint: endpoint,
        client:   &http.Client{Timeout: 30 * time.Second},
    }
    
    // Wait for server to be ready
    if err := svc.waitReady(); err != nil {
        cmd.Process.Kill()
        return nil, err
    }
    
    return svc, nil
}

func (s *LlamaService) Embed(text string) ([]float32, error) {
    reqBody, _ := json.Marshal(map[string]string{"content": text})
    
    resp, err := s.client.Post(
        s.endpoint+"/embedding",
        "application/json",
        bytes.NewReader(reqBody),
    )
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()
    
    var result struct {
        Embedding []float32 `json:"embedding"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
        return nil, err
    }
    
    return result.Embedding, nil
}

func (s *LlamaService) Close() error {
    return s.cmd.Process.Kill()
}
```

**Pros:**
- Uses existing llama.cpp server code
- Complete process isolation
- Can restart on crash
- No complex build integration

**Cons:**
- Higher latency (IPC overhead)
- Process management complexity
- Port conflicts possible
- More moving parts
---

### Real-world example: how Ollama solved Windows GPU support

Ollama chose a native Windows sidecar approach (a bundled MSVC-built native binary that exposes a local HTTP API) rather than forcing MSVC-built CUDA code into the same process as a MinGW-linked Go binary. The sidecar pattern they use closely matches Option 2 above and has proven to be a practical, user-friendly way to offer GPU acceleration on Windows.

Key takeaways from Ollama's approach that we should mirror:
- Build and ship a native Windows server (`llama-server.exe` or equivalent) that links CUDA/ROCm and exposes a small, stable HTTP API (Ollama uses `http://localhost:11434` by default).
- Bundle or document runtime dependencies and driver requirements (for NVIDIA, recent drivers such as >=531 are commonly required; Ollama also supports AMD GPUs where available).
- Provide a straightforward installer or ZIP that places the native server binary next to `nornicdb.exe` so the service can be launched automatically or on demand.
- Use the local HTTP API as the stable contract between the Go process and the native inference process — this completely avoids ABI/toolchain incompatibilities.

Recommended update to our strategy based on Ollama's real-world success:
- Continue to offer the C DLL bridge (Option 1) for users who need the lowest possible latency and are comfortable managing DLLs.
- Prioritize the sidecar/service distribution for Windows CUDA releases because it simplifies packaging and testing and avoids CGO/ABI issues in end-user environments.
- Implement `NORNICDB_LLAMA_MODE=dll|service|cpu` and default to `service` for Windows CUDA installers, falling back to `dll` if both are present and the user requests it.
- Ship pre-built native server binaries for supported GPU vendors and document driver/runtime minimums; provide a healthcheck endpoint and a small service manager in `pkg/localllm` that starts/stops/validates the server.
- Add troubleshooting docs: verify GPU drivers, check the service is running (or use a `nornicdb ps`-style command), and inspect GPU utilization with `nvidia-smi` or Task Manager.

Why this matches our goals:
- Distribution-friendly: pre-built native servers remove the need for end users to reproduce MSVC+CUDA builds.
- Safe isolation: crashes in native inference code don't take down the Go process and can be restarted automatically.
- Real-world precedent: Ollama demonstrates the approach scales to multiple GPU vendors and keeps the UX simple.

We'll keep the DLL bridge documented and available as an optimization path, but the Ollama-style sidecar should be the recommended Windows CUDA distribution method.

---

DECISION (OVERRIDE): DLL-FIRST (Official project directive)

The project owner requests the full advanced implementation: the C DLL bridge (`llama_bridge.dll`) is the official, recommended Windows CUDA distribution and support path. The sidecar/service approach remains supported only as an optional fallback.

Actionable next steps (implement immediately):
- Treat the C DLL bridge as the default shipping artifact for Windows CUDA releases and the primary integration target in `pkg/localllm`.
- Prioritize CI and release engineering for MSVC+CUDA DLL builds, including automated symbol/API verification and runtime version checks.
- Implement robust DLL loading in `llama_windows_cuda.go` with clear errors, runtime `VERSION` validation, and graceful fallbacks to `cpu` when necessary.
- Maintain `scripts/build-llama-bridge.ps1` and publish pre-built DLL artifacts in releases (include CUDA runtime DLLs or document required drivers).
- Update docs, installers, and quickstarts to present `dll` as the default and `service` as an alternative.

Operational defaults:
- Default mode on Windows CUDA distributions: `NORNICDB_LLAMA_MODE=dll`
- Allow `NORNICDB_LLAMA_MODE=service|cpu` for fallback usage, but prominently recommend `dll` in documentation and installers.

Testing & support:
- Add Windows CI/integration tests validating in-process DLL calls, version mismatches, driver-missing error handling, and memory-constrained failures.
- Provide clear maintainer instructions to reproduce MSVC+CUDA builds and to produce release DLLs.

This decision supersedes the earlier "service-first" recommendation; source-of-truth docs and release notes must be updated to reflect DLL-first as the canonical Windows GPU integration path.

---

### Option 3: MSYS2/Clang Unified Toolchain

Use MSYS2 with Clang, which can target both MSVC ABI and build CUDA code.

```
┌────────────────────────────────────────────────────────────────────────────┐
│                     CLANG UNIFIED TOOLCHAIN                                │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│   MSYS2 + Clang                                                            │
│   ─────────────────                                                        │
│   clang.exe with -target x86_64-pc-windows-msvc                           │
│       │                                                                    │
│       ▼                                                                    │
│   Produces MSVC-compatible object files                                    │
│       │                                                                    │
│       ▼                                                                    │
│   Can link with CUDA libraries                                             │
│       │                                                                    │
│       ▼                                                                    │
│   CGO uses clang as CC/CXX                                                 │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

**Setup:**

```powershell
# Install MSYS2 and Clang
pacman -S mingw-w64-clang-x86_64-clang

# Set CGO to use Clang with MSVC target
$env:CC = "clang --target=x86_64-pc-windows-msvc"
$env:CXX = "clang++ --target=x86_64-pc-windows-msvc"
$env:CGO_ENABLED = "1"

# Build with CUDA
go build -tags "cuda localllm" ./cmd/nornicdb
```

**Challenges:**
- CUDA toolkit integration with Clang is experimental
- Complex toolchain setup
- May not work with all CUDA features
- Requires specific Clang/CUDA version combinations

**Pros:**
- Single toolchain for everything
- No DLL bridge needed
- Cleaner long-term solution

**Cons:**
- Experimental CUDA support
- Complex setup requirements
- Version compatibility issues
- Not well-documented path

---

### Option 4: Pure Go CUDA Bindings (Long-term)

Use projects like `gorgonia/cu` or write custom CUDA bindings in pure Go.

```go
// Hypothetical pure Go CUDA embedding
package localllm

import (
    "github.com/gorgonia/cu"
)

type CUDAModel struct {
    ctx    cu.Context
    module cu.Module
    // ... GPU memory handles
}

func (m *CUDAModel) Embed(text string) ([]float32, error) {
    // Direct CUDA API calls from Go
    // Load custom CUDA kernels
    // No C++ dependency
}
```

**Pros:**
- No CGO complications
- Cross-platform from single codebase
- Full control over CUDA interaction

**Cons:**
- Would need to reimplement llama.cpp inference
- Massive undertaking
- Lose llama.cpp community updates
- Years of development work

---

## Recommended Approach: Service-first Windows CUDA Distribution (Official)

On Windows we will adopt a service-first distribution model as the official and recommended path for GPU-accelerated inference. Distributing a pre-built, MSVC-built native server (sidecar) avoids toolchain ABI issues, simplifies packaging, and provides robust isolation. The C DLL bridge remains available only as an advanced, opt-in performance optimization for experienced users — it is no longer the default or primary distribution method.

### Phase 1: Sidecar Service (Official, Recommended)

Adopt the sidecar/service approach as the primary delivery and support model for Windows CUDA builds:

1. **Bundle a native inference server**
   - Provide a pre-built MSVC-built server binary (e.g., `llama-server.exe`) linked with CUDA/ROCm for supported GPU vendors.
   - Place the server binary alongside `nornicdb.exe` in the Windows CUDA distribution ZIP/installer.

2. **Local HTTP API contract**
   - Expose a small, stable local HTTP API (e.g., `http://localhost:11434`) for embeddings and generation. This becomes the stable integration contract between NornicDB and the native server.

3. **Service manager in `pkg/localllm`**
   - Implement a lightweight service manager to start/stop/healthcheck the bundled server, validate readiness, and restart on crashes.
   - Provide a healthcheck endpoint and clear error messages when drivers or runtimes are missing.

4. **Configuration and defaults**
   - Support `NORNICDB_LLAMA_MODE=dll|service|cpu` but default to `service` on Windows CUDA distributions. Treat `dll` as advanced/opt-in.
   - Document how to switch modes and how the service manager selects the server binary.

5. **Packaging & runtime**
   - Bundle or clearly document runtime dependencies and driver requirements (NVIDIA driver minimums, optional bundled CUDA runtime DLLs).
   - Provide an installer or ZIP that ensures the server binary is discoverable and runnable without requiring end users to build MSVC+CUDA locally.

6. **Troubleshooting**
   - Document steps: verify GPU drivers, confirm the server process is running, inspect GPU usage (`nvidia-smi` / Task Manager), and use the service healthcheck endpoint.

### Phase 2: C DLL Bridge (Advanced, Opt-in)

Keep the pure C DLL bridge available as an advanced optimization path for users who need in-process, lower-latency embeddings and who accept the extra build/packaging complexity:

- Provide the `llama_bridge.dll` and `llama_bridge.h` in a separate advanced/opt-in download or release artifact.
- Clearly label this path as advanced: building and using the DLL requires MSVC/CUDA tooling, careful version matching, and increased support surface (ABI/version mismatches).
- Recommend `dll` only for performance-critical deployments where the user manages DLL compatibility and accepts the risk of harder distribution and debugging.

### Phase 3: Future Considerations

Monitor these developments:
- Go native CUDA bindings (gorgonia/cu maturity)
- LLVM/Clang CUDA improvements
- Microsoft's C++ interop improvements
- GGML/llama.cpp pure C refactoring

### Phase 3: Future Considerations

Monitor these developments:
- Go native CUDA bindings (gorgonia/cu maturity)
- LLVM/Clang CUDA improvements
- Microsoft's C++ interop improvements
- GGML/llama.cpp pure C refactoring

---

## Implementation Plan

### Directory Structure

```
nornicdb/
├── lib/
│   └── llama/
│       ├── libllama_windows_amd64.a      # CPU-only (MinGW)
│       ├── libllama_darwin_arm64.a       # macOS Metal
│       ├── libllama_linux_amd64.a        # Linux CUDA
│       └── windows_cuda/
│           ├── llama_bridge.dll          # CUDA bridge DLL
│           ├── llama_bridge.h            # C API header
│           └── VERSION                   # DLL version
├── scripts/
│   ├── build-llama-cuda.ps1              # CPU-only build (current)
│   └── build-llama-bridge.ps1            # CUDA DLL build (new)
└── pkg/
    └── localllm/
        ├── llama_windows.go              # CPU build (current)
        ├── llama_windows_cuda.go         # CUDA DLL loading (new)
        ├── llama_darwin.go               # macOS Metal
        └── llama_linux.go                # Linux CUDA
```

### Build Tags Strategy

```go
// llama_windows.go
//go:build cgo && windows && !cuda
// CPU-only, links .a file

// llama_windows_cuda.go  
//go:build windows && cuda
// Dynamic DLL loading, no CGO needed for CUDA part
```

### Release Artifacts

```
nornicdb-windows-amd64-cpu.zip
├── nornicdb.exe
└── models/

nornicdb-windows-amd64-cuda.zip
├── nornicdb.exe
├── llama_bridge.dll        # CUDA bridge
├── cudart64_*.dll          # CUDA runtime (or instructions to install)
└── models/
```

---

## Effort Estimation

| Phase | Task | Effort | Priority |
|-------|------|--------|----------|
| 1.1 | Design C API header | 2 hours | High |
| 1.2 | Implement llama_bridge.cpp | 8 hours | High |
| 1.3 | Create build-llama-bridge.ps1 | 4 hours | High |
| 1.4 | Implement Go DLL loading | 6 hours | High |
| 1.5 | Testing and debugging | 8 hours | High |
| 1.6 | CI/CD for DLL builds | 4 hours | Medium |
| **Total Phase 1** | | **32 hours** | |
| 2.1 | Sidecar service integration | 8 hours | Low |
| 2.2 | Configuration options | 4 hours | Low |
| **Total Phase 2** | | **12 hours** | |

---

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| DLL version mismatch | Medium | High | Version file, runtime check |
| CUDA driver issues | Medium | Medium | Clear error messages, fallback |
| Performance overhead | Low | Medium | Benchmark, optimize if needed |
| Maintenance burden | Medium | Medium | Automate builds, clear docs |
| User confusion | Medium | Low | Single "download CUDA version" option |

---

## Success Criteria

1. **Functional**: Windows users can run GPU-accelerated embeddings
2. **Performance**: <10% overhead vs direct linking (if it worked)
3. **Reliability**: Graceful fallback to CPU if CUDA unavailable
4. **Simplicity**: Single DLL file to add for CUDA support
5. **Maintainable**: Automated build process, version tracking

---

## Appendix: Why Not Just Use MSVC for Go?

Go's compiler and runtime are tightly integrated with GCC/MinGW on Windows:
- Go's calling conventions match GCC
- Go's runtime links against MinGW libc
- CGO generates GCC-compatible assembly
- Changing this would require forking Go itself

Microsoft's `cgo` alternative projects exist but are experimental and not production-ready.

---

## Appendix: CUDA Toolkit Requirements

For the DLL approach, users need:
- CUDA Toolkit 12.x installed (for cudart DLLs)
- OR we bundle the required CUDA runtime DLLs (licensing allows redistribution)

Bundling option:
```
llama_bridge.dll      # Our bridge
cudart64_12.dll       # CUDA runtime (redistributable)
cublas64_12.dll       # cuBLAS (redistributable)
cublasLt64_12.dll     # cuBLAS LT (redistributable)
```

This adds ~200MB to the CUDA distribution but eliminates CUDA Toolkit installation requirement.
