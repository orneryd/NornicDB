# NornicDB Build & Deploy Script (Windows)
# Supports both native Windows builds and Docker image builds
#
# Native Builds:
#   .\build.ps1 native cpu              CPU-only, no embeddings (smallest)
#   .\build.ps1 native cpu-localllm     CPU with llama.cpp (BYOM)
#   .\build.ps1 native cpu-bge          CPU with llama.cpp + BGE model
#   .\build.ps1 native cuda             CUDA with llama.cpp (BYOM)
#   .\build.ps1 native cuda-bge         CUDA with llama.cpp + BGE model
#   .\build.ps1 native all              Build all variants
#
# Docker Builds:
#   .\build.ps1 docker build amd64-cuda
#   .\build.ps1 docker deploy amd64-cuda-bge

param(
    [Parameter(Position=0)]
    [string]$Mode = "help",
    
    [Parameter(Position=1)]
    [string]$Target = "",
    
    [switch]$Headless,
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$LibDir = Join-Path $ScriptDir "lib\llama"
$ModelsDir = Join-Path $ScriptDir "models"
$BinDir = Join-Path $ScriptDir "bin"

$Registry = if ($env:REGISTRY) { $env:REGISTRY } else { "timothyswt" }
$Version = if ($env:VERSION) { $env:VERSION } else { "latest" }
$ReleaseUrl = "https://github.com/timothyswt/nornicdb/releases/download/libs-v1"
$ModelUrl = "https://huggingface.co/BAAI/bge-m3-gguf/resolve/main/bge-m3-Q4_K_M.gguf"

# =============================================================================
# Docker Configuration
# =============================================================================

$DockerImages = @{
    "amd64-cuda"      = "$Registry/nornicdb-amd64-cuda:$Version"
    "amd64-cuda-bge"  = "$Registry/nornicdb-amd64-cuda-bge:$Version"
    "amd64-cpu"       = "$Registry/nornicdb-amd64-cpu:$Version"
}

$Dockerfiles = @{
    "amd64-cuda"      = "docker/Dockerfile.amd64-cuda"
    "amd64-cuda-bge"  = "docker/Dockerfile.amd64-cuda"
    "amd64-cpu"       = "docker/Dockerfile.amd64-cpu"
}

$DockerBuildArgs = @{
    "amd64-cuda-bge" = "--build-arg EMBED_MODEL=true"
}

# =============================================================================
# Native Build Functions
# =============================================================================

function Test-Go {
    if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
        Write-Host "[ERROR] Go not found. Install from https://go.dev/dl/" -ForegroundColor Red
        return $false
    }
    return $true
}

function Test-Cuda {
    $nvcc = Get-Command nvcc -ErrorAction SilentlyContinue
    if (-not $nvcc) {
        # Try common CUDA paths
        $cudaPaths = @(
            "$env:CUDA_PATH\bin",
            "C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v13.0\bin",
            "C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v12.6\bin",
            "C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v12.4\bin"
        )
        foreach ($path in $cudaPaths) {
            if (Test-Path "$path\nvcc.exe") {
                $env:PATH = "$path;$env:PATH"
                Write-Host "        Found CUDA at: $path" -ForegroundColor Yellow
                return $true
            }
        }
        Write-Host "[ERROR] CUDA Toolkit not found" -ForegroundColor Red
        return $false
    }
    return $true
}

function Test-LlamaLibs {
    param([switch]$Cuda)
    
    $libFile = if ($Cuda) { "libllama_windows_amd64.lib" } else { "libllama_windows_amd64.lib" }
    if (Test-Path (Join-Path $LibDir $libFile)) { return $true }
    
    Write-Host "[ERROR] llama.cpp libraries not found!" -ForegroundColor Red
    Write-Host "        Run: .\build.ps1 download-libs" -ForegroundColor Yellow
    return $false
}

function Test-Model {
    $modelPath = Join-Path $ModelsDir "bge-m3.gguf"
    if (Test-Path $modelPath) { return $true }
    
    Write-Host "[ERROR] BGE model not found!" -ForegroundColor Red
    Write-Host "        Run: .\build.ps1 download-model" -ForegroundColor Yellow
    return $false
}

function Build-Native {
    param(
        [string]$Variant,
        [switch]$Headless
    )
    
    if (-not (Test-Go)) { return }
    if (-not (Test-Path $BinDir)) { New-Item -ItemType Directory -Path $BinDir | Out-Null }
    
    $tags = ""
    $outputName = "nornicdb"
    $cgoEnabled = "0"
    
    switch ($Variant) {
        "cpu" {
            $outputName = "nornicdb-cpu"
            Write-Host "[BUILD] CPU-only (no embeddings)" -ForegroundColor Cyan
        }
        "cpu-localllm" {
            if (-not (Test-LlamaLibs)) { return }
            $tags = "localllm"
            $cgoEnabled = "1"
            $outputName = "nornicdb-cpu-localllm"
            Write-Host "[BUILD] CPU + Local Embeddings (BYOM)" -ForegroundColor Cyan
        }
        "cpu-bge" {
            if (-not (Test-LlamaLibs)) { return }
            if (-not (Test-Model)) { return }
            $tags = "localllm"
            $cgoEnabled = "1"
            $outputName = "nornicdb-cpu-bge"
            Write-Host "[BUILD] CPU + Local Embeddings + BGE Model" -ForegroundColor Cyan
        }
        "cuda" {
            if (-not (Test-Cuda)) { return }
            if (-not (Test-LlamaLibs -Cuda)) { return }
            $tags = "cuda localllm"
            $cgoEnabled = "1"
            $outputName = "nornicdb-cuda"
            Write-Host "[BUILD] CUDA + Local Embeddings (BYOM)" -ForegroundColor Cyan
        }
        "cuda-bge" {
            if (-not (Test-Cuda)) { return }
            if (-not (Test-LlamaLibs -Cuda)) { return }
            if (-not (Test-Model)) { return }
            $tags = "cuda localllm"
            $cgoEnabled = "1"
            $outputName = "nornicdb-cuda-bge"
            Write-Host "[BUILD] CUDA + Local Embeddings + BGE Model" -ForegroundColor Cyan
        }
        default {
            Write-Host "[ERROR] Unknown variant: $Variant" -ForegroundColor Red
            return
        }
    }
    
    if ($Headless) {
        $tags = if ($tags) { "$tags noui" } else { "noui" }
        $outputName = "$outputName-headless"
    }
    
    $outputPath = Join-Path $BinDir "$outputName.exe"
    Write-Host "        Output: $outputPath"
    
    $env:CGO_ENABLED = $cgoEnabled
    $env:GOOS = "windows"
    $env:GOARCH = "amd64"
    
    $buildCmd = "go build"
    if ($tags) { $buildCmd += " -tags=`"$tags`"" }
    $buildCmd += " -ldflags=`"-s -w`" -o `"$outputPath`" .\cmd\nornicdb"
    
    Write-Host "        Building..." -ForegroundColor Yellow
    Invoke-Expression $buildCmd
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[DONE]  Build successful!" -ForegroundColor Green
        
        # Copy model for -bge variants
        if ($Variant -match "-bge$") {
            $modelSrc = Join-Path $ModelsDir "bge-m3.gguf"
            Copy-Item $modelSrc $BinDir -Force
            Write-Host "        Model copied to: $BinDir\bge-m3.gguf" -ForegroundColor Green
        }
    } else {
        Write-Host "[ERROR] Build failed!" -ForegroundColor Red
    }
}

function Build-AllNative {
    param([switch]$Headless)
    
    @("cpu", "cpu-localllm", "cpu-bge", "cuda", "cuda-bge") | ForEach-Object {
        Write-Host ""
        Build-Native -Variant $_ -Headless:$Headless
    }
    
    Write-Host ""
    Write-Host "╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Green
    Write-Host "║ All variants built!                                          ║" -ForegroundColor Green
    Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Green
    Get-ChildItem "$BinDir\nornicdb*.exe" | ForEach-Object { Write-Host "  $_" }
}

# =============================================================================
# Download Functions
# =============================================================================

function Download-Libs {
    Write-Host "[DOWNLOAD] Pre-built llama.cpp libraries" -ForegroundColor Cyan
    
    if (-not (Test-Path $LibDir)) { New-Item -ItemType Directory -Path $LibDir | Out-Null }
    
    $libFile = Join-Path $LibDir "libllama_windows_amd64.lib"
    Write-Host "        Downloading library..."
    try {
        Invoke-WebRequest -Uri "$ReleaseUrl/libllama_windows_amd64.lib" -OutFile $libFile
        Write-Host "[DONE]  Library downloaded!" -ForegroundColor Green
    } catch {
        Write-Host "[ERROR] Download failed. Build locally with:" -ForegroundColor Red
        Write-Host "        .\scripts\build-llama-cuda.ps1" -ForegroundColor Yellow
    }
}

function Download-Model {
    Write-Host "[DOWNLOAD] BGE-M3 embedding model (~400MB)" -ForegroundColor Cyan
    
    if (-not (Test-Path $ModelsDir)) { New-Item -ItemType Directory -Path $ModelsDir | Out-Null }
    
    $modelFile = Join-Path $ModelsDir "bge-m3.gguf"
    Write-Host "        Downloading model..."
    try {
        Invoke-WebRequest -Uri $ModelUrl -OutFile $modelFile
        Write-Host "[DONE]  Model downloaded!" -ForegroundColor Green
    } catch {
        Write-Host "[ERROR] Download failed" -ForegroundColor Red
    }
}

# =============================================================================
# Docker Functions
# =============================================================================

function Build-DockerImage {
    param([string]$Target)
    
    if (-not $DockerImages.ContainsKey($Target)) {
        Write-Host "[ERROR] Unknown Docker target: $Target" -ForegroundColor Red
        return
    }
    
    $image = $DockerImages[$Target]
    $dockerfile = $Dockerfiles[$Target]
    $args = if ($DockerBuildArgs.ContainsKey($Target)) { $DockerBuildArgs[$Target] } else { "" }
    
    Write-Host "[BUILD] $image" -ForegroundColor Cyan
    $cmd = "docker build --platform linux/amd64 $args -t $image -f $dockerfile ."
    Invoke-Expression $cmd
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[DONE]  Built $image" -ForegroundColor Green
    }
}

function Push-DockerImage {
    param([string]$Target)
    
    if (-not $DockerImages.ContainsKey($Target)) {
        Write-Host "[ERROR] Unknown Docker target: $Target" -ForegroundColor Red
        return
    }
    
    $image = $DockerImages[$Target]
    Write-Host "[PUSH]  $image" -ForegroundColor Yellow
    docker push $image
}

# =============================================================================
# Main Entry Point
# =============================================================================

switch ($Mode.ToLower()) {
    "native" {
        if ($Target -eq "all") {
            Build-AllNative -Headless:$Headless
        } elseif ($Target) {
            Build-Native -Variant $Target -Headless:$Headless
        } else {
            Write-Host "Usage: .\build.ps1 native <variant> [-Headless]"
            Write-Host ""
            Write-Host "Variants: cpu, cpu-localllm, cpu-bge, cuda, cuda-bge, all"
        }
    }
    "docker" {
        if ($Target -eq "build" -and $args[0]) {
            Build-DockerImage -Target $args[0]
        } elseif ($Target -eq "deploy" -and $args[0]) {
            Build-DockerImage -Target $args[0]
            Push-DockerImage -Target $args[0]
        } else {
            Write-Host "Usage: .\build.ps1 docker [build|deploy] <target>"
            Write-Host ""
            Write-Host "Targets: amd64-cuda, amd64-cuda-bge, amd64-cpu"
        }
    }
    "download-libs" {
        Download-Libs
    }
    "download-model" {
        Download-Model
    }
    "clean" {
        Write-Host "Cleaning build artifacts..."
        Remove-Item "$BinDir\nornicdb*.exe" -ErrorAction SilentlyContinue
        Write-Host "Done."
    }
    default {
        Write-Host "╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
        Write-Host "║ NornicDB Build Script                                        ║" -ForegroundColor Cyan
        Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "NATIVE BUILDS (Windows .exe):"
        Write-Host "  .\build.ps1 native cpu              CPU-only (smallest, ~15MB)"
        Write-Host "  .\build.ps1 native cpu-localllm     CPU + embeddings, BYOM (~25MB)"
        Write-Host "  .\build.ps1 native cpu-bge          CPU + embeddings + BGE (~425MB)"
        Write-Host "  .\build.ps1 native cuda             CUDA + embeddings, BYOM (~30MB)"
        Write-Host "  .\build.ps1 native cuda-bge         CUDA + embeddings + BGE (~430MB)"
        Write-Host "  .\build.ps1 native all              Build all variants"
        Write-Host ""
        Write-Host "  Add -Headless to build without web UI"
        Write-Host ""
        Write-Host "DOCKER BUILDS (Linux containers):"
        Write-Host "  .\build.ps1 docker build amd64-cuda"
        Write-Host "  .\build.ps1 docker deploy amd64-cuda-bge"
        Write-Host ""
        Write-Host "SETUP:"
        Write-Host "  .\build.ps1 download-libs           Get pre-built llama.cpp"
        Write-Host "  .\build.ps1 download-model          Get BGE-M3 model (~400MB)"
        Write-Host ""
        Write-Host "PREREQUISITES BY VARIANT:"
        Write-Host "  cpu:          Go 1.23+"
        Write-Host "  cpu-localllm: Go 1.23+, llama.cpp libs"
        Write-Host "  cpu-bge:      Go 1.23+, llama.cpp libs, BGE model"
        Write-Host "  cuda:         Go 1.23+, llama.cpp CUDA libs, CUDA Toolkit"
        Write-Host "  cuda-bge:     Go 1.23+, llama.cpp CUDA libs, CUDA Toolkit, BGE model"
    }
}
