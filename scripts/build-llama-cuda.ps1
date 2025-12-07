# Build llama.cpp static library for Windows (CPU-only)
#
# Note: CUDA on Windows requires MSVC which is incompatible with MinGW CGO.
# Windows builds use CPU-only for local embeddings. Docker/Linux builds support CUDA.
#
# Requirements:
#   - MinGW-w64 (GCC for Windows)
#   - CMake 3.24+
#   - Git
#   - Make (from MinGW or MSYS2)
#
# Usage:
#   .\scripts\build-llama-cuda.ps1 [-Version b7285] [-Clean]
#
# Output:
#   lib\llama\libllama_windows_amd64.a (static library, CPU-only)
#   lib\llama\llama.h, ggml*.h (headers)

param(
    [string]$Version = "b7285",  # Latest version with flash_attn_type and llama_memory API
    [switch]$Clean
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$OutDir = Join-Path $ProjectRoot "lib\llama"
$TmpDir = Join-Path $env:TEMP "llama-cpp-build"
$OriginalDir = Get-Location

Write-Host "[BUILD] llama.cpp $Version for Windows (CPU-only)" -ForegroundColor Cyan
Write-Host "        Output: $OutDir"

# Clean up any previous build directory
if (Test-Path $TmpDir) {
    Write-Host "[CLEAN] Removing previous build directory..." -ForegroundColor Yellow
    Remove-Item -Recurse -Force $TmpDir -ErrorAction SilentlyContinue
}

# Check for MinGW (required for compatibility with Go CGO)
$gcc = Get-Command gcc -ErrorAction SilentlyContinue
if (-not $gcc) {
    Write-Host "[ERROR] MinGW not found. Please install MinGW-w64" -ForegroundColor Red
    Write-Host "        Install via: choco install mingw" -ForegroundColor Red
    Write-Host "        Or: https://www.mingw-w64.org/" -ForegroundColor Red
    exit 1
}
$gccVersion = & gcc --version 2>&1 | Select-Object -First 1
Write-Host "        GCC: $gccVersion" -ForegroundColor Green

# Check for Make
$make = Get-Command make -ErrorAction SilentlyContinue
if (-not $make) {
    Write-Host "[ERROR] Make not found. Please install make (from MinGW or MSYS2)" -ForegroundColor Red
    Write-Host "        Install via: choco install make" -ForegroundColor Red
    exit 1
}
Write-Host "        Make: Found" -ForegroundColor Green

# Note: CURL support disabled for Windows (not needed for local model inference)
Write-Host "        CURL: Disabled (not required for local models)" -ForegroundColor Yellow

# Create directories
if (-not (Test-Path $OutDir)) {
    New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
}
if (-not (Test-Path $TmpDir)) {
    New-Item -ItemType Directory -Force -Path $TmpDir | Out-Null
}

# Clone or update llama.cpp
Write-Host ""
Set-Location $TmpDir

# Clean build directory if it exists with wrong generator
if (Test-Path "build\CMakeCache.txt") {
    Write-Host "[CLEAN] Removing stale CMake cache..." -ForegroundColor Yellow
    Remove-Item -Recurse -Force "build" -ErrorAction SilentlyContinue
}

# Check if this is already a git repo with the right version
$isGitRepo = Test-Path ".git"
$needsClone = $true

if ($isGitRepo) {
    Write-Host "[CHECK] Existing repo found, verifying version..." -ForegroundColor Cyan
    $currentBranch = & git rev-parse --abbrev-ref HEAD 2>&1 | Out-String
    $currentBranch = $currentBranch.Trim()
    if ($currentBranch -eq $Version) {
        Write-Host "        Already on $Version, using existing checkout" -ForegroundColor Green
        $needsClone = $false
    } else {
        Write-Host "        Different version ($currentBranch), fetching $Version..." -ForegroundColor Yellow
        Start-Process -FilePath "git" -ArgumentList "fetch","--depth","1","origin",$Version -NoNewWindow -Wait -ErrorAction SilentlyContinue
        Start-Process -FilePath "git" -ArgumentList "checkout",$Version -NoNewWindow -Wait -ErrorAction SilentlyContinue
        $checkBranch = & git rev-parse --abbrev-ref HEAD 2>&1 | Out-String
        if ($checkBranch.Trim() -eq $Version -or $checkBranch.Trim() -eq "HEAD") {
            Write-Host "        Switched to $Version" -ForegroundColor Green
            $needsClone = $false
        } else {
            Write-Host "[WARN]  Could not switch versions, will use existing" -ForegroundColor Yellow
            $needsClone = $false
        }
    }
}

if ($needsClone) {
    Write-Host "[CLONE] llama.cpp $Version..." -ForegroundColor Cyan
    & git clone --depth 1 --branch $Version https://github.com/ggerganov/llama.cpp.git .
    if ($LASTEXITCODE -ne 0) { 
        Write-Host "[ERROR] Git clone failed" -ForegroundColor Red
        Set-Location $OriginalDir
        exit 1
    }
}

# Patch log.cpp to fix missing <chrono> include (MSVC build issue)
$logCppPath = Join-Path $TmpDir "common\log.cpp"
if (Test-Path $logCppPath) {
    $logContent = Get-Content $logCppPath -Raw
    if ($logContent -notmatch '#include\s*<chrono>') {
        Write-Host "[PATCH] Adding missing #include <chrono> to log.cpp..." -ForegroundColor Yellow
        $logContent = $logContent -replace '(#include\s*<cstdio>)', "`$1`n#include <chrono>"
        $logContent | Set-Content $logCppPath -NoNewline
    }
}

# Disable cpp-httplib build (not needed, causes MinGW issues)
$httpCMakePath = Join-Path $TmpDir "vendor\cpp-httplib\CMakeLists.txt"
if (Test-Path $httpCMakePath) {
    Write-Host "[PATCH] Disabling cpp-httplib build (not needed for library)..." -ForegroundColor Yellow
    # Replace the entire CMakeLists.txt with a minimal version
    @"
cmake_minimum_required(VERSION 3.14)
project(cpp-httplib)
# Disabled - not needed for static library build
"@ | Set-Content $httpCMakePath -NoNewline
}

# Disable tools that depend on cpp-httplib
$rootCMakePath = Join-Path $TmpDir "CMakeLists.txt"
if (Test-Path $rootCMakePath) {
    Write-Host "[PATCH] Disabling tools build (we only need the library)..." -ForegroundColor Yellow
    $cmakeContent = Get-Content $rootCMakePath -Raw
    # Comment out add_subdirectory(tools)
    $cmakeContent = $cmakeContent -replace '(add_subdirectory\(tools\))', '# $1 # Disabled for library-only build'
    # Comment out add_subdirectory(examples)
    $cmakeContent = $cmakeContent -replace '(add_subdirectory\(examples\))', '# $1 # Disabled for library-only build'
    $cmakeContent | Set-Content $rootCMakePath -NoNewline
}

# Build with MinGW (CPU-only)
Write-Host ""
Write-Host "[BUILD] Building with MinGW (CPU-only)..." -ForegroundColor Cyan

# Configure with CMake
Write-Host "        Configuring with CMake..." -ForegroundColor Yellow
$cmakeArgs = @(
    "-B", "build",
    "-G", "MinGW Makefiles",
    "-DCMAKE_BUILD_TYPE=Release",
    "-DLLAMA_STATIC=ON",
    "-DBUILD_SHARED_LIBS=OFF",
    "-DLLAMA_BUILD_TESTS=OFF",
    "-DLLAMA_BUILD_EXAMPLES=OFF",
    "-DLLAMA_BUILD_SERVER=OFF",
    "-DGGML_CUDA=OFF",
    "-DLLAMA_CURL=OFF",
    "-DGGML_NATIVE=ON",
    "-DGGML_AVX=ON",
    "-DGGML_AVX2=ON",
    "-DGGML_FMA=ON",
    "-DLLAMA_STANDALONE=OFF"
)

& cmake @cmakeArgs
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] CMake configuration failed!" -ForegroundColor Red
    Set-Location $OriginalDir
    exit 1
}

# Build
Write-Host "        Building with make..." -ForegroundColor Yellow
& cmake --build build --config Release -j $env:NUMBER_OF_PROCESSORS
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Build failed!" -ForegroundColor Red
    Set-Location $OriginalDir
    exit 1
}

Write-Host "        Build completed successfully!" -ForegroundColor Green

# Copy all static libraries to output directory (preserving build structure)
Write-Host ""
Write-Host "[LIBS]  Copying libraries..." -ForegroundColor Cyan

# Create directory structure
$buildOutDir = Join-Path $OutDir "windows_amd64_cuda"
if (Test-Path $buildOutDir) {
    Remove-Item -Recurse -Force $buildOutDir
}
New-Item -ItemType Directory -Force -Path $buildOutDir | Out-Null

# Copy entire build directory (preserves library locations for linking)
Write-Host "        Copying build tree..." -ForegroundColor Yellow
Copy-Item -Recurse -Force "$TmpDir\build" $buildOutDir

# Find all static libraries for verification
$libFiles = Get-ChildItem -Path "$buildOutDir\build" -Recurse -Filter "*.a" | 
    Where-Object { $_.Name -match "^(lib|ggml)" }

if ($libFiles.Count -eq 0) {
    Write-Host "[ERROR] No static libraries found" -ForegroundColor Red
    Set-Location $OriginalDir
    exit 1
}

Write-Host "        Copied libraries:" -ForegroundColor Yellow
$libFiles | ForEach-Object { Write-Host "          - $($_.Name)" }

# Calculate total library size
$totalSize = ($libFiles | Measure-Object -Property Length -Sum).Sum / 1MB
Write-Host "        Total library size: $([math]::Round($totalSize, 1)) MB" -ForegroundColor Green

# Verify key libraries exist
$requiredLibs = @("libllama.a", "ggml.a", "ggml-cpu.a", "ggml-base.a", "libcommon.a")
foreach ($reqLib in $requiredLibs) {
    $found = $libFiles | Where-Object { $_.Name -eq $reqLib }
    if (-not $found) {
        Write-Host "[ERROR] Required library $reqLib not found!" -ForegroundColor Red
        Write-Host "        Available libraries:" -ForegroundColor Yellow
        $libFiles | ForEach-Object { Write-Host "          - $($_.Name)" }
        Set-Location $OriginalDir
        exit 1
    }
}

# Copy headers
Write-Host ""
Write-Host "[HDRS]  Copying headers..." -ForegroundColor Cyan

# llama.h
$llamaH = Get-ChildItem -Path $TmpDir -Recurse -Filter "llama.h" | 
    Where-Object { $_.DirectoryName -match "include|src" } | 
    Select-Object -First 1
if ($llamaH) {
    Copy-Item $llamaH.FullName $OutDir
    Write-Host "          - llama.h" -ForegroundColor Green
}

# ggml headers
$ggmlHeaders = Get-ChildItem -Path "$TmpDir\ggml\include" -Filter "*.h" -ErrorAction SilentlyContinue
if (-not $ggmlHeaders) {
    $ggmlHeaders = Get-ChildItem -Path "$TmpDir\include" -Filter "ggml*.h" -ErrorAction SilentlyContinue
}
if ($ggmlHeaders) {
    $ggmlHeaders | ForEach-Object {
        Copy-Item $_.FullName $OutDir
        Write-Host "          - $($_.Name)" -ForegroundColor Green
    }
}

# Update VERSION file
$Version | Out-File -FilePath (Join-Path $OutDir "VERSION") -Encoding ASCII -NoNewline

# Return to original directory
Set-Location $OriginalDir

# Cleanup temp directory
Write-Host ""
Write-Host "[CLEAN] Cleaning up temp directory..." -ForegroundColor Cyan
Remove-Item -Recurse -Force $TmpDir -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "[DONE]  Build complete!" -ForegroundColor Green
Write-Host "        Library: $outputLib (CPU-only)" -ForegroundColor White
Write-Host "        Headers: llama.h, ggml*.h" -ForegroundColor White
Write-Host ""
Write-Host "[NOTE]  Windows builds are CPU-only" -ForegroundColor Yellow
Write-Host "        For GPU acceleration, use Docker on Linux" -ForegroundColor Yellow
Write-Host ""
Write-Host "[NEXT]  Next steps:" -ForegroundColor Cyan
Write-Host "        1. Run: make build" -ForegroundColor White
Write-Host "        2. Download model: make download-bge" -ForegroundColor White
Write-Host "        3. Set: `$env:NORNICDB_EMBEDDING_PROVIDER='local'" -ForegroundColor White
Write-Host "        4. Run: .\bin\nornicdb.exe serve --no-auth" -ForegroundColor White
