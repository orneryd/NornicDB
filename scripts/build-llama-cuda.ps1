# Build llama.cpp static library for Windows with CUDA support
#
# Requirements:
#   - CUDA Toolkit 12.x installed (nvcc in PATH)
#   - Visual Studio 2022 with C++ Desktop development
#   - CMake 3.24+
#   - Git
#
# Usage:
#   .\scripts\build-llama-cuda.ps1 [-Version b4535] [-Clean]
#
# Output:
#   lib\llama\libllama_windows_amd64.lib (static library with CUDA)
#   lib\llama\llama.h, ggml*.h (headers)

param(
    [string]$Version = "b4785",  # Use newer version with MSVC chrono fix
    [switch]$Clean
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$OutDir = Join-Path $ProjectRoot "lib\llama"
$TmpDir = Join-Path $env:TEMP "llama-cpp-build"
$OriginalDir = Get-Location

Write-Host "[BUILD] llama.cpp $Version for Windows with CUDA" -ForegroundColor Cyan
Write-Host "        Output: $OutDir"

# Check for CUDA - try PATH first, then common install locations
$nvcc = Get-Command nvcc -ErrorAction SilentlyContinue
if (-not $nvcc) {
    # Try common CUDA install locations
    $cudaPaths = @(
        "$env:CUDA_PATH\bin",
        "$env:CUDA_HOME\bin",
        "C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v13.0\bin",
        "C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v12.6\bin",
        "C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v12.4\bin",
        "C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v12.2\bin",
        "C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v12.0\bin"
    )
    
    foreach ($path in $cudaPaths) {
        if (Test-Path "$path\nvcc.exe") {
            $env:PATH = "$path;$env:PATH"
            $nvcc = Get-Command nvcc -ErrorAction SilentlyContinue
            Write-Host "        Found CUDA at: $path" -ForegroundColor Yellow
            break
        }
    }
}

if (-not $nvcc) {
    Write-Host "[ERROR] CUDA Toolkit not found. Please install CUDA Toolkit and ensure nvcc is in PATH" -ForegroundColor Red
    Write-Host "        Or set CUDA_PATH environment variable" -ForegroundColor Red
    exit 1
}
$nvccOutput = & nvcc --version 2>&1 | Out-String
if ($nvccOutput -match 'release (\d+\.\d+)') {
    $cudaVersion = $Matches[1]
} else {
    $cudaVersion = "unknown"
}
Write-Host "        CUDA Version: $cudaVersion" -ForegroundColor Green

# Check for Visual Studio
$vsWhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
if (-not (Test-Path $vsWhere)) {
    Write-Host "[ERROR] Visual Studio not found. Please install VS 2022 with C++ Desktop development" -ForegroundColor Red
    exit 1
}
$vsPath = & $vsWhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
if (-not $vsPath) {
    Write-Host "[ERROR] Visual Studio C++ tools not found" -ForegroundColor Red
    exit 1
}
Write-Host "        Visual Studio: $vsPath" -ForegroundColor Green

# Always clean up temp directory from previous runs
if (Test-Path $TmpDir) {
    Write-Host ""
    Write-Host "[CLEAN] Removing previous build directory..." -ForegroundColor Yellow
    Remove-Item -Recurse -Force $TmpDir -ErrorAction SilentlyContinue
}

# Create directories
if (-not (Test-Path $OutDir)) {
    New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
}
if (-not (Test-Path $TmpDir)) {
    New-Item -ItemType Directory -Force -Path $TmpDir | Out-Null
}

# Verify temp directory was created
if (-not (Test-Path $TmpDir)) {
    Write-Host "[ERROR] Failed to create temp directory: $TmpDir" -ForegroundColor Red
    exit 1
}

# Clone llama.cpp
Write-Host ""
Write-Host "[CLONE] llama.cpp $Version..." -ForegroundColor Cyan
Set-Location $TmpDir
& git clone --depth 1 --branch $Version https://github.com/ggerganov/llama.cpp.git .
if ($LASTEXITCODE -ne 0) { 
    Write-Host "[ERROR] Git clone failed" -ForegroundColor Red
    Set-Location $OriginalDir
    exit 1
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

# Setup MSVC environment and build
Write-Host ""
Write-Host "[BUILD] Building with CUDA support..." -ForegroundColor Cyan

# Find vcvarsall.bat
$vcvarsall = Join-Path $vsPath "VC\Auxiliary\Build\vcvars64.bat"
if (-not (Test-Path $vcvarsall)) {
    Write-Host "[ERROR] vcvars64.bat not found at $vcvarsall" -ForegroundColor Red
    Set-Location $OriginalDir
    exit 1
}

# Create a batch script that sets up environment and runs cmake
$buildScript = @"
@echo off
call "$vcvarsall"
cd /d "$TmpDir"

cmake -B build -G "Ninja" ^
    -DCMAKE_BUILD_TYPE=Release ^
    -DLLAMA_STATIC=ON ^
    -DBUILD_SHARED_LIBS=OFF ^
    -DLLAMA_BUILD_TESTS=OFF ^
    -DLLAMA_BUILD_EXAMPLES=OFF ^
    -DLLAMA_BUILD_SERVER=OFF ^
    -DGGML_CUDA=ON ^
    -DGGML_CUDA_FA_ALL_QUANTS=ON

if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

cmake --build build --config Release -j %NUMBER_OF_PROCESSORS%
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%

echo Build completed successfully!
"@

$buildScriptPath = Join-Path $TmpDir "build-cuda.cmd"
$buildScript | Out-File -FilePath $buildScriptPath -Encoding ASCII

# Run the build script
Write-Host "        Running cmake with CUDA..." -ForegroundColor Yellow
& cmd.exe /c $buildScriptPath
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Build failed!" -ForegroundColor Red
    Set-Location $OriginalDir
    exit 1
}

# Find and combine static libraries
Write-Host ""
Write-Host "[LIBS]  Creating combined library..." -ForegroundColor Cyan

$libFiles = Get-ChildItem -Path "$TmpDir\build" -Recurse -Filter "*.lib" | 
    Where-Object { $_.Name -match "llama|ggml" }

if ($libFiles.Count -eq 0) {
    # Try .a files (MinGW/MSYS2 style)
    $libFiles = Get-ChildItem -Path "$TmpDir\build" -Recurse -Filter "*.a" | 
        Where-Object { $_.Name -match "llama|ggml" }
}

if ($libFiles.Count -eq 0) {
    Write-Host "[ERROR] No static libraries found in build directory" -ForegroundColor Red
    Set-Location $OriginalDir
    exit 1
}

Write-Host "        Found libraries:" -ForegroundColor Yellow
$libFiles | ForEach-Object { Write-Host "          - $($_.Name)" }

$outputLib = Join-Path $OutDir "libllama_windows_amd64.lib"

# Check if we have .lib files (MSVC) or .a files (MinGW/GCC)
$isLibFiles = $libFiles[0].Extension -eq ".lib"

if ($isLibFiles) {
    # Use lib.exe to combine MSVC .lib files - must run through vcvarsall
    Write-Host "        Combining with lib.exe..." -ForegroundColor Yellow
    
    # Build the lib.exe command with all library paths
    $libPaths = ($libFiles | ForEach-Object { "`"$($_.FullName)`"" }) -join " "
    $libCmd = "lib.exe /OUT:`"$outputLib`" $libPaths"
    
    # Run lib.exe through VS Developer environment
    $libBat = Join-Path $TmpDir "combine-libs.bat"
    @"
@echo off
call "$vcvarsall" x64
$libCmd
"@ | Out-File -FilePath $libBat -Encoding ASCII
    
    & cmd.exe /c $libBat
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[WARN]  lib.exe failed, copying individual libraries instead" -ForegroundColor Yellow
        # Copy all individual libraries to output dir
        $libFiles | ForEach-Object {
            Copy-Item $_.FullName $OutDir
            Write-Host "          - $($_.Name)" -ForegroundColor Yellow
        }
    }
} else {
    # For .a files, just copy the main llama library
    Write-Host "        Copying primary library..." -ForegroundColor Yellow
    $primaryLib = $libFiles | Where-Object { $_.Name -match "libllama" } | Select-Object -First 1
    if ($primaryLib) {
        Copy-Item $primaryLib.FullName (Join-Path $OutDir "libllama_windows_amd64.a")
        $outputLib = Join-Path $OutDir "libllama_windows_amd64.a"
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
Write-Host "        Library: $outputLib" -ForegroundColor White
Write-Host "        Headers: llama.h, ggml*.h" -ForegroundColor White
Write-Host ""
Write-Host "[NEXT]  Next steps:" -ForegroundColor Cyan
Write-Host "        1. Run: .\build-cuda.bat" -ForegroundColor White
Write-Host "        2. Place your .gguf model in a models directory" -ForegroundColor White
Write-Host "        3. Set NORNICDB_EMBEDDING_PROVIDER=local" -ForegroundColor White
