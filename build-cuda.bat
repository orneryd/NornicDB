@echo off
REM NornicDB Windows Build Script
REM Supports multiple build variants with optional CUDA and embedded models
REM
REM Variants:
REM   cpu              CPU-only, no embeddings (smallest)
REM   cpu-localllm     CPU with llama.cpp embeddings (BYOM)
REM   cpu-bge          CPU with llama.cpp + BGE model embedded
REM   cuda             CUDA with llama.cpp embeddings (BYOM)
REM   cuda-bge         CUDA with llama.cpp + BGE model embedded
REM
REM Usage:
REM   build.bat cpu              Build CPU-only (no embeddings)
REM   build.bat cpu-localllm     Build CPU with local embeddings
REM   build.bat cpu-bge          Build CPU with embedded BGE model
REM   build.bat cuda             Build CUDA with local embeddings
REM   build.bat cuda-bge         Build CUDA with embedded BGE model
REM   build.bat download-libs    Download pre-built llama.cpp libs
REM   build.bat download-model   Download BGE model
REM   build.bat all              Build all variants

setlocal EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
set "LIB_DIR=%SCRIPT_DIR%lib\llama"
set "MODELS_DIR=%SCRIPT_DIR%models"
set "BIN_DIR=%SCRIPT_DIR%bin"
set "LLAMA_VERSION=b4785"
set "RELEASE_URL=https://github.com/timothyswt/nornicdb/releases/download/libs-v1"
set "MODEL_URL=https://huggingface.co/BAAI/bge-m3-gguf/resolve/main/bge-m3-Q4_K_M.gguf"

REM Check arguments
if "%1"=="" goto :help
if "%1"=="help" goto :help
if "%1"=="-h" goto :help
if "%1"=="--help" goto :help
if "%1"=="download-libs" goto :download_libs
if "%1"=="download-model" goto :download_model
if "%1"=="clean" goto :clean
if "%1"=="all" goto :build_all

REM Validate variant
set "VARIANT=%1"
set "HEADLESS="
if "%2"=="headless" set "HEADLESS=1"

if "%VARIANT%"=="cpu" goto :build_cpu
if "%VARIANT%"=="cpu-localllm" goto :build_cpu_localllm
if "%VARIANT%"=="cpu-bge" goto :build_cpu_bge
if "%VARIANT%"=="cuda" goto :build_cuda
if "%VARIANT%"=="cuda-bge" goto :build_cuda_bge

echo ERROR: Unknown variant '%VARIANT%'
echo Run 'build.bat help' for usage
goto :eof

REM ==============================================================================
REM Build Variants
REM ==============================================================================

:build_cpu
echo ===============================================================
echo  Building: NornicDB CPU-only (no embeddings)
echo  Output:   %BIN_DIR%\nornicdb-cpu.exe
echo ===============================================================
call :check_go
if not exist "%BIN_DIR%" mkdir "%BIN_DIR%"

set "BUILD_TAGS="
if defined HEADLESS set "BUILD_TAGS=noui"

set CGO_ENABLED=0
set GOOS=windows
set GOARCH=amd64

if defined HEADLESS (
    go build -tags="%BUILD_TAGS%" -ldflags="-s -w" -o "%BIN_DIR%\nornicdb-cpu-headless.exe" .\cmd\nornicdb
) else (
    go build -tags="%BUILD_TAGS%" -ldflags="-s -w" -o "%BIN_DIR%\nornicdb-cpu.exe" .\cmd\nornicdb
)
if errorlevel 1 goto :build_failed
echo.
echo ✓ Build successful!
echo   This variant has NO embedding support - use external embedding service
goto :eof

:build_cpu_localllm
echo ===============================================================
echo  Building: NornicDB CPU + Local Embeddings (BYOM)
echo  Output:   %BIN_DIR%\nornicdb-cpu-localllm.exe
echo ===============================================================
call :check_go
call :check_llama_libs_cpu
if not exist "%BIN_DIR%" mkdir "%BIN_DIR%"

set "BUILD_TAGS=localllm"
if defined HEADLESS set "BUILD_TAGS=localllm noui"

set CGO_ENABLED=1
set GOOS=windows
set GOARCH=amd64

if defined HEADLESS (
    go build -tags="%BUILD_TAGS%" -ldflags="-s -w" -o "%BIN_DIR%\nornicdb-cpu-localllm-headless.exe" .\cmd\nornicdb
) else (
    go build -tags="%BUILD_TAGS%" -ldflags="-s -w" -o "%BIN_DIR%\nornicdb-cpu-localllm.exe" .\cmd\nornicdb
)
if errorlevel 1 goto :build_failed
echo.
echo ✓ Build successful!
echo   Place your .gguf model in: %MODELS_DIR%
echo   Set: NORNICDB_EMBEDDING_PROVIDER=local
goto :eof

:build_cpu_bge
echo ===============================================================
echo  Building: NornicDB CPU + Local Embeddings + BGE Model
echo  Output:   %BIN_DIR%\nornicdb-cpu-bge.exe
echo ===============================================================
call :check_go
call :check_llama_libs_cpu
call :check_model
if not exist "%BIN_DIR%" mkdir "%BIN_DIR%"

set "BUILD_TAGS=localllm"
if defined HEADLESS set "BUILD_TAGS=localllm noui"

set CGO_ENABLED=1
set GOOS=windows
set GOARCH=amd64

if defined HEADLESS (
    go build -tags="%BUILD_TAGS%" -ldflags="-s -w" -o "%BIN_DIR%\nornicdb-cpu-bge-headless.exe" .\cmd\nornicdb
) else (
    go build -tags="%BUILD_TAGS%" -ldflags="-s -w" -o "%BIN_DIR%\nornicdb-cpu-bge.exe" .\cmd\nornicdb
)
if errorlevel 1 goto :build_failed

REM Copy model alongside binary
copy "%MODELS_DIR%\bge-m3.gguf" "%BIN_DIR%\" >nul 2>&1
echo.
echo ✓ Build successful!
echo   Model embedded: %BIN_DIR%\bge-m3.gguf
echo   Ready to run - no additional setup needed!
goto :eof

:build_cuda
echo ===============================================================
echo  Building: NornicDB CUDA + Local Embeddings (BYOM)
echo  Output:   %BIN_DIR%\nornicdb-cuda.exe
echo ===============================================================
call :check_go
call :check_cuda
call :check_llama_libs_cuda
if not exist "%BIN_DIR%" mkdir "%BIN_DIR%"

set "BUILD_TAGS=cuda localllm"
if defined HEADLESS set "BUILD_TAGS=cuda localllm noui"

set CGO_ENABLED=1
set GOOS=windows
set GOARCH=amd64

if defined HEADLESS (
    go build -tags="%BUILD_TAGS%" -ldflags="-s -w" -o "%BIN_DIR%\nornicdb-cuda-headless.exe" .\cmd\nornicdb
) else (
    go build -tags="%BUILD_TAGS%" -ldflags="-s -w" -o "%BIN_DIR%\nornicdb-cuda.exe" .\cmd\nornicdb
)
if errorlevel 1 goto :build_failed
echo.
echo ✓ Build successful!
echo   Place your .gguf model in: %MODELS_DIR%
echo   Set: NORNICDB_EMBEDDING_PROVIDER=local
echo   Set: NORNICDB_EMBEDDING_GPU_LAYERS=-1 (all layers on GPU)
goto :eof

:build_cuda_bge
echo ===============================================================
echo  Building: NornicDB CUDA + Local Embeddings + BGE Model
echo  Output:   %BIN_DIR%\nornicdb-cuda-bge.exe
echo ===============================================================
call :check_go
call :check_cuda
call :check_llama_libs_cuda
call :check_model
if not exist "%BIN_DIR%" mkdir "%BIN_DIR%"

set "BUILD_TAGS=cuda localllm"
if defined HEADLESS set "BUILD_TAGS=cuda localllm noui"

set CGO_ENABLED=1
set GOOS=windows
set GOARCH=amd64

if defined HEADLESS (
    go build -tags="%BUILD_TAGS%" -ldflags="-s -w" -o "%BIN_DIR%\nornicdb-cuda-bge-headless.exe" .\cmd\nornicdb
) else (
    go build -tags="%BUILD_TAGS%" -ldflags="-s -w" -o "%BIN_DIR%\nornicdb-cuda-bge.exe" .\cmd\nornicdb
)
if errorlevel 1 goto :build_failed

REM Copy model alongside binary
copy "%MODELS_DIR%\bge-m3.gguf" "%BIN_DIR%\" >nul 2>&1
echo.
echo ✓ Build successful!
echo   Model embedded: %BIN_DIR%\bge-m3.gguf
echo   GPU acceleration enabled - ready to run!
goto :eof

:build_all
echo Building all variants...
echo.
call :build_cpu
call :build_cpu_localllm
call :build_cpu_bge
call :build_cuda
call :build_cuda_bge
echo.
echo ===============================================================
echo  All variants built!
echo ===============================================================
dir /b "%BIN_DIR%\nornicdb*.exe"
goto :eof

REM ==============================================================================
REM Checks
REM ==============================================================================

:check_go
where go >nul 2>&1
if errorlevel 1 (
    echo ERROR: Go not found in PATH. Please install Go 1.23+
    echo        https://go.dev/dl/
    exit /b 1
)
goto :eof

:check_cuda
where nvcc >nul 2>&1
if errorlevel 1 (
    REM Try common CUDA paths
    for %%v in (13.0 12.6 12.4 12.2 12.0) do (
        if exist "C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v%%v\bin\nvcc.exe" (
            set "PATH=C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v%%v\bin;%PATH%"
            echo         Found CUDA v%%v
            goto :eof
        )
    )
    echo ERROR: CUDA Toolkit not found. Please install CUDA Toolkit 12.x
    echo        https://developer.nvidia.com/cuda-downloads
    exit /b 1
)
goto :eof

:check_llama_libs_cpu
if exist "%LIB_DIR%\libllama_windows_amd64.lib" goto :eof
if exist "%LIB_DIR%\libllama_windows_amd64_cpu.lib" goto :eof
echo ERROR: llama.cpp CPU libraries not found!
echo.
echo   Option 1: Build locally
echo     powershell -ExecutionPolicy Bypass -File scripts\build-llama.ps1
echo.
echo   Option 2: Download pre-built
echo     build.bat download-libs
echo.
exit /b 1

:check_llama_libs_cuda
if exist "%LIB_DIR%\libllama_windows_amd64.lib" goto :eof
if exist "%LIB_DIR%\libllama_windows_amd64_cuda.lib" goto :eof
echo ERROR: llama.cpp CUDA libraries not found!
echo.
echo   Option 1: Build locally (requires VS2022 + CUDA Toolkit)
echo     powershell -ExecutionPolicy Bypass -File scripts\build-llama-cuda.ps1
echo.
echo   Option 2: Download pre-built
echo     build.bat download-libs
echo.
exit /b 1

:check_model
if exist "%MODELS_DIR%\bge-m3.gguf" goto :eof
echo ERROR: BGE model not found at %MODELS_DIR%\bge-m3.gguf
echo.
echo   Download with:
echo     build.bat download-model
echo.
exit /b 1

:build_failed
echo.
echo ERROR: Build failed!
exit /b 1

REM ==============================================================================
REM Downloads
REM ==============================================================================

:download_libs
echo ===============================================================
echo  Downloading pre-built llama.cpp libraries
echo ===============================================================
echo.

if not exist "%LIB_DIR%" mkdir "%LIB_DIR%"

echo Downloading CUDA library...
powershell -Command "Invoke-WebRequest -Uri '%RELEASE_URL%/libllama_windows_amd64.lib' -OutFile '%LIB_DIR%\libllama_windows_amd64.lib'"
if errorlevel 1 (
    echo ERROR: Download failed. Build locally instead:
    echo   powershell -ExecutionPolicy Bypass -File scripts\build-llama-cuda.ps1
    goto :eof
)

echo Downloading headers...
for %%h in (llama.h ggml.h ggml-alloc.h ggml-backend.h ggml-cuda.h) do (
    powershell -Command "Invoke-WebRequest -Uri '%RELEASE_URL%/%%h' -OutFile '%LIB_DIR%\%%h'" 2>nul
)

echo.
echo ✓ Libraries downloaded to %LIB_DIR%
goto :eof

:download_model
echo ===============================================================
echo  Downloading BGE-M3 embedding model (~400MB)
echo ===============================================================
echo.

if not exist "%MODELS_DIR%" mkdir "%MODELS_DIR%"

echo Downloading bge-m3-Q4_K_M.gguf...
powershell -Command "Invoke-WebRequest -Uri '%MODEL_URL%' -OutFile '%MODELS_DIR%\bge-m3.gguf'"
if errorlevel 1 (
    echo ERROR: Download failed. Download manually from:
    echo   https://huggingface.co/BAAI/bge-m3-gguf
    goto :eof
)

echo.
echo ✓ Model downloaded to %MODELS_DIR%\bge-m3.gguf
goto :eof

REM ==============================================================================
REM Cleanup
REM ==============================================================================

:clean
echo Cleaning build artifacts...
del /q "%BIN_DIR%\nornicdb*.exe" 2>nul
echo Done.
goto :eof

REM ==============================================================================
REM Help
REM ==============================================================================

:help
echo ===============================================================
echo  NornicDB Windows Build Script
echo ===============================================================
echo.
echo USAGE: build.bat ^<variant^> [headless]
echo.
echo VARIANTS:
echo   cpu              CPU-only, no embeddings (smallest, ~15MB)
echo                    Use with external embedding service (Ollama, OpenAI)
echo.
echo   cpu-localllm     CPU + llama.cpp embeddings, BYOM (~25MB)
echo                    Bring Your Own Model - place .gguf in models/
echo.
echo   cpu-bge          CPU + llama.cpp + BGE model (~425MB)
echo                    Ready to run - no setup needed
echo.
echo   cuda             CUDA + llama.cpp embeddings, BYOM (~30MB)
echo                    GPU-accelerated, requires NVIDIA GPU + CUDA
echo.
echo   cuda-bge         CUDA + llama.cpp + BGE model (~430MB)
echo                    GPU-accelerated with embedded model
echo.
echo OPTIONS:
echo   headless         Build without web UI (add as second argument)
echo.
echo COMMANDS:
echo   download-libs    Download pre-built llama.cpp libraries
echo   download-model   Download BGE-M3 embedding model
echo   all              Build all variants
echo   clean            Remove build artifacts
echo   help             Show this help
echo.
echo EXAMPLES:
echo   build.bat cpu                    Build smallest variant
echo   build.bat cuda-bge               Build GPU version with model
echo   build.bat cuda headless          Build GPU version without UI
echo   build.bat download-libs          Get pre-built libs first
echo   build.bat download-model         Get BGE model
echo.
echo PREREQUISITES:
echo   All variants:    Go 1.23+ (https://go.dev/dl/)
echo   localllm/bge:    Pre-built llama.cpp libs (build.bat download-libs)
echo   cuda variants:   CUDA Toolkit 12.x + NVIDIA GPU
echo   bge variants:    BGE model file (build.bat download-model)
echo.
goto :eof
