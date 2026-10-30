@echo off
REM Build CUDA backend DLL for Windows using MSVC
REM This creates ggml-cuda.dll that is loaded at runtime by the base library

echo Building CUDA backend DLL for Windows...

REM Check for Visual Studio
where cl >nul 2>nul
if %errorlevel% neq 0 (
    echo Error: Visual Studio not found or not in PATH
    echo Please run this from "Developer Command Prompt for VS 2022"
    echo Or run: "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat"
    exit /b 1
)

REM Check for CUDA
where nvcc >nul 2>nul
if %errorlevel% neq 0 (
    echo Error: CUDA Toolkit not found
    echo Install from: https://developer.nvidia.com/cuda-downloads
    exit /b 1
)

REM Check for llama.cpp
if not exist "vendor\llama.cpp" (
    echo Error: vendor\llama.cpp not found
    echo Run: git submodule update --init --recursive
    exit /b 1
)

REM Create build directory
if not exist "build-cuda-windows" mkdir build-cuda-windows
cd build-cuda-windows

REM Configure CMake with Visual Studio and CUDA
cmake .. ^
    -G "Visual Studio 17 2022" ^
    -A x64 ^
    -DCMAKE_BUILD_TYPE=Release ^
    -DGGML_BUILD=ON ^
    -DGGML_SHARED=ON ^
    -DGGML_BACKEND_DL=ON ^
    -DGGML_BACKEND_SHARED=ON ^
    -DGGML_CUDA=ON ^
    -DGGML_CUDA_FA=ON ^
    -DCMAKE_CUDA_ARCHITECTURES="60;70;75;80;86;89;90" ^
    -DGGML_METAL=OFF ^
    -DGGML_VULKAN=OFF ^
    -DGGML_OPENMP=OFF ^
    -S vendor\llama.cpp\ggml

if %errorlevel% neq 0 (
    echo Error: CMake configuration failed
    cd ..
    exit /b 1
)

REM Build
cmake --build . --config Release

if %errorlevel% neq 0 (
    echo Error: Build failed
    cd ..
    exit /b 1
)

REM Create output directory
if not exist "..\lib\llama" mkdir ..\lib\llama

REM Copy DLLs
if exist "bin\Release\ggml-cuda.dll" (
    copy /Y "bin\Release\ggml-cuda.dll" "..\lib\llama\"
    echo ✓ Built: lib\llama\ggml-cuda.dll
) else if exist "Release\ggml-cuda.dll" (
    copy /Y "Release\ggml-cuda.dll" "..\lib\llama\"
    echo ✓ Built: lib\llama\ggml-cuda.dll
) else (
    echo Error: Could not find ggml-cuda.dll
    cd ..
    exit /b 1
)

REM Copy CUDA runtime DLLs (required for distribution)
where cudart64_12.dll >nul 2>nul
if %errorlevel% equ 0 (
    for /f "delims=" %%i in ('where cudart64_12.dll') do (
        copy /Y "%%i" "..\lib\llama\"
        echo ✓ Copied: cudart64_12.dll
        goto :found_cudart
    )
)
:found_cudart

where cublas64_12.dll >nul 2>nul
if %errorlevel% equ 0 (
    for /f "delims=" %%i in ('where cublas64_12.dll') do (
        copy /Y "%%i" "..\lib\llama\"
        echo ✓ Copied: cublas64_12.dll
        goto :found_cublas
    )
)
:found_cublas

cd ..

echo.
echo ✓ CUDA backend DLL built successfully!
echo.
echo Files created:
echo   - lib\llama\ggml-cuda.dll       (CUDA backend)
echo   - lib\llama\cudart64_12.dll     (CUDA runtime)
echo   - lib\llama\cublas64_12.dll     (cuBLAS library)
echo.
echo Next steps:
echo 1. Copy these DLLs alongside nornicdb.exe when distributing
echo 2. NornicDB will automatically detect and use CUDA at runtime
echo.
