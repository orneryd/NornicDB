@echo off
REM Compile GLSL shaders to SPIR-V for NornicDB Vulkan compute
REM Requires Vulkan SDK with glslc compiler

setlocal enabledelayedexpansion

set SCRIPT_DIR=%~dp0

REM Check for glslc
where glslc >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo Error: glslc not found. Please install the Vulkan SDK.
    echo   - Download from: https://vulkan.lunarg.com/sdk/home
    echo   - Add to PATH: %%VULKAN_SDK%%\Bin
    exit /b 1
)

echo Compiling GLSL compute shaders to SPIR-V...

for %%f in ("%SCRIPT_DIR%*.comp") do (
    set "name=%%~nf"
    echo   Compiling !name!.comp -^> !name!.spv
    glslc -fshader-stage=compute -O "%%f" -o "%SCRIPT_DIR%!name!.spv"
    if !ERRORLEVEL! neq 0 (
        echo Error compiling !name!.comp
        exit /b 1
    )
)

echo Done! SPIR-V files generated.
dir /b "%SCRIPT_DIR%*.spv" 2>nul

endlocal
