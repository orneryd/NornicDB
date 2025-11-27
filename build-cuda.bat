@echo off
REM Build NornicDB with CUDA support using MSVC
REM Run this from a regular command prompt - it will set up the VS environment

REM Initialize VS 2022 Build Tools x64 environment
call "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvars64.bat"

REM Set Go CGO to use MSVC
set CGO_ENABLED=1
set CC=cl
set CXX=cl

REM Build with cuda tag
echo Building NornicDB with CUDA support...
go build -tags cuda -o bin\nornicdb.exe .\cmd\nornicdb
if %ERRORLEVEL% NEQ 0 (
    echo CUDA build failed!
    exit /b 1
)

go build -o bin\nornicdb-bolt.exe .\cmd\nornicdb-bolt
if %ERRORLEVEL% NEQ 0 (
    echo Bolt build failed!
    exit /b 1
)

echo.
echo Build successful!
echo   bin\nornicdb.exe (with CUDA)
echo   bin\nornicdb-bolt.exe
