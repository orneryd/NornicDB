@echo off
REM Install the repository-pinned CUDA llama.cpp libraries.
powershell.exe -NoProfile -File "%~dp0setup-windows-llama.ps1" -Processor cuda %*
exit /b %ERRORLEVEL%
