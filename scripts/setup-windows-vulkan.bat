@echo off
REM Install the repository-pinned Vulkan llama.cpp libraries.
powershell.exe -NoProfile -File "%~dp0setup-windows-llama.ps1" -Processor vulkan %*
exit /b %ERRORLEVEL%
