param([switch]$AfterBuild)
$ErrorActionPreference = "Stop"
if ($AfterBuild) {
    $backend = (Get-Content .gpu-backend -ErrorAction SilentlyContinue) -replace 'GPU_BACKEND=',''
    Remove-Item .gpu-backend -ErrorAction SilentlyContinue
    Write-Host ''
    if ($backend -eq 'cuda') {
        Write-Host 'Next: Run scripts\setup-windows-cuda.bat to install runtime libraries' -ForegroundColor Green
    } elseif ($backend -eq 'vulkan') {
        Write-Host 'Next: Run scripts\setup-windows-vulkan.bat to install runtime libraries' -ForegroundColor Green
    } else {
        Write-Host 'Note: GPU acceleration disabled. For better performance:' -ForegroundColor Yellow
        Write-Host '  NVIDIA: Install CUDA Toolkit from https://developer.nvidia.com/cuda-downloads' -ForegroundColor Yellow
        Write-Host '  AMD/Intel: Install Vulkan SDK from https://vulkan.lunarg.com/sdk/home' -ForegroundColor Yellow
    }
} else {
    $forceBackend = $env:NORNICDB_GPU_BACKEND
    $cuda = $false
    $vulkan = $false
    if ($forceBackend) {
        Write-Host '  [**] Forced backend: '$forceBackend -ForegroundColor Magenta
    }
    if (Get-Command nvcc -ErrorAction SilentlyContinue) {
        $cudaVer = (nvcc --version 2>$null | Select-String 'release' | ForEach-Object { $_ -replace '.*release ([0-9.]+).*','$1' })
        if ($cudaVer) {
            Write-Host '  [OK] CUDA Toolkit detected: v'$cudaVer -ForegroundColor Green
            $cuda = $true
        }
    }
    if (-not $cuda) {
        if (Get-Command nvidia-smi -ErrorAction SilentlyContinue) {
            Write-Host '  [!!] NVIDIA GPU detected but CUDA Toolkit not installed' -ForegroundColor Yellow
            Write-Host '       Install from: https://developer.nvidia.com/cuda-downloads' -ForegroundColor Yellow
        } else {
            Write-Host '  [--] No NVIDIA GPU/CUDA detected' -ForegroundColor Gray
        }
    }
    if (Get-Command vulkaninfo -ErrorAction SilentlyContinue) {
        Write-Host '  [OK] Vulkan SDK detected' -ForegroundColor Green
        $vulkan = $true
    } elseif (Test-Path 'C:\VulkanSDK') {
        Write-Host '  [OK] Vulkan SDK found at C:\VulkanSDK' -ForegroundColor Green
        $vulkan = $true
    } else {
        Write-Host '  [--] No Vulkan SDK detected' -ForegroundColor Gray
    }
    Write-Host ''
    if ($forceBackend -eq 'vulkan' -and $vulkan) {
        Write-Host 'Building with Vulkan support (forced)...' -ForegroundColor Cyan
        'GPU_BACKEND=vulkan' | Out-File -FilePath .gpu-backend -Encoding ascii
    } elseif ($forceBackend -eq 'cuda' -and $cuda) {
        Write-Host 'Building with CUDA support (forced)...' -ForegroundColor Cyan
        'GPU_BACKEND=cuda' | Out-File -FilePath .gpu-backend -Encoding ascii
    } elseif ($forceBackend -eq 'cpu') {
        Write-Host 'Building CPU-only (forced)...' -ForegroundColor Yellow
        'GPU_BACKEND=cpu' | Out-File -FilePath .gpu-backend -Encoding ascii
    } elseif ($cuda) {
        Write-Host 'Building with CUDA support (NVIDIA GPU)...' -ForegroundColor Cyan
        'GPU_BACKEND=cuda' | Out-File -FilePath .gpu-backend -Encoding ascii
    } elseif ($vulkan) {
        Write-Host 'Building with Vulkan support (Universal GPU)...' -ForegroundColor Cyan
        'GPU_BACKEND=vulkan' | Out-File -FilePath .gpu-backend -Encoding ascii
    } else {
        Write-Host 'Building CPU-only (no GPU acceleration)...' -ForegroundColor Yellow
        'GPU_BACKEND=cpu' | Out-File -FilePath .gpu-backend -Encoding ascii
    }
}
