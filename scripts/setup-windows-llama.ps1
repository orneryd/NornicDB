param(
    [ValidateSet('cpu', 'cuda', 'vulkan')]
    [string]$Processor = 'cuda',
    [Parameter(Position = 0)]
    [string]$LibPath
)

$ErrorActionPreference = 'Stop'
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
if (-not $LibPath) { $LibPath = Join-Path $repoRoot 'lib/llama' }
$LibPath = [IO.Path]::GetFullPath($LibPath)
$llamaVersion = (Get-Content -LiteralPath (Join-Path $repoRoot 'lib/llama/VERSION') -Raw).Trim()
$yzmaVersion = & go -C $repoRoot list -m -f '{{.Version}}' github.com/hybridgroup/yzma
if ($LASTEXITCODE -ne 0 -or -not $yzmaVersion) {
    throw 'Cannot resolve the repository-pinned yzma version.'
}
$yzmaVersion = $yzmaVersion.Trim()
New-Item -ItemType Directory -Path $LibPath -Force | Out-Null
Write-Host "Installing llama.cpp $llamaVersion ($Processor) with yzma $yzmaVersion into $LibPath"
# The supported CLI is at the module root. An explicit module version avoids
# reusing a stale globally installed CLI or changing the application's go.mod.
& go run "github.com/hybridgroup/yzma@$yzmaVersion" install --lib $LibPath --processor $Processor --version $llamaVersion --upgrade
if ($LASTEXITCODE -ne 0) { throw 'llama.cpp library installation failed.' }
Write-Host "Set the runtime library directory in PowerShell:"
Write-Host ('$env:NORNICDB_LIB = ' + "'$LibPath'")
