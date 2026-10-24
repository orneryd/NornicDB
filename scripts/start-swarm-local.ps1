param(
    [string]$ToolsRoot = (Join-Path $PSScriptRoot '..\..\.swarm-local'),
    [string]$T3Root = (Join-Path $ToolsRoot 'T3'),
    [string]$StateRoot = (Join-Path $ToolsRoot 'state'),
    [string]$CodeIntelEnvironment = (Join-Path $ToolsRoot 'code-intel'),
    [string]$CheckerRepository = (Join-Path $PSScriptRoot '..'),
    [string]$GitDirectory = ''
)
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'swarm-local-common.ps1')
$pathsFile = Join-Path $PSScriptRoot '..\docker\.swarm-local.paths.json'
if (!( $PSBoundParameters.ContainsKey('ToolsRoot') ) -and (Test-Path $pathsFile)) {
    $paths = Get-Content $pathsFile -Raw | ConvertFrom-Json
    foreach ($name in @('T3Root', 'StateRoot', 'CodeIntelEnvironment', 'CheckerRepository', 'GitDirectory')) {
        if (!$PSBoundParameters.ContainsKey($name) -and $paths.$name) {
            Set-Variable -Name $name -Value ([string]$paths.$name)
        }
    }
}
if ($GitDirectory) { $env:PATH = $GitDirectory + ';' + $env:PATH }
$env:NORNICDB_WORKFLOW_URL = 'http://127.0.0.1:7474'
$env:NORNICDB_WORKFLOW_DATABASE = 't3workflow'
$env:NORNIC_CODE_DATABASE = 'nornicdbcode'
$env:NORNIC_OLLAMA_URL = 'http://127.0.0.1:11434'
$env:NORNIC_CLASSIFIER_MODEL = 'nornic-coder'
$env:NORNIC_FRONTIER_INSTANCES = 'codex,claude'
# Use the same context admission owner as the Unix launcher. Read the installed
# provider manifest; emit only non-secret context/model configuration.
$contextPython = Join-Path $CodeIntelEnvironment 'Scripts\python.exe'
$contextConfiguration = Invoke-SwarmCommand $contextPython -Arguments @(
    (Join-Path $PSScriptRoot 'swarm_context.py'), '--models',
    (Join-Path $ToolsRoot 'pi-state\models.json'), '--provider', 'ollama', '--model', 'nornic-agent-remote'
) | ConvertFrom-Json
foreach ($property in $contextConfiguration.PSObject.Properties) {
    [Environment]::SetEnvironmentVariable($property.Name, [string]$property.Value, 'Process')
}
$env:NORNIC_DUPLICATE_PYTHON = Join-Path $CodeIntelEnvironment 'Scripts\python.exe'
$env:NORNIC_GRAPHIFY = Join-Path $CodeIntelEnvironment 'Scripts\graphify.exe'
$env:NORNIC_DUPLICATE_RUNNER = Join-Path $CheckerRepository 'scripts\code_intelligence.py'
# The local Docker instance has authentication disabled. These satisfy the
# importer's explicit connection configuration; they are not account credentials.
$env:NORNICDB_USER = 'local'
$env:NORNICDB_PASSWORD = 'local'
$env:PYTHONUTF8 = '1'
$privateEnv = Join-Path $PSScriptRoot '..\docker\.env.swarm-local'
foreach ($line in Get-Content $privateEnv) {
    if ($line -match '^\s*LINEAR_API_KEY\s*=\s*(.+?)\s*$') {
        $env:LINEAR_API_KEY = $Matches[1].Trim('"').Trim("'")
    }
}

$compose = Join-Path $PSScriptRoot '..\docker\docker-compose.swarm-local.yml'
Invoke-SwarmCommand docker -Arguments @('compose', '--env-file', (Join-Path $PSScriptRoot '..\docker\.env.swarm-local'), '-f', $compose, 'up', '-d')
$ready = $false
for ($attempt = 0; $attempt -lt 30; $attempt++) {
    try { $null = Invoke-RestMethod "$env:NORNICDB_WORKFLOW_URL/health"; $ready = $true; break } catch { Start-Sleep -Seconds 1 }
}
if (!$ready) { throw 'NornicDB did not become ready' }
$body = @{statements=@(@{statement='CREATE DATABASE t3workflow IF NOT EXISTS'})} | ConvertTo-Json -Depth 6
$response = Invoke-RestMethod "$env:NORNICDB_WORKFLOW_URL/db/system/tx/commit" -Method Post -ContentType 'application/json' -Body $body
if ($response.errors.Count) { throw ($response.errors | ConvertTo-Json) }
. (Join-Path $env:APPDATA 'vite-plus\env.ps1')
Push-Location $T3Root
try {
    vp run dev --home-dir $StateRoot --no-seed
} finally {
    Pop-Location
}
