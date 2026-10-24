param(
    [string]$Repository = (Join-Path $PSScriptRoot '..'),
    [string]$Revision = 'release/v1.4.4',
    [string]$ToolsRoot = (Join-Path $PSScriptRoot '..\..\.swarm-local'),
    [string]$CodeIntelEnvironment = (Join-Path $ToolsRoot 'code-intel'),
    [string]$Main = '',
    [switch]$DocumentsOnly
)
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'swarm-local-common.ps1')
$env:NORNICDB_URI = 'http://127.0.0.1:7474'
$env:NORNICDB_DATABASE = 'nornicdbcode'
$env:NORNICDB_USER = 'local'
$env:NORNICDB_PASSWORD = 'local'
$env:PYTHONUTF8 = '1'
$env:CODE_INTEL_GRAPHIFY = Join-Path $CodeIntelEnvironment 'Scripts\graphify.exe'
$body = @{statements=@(@{statement='CREATE DATABASE nornicdbcode IF NOT EXISTS'})} | ConvertTo-Json -Depth 6
$response = Invoke-RestMethod 'http://127.0.0.1:7474/db/system/tx/commit' -Method Post -ContentType 'application/json' -Body $body
if ($response.errors.Count) { throw ($response.errors | ConvertTo-Json) }
$command = if ($DocumentsOnly) { 'ingest-docs' } else { 'ingest' }
$previousMain = $env:CODE_INTEL_MAIN
try {
    if ($Main) { $env:CODE_INTEL_MAIN = $Main }
    Invoke-SwarmCommand (Join-Path $CodeIntelEnvironment 'Scripts\python.exe') -Arguments @((Join-Path $PSScriptRoot 'code_intelligence.py'), $command, '--repo-root', $Repository, '--revision', $Revision)
} finally { $env:CODE_INTEL_MAIN = $previousMain }
Write-Output 'Graph ingestion complete. Check /nornicdb/embed/stats for remaining embedding work before qualifying code search.'
