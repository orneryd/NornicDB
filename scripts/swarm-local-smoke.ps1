param(
    [string]$BaseUrl = 'http://127.0.0.1:7474',
    [switch]$RecreateContainer
)
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'swarm-local-common.ps1')

function Invoke-Cypher([string]$Statement, [hashtable]$Parameters = @{}) {
    $body = @{ statements = @(@{ statement = $Statement; parameters = $Parameters }) } | ConvertTo-Json -Depth 12
    $response = Invoke-RestMethod -Method Post -Uri "$BaseUrl/db/nornic/tx/commit" -ContentType 'application/json' -Body $body
    if ($response.errors.Count -gt 0) { throw ($response.errors | ConvertTo-Json -Depth 6) }
    return $response.results
}

$health = Invoke-RestMethod "$BaseUrl/health"
Write-Output ('Health: ' + ($health | ConvertTo-Json -Compress))
$result = Invoke-Cypher 'RETURN 1 AS value'
if ($result[0].data[0].row[0] -ne 1) { throw 'Cypher round trip failed' }

$result = Invoke-Cypher 'RETURN apoc.text.join(["T3", "NornicDB"], " + ") AS value'
if ($result[0].data[0].row[0] -ne 'T3 + NornicDB') { throw 'APOC call failed' }

$id = [guid]::NewGuid().ToString()
try {
    $null = Invoke-Cypher 'CREATE (n:SwarmLocalSmoke {id: $id, value: "plugin-test"}) RETURN n.id' @{ id = $id }
    $result = Invoke-Cypher 'MATCH (n:SwarmLocalSmoke {id: $id}) RETURN n.value' @{ id = $id }
    if ($result[0].data[0].row[0] -ne 'plugin-test') { throw 'Graph read after write failed' }
    if ($RecreateContainer) {
        $compose = Join-Path $PSScriptRoot '..\docker\docker-compose.swarm-local.yml'
        Invoke-SwarmCommand docker -Arguments @('compose', '--env-file', (Join-Path $PSScriptRoot '..\docker\.env.swarm-local'), '-f', $compose, 'up', '-d', '--force-recreate')
        $ready = $false
        for ($attempt = 0; $attempt -lt 30; $attempt++) {
            try { $null = Invoke-RestMethod "$BaseUrl/health"; $ready = $true; break } catch { Start-Sleep -Seconds 1 }
        }
        if (!$ready) { throw 'Database did not become ready after recreation' }
        $result = Invoke-Cypher 'MATCH (n:SwarmLocalSmoke {id: $id}) RETURN n.value' @{ id = $id }
        if ($result[0].data[0].row[0] -ne 'plugin-test') { throw 'Persistent volume did not preserve the graph' }
        Write-Output 'PASS: graph survived container recreation'
    }
} finally {
    $null = Invoke-Cypher 'MATCH (n:SwarmLocalSmoke {id: $id}) DETACH DELETE n' @{ id = $id }
}

$body = @{ jsonrpc = '2.0'; id = 1; method = 'tools/list'; params = @{} } | ConvertTo-Json -Depth 8
$response = Invoke-RestMethod -Method Post -Uri "$BaseUrl/mcp" -ContentType 'application/json' -Body $body
if ($response.error) { throw ($response.error | ConvertTo-Json) }
if (-not $response.result.tools.Count) { throw 'MCP tools/list returned no tools' }
Write-Output ('MCP tools: ' + (($response.result.tools | ForEach-Object { $_.name }) -join ', '))
Write-Output 'PASS: HTTP Cypher, APOC, graph write/read/delete, MCP discovery'
