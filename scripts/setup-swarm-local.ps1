param(
    [string]$ToolsRoot = (Join-Path $PSScriptRoot '..\..\.swarm-local'),
    [string]$CodexBinary = '',
    [switch]$BuildImage
)
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$ToolsRoot = [IO.Path]::GetFullPath($ToolsRoot)
$t3 = Join-Path $ToolsRoot 'T3'
$pin = '2c8bdf6177159d8fcd12a897d7c9311f6eb93237'
. (Join-Path $PSScriptRoot 'swarm-local-common.ps1')
foreach ($tool in @('git', 'docker', 'ollama', 'uv', 'npm', 'vp')) {
    if (!(Get-Command $tool -ErrorAction SilentlyContinue)) { throw "Install $tool first; see docs/development/swarm-local.md" }
}
# The importer and T3 need modern worktree porcelain support.
Push-Location $repo
try { $null = Invoke-SwarmCommand git -QuietStderr -Arguments @('worktree', 'list', '--porcelain', '-z') } finally { Pop-Location }
New-Item -ItemType Directory -Force $ToolsRoot | Out-Null
if (!(Test-Path $t3)) { Invoke-SwarmCommand git -Arguments @('clone', 'https://github.com/pingdotgg/t3code.git', $t3) }
Push-Location $t3
try {
    $head = (Invoke-SwarmCommand git -QuietStderr -Arguments @('rev-parse', 'HEAD')).Trim()
    if ($head -ne $pin) {
        if (Invoke-SwarmCommand git -QuietStderr -Arguments @('status', '--porcelain')) { throw 'T3 has local changes; use a fresh ToolsRoot.' }
        Invoke-SwarmCommand git -Arguments @('checkout', '--detach', $pin)
    }
    $patch = Join-Path $repo 'testing\swarm-local\t3-nornic.patch'
    $ErrorActionPreference = 'Continue'
    & git apply --reverse --check $patch 2>$null
    $reverseExit = $LASTEXITCODE
    $ErrorActionPreference = 'Stop'
    if ($reverseExit -ne 0) {
        Invoke-SwarmCommand git -Arguments @('apply', '--check', $patch)
        Invoke-SwarmCommand git -Arguments @('apply', $patch)
    }
    Invoke-SwarmCommand vp -Arguments @('install', '--frozen-lockfile')
} finally { Pop-Location }
$provider = Join-Path $ToolsRoot 'pi'
New-Item -ItemType Directory -Force $provider | Out-Null
Invoke-SwarmCommand npm -Arguments @('install', '--prefix', $provider, '@earendil-works/pi-coding-agent@1.4.0')
$venv = Join-Path $ToolsRoot 'code-intel'
if (!(Test-Path (Join-Path $venv 'Scripts\python.exe'))) {
    Invoke-SwarmCommand uv -Arguments @('venv', '--python', '3.13', $venv)
}
Invoke-SwarmCommand uv -Arguments @('pip', 'install', '--python', (Join-Path $venv 'Scripts\python.exe'), '-r', (Join-Path $repo 'scripts\requirements-code-intelligence.txt'))
Invoke-SwarmCommand ollama -Arguments @('pull', 'qwen3.5:9b')
Invoke-SwarmCommand ollama -Arguments @('create', 'nornic-coder', '-f', (Join-Path $repo 'testing\swarm-local\Coder.Modelfile'))
$piHome = Join-Path $ToolsRoot 'pi-state'
$userdata = Join-Path $ToolsRoot 'state\userdata'
New-Item -ItemType Directory -Force $piHome, $userdata | Out-Null
$utf8 = New-Object Text.UTF8Encoding($false)
$models = @{providers=@{ollama=@{baseUrl='http://127.0.0.1:11434/v1';api='openai-completions';apiKey='ollama';models=@(@{id='nornic-coder';name='Qwen 3.5 9B Local (32K)';reasoning=$false;input=@('text');contextWindow=32768;maxTokens=4096;samplingParams=@{reasoning_effort='none'};cost=@{input=0;output=0;cacheRead=0;cacheWrite=0}})}}}
[IO.File]::WriteAllText((Join-Path $piHome 'models.json'), ($models | ConvertTo-Json -Depth 15), $utf8)
$settingsPath = Join-Path $userdata 'settings.json'
if (!(Test-Path $settingsPath)) {
    $instances = @{ollama=@{driver='pi';displayName='Ollama Local';enabled=$true;environment=@(@{name='PI_CODING_AGENT_DIR';value=$piHome;sensitive=$false},@{name='T3_NORNIC_LOCAL_WORKER';value='1';sensitive=$false});config=@{enabled=$true;binaryPath=(Join-Path $provider 'node_modules\.bin\pi.cmd');launchArgs='--provider ollama --model nornic-coder --no-context-files --no-skills --no-builtin-tools';customModels=@()}}}
    if (!$CodexBinary) {
        $found = Get-Command codex -ErrorAction SilentlyContinue
        if ($found) { $CodexBinary = $found.Source }
    }
    if ($CodexBinary) { $instances.codex=@{driver='codex';displayName='Codex Frontier';enabled=$true;config=@{enabled=$true;binaryPath=$CodexBinary}} }
    [IO.File]::WriteAllText($settingsPath, (@{providerInstances=$instances} | ConvertTo-Json -Depth 15), $utf8)
}
$envPath = Join-Path $repo 'docker\.env.swarm-local'
if (!(Test-Path $envPath)) { Copy-Item (Join-Path $repo 'docker\.env.swarm-local.example') $envPath }
if ($BuildImage) {
    $release = Join-Path $ToolsRoot 'NornicDB-release'
    if (!(Test-Path $release)) { Invoke-SwarmCommand git -Arguments @('clone', '--branch', 'release/v1.4.4', '--single-branch', 'https://github.com/orneryd/NornicDB.git', $release) }
    Push-Location $release
    try {
        if (Invoke-SwarmCommand git -QuietStderr -Arguments @('status', '--porcelain')) { throw 'Release build checkout has local changes.' }
        Invoke-SwarmCommand git -Arguments @('checkout', '--detach', 'ba8d6d13b48452ec4e38d826869ad63a64063848')
        Invoke-SwarmCommand docker -Arguments @('build', '-f', 'docker/Dockerfile.amd64-cuda', '--build-arg', 'LLAMA_CUDA_IMAGE=timothyswt/llama-cuda-libs:v0.4.1', '-t', 'nornicdb:1.4.4-cuda-local', '.')
    } finally { Pop-Location }
}
Write-Output "Setup complete. Set OPENAI_API_KEY in $envPath, authenticate a frontier provider, then run scripts/start-swarm-local.ps1 -ToolsRoot `"$ToolsRoot`"."
