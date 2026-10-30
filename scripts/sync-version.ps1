# sync-version.ps1
# Windows PowerShell port of scripts/sync-version.sh.
#
# Resolves the project version (from $env:VERSION override, latest git tag, or
# pkg/buildinfo/VERSION) and synchronises it into:
#   - pkg/buildinfo/VERSION
#   - README.md badge markup
#
# This file is invoked from the Makefile's `sync-version` target on Windows
# because the POSIX shell variant requires `sh` and `perl`, neither of which
# is guaranteed on a stock Windows toolchain.

$ErrorActionPreference = 'Stop'

$RootDir     = (Resolve-Path -Path (Join-Path $PSScriptRoot '..')).Path
$VersionFile = Join-Path $RootDir 'pkg/buildinfo/VERSION'
$ReadmeFile  = Join-Path $RootDir 'README.md'

function Get-CleanVersion {
    param([string]$Value)
    if ($null -eq $Value) { return '' }
    return ($Value -replace '\s', '')
}

$override = Get-CleanVersion $env:VERSION
$version  = $null

if ($override -and $override -ne 'latest') {
    $version = $override -replace '^v', ''
} elseif (Get-Command git -ErrorAction SilentlyContinue) {
    $latestTag = (& git -C $RootDir tag --sort=-version:refname 2>$null | Select-Object -First 1)
    if ($latestTag) {
        $version = ($latestTag -replace '^v', '').Trim()
    }
}

if (-not $version -and (Test-Path $VersionFile)) {
    $version = Get-CleanVersion (Get-Content -Raw $VersionFile)
}

if (-not $version) {
    Write-Error "sync-version: unable to determine version from git tags or $VersionFile"
    exit 1
}

# Update pkg/buildinfo/VERSION only if changed (avoid spurious mtime churn).
$newContent     = "$version`n"
$currentContent = if (Test-Path $VersionFile) { Get-Content -Raw $VersionFile } else { '' }
if ($currentContent -ne $newContent) {
    Set-Content -Path $VersionFile -Value $newContent -NoNewline
    Write-Host "sync-version: updated pkg/buildinfo/VERSION to $version"
}

# Update README badge markup. Mirrors the perl substitutions in sync-version.sh.
if (Test-Path $ReadmeFile) {
    $readme  = Get-Content -Raw $ReadmeFile
    $updated = [System.Text.RegularExpressions.Regex]::Replace(
        $readme,
        'version-\d+\.\d+\.\d+-success',
        "version-$version-success"
    )
    $updated = [System.Text.RegularExpressions.Regex]::Replace(
        $updated,
        'alt="Version \d+\.\d+\.\d+"',
        "alt=`"Version $version`""
    )
    if ($updated -ne $readme) {
        Set-Content -Path $ReadmeFile -Value $updated -NoNewline
    }
    Write-Host "sync-version: README badge set to $version"
}
