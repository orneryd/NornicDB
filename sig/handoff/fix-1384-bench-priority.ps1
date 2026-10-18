# Same credential-cache transport as bench1043/tnrun.ps1; credentials go only to the client child.
param([switch]$Cleanup,[switch]$Resume)
$ErrorActionPreference = 'Stop'
$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$credentialScripts = 'C:\Users\mm0ns\.claude\skills\onepassword-maintenance\scripts'
. (Join-Path $credentialScripts 'OnePasswordCli.ps1')
. (Join-Path $credentialScripts 'credential-cache.ps1')
$itemReference = "op://$VaultId/tkm5vx3rzinlitmbx62am67fxy"
$itemJson = Get-OnePasswordCachedValue -Reference $itemReference
if ($null -eq $itemJson) { throw 'Existing cached TrueNAS credentials required; no credential acquisition attempted' }
$item = $itemJson | ConvertFrom-Json
$psi = [System.Diagnostics.ProcessStartInfo]::new('node')
$psi.ArgumentList.Add((Join-Path $repoRoot 'sig/handoff/fix-1384-bench-priority-client.mjs'))
if ($Cleanup) { $psi.ArgumentList.Add('--cleanup') }
if ($Resume) { $psi.ArgumentList.Add('--resume') }
$psi.UseShellExecute = $false
$psi.CreateNoWindow = $true
$psi.RedirectStandardOutput = $true
$psi.RedirectStandardError = $true
$psi.Environment['TRUENAS_USERNAME'] = ($item.fields | Where-Object id -eq 'username').value
$psi.Environment['TRUENAS_PASSWORD'] = ($item.fields | Where-Object id -eq 'password').value
$psi.Environment['TRUENAS_ATTEMPT_LOG'] = (Join-Path $PSScriptRoot 'fix-1384-bench-priority-attempts.jsonl')
try {
 $child = [System.Diagnostics.Process]::Start($psi)
 $stdout = $child.StandardOutput.ReadToEndAsync()
 $stderr = $child.StandardError.ReadToEndAsync()
 $child.WaitForExit()
 Write-Output ($stdout.GetAwaiter().GetResult())
 Write-Output ($stderr.GetAwaiter().GetResult())
 exit $child.ExitCode
} finally { $item=$null; $itemJson=$null }
