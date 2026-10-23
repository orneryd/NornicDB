function Invoke-SwarmCommand {
    param([string]$Command, [string[]]$Arguments, [switch]$QuietStderr)
    $previousPreference = $ErrorActionPreference
    try {
        # Windows PowerShell 5 may turn native stderr progress into ErrorRecords.
        # Preserve command output while judging success by the actual exit code.
        $ErrorActionPreference = 'Continue'
        if ($QuietStderr) {
            & $Command @Arguments 2>$null
        } else {
            & $Command @Arguments 2>&1 | ForEach-Object { $_.ToString() }
        }
        $commandExit = $LASTEXITCODE
    } finally { $ErrorActionPreference = $previousPreference }
    if ($commandExit -ne 0) { throw "$Command failed with exit code $commandExit" }
}
