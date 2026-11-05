param(
    [string]$Package = './...',
    [string]$Timeout = '30m',
    [int]$PackageParallelism = 0,
    [int]$Parallel = 0,
    [switch]$ShowAll,
    [switch]$VerboseTests
)

# Keep PowerShell expressions out of Make recipes: Bash expands $env and
# $LASTEXITCODE inside double quotes before PowerShell can evaluate them.
$env:GOMEMLIMIT = '4GiB'
$goArguments = @('test', '-timeout', $Timeout)
if ($PackageParallelism -gt 0) { $goArguments += @('-p', "$PackageParallelism") }
if ($Parallel -gt 0) { $goArguments += @('-parallel', "$Parallel") }
if ($VerboseTests) { $goArguments += '-v' }
$goArguments += $Package

if ($ShowAll) {
    & go @goArguments
} else {
    & go @goArguments 2>&1 | Select-String -Pattern '^(FAIL|ok|---\s+FAIL)'
}
$goTestExitCode = $LASTEXITCODE
exit $goTestExitCode
