$ErrorActionPreference = 'Stop'
$taskRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$cells = [Collections.Generic.List[object]]::new()
$diffs = [Collections.Generic.List[object]]::new()
$totals = [Collections.Generic.List[object]]::new()
$jobs = [Collections.Generic.List[object]]::new()
foreach ($file in Get-ChildItem -LiteralPath $PSScriptRoot -Filter 'fix-1384-r73-*.log') {
    if ($file.Name -notmatch '^fix-1384-r73-(dev|release)-(bolt|http)(?:-(extra|params|projection|tx|codepaths|rewrites))?\.log$') { continue }
    $headRole = $Matches[1]
    $phase = if ($Matches[3]) { $Matches[3] } else { 'base' }
    $parser = 'unknown'
    $jobId = ''
    $exitCode = $null
    foreach ($line in [IO.File]::ReadLines($file.FullName)) {
        if ($line -match '^=== parser (default|antlr):') { $parser = $Matches[1] }
        if ($line -match '^tnrun: job (i\S+): .*submitted') { $jobId = $Matches[1] }
        if ($line -match '^tnrun: job (i\S+): DONE exit (\d+)') { $jobId = $Matches[1]; $exitCode = [int]$Matches[2] }
        if ($line -match 'R73_CELL (\S+) (.+) match=(\d+) diff=(\d+)$') {
            $route,$state,$matched,$different = $Matches[1],$Matches[2],[int]$Matches[3],[int]$Matches[4]
            $member = $state.Split('/')[0]
            $cells.Add([pscustomobject]@{Head=$headRole;Parser=$parser;Route=$route;Phase=$phase;Member=$member;State=$state;Match=$matched;Diff=$different})
        }
        if ($line -match 'R73_TOTAL (\S+) compared=(\d+) match=(\d+) diff=(\d+)') {
            $totals.Add([pscustomobject]@{Head=$headRole;Parser=$parser;Route=$Matches[1];Phase=$phase;Compared=[int]$Matches[2];Match=[int]$Matches[3];Diff=[int]$Matches[4]})
        }
        if ($line -match 'R73_DIFF (\S+) (\{.*)$') {
            $route=$Matches[1]
            $result=$Matches[2]|ConvertFrom-Json
            $category=if(-not $result.neo4j.code -and -not $result.nornicdb.code){'different rows'}elseif($result.neo4j.code -and -not $result.nornicdb.code){'Neo4j-only error'}elseif(-not $result.neo4j.code -and $result.nornicdb.code){'NornicDB-only error'}else{'different error codes'}
            $diffs.Add([pscustomobject]@{Head=$headRole;Parser=$parser;Route=$route;Phase=$phase;Category=$category;Result=$result})
        }
    }
    $jobs.Add([pscustomobject]@{File=$file.Name;Id=$jobId;Exit=$exitCode})
}
$cells | Export-Csv -LiteralPath (Join-Path $PSScriptRoot 'fix-1384-rule73-cells.csv') -NoTypeInformation
$totals | Export-Csv -LiteralPath (Join-Path $PSScriptRoot 'fix-1384-rule73-totals.csv') -NoTypeInformation
$jobs | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $PSScriptRoot 'fix-1384-rule73-jobs.json')
$diffs | ConvertTo-Json -Depth 35 -Compress | Set-Content -LiteralPath (Join-Path $PSScriptRoot 'fix-1384-rule73-differences.json')
$family = foreach ($group in ($cells | Group-Object Head,Parser,Route,Member)) {
    $first = $group.Group[0]
    [pscustomobject]@{Head=$first.Head;Parser=$first.Parser;Route=$first.Route;Member=$first.Member;Match=($group.Group|Measure-Object Match -Sum).Sum;Diff=($group.Group|Measure-Object Diff -Sum).Sum}
}
$family | Export-Csv -LiteralPath (Join-Path $PSScriptRoot 'fix-1384-rule73-members.csv') -NoTypeInformation
$family | Where-Object {$_.Parser -eq 'default' -and $_.Route -eq 'bolt-auto'} | Sort-Object Member,Head | Format-Table -AutoSize
Write-Output 'Totals per head / parser / route:'
$totals | Group-Object Head,Parser,Route | ForEach-Object {
    [pscustomobject]@{Key=$_.Name;Compared=($_.Group|Measure-Object Compared -Sum).Sum;Match=($_.Group|Measure-Object Match -Sum).Sum;Diff=($_.Group|Measure-Object Diff -Sum).Sum}
} | Format-Table -AutoSize
Write-Output 'Current owned jobs:'
$jobs | Format-Table -AutoSize

