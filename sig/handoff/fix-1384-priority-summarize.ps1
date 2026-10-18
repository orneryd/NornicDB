$ErrorActionPreference='Stop'
$totals=[Collections.Generic.List[object]]::new()
$diffs=[Collections.Generic.List[object]]::new()
$jobs=[Collections.Generic.List[object]]::new()
foreach($inputLog in @(
 @{Role='release';File='fix-1384-priority-release.log'},
 @{Role='v143';File='fix-1384-priority-v143.log'},
 @{Role='v143';File='fix-1384-priority-v143-antlr-retry.log'}
)){
 $parser='unknown';$phase='unknown';$job='';$done=$false
 foreach($line in [IO.File]::ReadLines((Join-Path $PSScriptRoot $inputLog.File))){
  if($line -match '^=== parser (default|antlr):'){$parser=$Matches[1]}
  if($line -match 'R73_PHASE (\S+)'){$phase=$Matches[1]}
  if($line -match '^tnrun: job (i\S+): .*submitted'){$job=$Matches[1]}
  if($line -match '^tnrun: job (i\S+): DONE exit (\d+)'){$job=$Matches[1];$done=$true}
  if($line -match 'R73_TOTAL (\S+) compared=(\d+) match=(\d+) diff=(\d+)'){
   $totals.Add([pscustomobject]@{Head=$inputLog.Role;Parser=$parser;Phase=$phase;Compared=[int]$Matches[2];Match=[int]$Matches[3];Diff=[int]$Matches[4]})
  }
  if($line -match 'R73_DIFF (\S+) (\{.*)$'){
   $diffs.Add([pscustomobject]@{Head=$inputLog.Role;Parser=$parser;Phase=$phase;Result=($Matches[2]|ConvertFrom-Json)})
  }
 }
 if(-not $done){throw "Job not terminal: $($inputLog.File)"}
 $jobs.Add([pscustomobject]@{Head=$inputLog.Role;Id=$job;File=$inputLog.File;Terminal=$done})
}
$totals|Export-Csv (Join-Path $PSScriptRoot 'fix-1384-priority-totals.csv') -NoTypeInformation
$diffs|ConvertTo-Json -Depth 45 -Compress|Set-Content (Join-Path $PSScriptRoot 'fix-1384-priority-differences.json')
$jobs|ConvertTo-Json|Set-Content (Join-Path $PSScriptRoot 'fix-1384-priority-jobs.json')
$summary=@($totals|Group-Object Head,Parser|ForEach-Object{
 if($_.Count -ne 7){throw "Missing phase in $($_.Name)"}
 [pscustomobject]@{Cell=$_.Name;Compared=($_.Group|Measure-Object Compared -Sum).Sum;Match=($_.Group|Measure-Object Match -Sum).Sum;Diff=($_.Group|Measure-Object Diff -Sum).Sum}
})
if($summary.Count -ne 4 -or @($summary|Where-Object Compared -ne 1254).Count){throw 'Incomplete matrix'}

# Error codes define error equivalence. Graph snapshots are unordered rows;
# ordinary query rows and list contents retain their order.
function OutcomeKey($result){
 $outcome=$result.nornicdb
 if($outcome.code){return 'code:'+$outcome.code}
 $rows=@($outcome.rows|ForEach-Object{ConvertTo-Json -InputObject $_ -Depth 35 -Compress})
 if($result.kind -eq 'graph'){$rows=@($rows|Sort-Object -CaseSensitive)}
 return (ConvertTo-Json -InputObject $outcome.columns -Compress)+'|'+($rows -join "`n")
}
$baseline=@{}
foreach($d in $diffs|Where-Object {$_.Head -eq 'v143' -and $_.Parser -eq 'default'}){$baseline[$d.Result.id]=$d}
$changes=[Collections.Generic.List[object]]::new()
foreach($cell in @('release,default','release,antlr','v143,antlr')){
 $role,$parser=$cell.Split(',');$current=@{}
 foreach($d in $diffs|Where-Object {$_.Head -eq $role -and $_.Parser -eq $parser}){
  $current[$d.Result.id]=$d
  if(-not $baseline.ContainsKey($d.Result.id)){$changes.Add([pscustomobject]@{Cell=$cell;Kind='new difference';ID=$d.Result.id;Query=$d.Result.query})}
  elseif((OutcomeKey $d.Result) -cne (OutcomeKey $baseline[$d.Result.id].Result)){$changes.Add([pscustomobject]@{Cell=$cell;Kind='different failing outcome';ID=$d.Result.id;Query=$d.Result.query})}
 }
 foreach($id in $baseline.Keys){if(-not $current.ContainsKey($id)){$changes.Add([pscustomobject]@{Cell=$cell;Kind='fixed difference';ID=$id;Query=$baseline[$id].Result.query})}}
}
$summary|Format-Table -AutoSize
"Outcome changes relative to v1.4.3/default (graph order ignored): $($changes.Count)"
$changes|ConvertTo-Json -Depth 15|Set-Content (Join-Path $PSScriptRoot 'fix-1384-priority-outcome-changes.json')
$changes|Format-Table -AutoSize

$members=@(
 @{N=1;ID='019f370bef7572d53fc2';Failure='Comprehension after WITH renaming'},
 @{N=2;ID='09bf5a9881b509d0c3c7';Failure='Imported null becomes local scan'},
 @{N=3;ID='45c0ce713a25679a615f';Failure='WITH * endpoint property map'},
 @{N=4;ID='e7763c11f8f617a74224';Failure='Nested comprehension loses its current node'},
 @{N=5;ID='05c617080b3abc75b5bb';Failure='UNION ALL COUNT loses imports'},
 @{N=6;ID='14604bebf4c98709c319';Failure='Valid CALL import disappears'},
 @{N=7;ID='b350dc67dd6be48d24d6';Failure='CREATE/MERGE/SET stores false instead of true'},
 @{N=8;ID='4435c5697643cd4f611c';Failure='NOT EXISTS returns extra rows'},
 @{N=9;ID='bdce0002f06bfa9aa5a9';Failure='Normalisation accepts unbound named endpoint'},
 @{N=10;ID='accb5a19275a7575bae6';Failure='OPTIONAL cardinality loses correlation'}
)
$memberRows=foreach($m in $members){
 $r=[ordered]@{N=$m.N;Failure=$m.Failure;ID=$m.ID}
 foreach($role in @('release','v143')){foreach($parser in @('default','antlr')){
  $r[$role+'_'+$parser]=[bool]@($diffs|Where-Object {$_.Head -eq $role -and $_.Parser -eq $parser -and $_.Result.id -eq $m.ID}).Count
 }}
 [pscustomobject]$r
}
$memberRows|Export-Csv (Join-Path $PSScriptRoot 'fix-1384-priority-ten-failures.csv') -NoTypeInformation
$memberRows|Format-Table -AutoSize
$stored=@($diffs|Where-Object {$_.Result.kind -eq 'graph'})
$stored|ConvertTo-Json -Depth 45 -Compress|Set-Content (Join-Path $PSScriptRoot 'fix-1384-priority-stored-effects.json')
$stored|Group-Object Head,Parser|Select-Object Name,Count|Format-Table
