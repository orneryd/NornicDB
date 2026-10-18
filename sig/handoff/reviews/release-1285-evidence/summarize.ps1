# Format the canonical analysis produced by the TrueNAS A/B client's analyse().
$ErrorActionPreference='Stop'
$summary=Get-Content $PSScriptRoot/ab-summary.json -Raw | ConvertFrom-Json
$rows=foreach ($row in ($summary.rows | Sort-Object name,unit)) {
    [pscustomobject]@{
        name=$row.name; unit=$row.unit; base=$row.base; head=$row.head
        delta=('{0:F2}' -f $row.delta); p=('{0:F3}' -f $row.p); n=($row.n -join '+')
        spreadBase=('{0:F1}' -f (100*($row.spread[1]-$row.spread[0])/$row.base))
        spreadHead=('{0:F1}' -f (100*($row.spread[3]-$row.spread[2])/$row.head))
    }
}
$rows | Format-Table -AutoSize | Out-String -Width 220
'Foreign CPU share: median {0:F2}%, worst {1:F2}%' -f $summary.isolation.median,$summary.isolation.worst
