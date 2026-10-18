bash sig/tnrun.sh go wt-fix-1384-r73-release --allow-dirty --ref neo-ref --parser both --env R73_TX_STATE=1 -- ./testing/cypher/tck -run '^TestRule73PatternFamilyAttack$' -count=1 -timeout 30m -v
