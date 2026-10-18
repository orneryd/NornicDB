bash sig/tnrun.sh go wt-fix-1384-r73-release --allow-dirty --parser both -- ./pkg/cypher -run '^TestRule73PatternFabricStateAttack$' -count=1 -timeout 15m -v
