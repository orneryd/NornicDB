bash sig/tnrun.sh go wt-fix-1384-release --allow-dirty --parser both -- ./pkg/cypher -run '^TestPatternPredicate(Imported|Sparse)' -count=1 -v
