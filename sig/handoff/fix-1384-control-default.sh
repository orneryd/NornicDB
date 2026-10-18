bash sig/tnrun.sh ab wt-fix-1384-red wt-fix-1384-dev --rounds 10 --parser default -- ./pkg/cypher -bench '^BenchmarkBindingWherePipelineHandlers$' -benchtime 250ms
