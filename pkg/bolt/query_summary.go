package bolt

// Summary type describes completed work. Dynamic writes can have read routing
// requirements; reporting their counters must not alter transaction promotion.
func boltSummaryQueryType(write bool, stats *QueryStats) string {
	if write || (stats != nil && *stats != (QueryStats{})) {
		return "w"
	}
	return "r"
}
