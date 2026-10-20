package bolt

import "github.com/orneryd/nornicdb/pkg/cypher"

// Summary type describes completed work. Dynamic writes can have read routing
// requirements; reporting their counters must not alter transaction promotion.
func boltSummaryQueryType(write bool, stats *QueryStats, metadata ...map[string]interface{}) string {
	var resolved map[string]interface{}
	if len(metadata) > 0 {
		resolved = metadata[0]
	}
	if mode := cypher.CompletedStatementQueryType(resolved, write || (stats != nil && *stats != (QueryStats{}))); mode != "" {
		return mode
	}
	return "r"
}
