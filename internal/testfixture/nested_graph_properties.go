package testfixture

// NestedGraphProperties creates fresh scalar, list and map values for graph
// ownership tests and benchmarks. Each call owns its mutable containers.
func NestedGraphProperties(index int) map[string]interface{} {
	return map[string]interface{}{
		"name": "node", "rank": int64(index),
		"tags":    []interface{}{"alpha", "beta", "gamma", "delta"},
		"profile": map[string]interface{}{"city": "Berlin", "since": int64(2020), "active": true, "score": 1.5},
	}
}
