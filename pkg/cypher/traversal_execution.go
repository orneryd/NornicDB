package cypher

// traversalMaterializeKey selects the reference materializing execution
// strategy. It does not alter access-path planning or endpoint visibility.
// Differential tests use it through the public Execute entrypoint.
type traversalMaterializeKey struct{}
