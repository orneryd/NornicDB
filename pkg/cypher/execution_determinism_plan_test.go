package cypher

import (
    "context"
    "testing"

    "github.com/stretchr/testify/require"
)

// Admission is on every result-cache hit. Unchanged syntax must not allocate
// another permission map, argument slice or literal request map on that path.
func TestExecutionDeterminismCachedAdmissionAllocations(t *testing.T) {
    exec := solR2CacheExecutor(t)
    ctx := withQueryParams(context.Background(), map[string]any{
        "request": map[string]any{"query": "transcript", "limit": int64(2)},
    })
    for _, query := range []string{
        "CALL db.rretrieve($request)",
        "CALL db.rretrieve({query: 'transcript', limit: 2})",
    } {
        t.Run(query, func(t *testing.T) {
            info := exec.analyzer.Analyze(query)
            require.True(t, exec.canUseResultCache(ctx, info, query))
            allocations := testing.AllocsPerRun(1000, func() {
                if !exec.canUseResultCache(ctx, info, query) {
                    t.Fatal("ordinary one-shot request must remain cacheable")
                }
            })
            require.Zero(t, allocations, "a cached analysis needs only binding-dependent admission")
        })
    }
}
