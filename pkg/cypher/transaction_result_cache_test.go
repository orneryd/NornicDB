package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// An embedded executor can serve auto-commit and explicit transaction statements
// in sequence. A result cached before BEGIN must not hide its own pending writes.
func TestExplicitTransactionResultCacheReadsOwnWrites(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, tc := range []struct {
				name, setup, write, read string
				before, pending          int64
			}{
				{"create", "", "CREATE (:CachedTx)", "MATCH (n:CachedTx) RETURN count(n) AS v", 0, 1},
				{"set", "CREATE (:CachedTx {v: 1})", "MATCH (n:CachedTx) SET n.v = 2", "MATCH (n:CachedTx) RETURN n.v AS v", 1, 2},
				{"delete", "CREATE (:CachedTx)", "MATCH (n:CachedTx) DELETE n", "MATCH (n:CachedTx) RETURN count(n) AS v", 1, 0},
				{"merge", "", "MERGE (:CachedTx {k: 1})", "MATCH (n:CachedTx) RETURN count(n) AS v", 0, 1},
			} {
				for _, end := range []string{"COMMIT", "ROLLBACK"} {
					t.Run(tc.name+"/"+end, func(t *testing.T) {
						store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic")
						exec := NewStorageExecutor(store)
						ctx := context.Background()
						run := func(query string) *ExecuteResult {
							t.Helper()
							result, err := exec.Execute(ctx, query, nil)
							require.NoError(t, err, query)
							return result
						}
						if tc.setup != "" {
							run(tc.setup)
						}
						require.Equal(t, tc.before, countFromResult(t, run(tc.read)))
						run("BEGIN")
						t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
						run(tc.write)
						require.Equal(t, tc.pending, countFromResult(t, run(tc.read)), "cached auto-commit read must not answer inside BEGIN")
						run(end)
						want := tc.pending
						if end == "ROLLBACK" {
							want = tc.before
						}
						require.Equal(t, want, countFromResult(t, run(tc.read)), "transaction results must not leak into auto-commit cache")
					})
				}
			}
		})
	}
}
