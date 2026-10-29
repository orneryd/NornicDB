package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
)

func TestStatementCacheFamilyFreshExecutorCacheFamily(t *testing.T) {
	store := newTestMemoryEngine(t)
	base := NewStorageExecutor(store)
	const query = "MATCH (n:StatementCacheFamily) RETURN count(n) AS c"
	if _, err := base.Execute(context.Background(), query, nil); err != nil {
		t.Fatal(err)
	}
	t.Run("USE storage clone", func(t *testing.T) {
		clone := base.cloneWithStorage(store, selectedDatabaseStorage)
		if !clone.hasCachedValidSyntax(query, activeSyntaxValidator()) {
			t.Error("USE clone lost warmed syntax verdict")
		}
		if clone.ensureUpperQueryCache() != base.ensureUpperQueryCache() {
			t.Error("USE clone lost uppercase cache")
		}
	})
	t.Run("Fabric shard transaction", func(t *testing.T) {
		c := &cypherFabricExecutor{base: base}
		exec, err := c.ensureLocalShardTxExecutor(context.Background(), &fabric.SubTransaction{ShardName: "review1477"}, "nornic", store)
		if err != nil {
			t.Fatal(err)
		}
		defer exec.Execute(context.Background(), "ROLLBACK", nil)
		if !exec.hasCachedValidSyntax(query, activeSyntaxValidator()) {
			t.Error("Fabric shard transaction lost warmed syntax verdict")
		}
		if exec.ensureUpperQueryCache() != base.ensureUpperQueryCache() {
			t.Error("Fabric shard transaction lost uppercase cache")
		}
	})
}
