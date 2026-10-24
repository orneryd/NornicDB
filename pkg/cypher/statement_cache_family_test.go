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
		clone := base.cloneForStorage(store)
		if !clone.hasCachedValidSyntax(query, activeSyntaxValidator()) {
			t.Error("USE clone lost warmed syntax verdict")
		}
		if clone.ensureUpperQueryCache() != base.ensureUpperQueryCache() {
			t.Error("USE clone lost uppercase cache")
		}
	})
	t.Run("Fabric shard transaction", func(t *testing.T) {
		c := &cypherFabricExecutor{base: base}
		tx := fabric.NewFabricTransaction("review1477")
		composite := c.statementFor(context.Background(), tx)
		participant, err := composite.participant("nornic", store)
		if err != nil {
			t.Fatal(err)
		}
		exec, _ := participant.run(context.Background(), composite)
		defer tx.Rollback(nil)
		if !exec.hasCachedValidSyntax(query, activeSyntaxValidator()) {
			t.Error("Fabric shard transaction lost warmed syntax verdict")
		}
		if exec.ensureUpperQueryCache() != base.ensureUpperQueryCache() {
			t.Error("Fabric shard transaction lost uppercase cache")
		}
	})
}
