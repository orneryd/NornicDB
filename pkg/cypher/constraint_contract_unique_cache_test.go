package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// An IS UNIQUE entry inside a block constraint contract must leave its
// unique-value cache complete, exactly like a standalone UNIQUE constraint.
// When the cache is incomplete, checkUniqueConstraint falls back to scanning
// every node of the label on each write, which makes bulk loads quadratic
// (#1120: each 200-node statement slower than the last).
func TestConstraintContractUnique_ValueCacheCompleteAfterDDL(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Nodes that exist before the contract must be registered in the cache.
	_, err := exec.Execute(ctx, "UNWIND range(1, 50) AS i CREATE (:Account {aid: i, abalance: 0})", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `CREATE CONSTRAINT account_contract IF NOT EXISTS
FOR (n:Account)
REQUIRE {
  n.aid IS UNIQUE
  n.aid IS NOT NULL
  n.aid IS :: INTEGER
  n.abalance IS :: INTEGER
}`, nil)
	require.NoError(t, err)

	schema := baseStore.GetSchemaForNamespace("test")
	require.NotNil(t, schema)
	_, found, constrained, complete := schema.LookupUniqueConstraintValue("Account", "aid", int64(7))
	require.True(t, constrained, "the contract's IS UNIQUE entry must register a unique constraint")
	require.True(t, complete, "the unique-value cache must be complete right after the contract DDL, or writes take the label-scan fallback")
	require.True(t, found, "pre-existing values must be registered in the cache")

	// Writes keep the cache authoritative: new values are found, duplicates rejected.
	_, err = exec.Execute(ctx, "UNWIND range(51, 100) AS i MERGE (n:Account {aid: i}) SET n.abalance = 0", nil)
	require.NoError(t, err)
	_, found, _, complete = schema.LookupUniqueConstraintValue("Account", "aid", int64(99))
	require.True(t, complete)
	require.True(t, found)

	_, err = exec.Execute(ctx, "CREATE (:Account {aid: 7, abalance: 1})", nil)
	require.Error(t, err, "a duplicate of a value that existed before the contract must be rejected")
	_, err = exec.Execute(ctx, "CREATE (:Account {aid: 99, abalance: 1})", nil)
	require.Error(t, err, "a duplicate of a value written after the contract must be rejected")
}

// The refresh after a contract DDL must only touch the contract's own unique
// constraints: other labels keep their caches (no full rebuild per DDL).
func TestConstraintContractUnique_RefreshIsScopedToContract(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_, err := exec.Execute(ctx, fmt.Sprintf(`CREATE CONSTRAINT c%d IF NOT EXISTS
FOR (n:L%d)
REQUIRE {
  n.k IS UNIQUE
  n.k IS :: INTEGER
}`, i, i), nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, fmt.Sprintf("UNWIND range(1, 20) AS i CREATE (:L%d {k: i})", i), nil)
		require.NoError(t, err)
	}
	schema := baseStore.GetSchemaForNamespace("test")
	for i := 0; i < 5; i++ {
		_, found, constrained, complete := schema.LookupUniqueConstraintValue(fmt.Sprintf("L%d", i), "k", int64(20))
		require.True(t, constrained)
		require.True(t, complete, "label L%d cache must stay complete after later contract DDL", i)
		require.True(t, found, "label L%d value written before later DDL must still be registered", i)
	}
}
