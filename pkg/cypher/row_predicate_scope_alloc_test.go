package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCompiledNodeWhereFilterDoesNotAllocatePerCandidate(t *testing.T) {
	exec := &StorageExecutor{}
	ctx := withQueryParams(context.Background(), map[string]interface{}{"asset": "wanted"})
	fixtureAdmission1390Context, fixtureAdmission1390 := expressionTestContext(ctx, "n.asset_id = $asset")
	_ = fixtureAdmission1390Context
	filter, ok := exec.getCompiledSimpleWhere(fixtureAdmission1390, fixtureAdmission1390Context, "n", "n.asset_id = $asset")
	require.True(t, ok)
	node := &storage.Node{ID: "candidate", Properties: map[string]interface{}{"asset_id": "wanted"}}
	require.True(t, filter(node))

	allocs := testing.AllocsPerRun(100, func() {
		if !filter(node) {
			t.Fatal("matching candidate was filtered out")
		}
	})
	require.Zero(t, allocs, "a compiled filter must not allocate a binding map for each candidate")
}
