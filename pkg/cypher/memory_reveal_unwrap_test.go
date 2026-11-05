package cypher

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMemoryEngineRevealReachesCanonicalStorageOwner(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	store := storage.NewNamespacedEngine(base, "memory-reveal")
	exec := NewStorageExecutor(store)
	for _, q := range []string{"CREATE (n:Document {id:'one'})", "MATCH (n:Document {id:'one'}) CALL kp.suppress(n)"} {
		_, err := exec.Execute(t.Context(), q, nil)
		require.NoError(t, err)
	}
	for _, tc := range []struct {
		query string
		rows  [][]interface{}
	}{
		{"MATCH (n:Document {id:'one'}) RETURN n.id", [][]interface{}{}},
		{"MATCH (n:Document {id:'one'}) RETURN reveal(n).id", [][]interface{}{{"one"}}},
		{"MATCH (n:Document {id:'one'}) RETURN n.id", [][]interface{}{}},
		{"MATCH (n:Document {id:'one'}) RETURN reveal(n).id", [][]interface{}{{"one"}}},
	} {
		result, err := exec.Execute(t.Context(), tc.query, nil)
		require.NoError(t, err)
		require.Equal(t, tc.rows, result.Rows, tc.query)
	}
	require.Same(t, base.BadgerEngine, storage.UnwrapEngine(store))
}
