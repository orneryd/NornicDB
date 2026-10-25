package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMergeCacheAndIndexHelpers(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "merge_helper_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (n:Person {id:'p1', email:'a@x'})", nil)
	require.NoError(t, err)
	nodes, err := store.GetNodesByLabel("Person")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	n := nodes[0]

	props := map[string]interface{}{"id": "p1", "email": "a@x"}
	exec.cacheMergeNode([]string{"Person"}, props, n)
	cached := exec.findMergeNodeInCache(store, []string{"Person"}, props)
	require.NotNil(t, cached)
	require.Equal(t, n.ID, cached.ID)

	exec.evictMergeNodeCacheEntries([]string{"Person"}, map[string]interface{}{"id": "p1"}, n.ID)
	cached = exec.findMergeNodeInCache(store, []string{"Person"}, map[string]interface{}{"id": "p1"})
	require.Nil(t, cached)

	exec.cacheMergeNode([]string{"Person"}, props, n)
	require.NoError(t, store.DeleteNode(n.ID))
	cached = exec.findMergeNodeInCache(store, []string{"Person"}, props)
	require.Nil(t, cached)

	idx := &storage.CompositeIndex{Properties: []string{"id", "email"}}
	require.True(t, mergeIndexMatchesAllProperties(idx, props))
	require.False(t, mergeIndexMatchesAllProperties(&storage.CompositeIndex{Properties: []string{"id"}}, props))
	require.False(t, mergeIndexMatchesAllProperties(nil, props))
	require.Equal(t, []interface{}{"p1", "a@x"}, compositeLookupValues(idx, props))

	_, err = exec.Execute(ctx, "CREATE (a:L {id:'1'}), (b:L {id:'2'})", nil)
	require.NoError(t, err)
	all, err := store.GetNodesByLabel("L")
	require.NoError(t, err)
	require.Len(t, all, 2)
	loaded, err := exec.loadMergeCandidateNodes(ctx, store, []storage.NodeID{all[0].ID, all[1].ID, all[0].ID})
	require.NoError(t, err)
	require.Len(t, loaded, 2)
}
