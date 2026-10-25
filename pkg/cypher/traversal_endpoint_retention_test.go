package cypher

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Generate unrelated payloads only in canonical caller-owned point reads.
// Storage keeps tiny records, so its caches cannot retain the large bodies on
// behalf of the query and hide the traversal's working set.
type endpointRetentionEngine struct {
	*storage.NamespacedEngine
	payloadBytes int
	reads        atomic.Int64
	blocked      chan struct{}
	release      chan struct{}
}

func (s *endpointRetentionEngine) endpointBody(id storage.NodeID, node *storage.Node, err error) (*storage.Node, error) {
	if err != nil || node == nil || !strings.HasPrefix(string(id), "end-") {
		return node, err
	}
	if s.blocked != nil && s.reads.Add(1) == 128 {
		close(s.blocked)
		<-s.release
	}
	node.Properties["unrelated"] = strings.Repeat("x", s.payloadBytes)
	return node, nil
}

func (s *endpointRetentionEngine) GetQueryNode(id storage.NodeID) (*storage.Node, error) {
	node, err := s.NamespacedEngine.GetQueryNode(id)
	return s.endpointBody(id, node, err)
}

func (s *endpointRetentionEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	node, err := s.NamespacedEngine.GetNode(id)
	return s.endpointBody(id, node, err)
}

func TestTraversalEndpointRetentionDoesNotScaleWithUnrelatedProperties(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		want        int64
	}{
		{"fixed_count", "MATCH (s:Seed)-[:LINK]->(:Endpoint) WHERE s.rank = 0 RETURN count(*)", 128},
		{"variable_count", "MATCH (s:Seed)-[:LINK*1..2]->(:Endpoint) RETURN count(*)", 128},
		{"variable_scalar", "MATCH (s:Seed)-[:LINK*1..2]->(e:Endpoint) RETURN sum(e.rank)", 8256},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &endpointRetentionEngine{NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "retained")}
			_, err := store.CreateNode(&storage.Node{ID: "root", Labels: []string{"Seed"}, Properties: map[string]interface{}{"rank": int64(0)}})
			require.NoError(t, err)
			for i := 1; i <= 128; i++ {
				id := storage.NodeID(fmt.Sprintf("end-%03d", i))
				_, err = store.CreateNode(&storage.Node{ID: id, Labels: []string{"Endpoint"}, Properties: map[string]interface{}{"rank": int64(i)}})
				require.NoError(t, err)
				require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("link-%03d", i)), StartNode: "root", EndNode: id, Type: "LINK"}))
			}
			exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
			warm, err := exec.Execute(context.Background(), tc.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tc.want}}, warm.Rows)

			measure := func(payloadBytes int) int64 {
				store.payloadBytes = payloadBytes
				store.reads.Store(0)
				store.blocked, store.release = make(chan struct{}), make(chan struct{})
				runtime.GC()
				var before, paused runtime.MemStats
				runtime.ReadMemStats(&before)
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				done := make(chan struct{})
				var result *ExecuteResult
				var queryErr error
				go func() {
					result, queryErr = exec.Execute(ctx, tc.query, nil)
					close(done)
				}()
				defer func() {
					close(store.release)
					<-done
					require.NoError(t, queryErr)
					if result != nil {
						require.Equal(t, [][]interface{}{{tc.want}}, result.Rows)
					}
				}()
				select {
				case <-store.blocked:
				case <-done:
					t.Fatalf("query did not read the endpoint population: %v", queryErr)
				case <-ctx.Done():
					t.Fatal("query did not reach the retained-memory checkpoint")
				}
				// The public query is paused with 127 endpoints already visited.
				// Collect transient decoder bodies; retained query caches stay live.
				runtime.GC()
				runtime.ReadMemStats(&paused)
				return int64(paused.HeapAlloc) - int64(before.HeapAlloc)
			}
			small, large := measure(1<<10), measure(256<<10)
			t.Logf("retained bytes: 1KiB properties=%d, 256KiB properties=%d", small, large)
			require.Less(t, large, int64(8<<20), "COUNT/scalar traversal must not retain unrelated endpoint bodies")
			require.Less(t, large-small, int64(4<<20), "working set must not scale with unrelated property size")
		})
	}
}

func TestTraversalEndpointRetentionPreservesRequestedValues(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "retained_values")
	_, err := store.CreateNode(&storage.Node{ID: "a", Labels: []string{"Seed"}, Properties: map[string]interface{}{"rank": int64(0)}})
	require.NoError(t, err)
	properties := map[string]interface{}{
		"rank": int64(7), "nested": map[string]interface{}{"k": "kept"}, "unrelated": strings.Repeat("x", 32<<10),
	}
	for i := 0; i < 16; i++ {
		properties[fmt.Sprintf("extra-%02d", i)] = strings.Repeat("z", 32)
	}
	_, err = store.CreateNode(&storage.Node{ID: "b", Labels: []string{"Endpoint"}, Properties: properties,
		ChunkEmbeddings: [][]float32{{1, 0}}, EmbedMeta: map[string]interface{}{"has_embedding": true}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "LINK"}))
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	for _, length := range []string{"", "*1..2"} {
		result, err := exec.Execute(context.Background(), "MATCH (:Seed)-[:LINK"+length+"]->(e:Endpoint) RETURN e.rank, e.nested.k, e.has_embedding, labels(e)", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.Equal(t, []interface{}{int64(7), "kept", true}, result.Rows[0][:3])
		require.ElementsMatch(t, []string{"Endpoint"}, result.Rows[0][3])
	}
	// A later whole-entity read and a write still own the complete stored body.
	result, err := exec.Execute(context.Background(), "MATCH (:Seed)-[:LINK*1..2]->(e:Endpoint) SET e.rank = 8 RETURN e", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	node := result.Rows[0][0].(*storage.Node)
	require.Equal(t, int64(8), node.Properties["rank"])
	require.Len(t, node.Properties["unrelated"], 32<<10)
	require.Equal(t, map[string]interface{}{"k": "kept"}, node.Properties["nested"])
	require.Equal(t, strings.Repeat("z", 32), node.Properties["extra-15"])
}
