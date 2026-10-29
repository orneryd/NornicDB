package cypher

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"
	"weak"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Generate unrelated payloads only in canonical caller-owned point reads.
// Storage keeps tiny records, so its caches cannot retain the large bodies on
// behalf of the query and hide the traversal's working set.
type endpointRetentionEngine struct {
	*storage.NamespacedEngine
	payloadBytes int
	mu           sync.Mutex
	reads        int
	payloads     []weak.Pointer[byte]
	holdPayloads bool
	heldPayloads []string
	blocked      chan struct{}
	release      chan struct{}
}

func (s *endpointRetentionEngine) endpointBody(id storage.NodeID, node *storage.Node, err error) (*storage.Node, error) {
	if err != nil || node == nil || !strings.HasPrefix(string(id), "end-") {
		return node, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	if s.blocked != nil && s.reads == 128 {
		close(s.blocked)
		s.mu.Unlock()
		<-s.release
		s.mu.Lock()
	}
	payload := strings.Repeat("x", s.payloadBytes)
	node.Properties["unrelated"] = payload
	if s.blocked != nil {
		// Observe the actual string backing allocation, not the node wrapper:
		// a copied property map can retain the body without retaining its node.
		s.payloads = append(s.payloads, weak.Make(unsafe.StringData(payload)))
		if s.holdPayloads {
			s.heldPayloads = append(s.heldPayloads, payload)
		}
	}
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

			measure := func(payloadBytes int, holdPayloads bool) int64 {
				store.payloadBytes = payloadBytes
				store.reads = 0
				store.payloads = nil
				store.holdPayloads = holdPayloads
				store.blocked, store.release = make(chan struct{}), make(chan struct{})
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
					store.heldPayloads = nil
					require.NoError(t, queryErr)
					require.NotNil(t, result)
					require.Equal(t, [][]interface{}{{tc.want}}, result.Rows)
				}()
				select {
				case <-store.blocked:
				case <-done:
					t.Fatalf("query did not read the endpoint population: %v", queryErr)
				case <-ctx.Done():
					t.Fatal("query did not reach the retained-memory checkpoint")
				}
				// The public query is paused with 127 endpoints already visited.
				// Count only this query's endpoint payload allocations that remain
				// reachable after GC. Unrelated heap reclamation cannot subtract
				// from this cohort, and weak references do not keep it alive.
				runtime.GC()
				store.mu.Lock()
				defer store.mu.Unlock()
				require.Len(t, store.payloads, 127, "checkpoint must follow the same endpoint population")
				var retained int64
				for _, payload := range store.payloads {
					if payload.Value() != nil {
						retained += int64(payloadBytes)
					}
				}
				runtime.KeepAlive(exec)
				runtime.KeepAlive(store)
				return retained
			}
			small, large := measure(1<<10, false), measure(256<<10, false)
			t.Logf("retained bytes: 1KiB properties=%d, 256KiB properties=%d", small, large)
			require.Less(t, large, int64(8<<20), "COUNT/scalar traversal must not retain unrelated endpoint bodies")
			require.Less(t, large-small, int64(4<<20), "working set must not scale with unrelated property size")
			// A retaining owner must be visible to this same paused-query
			// measurement and exceed BOTH unchanged regression limits.
			held := measure(256<<10, true)
			require.Equal(t, int64(127*(256<<10)), held)
			require.Greater(t, held, int64(8<<20))
			require.Greater(t, held-small, int64(4<<20))
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
